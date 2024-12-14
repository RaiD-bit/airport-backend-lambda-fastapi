import io
from contextlib import asynccontextmanager
from datetime import datetime
import os
import sys
from enum import unique
from mangum import Mangum

import pandas as pd
from fastapi import FastAPI, Path, status, HTTPException, Depends
import uvicorn
from motor.motor_asyncio import AsyncIOMotorClient
import random
from dal import JobsDAL, ShiftUpdateRequest, UserListDAL, User, UserRequest, JobUserItem, ShiftDetail, \
    EmployeeByShiftResponse, RandomizerResponse1

# from api.dal import JobsDAL, ShiftUpdateRequest, UserListDAL, User, UserRequest, JobUserItem, ShiftDetail, \
#     EmployeeByShiftResponse, RandomizerResponse1

from typing import Annotated, List


from dotenv import load_dotenv
import resend
from pymongo.errors import DuplicateKeyError
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import StreamingResponse
from functools import lru_cache

env_path = '.env'
load_dotenv(env_path)

DEBUG = os.environ.get("DEBUG", "").strip().lower() in {"1", "true", "on", "yes"}



@lru_cache()
def get_config():
    return {
        "MONGODB_URI": os.environ["MONGODB_URI"],
        "USER_COLLECTION_NAME": os.environ["USER_DB_COLLECTION_NAME"],
        "JOB_COLLECTION_NAME": os.environ["JOB_COLLECTION_NAME"],
        "RESEND_API_KEY": os.environ["RESEND_API_KEY"],     
    }

async def get_database_connection():
    if not hasattr(get_database_connection, "client"):
        get_database_connection.client = AsyncIOMotorClient(get_config()["MONGODB_URI"])
    return get_database_connection.client.get_default_database()


origins = [
    "http://localhost",
    "http://localhost:8080",
    "https://farmtest-muqdlzj4k-raid-bits-projects.vercel.app",
    "https://farmtest.vercel.app",
    "https://farmtest-raid-bits-projects.vercel.app"
]

app = FastAPI(debug=DEBUG)
app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

async def get_users_dal():
    db = await get_database_connection()
    return UserListDAL(db.get_collection(get_config()["USER_COLLECTION_NAME"]))

async def get_jobs_dal():
    db = await get_database_connection()
    return JobsDAL(db.get_collection(get_config()["JOB_COLLECTION_NAME"]))

@app.get("/api/users")
async def get_all_users(users_dal: UserListDAL = Depends(get_users_dal)) -> list[User]:
    return await users_dal.get_user_list()


# add user
@app.post("/api/users")
async def create_user(user: UserRequest, users_dal: UserListDAL = Depends(get_users_dal), jobs_dal: JobsDAL = Depends(get_jobs_dal)):
    try:
        await users_dal.create_user(user)
        # add that user to todays job document as well
        await jobs_dal.add_user_to_current_job_doc(datetime.today().strftime('%Y-%m-%d'), user.employeeId)
        return {"message": "User created successfully"}
    except DuplicateKeyError:
        raise HTTPException(status_code=400, detail="User already exists")


# update user

# update shift
@app.patch("/api/users/update_shift/{employeeId}")
async def update_user_shift(
        employeeId: Annotated[str, Path(title="employee id for the employee whose shift you want to change")],
        shift: ShiftUpdateRequest,
        users_dal: UserListDAL = Depends(get_users_dal),
        jobs_dal: JobsDAL = Depends(get_jobs_dal)):
    return await users_dal.update_user_shift(employeeId, shift.shift)


@app.get("/api")
async def index():
    return {"message": "yo yo honey singh!!"}


@app.get("/api/jobdoc/{date_string}")
async def getJobDoc(date_string: str, jobs_dal: JobsDAL = Depends(get_jobs_dal)):
    return await jobs_dal.get_job_doc(date_string)


@app.post("/api/jobdoc")
async def createJobDoc(date: datetime, jobs_dal: JobsDAL = Depends(get_jobs_dal)):
    emp_id_list = await get_employee_list()

    return await jobs_dal.create_job_doc(date, emp_id_list)

async def get_employee_list(users_dal: UserListDAL = Depends(get_users_dal)):
    emp_ids = await users_dal.get_user_info({}, {"_id": 0, "employeeId": 1})
    # get list of employee ids
    emp_id_list = l2 = [e['employeeId'] for e in emp_ids]
    print(f"emp_ids: {emp_id_list}")
    return emp_id_list


@app.post("/api/jobdoc/{date_string}")
async def update_user_status(date_string: str, user_update_request: list[JobUserItem], jobs_dal: JobsDAL = Depends(get_jobs_dal)):
    return await jobs_dal.update_user_status(date_string, user_update_request)


async def create_daily_job_doc(users_dal: UserListDAL = Depends(get_users_dal), jobs_dal: JobsDAL = Depends(get_jobs_dal)):
    print("create_daily_job_doc is triggered")
    emp_ids = await users_dal.get_user_info({}, {"_id": 0, "employeeId": 1})
    # get list of employee ids
    emp_id_list = l2 = [e['employeeId'] for e in emp_ids]
    print(f"emp_ids: {emp_id_list}")
    today = datetime.today()
    return await jobs_dal.create_job_doc(today, emp_id_list)


# change shifts in job doc

@app.post("/api/jobdoc/update_shift/{date_string}")
async def update_shift_details_in_jobdoc(date_string: str, shift_update_reguest: ShiftDetail, jobs_dal: JobsDAL = Depends(get_jobs_dal)):
    return await jobs_dal.update_shift_details_in_jobdoc(date_string, shift_update_reguest)
    pass


@app.get("/api/jobdoc/{date_string}/getEmployeeByShift/{shift}")
async def get_employee_by_shift(date_string: str, shift: str, jobs_dal: JobsDAL = Depends(get_jobs_dal)):
    userlist = await jobs_dal.get_active_users_id_by_shift(date_string, shift)
    # print(userlist)
    userDetailsList = await map_user_details(userlist)
    return userDetailsList


async def map_user_details(userlist) -> List[EmployeeByShiftResponse]:
    userDetailsList = []
    for user in userlist:
        userDetailsList.append(EmployeeByShiftResponse.from_doc(user))
    return userDetailsList


def get_random_x_percent(user_list, percent):
    sz = int(percent * len(user_list))
    # print(f"sz: {sz} len: {len(user_list)}")
    if sz == 0:
        sz = len(user_list)
    random_sample = random.sample(user_list, sz)
    return random_sample


def get_random(user_list: List[EmployeeByShiftResponse]):
    tmp_list: List[EmployeeByShiftResponse] = get_random_x_percent(user_list, 0.4)
    split = (5 * len(tmp_list)) // 8
    main_list: List[EmployeeByShiftResponse] = tmp_list[:split]
    standby_list: List[EmployeeByShiftResponse] = tmp_list[split:]
    type(main_list)
    return {"mainList": main_list, "standbyList": standby_list}


@app.get("/api/randomizer/{shift}")
async def get_random_users_by_shift(shift: str, date_string: str, jobs_dal: JobsDAL = Depends(get_jobs_dal)) -> RandomizerResponse1:
    user_list = await jobs_dal.get_active_users_id_by_shift(date_string, shift)
    # print(f"user_list: {user_list}")
    res = get_random(user_list)
    final_response = RandomizerResponse1.from_doc(res)
    await jobs_dal.update_randomizer_run_in_job_doc(final_response,date_string, shift)
    return final_response


@app.post("/api/randomizer/send/{shift}")
async def randomize_and_send(shift: str, date_str: str, jobs_dal: JobsDAL = Depends(get_jobs_dal)):
    user_list = await jobs_dal.get_active_users_id_by_shift(date_str, shift)
    res = get_random(user_list)
    random_response = RandomizerResponse1.from_doc(res)

    # persist the user in the mail_job_doc

    # send the actual mail
    main_list_mail_ids = []
    standby_list_mail_ids = []
    for user in random_response.mainList:
        main_list_mail_ids.append((user.name, user.email))

    for user in random_response.standbyList:
        standby_list_mail_ids.append((user.name, user.email))

    return {"mainList": main_list_mail_ids, "standbyList": standby_list_mail_ids}


def send_emails(info: list[tuple[str, str]]):
    emails = [mail[0] for mail in info]
    print(f"emails: {emails}")
    params: resend.Emails.SendParams = {
        "from": "Acme <onboarding@resend.dev>",
        "to": emails,
        "subject": f"Be sober before shift",
        "html": f"<p>Hi! Please be ready for drug test before your shift</p>"
    }

    email = resend.Emails.send(params)

    print(f"email: {email}")


def clean_data_for_csv(data):
    rows = []
    for entry in data:
        print(f"entry --> {entry}")
        trigger_time = entry['triggerDateTime'].replace(tzinfo=None)  # Make timezone naive
        shift = entry['shift']
        main_list = entry['randomizerResult'].get('mainList', [])
        standby_list = entry['randomizerResult'].get('standbyList', [])

        for person in main_list:
            rows.append({
                'TriggerDateTime': trigger_time,
                'Shift': shift,
                'Category': 'Main',
                **person
            })
        for person in standby_list:
            rows.append({
                'TriggerDateTime': trigger_time,
                'Shift': shift,
                'Category': 'Standby',
                **person
            })
        # rows.append(pd.DataFrame([{}]))  # Blank row
    return rows


@app.get("/api/generateReport/{date_string}")
async def generate_report(date_string: str, jobs_dal: JobsDAL = Depends(get_jobs_dal)):
    res = await jobs_dal.get_job_doc(date_string)
    
    # Use chunks for large datasets
    log_as_dicts = [item.dict() for item in res.randomizerLog]
    data = log_as_dicts
    data = clean_data_for_csv(data)
    df = pd.DataFrame(data)
    buffer = io.BytesIO()
    with pd.ExcelWriter(buffer, engine="openpyxl", mode='w') as writer:
        df.to_excel(writer, index=False, sheet_name="sheet1")
    buffer.seek(0)
    
    return StreamingResponse(
        buffer,
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        headers={"Content-Disposition": f"attachment; filename=report-{date_string}.xlsx"}
    )

@app.get("/api/health")
async def get_health():
    print("aaya hu yha tak dekh")
    return {"message": "all ok" , "db": get_config()["USER_COLLECTION_NAME"]}


@app.post("/api/sendmail")
async def send_mail(response: RandomizerResponse1, shift: str | None = None):
    # handle main list
    selected_emails = [(user.email, user.name) for user in response.mainList]

    # update daily doc

    batches = [selected_emails[i: i + 50] for i in range(0, len(selected_emails), 50)]
    for batch in batches:
        print(f"batch: {batch}")
        send_emails(batch)
    return {"status": "success"}


def main(argv=sys.argv[1:]):
    try:
        uvicorn.run("server:app", host="0:0:0:0", port=3001, reload=DEBUG)
    except KeyboardInterrupt:
        pass


handler = Mangum(app=app, lifespan="off")
