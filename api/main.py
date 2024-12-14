import io
from contextlib import asynccontextmanager
from datetime import datetime
import os
import sys
from enum import unique
from mangum import Mangum

import pandas as pd
from fastapi import FastAPI, Path, status, HTTPException
import uvicorn
from motor.motor_asyncio import AsyncIOMotorClient
import random
from api.dal import JobsDAL, ShiftUpdateRequest, UserListDAL, User, UserRequest, JobUserItem, ShiftDetail, \
    EmployeeByShiftResponse, RandomizerResponse1

from typing import Annotated, List
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.jobstores.memory import MemoryJobStore
from apscheduler.triggers.cron import CronTrigger
from dotenv import load_dotenv
import resend
from pymongo.errors import DuplicateKeyError
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import StreamingResponse

env_path = '.env'
load_dotenv(env_path)
USER_COLLECTION_NAME = os.environ["USER_DB_COLLECTION_NAME"]
JOB_COLLECTION_NAME = os.environ["JOB_COLLECTION_NAME"]
MONGODB_URI = os.environ["MONGODB_URI"]
RESEND_API_KEY = os.environ["RESEND_API_KEY"]

DEBUG = os.environ.get("DEBUG", "").strip().lower() in {"1", "true", "on", "yes"}

jobstores = {
    'default': MemoryJobStore()
}

# Initialize an AsyncIOScheduler with the jobstore
scheduler = AsyncIOScheduler(jobstores=jobstores, timezone='Asia/Kolkata')

origins = [
    "http://localhost",
    "http://localhost:8080",
    "https://farmtest-muqdlzj4k-raid-bits-projects.vercel.app",
    "https://farmtest.vercel.app",
    "https://farmtest-raid-bits-projects.vercel.app"
]


@asynccontextmanager
async def lifespan(app: FastAPI):
    client = AsyncIOMotorClient(MONGODB_URI)
    db = client.get_default_database()
    pong = await db.command("ping")
    if int(pong["ok"]) != 1:
        raise Exception("cluster connection in not ok")

    user_coll = db.get_collection(USER_COLLECTION_NAME)
    job_coll = db.get_collection(JOB_COLLECTION_NAME)
    app.users = UserListDAL(user_coll)
    app.jobs = JobsDAL(job_coll)
    scheduler.start()
    scheduler.add_job(create_daily_job_doc, CronTrigger(hour=0, minute=5))
    # create indexes for user methods
    await user_coll.create_index([("employeeId", 1)], unique=True)
    await user_coll.create_index([("phone", 1)], unique=True)
    await user_coll.create_index([("email", 1)], unique=True)

    # check if job doc for today is present
    today = datetime.today()
    emp_id_list = await get_employee_list()
    await app.jobs.create_job_doc(today, emp_id_list)

    yield
    client.close()


app = FastAPI(lifespan=lifespan, debug=DEBUG)
app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.get("/api/users")
async def get_all_users() -> list[User]:
    return await app.users.get_user_list()


# add user
@app.post("/api/users")
async def create_user(user: UserRequest):
    try:
        await app.users.create_user(user)
        # add that user to todays job document as well
        await app.jobs.add_user_to_current_job_doc(datetime.today().strftime('%Y-%m-%d'), user.employeeId)
        return {"message": "User created successfully"}
    except DuplicateKeyError:
        raise HTTPException(status_code=400, detail="User already exists")


# update user

# update shift
@app.patch("/api/users/update_shift/{employeeId}")
async def update_user_shift(
        employeeId: Annotated[str, Path(title="employee id for the employee whose shift you want to change")],
        shift: ShiftUpdateRequest):
    return await app.users.update_user_shift(employeeId, shift.shift)


@app.get("/api")
async def index():
    return {"message": "yo yo honey singh!!"}


@app.get("/api/jobdoc/{date_string}")
async def getJobDoc(date_string: str):
    return await app.jobs.get_job_doc(date_string)


@app.post("/api/jobdoc")
async def createJobDoc(date: datetime):
    emp_id_list = await get_employee_list()

    return await app.jobs.create_job_doc(date, emp_id_list)

async def get_employee_list():
    emp_ids = await app.users.get_user_info({}, {"_id": 0, "employeeId": 1})
    # get list of employee ids
    emp_id_list = l2 = [e['employeeId'] for e in emp_ids]
    print(f"emp_ids: {emp_id_list}")
    return emp_id_list


@app.post("/api/jobdoc/{date_string}")
async def update_user_status(date_string: str, user_update_request: list[JobUserItem]):
    return await app.jobs.update_user_status(date_string, user_update_request)


async def create_daily_job_doc():
    print("create_daily_job_doc is triggered")
    emp_ids = await app.users.get_user_info({}, {"_id": 0, "employeeId": 1})
    # get list of employee ids
    emp_id_list = l2 = [e['employeeId'] for e in emp_ids]
    print(f"emp_ids: {emp_id_list}")
    today = datetime.today()
    return await app.jobs.create_job_doc(today, emp_id_list)


# change shifts in job doc

@app.post("/api/jobdoc/update_shift/{date_string}")
async def update_shift_details_in_jobdoc(date_string: str, shift_update_reguest: ShiftDetail):
    return await app.jobs.update_shift_details_in_jobdoc(date_string, shift_update_reguest)
    pass


@app.get("/api/jobdoc/{date_string}/getEmployeeByShift/{shift}")
async def get_employee_by_shift(date_string: str, shift: str):
    userlist = await app.jobs.get_active_users_id_by_shift(date_string, shift)
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
async def get_random_users_by_shift(shift: str, date_string: str) -> RandomizerResponse1:
    user_list = await app.jobs.get_active_users_id_by_shift(date_string, shift)
    # print(f"user_list: {user_list}")
    res = get_random(user_list)
    final_response = RandomizerResponse1.from_doc(res)
    await app.jobs.update_randomizer_run_in_job_doc(final_response,date_string, shift)
    return final_response


@app.post("/api/randomizer/send/{shift}")
async def randomize_and_send(shift: str, date_str: str):
    user_list = await app.jobs.get_active_users_id_by_shift(date_str, shift)
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

    return ""


def send_emails(info: []):
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
async def generate_report(date_string: str):
    res = await app.jobs.get_job_doc(date_string)

    print(f"res: {res} type: {type(res)} ")

    log_as_dicts = [item.dict() for item in res.randomizerLog]
    print(log_as_dicts)
    data = log_as_dicts
    # print(data)
    data = clean_data_for_csv(data)
    df = pd.DataFrame(data)
    # df["triggerDateTime"] = df["triggerDateTime"].dt.tz_localize(None)
    # print(df)
    buffer = io.BytesIO()
    with pd.ExcelWriter(buffer, engine="openpyxl") as writer:
        df.to_excel(writer, index=False, sheet_name="sheet1")
    buffer.seek(0)
    return StreamingResponse(buffer,
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        headers={"Content-Disposition": "attachment; filename=report.xlsx"})

@app.get("/api/health")
async def get_health():
    return "all OK"


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


handler = Mangum(app=app)


# if __name__ == "__main__":
#     main()

