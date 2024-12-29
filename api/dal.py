from datetime import datetime
from typing import Optional, List
from motor.motor_asyncio import AsyncIOMotorCollection
import random

from pydantic import BaseModel


class EmailRequest(BaseModel):
    name: str
    email: str
    empId: str
    shift: str
    phone: str


class ShiftUpdateRequest(BaseModel):
    shift: str


class UserRequest(BaseModel):
    employeeId: str
    name: str
    designation: str
    email: str
    phone: str
    shift: str


class User(BaseModel):
    employeeId: str
    name: str
    designation: str
    email: str
    phone: str
    shift: str
    id: str

    @staticmethod
    def from_doc(doc) -> "User":
        return User(id=str(doc["_id"]),
                    designation=doc["designation"],
                    email=doc["email"],
                    phone=doc["phone"],
                    shift=doc["shift"],
                    name=doc["name"],
                    employeeId=doc["employeeId"])


class UserListDAL:
    def __init__(self, user_collection: AsyncIOMotorCollection):
        print(user_collection.name)
        self._user_collection = user_collection

    async def get_user_list(self, session=None) -> list[User]:
        print("we are here")
        res = self._user_collection.find({}, {
            "_id": 1,
            "employeeId": 1,
            "name": 1,
            "designation": 1,
            "email": 1,
            "phone": 1,
            "shift": 1
        })
        user_list = []
        async for user in res:
            print(user)
            user_list.append(User.from_doc(user))
        # yield user_list
        return user_list

    async def get_user_info(self, query_filter: dict, projection_filter: Optional[dict] = None):
        print(f"query : {query_filter}, projection: {projection_filter}")
        users = self._user_collection.find(query_filter, projection_filter)
        ans = await users.to_list(length=None)
        print(f"Ans: {ans}")
        return ans

    async def update_user_shift(self, employeeId, shift, session=None):
        print(f"we are inside patch {employeeId} , {shift}")
        res = self._user_collection.update_one(
            {"employeeId": employeeId},
            {"$set": {"shift": shift}}
        )
        response = await res
        return str(response.acknowledged)

    async def create_user(self, user: UserRequest) -> User:
        res = self._user_collection.insert_one(
            {
                "employeeId": user.employeeId,
                "name": user.name,
                "designation": user.designation,
                "email": user.email,
                "phone": user.phone,
                "shift": user.shift
            }
        )
        response = await res
        print(f"response => {response.inserted_id}")
        return str(response.inserted_id)

    async def delete_user_by_email(self, email):
        res = self._user_collection.delete_one({"email": email})


class JobUserItem(BaseModel):
    userid: str
    status: bool

    @staticmethod
    def from_doc(doc) -> "JobUserItem":
        return JobUserItem(
            status=doc['status'],
            userid=doc['userid']
        )


class ShiftDetail(BaseModel):
    morning: Optional[str] = None
    afternoon: Optional[str] = None
    night: Optional[str] = None
    general: Optional[str] = None
    ramc: Optional[str] = None






class EmployeeByShiftResponse(BaseModel):
    employeeId: str
    name: str
    designation: str
    email: str
    phone: str
    shift: str

    @staticmethod
    def from_doc(doc) -> "EmployeeByShiftResponse":
        return EmployeeByShiftResponse(
            employeeId=doc["users"]["userid"],
            name=doc["userDetails"]["name"],
            designation=doc["userDetails"]["designation"],
            email=doc["userDetails"]["email"],
            phone=doc["userDetails"]["phone"],
            shift=doc["userDetails"]["shift"]
        )


class RandomizerResponse1(BaseModel):
    mainList: List[EmployeeByShiftResponse]
    standbyList: List[EmployeeByShiftResponse]

    @staticmethod
    def from_doc(doc) -> "RandomizerResponse1":
        main_list: List[EmployeeByShiftResponse] = [
            EmployeeByShiftResponse.from_doc(item) for item in doc["mainList"]
        ]

        standby_list: List[EmployeeByShiftResponse] = [
            EmployeeByShiftResponse.from_doc(item) for item in doc["standbyList"]
        ]
        return RandomizerResponse1(
            mainList=main_list,
            standbyList=standby_list
        )


class RandomizerResponse(BaseModel):
    main: List[EmployeeByShiftResponse]
    standby: List[EmployeeByShiftResponse]
    id: str

class RandomizerLogItem(BaseModel):
    triggerDateTime: datetime
    shift: str
    randomizerResult: RandomizerResponse1

class JobDocumentRequest(BaseModel):
   dateDocId: str
   shiftDetail: ShiftDetail
   createdOn: datetime
   users: list[JobUserItem]
   prevDocId: str
   randomizerLog: list[RandomizerLogItem]

class JobDocument(BaseModel):
    id: str
    dateDocId: str
    shiftDetail: ShiftDetail
    createdOn: datetime
    users: list[JobUserItem]
    prevDocId: str
    randomizerLog: list[RandomizerLogItem]


    @staticmethod
    def from_doc(doc) -> "JobDocument":
        print(f"doc -> {doc}")
        return JobDocument(
            id=str(doc["_id"]),
            shiftDetail=doc["shiftDetail"],
            users=[JobUserItem.from_doc(userItem) for userItem in doc['users']],
            dateDocId=doc['dateDocId'],
            createdOn=doc['createdOn'],
            prevDocId=doc['prevDocId'],
            randomizerLog=doc['randomizerLog']
        )



class JobsDAL:
    def __init__(self, jobs_collection: AsyncIOMotorCollection):
        self._jobs_collection = jobs_collection

    async def get_job_doc(self, date_str: str, session=None) -> JobDocument:
        print("we are here")
        res = await self._jobs_collection.find_one({"dateDocId": date_str})
        return JobDocument.from_doc(res)

    def populate_daily_shift(date_today: datetime):
        return None

    async def create_job_doc(self, date_str: datetime, employee_ids: list[str], session=None) -> JobDocument:
        document = await self._jobs_collection.find_one({"dateDocId": date_str.strftime("%Y-%m-%d")})

        if document:
            return {"msg": "doc already exists"}
        
        print(f"we are here-> date : {date_str} \n employee_ids {employee_ids}")
        userItems = [JobUserItem(status=True, userid=e) for e in employee_ids]
        print(f"user items: {userItems}")
        # create the job Doc
        shift_detail = ShiftDetail()

        self.populate_shifts(date_str, shift_detail) 
        jobdoc = JobDocumentRequest(users=userItems, shiftDetail=shift_detail, createdOn=datetime.now(),
                                    dateDocId=date_str.strftime("%Y-%m-%d"), prevDocId="", randomizerLog=[])
        print(f"jobDoc: {jobdoc}")

        # get all employee ids
        res = await self._jobs_collection.insert_one(jobdoc.model_dump())
        return {"inserted_id": str(res.inserted_id)}

    def populate_shifts(self, date_str, shift_detail):
        days_passed_since_bigbang = (date_str - datetime(2024, 11, 1)).days
        print(f"kitne din ho gye {days_passed_since_bigbang}")
        shift_info = ['alpha', 'bravo', 'charlie', 'delta', 'echo']
        current_index = days_passed_since_bigbang%5  
        next_index_1 = (current_index + 1) % len(shift_info)
        next_index_2 = (current_index + 2) % len(shift_info)
        shift_detail.morning = shift_info[current_index]
        shift_detail.afternoon = shift_info[next_index_1]
        shift_detail.night = shift_info[next_index_2]
        if date_str.weekday() != 5 or date_str.weekday() != 6:
            shift_detail.general = "general"
            shift_detail.ramc = "ramc"

    async def add_user_to_current_job_doc(self, date_str: str, employee_id: str, session=None):
        print(f"date str : {date_str}")
        query = self._jobs_collection.aggregate([
            {
                '$match': {
                    'dateDocId': date_str
                }
            }, {
                '$set': {
                    'users': {
                        '$concatArrays': [
                            '$users', [
                                {
                                    'userid': employee_id,
                                    'status': False
                                }
                            ]
                        ]
                    }
                }
            },
            {
                '$merge': {
                    'into': 'daily_jobs',
                    'whenMatched': 'merge'
                }
            }
        ])
        await query.to_list(length=None)


    async def update_user_status(self, date_str: str, user_update_request: list[JobUserItem], session=None):
        # create set and array filter to update
        set_dict = {}
        array_filter_list = []
        for user in user_update_request:
            tmp = f"userid{user.userid}"
            key = f"users.$[{tmp}].status"
            set_dict[key] = user.status
            array_filter_list.append({f"{tmp}.userid": user.userid})

        res = await self._jobs_collection.update_one(filter={"dateDocId": date_str}, update={"$set": set_dict},
                                                     array_filters=array_filter_list)
        return {"updated_id": str(res.modified_count)}
        # res = await self._jobs_collection.find_one({"dateDocId": date_str})

    async def update_shift_details_in_jobdoc(self, date_str: str, shift_detail: ShiftDetail, session=None):
        set_dict = {}
        for shift in shift_detail:
            set_dict[f"shiftDetail.{shift[0]}"] = shift[1]
        res = await self._jobs_collection.update_one(filter={"dateDocId": date_str}, update={"$set": set_dict})
        return {"updated_id": str(res.modified_count)}

    async def get_shift_details_from_job_doc(self, date_str: str, session=None):
        res = await self._jobs_collection.find_one({"dateDocId": date_str})
        return res["shiftDetail"]


    async def get_active_users_id_by_shift(self, date_str: str, shift: str, session=None):
        query = self._jobs_collection.aggregate(
            [
                {
                    '$match': {
                        'dateDocId': date_str
                    }
                }, {
                '$unwind': '$users'
            }, {
                '$match': {
                    'users.status': True
                }
            }, {
                '$lookup': {
                    'from': 'Users_db',
                    'localField': 'users.userid',
                    'foreignField': 'employeeId',
                    'as': 'userDetails'
                }
            }, {
                '$unwind': '$userDetails'
            }, {
                '$match': {
                    'userDetails.shift': shift
                }
            }, {
                '$project': {
                    '_id': 0,
                    'userDetails.name': 1,
                    'userDetails.designation': 1,
                    'userDetails.email': 1,
                    'userDetails.phone': 1,
                    'userDetails.shift': 1,
                    'users.userid': 1
                }
            }
            ]
        )
        res = await query.to_list(length=None)
        return res


    async def update_randomizer_run_in_job_doc(self, response: RandomizerResponse1,date_str: str,team: str,shift: str, session=None):
        print(f"response : {response}")
        hh = response.model_dump()
        print(f"hh : {hh}")
        print(f"date_str: {date_str}")
        pipeline = [
                {
                    '$match': {
                        'dateDocId': date_str
                    }
                }, {
                    '$set': {
                        'randomizerLog': {
                            '$concatArrays': [
                                '$randomizerLog', [
                                    {
                                        'triggerDateTime': datetime.now(),
                                        'shift': shift,
                                        'allotedTeam': team,
                                        'randomizerResult': hh
                                    }
                                ]
                            ]
                        }
                    }
                }, {
                    '$merge': {
                        'into': 'daily_jobs',
                        'whenMatched': 'merge'
                    }
                }
            ]
        query = self._jobs_collection.aggregate(
            pipeline
        )
        await query.to_list(length=None)
        print(f"done adding randomizer response to job doc {pipeline}")

    async def remove_user_from_current_job_doc(self, date, userId):
        print(f"date str : {date}")
        await self._jobs_collection.update_one(
            {"dateDocId": date},  # Match the document with the specific dateDocId
            {"$pull": {"users": {"userid": userId}}}  # Remove the user with the given userid
        )


'''
{"dateDocId": date_str}, {"$set": set_dict}, {"array_filter": array_filter_list}
db.collection.update_one(
  { "dateDocId": "2024-10-02" },  
  {
    "$set": {
      "users.$[userid11].status": false,        
      "users.$[userid12].status": false        
    }
  },
  {
    "arrayFilters": [
      { "userid11.userid": "11" },        
      { "userid12.userid": "12" }
    ]
  }
)


db.daily_jobs.updateOne({ "dateDocId": "2024-10-02" },  
  {
    "$set": {
      "users.$[userid11].status": false,        
      "users.$[userid12].status": false        
    }
  },
  {
    "arrayFilters": [
      { "userid11.userid": "11" },        
      { "userid12.userid": "12" }
    ]
  })

[
    {
        '$match': {
            'dateDocId': '2024-09-30'
        }
    }, {
        '$unwind': '$users'
    }, {
        '$match': {
            'users.status': True
        }
    }, {
        '$lookup': {
            'from': 'Users_db', 
            'localField': 'users.userid', 
            'foreignField': 'employeeId', 
            'as': 'userDetails'
        }
    }, {
        '$unwind': '$userDetails'
    }, {
        '$match': {
            'userDetails.shift': 'beta'
        }
    }, {
        '$project': {
            '_id': 0, 
            'userDetails.name': 1, 
            'userDetails.designation': 1, 
            'userDetails.email': 1, 
            'userDetails.phone': 1, 
            'userDetails.shift': 1, 
            'users.userid': 1
        }
    }
]




'''