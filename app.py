from fastapi import FastAPI, HTTPException
from pymongo import MongoClient, WriteConcern, ReadPreference
from bson import ObjectId
from pydantic import BaseModel
import os

MONGO_URI = "mongodb+srv://tikuanikaz_db_user:rGTvZc4E8FsRRME5@cluster-hw3.tp8k8jz.mongodb.net/?appName=Cluster-hw3"

client = MongoClient(MONGO_URI)
db = client["ev_db"]
collection = db["vehicles"]

class EVRecord(BaseModel):
    VIN: str
    Make: str
    Model: str
    Year: int
    ElectricRange: float = None
    BatteryCapacity: float = None

app = FastAPI(title="EV Data API", version="1.0")

@app.post("/insert-fast")
def insert_fast(ev: EVRecord):
    try:
        fast_coll = collection.with_options(write_concern=WriteConcern(w=1))
        result = fast_coll.insert_one(ev.dict())
        return {"inserted_id": str(result.inserted_id)}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/insert-safe")
def insert_safe(ev: EVRecord):
    try:
        safe_coll = collection.with_options(write_concern=WriteConcern(w="majority"))
        result = safe_coll.insert_one(ev.dict())
        return {"inserted_id": str(result.inserted_id)}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/count-tesla-primary")
def count_tesla_primary():
    try:
        primary_coll = collection.with_options(read_preference=ReadPreference.PRIMARY)
        count = primary_coll.count_documents({"Make": "Tesla"})
        return {"count": count}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/count-bmw-secondary")
def count_bmw_secondary():
    try:
        secondary_coll = collection.with_options(read_preference=ReadPreference.SECONDARY_PREFERRED)
        count = secondary_coll.count_documents({"Make": "BMW"})
        return {"count": count}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
