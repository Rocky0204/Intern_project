from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session
from typing import List
from datetime import time # Import time for time objects

from ..database import get_db
from ..models import StopActivity
from ..schemas import (
    StopActivityCreate,
    StopActivityRead,
    StopActivityUpdate
)

router = APIRouter(
    prefix="/stop_activities",
    tags=["stop_activities"],
    responses={404: {"description": "Not found"}},
)

@router.post("/", response_model=StopActivityRead)
def create_stop_activity(
    activity: StopActivityCreate,
    db: Session = Depends(get_db)
):
    # Manually map schema fields to model fields for creation
    # The schema uses 'atco_code', but the model uses 'stop_point_id'
    db_activity = StopActivity(
        activity_type=activity.activity_type,
        activity_time=activity.activity_time, # Pydantic already converted this to time object
        pax_count=activity.pax_count,
        stop_point_id=activity.atco_code, # Map schema's 'atco_code' to model's 'stop_point_id'
        vj_id=activity.vj_id
    )
    db.add(db_activity)
    db.commit()
    db.refresh(db_activity)

    # Return a dictionary that matches the response_model schema
    # The schema expects 'atco_code', so map model's 'stop_point_id' back
    return {
        "activity_id": db_activity.activity_id,
        "activity_type": db_activity.activity_type,
        "activity_time": db_activity.activity_time.isoformat(), # Ensure time is ISO 8601 string for JSON response
        "pax_count": db_activity.pax_count,
        "atco_code": db_activity.stop_point_id, # Map model's 'stop_point_id' to schema's 'atco_code'
        "vj_id": db_activity.vj_id
    }

@router.get("/", response_model=List[StopActivityRead])
def read_stop_activities(
    vj_id: int = None,
    skip: int = 0,
    limit: int = 100,
    db: Session = Depends(get_db)
):
    query = db.query(StopActivity)
    if vj_id:
        query = query.filter(StopActivity.vj_id == vj_id)
    db_activities = query.offset(skip).limit(limit).all()

    # Manually map each SQLAlchemy object to a dictionary matching the schema
    response_list = []
    for db_activity in db_activities:
        response_list.append({
            "activity_id": db_activity.activity_id,
            "activity_type": db_activity.activity_type,
            "activity_time": db_activity.activity_time.isoformat(),
            "pax_count": db_activity.pax_count,
            "atco_code": db_activity.stop_point_id, # Map model's 'stop_point_id' to schema's 'atco_code'
            "vj_id": db_activity.vj_id
        })
    return response_list

@router.get("/{activity_id}", response_model=StopActivityRead)
def read_stop_activity(activity_id: int, db: Session = Depends(get_db)):
    db_activity = db.query(StopActivity).filter(StopActivity.activity_id == activity_id).first()
    if db_activity is None:
        raise HTTPException(status_code=404, detail="Stop activity not found")

    # Return a dictionary that matches the response_model schema
    return {
        "activity_id": db_activity.activity_id,
        "activity_type": db_activity.activity_type,
        "activity_time": db_activity.activity_time.isoformat(),
        "pax_count": db_activity.pax_count,
        "atco_code": db_activity.stop_point_id, # Map model's 'stop_point_id' to schema's 'atco_code'
        "vj_id": db_activity.vj_id
    }

@router.put("/{activity_id}", response_model=StopActivityRead)
def update_stop_activity(
    activity_id: int,
    activity: StopActivityUpdate,
    db: Session = Depends(get_db)
):
    db_activity = db.query(StopActivity).filter(StopActivity.activity_id == activity_id).first()
    if db_activity is None:
        raise HTTPException(status_code=404, detail="Stop activity not found")

    # Update model fields, mapping from schema fields
    if activity.activity_type is not None:
        db_activity.activity_type = activity.activity_type
    if activity.activity_time is not None:
        db_activity.activity_time = activity.activity_time
    if activity.pax_count is not None:
        db_activity.pax_count = activity.pax_count
    if activity.atco_code is not None:
        db_activity.stop_point_id = activity.atco_code # Map schema's 'atco_code' to model's 'stop_point_id'
    if activity.vj_id is not None:
        db_activity.vj_id = activity.vj_id

    db.commit()
    db.refresh(db_activity)

    # Return a dictionary that matches the response_model schema
    return {
        "activity_id": db_activity.activity_id,
        "activity_type": db_activity.activity_type,
        "activity_time": db_activity.activity_time.isoformat(),
        "pax_count": db_activity.pax_count,
        "atco_code": db_activity.stop_point_id, # Map model's 'stop_point_id' to schema's 'atco_code'
        "vj_id": db_activity.vj_id
    }

@router.delete("/{activity_id}")
def delete_stop_activity(activity_id: int, db: Session = Depends(get_db)):
    db_activity = db.query(StopActivity).filter(StopActivity.activity_id == activity_id).first()
    if db_activity is None:
        raise HTTPException(status_code=404, detail="Stop activity not found")

    db.delete(db_activity)
    db.commit()
    return {"message": "Stop activity deleted successfully"}
