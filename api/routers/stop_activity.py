# api/routers/stop_activity.py

from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy.orm import Session
from typing import List
from datetime import time # Ensure time is imported

from ..database import get_db
from ..models import StopActivity, StopPoint, VehicleJourney
from ..schemas import (
    StopActivityCreate,
    StopActivityRead,
    StopActivityUpdate,
)

router = APIRouter(
    prefix="/stop_activities",
    tags=["Stop Activities"],
    responses={404: {"description": "Not found"}},
)


@router.post("/", response_model=StopActivityRead, status_code=status.HTTP_201_CREATED)
def create_stop_activity(
    activity: StopActivityCreate, db: Session = Depends(get_db)
):
    # Validate foreign keys
    stop_point = db.query(StopPoint).filter(StopPoint.atco_code == activity.stop_point_id).first()
    if not stop_point:
        raise HTTPException(status_code=404, detail=f"Stop Point with ATCO Code {activity.stop_point_id} not found")

    if activity.vj_id:
        vehicle_journey = db.query(VehicleJourney).filter(VehicleJourney.vj_id == activity.vj_id).first()
        if not vehicle_journey:
            raise HTTPException(status_code=404, detail=f"Vehicle Journey with ID {activity.vj_id} not found")

    db_activity = StopActivity(
        activity_type=activity.activity_type,
        activity_time=activity.activity_time,
        pax_count=activity.pax_count,
        stop_point_id=activity.stop_point_id, # Correctly use stop_point_id
        vj_id=activity.vj_id,
    )
    db.add(db_activity)
    db.commit()
    db.refresh(db_activity)

    # Manually serialize the response to ensure time objects are ISO 8601 strings
    return {
        "activity_id": db_activity.activity_id,
        "activity_type": db_activity.activity_type,
        "activity_time": db_activity.activity_time.isoformat(), # Convert to string
        "pax_count": db_activity.pax_count,
        "stop_point_id": db_activity.stop_point_id,
        "vj_id": db_activity.vj_id,
    }


@router.get("/", response_model=List[StopActivityRead])
def read_stop_activities(
    stop_point_id: int = None, skip: int = 0, limit: int = 100, db: Session = Depends(get_db)
):
    query = db.query(StopActivity)
    if stop_point_id:
        query = query.filter(StopActivity.stop_point_id == stop_point_id)
    activities = query.offset(skip).limit(limit).all()

    # Manually serialize each activity to match StopActivityRead schema
    response_list = []
    for activity in activities:
        response_list.append({
            "activity_id": activity.activity_id,
            "activity_type": activity.activity_type,
            "activity_time": activity.activity_time.isoformat(), # Convert to string
            "pax_count": activity.pax_count,
            "stop_point_id": activity.stop_point_id,
            "vj_id": activity.vj_id,
        })
    return response_list


@router.get("/{activity_id}", response_model=StopActivityRead)
def read_single_stop_activity(activity_id: int, db: Session = Depends(get_db)):
    db_activity = (
        db.query(StopActivity).filter(StopActivity.activity_id == activity_id).first()
    )
    if db_activity is None:
        raise HTTPException(status_code=404, detail="Stop activity not found")
    
    # Manually serialize to match StopActivityRead schema
    return {
        "activity_id": db_activity.activity_id,
        "activity_type": db_activity.activity_type,
        "activity_time": db_activity.activity_time.isoformat(), # Convert to string
        "pax_count": db_activity.pax_count,
        "stop_point_id": db_activity.stop_point_id,
        "vj_id": db_activity.vj_id,
    }


@router.put("/{activity_id}", response_model=StopActivityRead)
def update_stop_activity(
    activity_id: int, activity_update: StopActivityUpdate, db: Session = Depends(get_db)
):
    db_activity = (
        db.query(StopActivity).filter(StopActivity.activity_id == activity_id).first()
    )

    if db_activity is None:
        raise HTTPException(status_code=404, detail="Stop activity not found")

    if activity_update.activity_type is not None:
        db_activity.activity_type = activity_update.activity_type
    
    if activity_update.activity_time is not None:
        db_activity.activity_time = activity_update.activity_time
    
    if activity_update.pax_count is not None:
        db_activity.pax_count = activity_update.pax_count
    
    if activity_update.stop_point_id is not None:
        # Validate if the new stop_point_id exists
        stop_point = db.query(StopPoint).filter(StopPoint.atco_code == activity_update.stop_point_id).first()
        if not stop_point:
            raise HTTPException(status_code=404, detail=f"Stop Point with ATCO Code {activity_update.stop_point_id} not found")
        db_activity.stop_point_id = activity_update.stop_point_id # Correctly use stop_point_id
    
    if activity_update.vj_id is not None:
        # Validate if the new vj_id exists
        if activity_update.vj_id is not None: # Check if it's explicitly provided, not just None by default
            if activity_update.vj_id is not None: # Check if it's explicitly provided
                vehicle_journey = db.query(VehicleJourney).filter(VehicleJourney.vj_id == activity_update.vj_id).first()
                if not vehicle_journey:
                    raise HTTPException(status_code=404, detail=f"Vehicle Journey with ID {activity_update.vj_id} not found")
        db_activity.vj_id = activity_update.vj_id


    db.commit()
    db.refresh(db_activity)

    # Manually serialize the response
    return {
        "activity_id": db_activity.activity_id,
        "activity_type": db_activity.activity_type,
        "activity_time": db_activity.activity_time.isoformat(), # Convert to string
        "pax_count": db_activity.pax_count,
        "stop_point_id": db_activity.stop_point_id,
        "vj_id": db_activity.vj_id,
    }


@router.delete("/{activity_id}", status_code=status.HTTP_204_NO_CONTENT)
def delete_stop_activity(activity_id: int, db: Session = Depends(get_db)):
    db_activity = (
        db.query(StopActivity).filter(StopActivity.activity_id == activity_id).first()
    )

    if db_activity is None:
        raise HTTPException(status_code=404, detail="Stop activity not found")

    db.delete(db_activity)
    db.commit()
    return {"message": "Stop activity deleted successfully"}