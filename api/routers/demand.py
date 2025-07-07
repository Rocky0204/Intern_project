# api/routers/demand.py
from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy.orm import Session
from sqlalchemy.exc import IntegrityError
from typing import List
from datetime import time

from ..database import get_db
from ..models import (
    Demand,
    StopArea,
)  # Import Demand and StopArea for foreign key validation
from ..schemas import DemandCreate, DemandRead, DemandUpdate

router = APIRouter(prefix="/demand", tags=["demand"])


@router.post("/", response_model=DemandRead, status_code=status.HTTP_201_CREATED)
def create_demand(demand: DemandCreate, db: Session = Depends(get_db)):
    """
    Create a new demand entry.
    Validates that origin and destination StopArea codes exist.
    """
    # Validate origin StopArea
    origin_stop_area = (
        db.query(StopArea).filter(StopArea.stop_area_code == demand.origin).first()
    )
    if not origin_stop_area:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Origin StopArea with code {demand.origin} not found.",
        )

    # Validate destination StopArea
    destination_stop_area = (
        db.query(StopArea).filter(StopArea.stop_area_code == demand.destination).first()
    )
    if not destination_stop_area:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Destination StopArea with code {demand.destination} not found.",
        )

    db_demand = Demand(**demand.model_dump())
    try:
        db.add(db_demand)
        db.commit()
        db.refresh(db_demand)
        return db_demand
    except IntegrityError:
        db.rollback()
        # This handles cases like duplicate (origin, destination, start_time, end_time) primary key
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,  # 409 Conflict for duplicate entry
            detail="Demand entry with these origin, destination, start_time, and end_time already exists.",
        )


@router.get("/", response_model=List[DemandRead])
def read_demands(skip: int = 0, limit: int = 100, db: Session = Depends(get_db)):
    """
    Retrieve a list of demand entries.
    """
    demands = db.query(Demand).offset(skip).limit(limit).all()
    return demands


@router.get(
    "/{origin}/{destination}/{start_time}/{end_time}", response_model=DemandRead
)
def read_demand(
    origin: int,
    destination: int,
    start_time: time,  # FastAPI automatically parses time strings
    end_time: time,  # FastAPI automatically parses time strings
    db: Session = Depends(get_db),
):
    """
    Retrieve a specific demand entry by its composite primary key.
    """
    db_demand = (
        db.query(Demand)
        .filter(
            Demand.origin == origin,
            Demand.destination == destination,
            Demand.start_time == start_time,
            Demand.end_time == end_time,
        )
        .first()
    )
    if db_demand is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND, detail="Demand entry not found"
        )
    return db_demand


@router.put(
    "/{origin}/{destination}/{start_time}/{end_time}", response_model=DemandRead
)
def update_demand(
    origin: int,
    destination: int,
    start_time: time,
    end_time: time,
    demand: DemandUpdate,
    db: Session = Depends(get_db),
):
    """
    Update an existing demand entry.
    Only 'count' is updatable as per schemas.py.
    """
    db_demand = (
        db.query(Demand)
        .filter(
            Demand.origin == origin,
            Demand.destination == destination,
            Demand.start_time == start_time,
            Demand.end_time == end_time,
        )
        .first()
    )
    if db_demand is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND, detail="Demand entry not found"
        )

    update_data = demand.model_dump(exclude_unset=True)
    for field, value in update_data.items():
        setattr(db_demand, field, value)

    try:
        db.commit()
        db.refresh(db_demand)
        return db_demand
    except IntegrityError:
        db.rollback()
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Could not update demand entry due to a database integrity issue.",
        )


@router.delete(
    "/{origin}/{destination}/{start_time}/{end_time}",
    status_code=status.HTTP_204_NO_CONTENT,
)
def delete_demand(
    origin: int,
    destination: int,
    start_time: time,
    end_time: time,
    db: Session = Depends(get_db),
):
    """
    Delete a demand entry.
    """
    db_demand = (
        db.query(Demand)
        .filter(
            Demand.origin == origin,
            Demand.destination == destination,
            Demand.start_time == start_time,
            Demand.end_time == end_time,
        )
        .first()
    )
    if db_demand is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND, detail="Demand entry not found"
        )

    try:
        db.delete(db_demand)
        db.commit()
        return {
            "message": "Demand entry deleted successfully"
        }  # 204 No Content typically doesn't return a body
    except IntegrityError:
        db.rollback()
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Cannot delete demand entry due to existing dependencies.",
        )
