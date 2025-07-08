from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy.orm import Session
from sqlalchemy.exc import (
    IntegrityError,
)  
from typing import List

from ..database import get_db
from ..models import StopArea  
from ..schemas import (
    StopAreaCreate,
    StopAreaRead,
    StopAreaUpdate,
)  

router = APIRouter(prefix="/stop_areas", tags=["stop_areas"])


@router.post("/", response_model=StopAreaRead, status_code=status.HTTP_201_CREATED)
def create_stop_area(stop_area: StopAreaCreate, db: Session = Depends(get_db)):
    
    existing_stop_area = (
        db.query(StopArea)
        .filter(StopArea.admin_area_code == stop_area.admin_area_code)
        .first()
    )
    if existing_stop_area:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,  # 409 Conflict for duplicate resource
            detail=f"Stop area with admin_area_code '{stop_area.admin_area_code}' already exists.",
        )

    db_stop_area = StopArea(**stop_area.model_dump())
    try:
        db.add(db_stop_area)
        db.commit()
        db.refresh(db_stop_area)
        return db_stop_area
    except IntegrityError:
        db.rollback()
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Could not create stop area due to a database integrity issue (e.g., duplicate primary key).",
        )


@router.get("/", response_model=List[StopAreaRead])
def read_stop_areas(skip: int = 0, limit: int = 100, db: Session = Depends(get_db)):
  
    stop_areas = db.query(StopArea).offset(skip).limit(limit).all()
    return stop_areas


@router.get("/{stop_area_code}", response_model=StopAreaRead)
def read_stop_area(stop_area_code: int, db: Session = Depends(get_db)):
  
    db_stop_area = (
        db.query(StopArea).filter(StopArea.stop_area_code == stop_area_code).first()
    )
    if db_stop_area is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND, detail="Stop area not found"
        )
    return db_stop_area


@router.put("/{stop_area_code}", response_model=StopAreaRead)
def update_stop_area(
    stop_area_code: int, stop_area: StopAreaUpdate, db: Session = Depends(get_db)
):
   
    db_stop_area = (
        db.query(StopArea).filter(StopArea.stop_area_code == stop_area_code).first()
    )
    if db_stop_area is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND, detail="Stop area not found"
        )

    update_data = stop_area.model_dump(exclude_unset=True)

    if (
        "admin_area_code" in update_data
        and update_data["admin_area_code"] != db_stop_area.admin_area_code
    ):
        existing_stop_area = (
            db.query(StopArea)
            .filter(StopArea.admin_area_code == update_data["admin_area_code"])
            .first()
        )
        if existing_stop_area and existing_stop_area.stop_area_code != stop_area_code:
            raise HTTPException(
                status_code=status.HTTP_409_CONFLICT,
                detail=f"Stop area with admin_area_code '{update_data['admin_area_code']}' already exists.",
            )

    for field, value in update_data.items():
        setattr(db_stop_area, field, value)

    try:
        db.commit()
        db.refresh(db_stop_area)
        return db_stop_area
    except IntegrityError:
        db.rollback()
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Could not update stop area due to a database integrity issue.",
        )


@router.delete("/{stop_area_code}", status_code=status.HTTP_204_NO_CONTENT)
def delete_stop_area(stop_area_code: int, db: Session = Depends(get_db)):
  
    db_stop_area = (
        db.query(StopArea).filter(StopArea.stop_area_code == stop_area_code).first()
    )
    if db_stop_area is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND, detail="Stop area not found"
        )


    try:
        db.delete(db_stop_area)
        db.commit()
        return {
            "message": "Stop area deleted successfully"
        }  
    except IntegrityError:
        db.rollback()
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Cannot delete stop area due to existing dependencies (e.g., stop points).",
        )
