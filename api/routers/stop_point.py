from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy.orm import Session
from sqlalchemy.exc import IntegrityError
from typing import List

from ..database import get_db
from ..models import StopPoint, StopArea
from ..schemas import StopPointCreate, StopPointRead, StopPointUpdate

router = APIRouter(prefix="/stop_points", tags=["stop_points"])


@router.post("/", response_model=StopPointRead, status_code=status.HTTP_201_CREATED)
def create_stop_point(stop_point: StopPointCreate, db: Session = Depends(get_db)):
    stop_area = (
        db.query(StopArea)
        .filter(StopArea.stop_area_code == stop_point.stop_area_code)
        .first()
    )
    if not stop_area:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"StopArea with code {stop_point.stop_area_code} not found.",
        )

    db_stop_point = StopPoint(**stop_point.model_dump())
    try:
        db.add(db_stop_point)
        db.commit()
        db.refresh(db_stop_point)
        return db_stop_point
    except IntegrityError:
        db.rollback()
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Could not create stop point due to a database integrity issue (e.g., duplicate primary key).",
        )


@router.get("/", response_model=List[StopPointRead])
def read_stop_points(skip: int = 0, limit: int = 100, db: Session = Depends(get_db)):
    stop_points = db.query(StopPoint).offset(skip).limit(limit).all()
    return stop_points


@router.get("/{atco_code}", response_model=StopPointRead)
def read_stop_point(atco_code: int, db: Session = Depends(get_db)):
    db_stop_point = db.query(StopPoint).filter(StopPoint.atco_code == atco_code).first()
    if db_stop_point is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND, detail="Stop point not found"
        )
    return db_stop_point


@router.put("/{atco_code}", response_model=StopPointRead)
def update_stop_point(
    atco_code: int, stop_point: StopPointUpdate, db: Session = Depends(get_db)
):
    db_stop_point = db.query(StopPoint).filter(StopPoint.atco_code == atco_code).first()
    if db_stop_point is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND, detail="Stop point not found"
        )

    update_data = stop_point.model_dump(exclude_unset=True)

    if (
        "stop_area_code" in update_data
        and update_data["stop_area_code"] != db_stop_point.stop_area_code
    ):
        stop_area = (
            db.query(StopArea)
            .filter(StopArea.stop_area_code == update_data["stop_area_code"])
            .first()
        )
        if not stop_area:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=f"StopArea with code {update_data['stop_area_code']} not found.",
            )

    for field, value in update_data.items():
        setattr(db_stop_point, field, value)

    try:
        db.commit()
        db.refresh(db_stop_point)
        return db_stop_point
    except IntegrityError:
        db.rollback()
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Could not update stop point due to a database integrity issue.",
        )


@router.delete("/{atco_code}", status_code=status.HTTP_204_NO_CONTENT)
def delete_stop_point(atco_code: int, db: Session = Depends(get_db)):
    db_stop_point = db.query(StopPoint).filter(StopPoint.atco_code == atco_code).first()
    if db_stop_point is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND, detail="Stop point not found"
        )

    try:
        db.delete(db_stop_point)
        db.commit()
        return {"message": "Stop point deleted successfully"}
    except IntegrityError:
        db.rollback()
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Cannot delete stop point due to existing dependencies.",
        )
