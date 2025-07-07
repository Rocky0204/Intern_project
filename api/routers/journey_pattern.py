from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session
from typing import List

from ..database import get_db
from ..models import JourneyPattern  # Removed JourneyPatternDefinition as it's not used
from ..schemas import JourneyPatternCreate, JourneyPatternRead, JourneyPatternUpdate

router = APIRouter(
    prefix="/journey_patterns",
    tags=["journey_patterns"],
    responses={404: {"description": "Not found"}},
)


@router.post("/", response_model=JourneyPatternRead)
def create_journey_pattern(
    journey_pattern: JourneyPatternCreate, db: Session = Depends(get_db)
):
    db_journey_pattern = JourneyPattern(**journey_pattern.model_dump())
    db.add(db_journey_pattern)
    db.commit()
    db.refresh(db_journey_pattern)
    return db_journey_pattern


@router.get("/", response_model=List[JourneyPatternRead])
def read_journey_patterns(
    skip: int = 0, limit: int = 100, db: Session = Depends(get_db)
):
    return db.query(JourneyPattern).offset(skip).limit(limit).all()


@router.get("/{jp_id}", response_model=JourneyPatternRead)
def read_journey_pattern(jp_id: int, db: Session = Depends(get_db)):
    db_journey_pattern = (
        db.query(JourneyPattern).filter(JourneyPattern.jp_id == jp_id).first()
    )
    if db_journey_pattern is None:
        raise HTTPException(status_code=404, detail="Journey pattern not found")
    return db_journey_pattern


@router.put("/{jp_id}", response_model=JourneyPatternRead)
def update_journey_pattern(
    jp_id: int, journey_pattern: JourneyPatternUpdate, db: Session = Depends(get_db)
):
    db_journey_pattern = (
        db.query(JourneyPattern).filter(JourneyPattern.jp_id == jp_id).first()
    )
    if db_journey_pattern is None:
        raise HTTPException(status_code=404, detail="Journey pattern not found")

    for key, value in journey_pattern.model_dump(exclude_unset=True).items():
        setattr(db_journey_pattern, key, value)

    db.commit()
    db.refresh(db_journey_pattern)
    return db_journey_pattern


@router.delete("/{jp_id}")
def delete_journey_pattern(jp_id: int, db: Session = Depends(get_db)):
    db_journey_pattern = (
        db.query(JourneyPattern).filter(JourneyPattern.jp_id == jp_id).first()
    )
    if db_journey_pattern is None:
        raise HTTPException(status_code=404, detail="Journey pattern not found")

    db.delete(db_journey_pattern)
    db.commit()
    return {"message": "Journey pattern deleted successfully"}
