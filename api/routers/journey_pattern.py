from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session

from api import schemas
from api.database import get_db
from api.models import JourneyPattern

router = APIRouter(prefix="/api/journeypattern", tags=["JourneyPattern"])


@router.post("", response_model=schemas.JourneyPatternRead)
def create_journeypattern(
    obj_in: schemas.JourneyPatternCreate, db: Session = Depends(get_db)
):
    obj = JourneyPattern(**obj_in.model_dump())
    db.add(obj)
    db.commit()
    db.refresh(obj)
    return obj


@router.get("/{jp_id}", response_model=schemas.JourneyPatternRead)
def read_journeypattern(jp_id: int, db: Session = Depends(get_db)):
    obj = db.query(JourneyPattern).filter_by(jp_id=jp_id).first()
    if not obj:
        raise HTTPException(status_code=404, detail="JourneyPattern not found")
    return obj


@router.put("/{jp_id}", response_model=schemas.JourneyPatternRead)
def update_journeypattern(
    jp_id: int, update: schemas.JourneyPatternUpdate, db: Session = Depends(get_db)
):
    obj = db.query(JourneyPattern).filter_by(jp_id=jp_id).first()
    if not obj:
        raise HTTPException(status_code=404, detail="JourneyPattern not found")
    for key, value in update.model_dump(exclude_unset=True).items():
        setattr(obj, key, value)
    db.commit()
    db.refresh(obj)
    return obj


@router.delete("/{jp_id}", response_model=schemas.JourneyPatternRead)
def delete_journeypattern(jp_id: int, db: Session = Depends(get_db)):
    obj = db.query(JourneyPattern).filter_by(jp_id=jp_id).first()
    if obj:
        db.delete(obj)
        db.commit()
    return obj
