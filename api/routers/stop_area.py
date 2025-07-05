from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session

from api import schemas
from api.database import get_db
from api.models import StopArea

router = APIRouter(prefix="/api/stop_area", tags=["StopArea"])


@router.post("", response_model=schemas.StopAreaRead)
def create_stop_area(obj_in: schemas.StopAreaCreate, db: Session = Depends(get_db)):
    obj = StopArea(**obj_in.model_dump())
    db.add(obj)
    db.commit()
    db.refresh(obj)
    return obj


@router.get("/{stop_area_code}", response_model=schemas.StopAreaRead)
def read_stop_area(stop_area_code: int, db: Session = Depends(get_db)):
    obj = db.query(StopArea).filter_by(stop_area_code=stop_area_code).first()
    if not obj:
        raise HTTPException(status_code=404, detail="StopArea not found")
    return obj


@router.put("/{stop_area_code}", response_model=schemas.StopAreaRead)
def update_stop_area(
    stop_area_code: int, update: schemas.StopAreaUpdate, db: Session = Depends(get_db)
):
    obj = db.query(StopArea).filter_by(stop_area_code=stop_area_code).first()
    if not obj:
        raise HTTPException(status_code=404, detail="StopArea not found")
    for key, value in update.model_dump(exclude_unset=True).items():
        setattr(obj, key, value)
    db.commit()
    db.refresh(obj)
    return obj


@router.delete("/{stop_area_code}", response_model=schemas.StopAreaRead)
def delete_stop_area(stop_area_code: int, db: Session = Depends(get_db)):
    obj = db.query(StopArea).filter_by(stop_area_code=stop_area_code).first()
    if obj:
        db.delete(obj)
        db.commit()
    return obj
