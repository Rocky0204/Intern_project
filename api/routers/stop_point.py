from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session

from api import schemas
from api.database import get_db
from api.models import StopPoint

router = APIRouter(prefix="/api/stop_point", tags=["StopPoint"])


@router.post("", response_model=schemas.StopPointRead)
def create_stop_point(obj_in: schemas.StopPointCreate, db: Session = Depends(get_db)):
    obj = StopPoint(**obj_in.model_dump())
    db.add(obj)
    db.commit()
    db.refresh(obj)
    return obj


@router.get("/{atco_code}", response_model=schemas.StopPointRead)
def read_stop_point(atco_code: int, db: Session = Depends(get_db)):
    obj = db.query(StopPoint).filter_by(atco_code=atco_code).first()
    if not obj:
        raise HTTPException(status_code=404, detail="StopPoint not found")
    return obj


@router.put("/{atco_code}", response_model=schemas.StopPointRead)
def update_stop_point(
    atco_code: int, update: schemas.StopPointUpdate, db: Session = Depends(get_db)
):
    obj = db.query(StopPoint).filter_by(atco_code=atco_code).first()
    if not obj:
        raise HTTPException(status_code=404, detail="StopPoint not found")
    for key, value in update.model_dump(exclude_unset=True).items():
        setattr(obj, key, value)
    db.commit()
    db.refresh(obj)
    return obj


@router.delete("/{atco_code}", response_model=schemas.StopPointRead)
def delete_stop_point(atco_code: int, db: Session = Depends(get_db)):
    obj = db.query(StopPoint).filter_by(atco_code=atco_code).first()
    if obj:
        db.delete(obj)
        db.commit()
    return obj
