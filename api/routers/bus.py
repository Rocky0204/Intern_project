from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session

from api import schemas
from api.database import get_db
from api.models import Bus

router = APIRouter(prefix="/api/bus", tags=["Bus"])


@router.post("", response_model=schemas.BusRead)
def create_bus(obj_in: schemas.BusCreate, db: Session = Depends(get_db)):
    obj = Bus(**obj_in.model_dump())
    db.add(obj)
    db.commit()
    db.refresh(obj)
    return obj


@router.get("/{reg_num}", response_model=schemas.BusRead)
def read_bus(reg_num: str, db: Session = Depends(get_db)):
    obj = db.query(Bus).filter_by(reg_num=reg_num).first()
    if not obj:
        raise HTTPException(status_code=404, detail="Bus not found")
    return obj


@router.put("/{reg_num}", response_model=schemas.BusRead)
def update_bus(reg_num: str, update: schemas.BusUpdate, db: Session = Depends(get_db)):
    obj = db.query(Bus).filter_by(reg_num=reg_num).first()
    if not obj:
        raise HTTPException(status_code=404, detail="Bus not found")
    for key, value in update.model_dump(exclude_unset=True).items():
        setattr(obj, key, value)
    db.commit()
    db.refresh(obj)
    return obj


@router.delete("/{reg_num}", response_model=schemas.BusRead)
def delete_bus(reg_num: str, db: Session = Depends(get_db)):
    obj = db.query(Bus).filter_by(reg_num=reg_num).first()
    if obj:
        db.delete(obj)
        db.commit()
    return obj
