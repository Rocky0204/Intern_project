from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session

from api import schemas
from api.database import get_db
from api.models import BusType

router = APIRouter(prefix="/api/bus_type", tags=["BusType"])


@router.post("", response_model=schemas.BusTypeRead)
def create_bus_type(obj_in: schemas.BusTypeCreate, db: Session = Depends(get_db)):
    obj = BusType(**obj_in.model_dump())
    db.add(obj)
    db.commit()
    db.refresh(obj)
    return obj


@router.get("/{type_id}", response_model=schemas.BusTypeRead)
def read_bus_type(type_id: int, db: Session = Depends(get_db)):
    obj = db.query(BusType).filter_by(type_id=type_id).first()
    if not obj:
        raise HTTPException(status_code=404, detail="BusType not found")
    return obj


@router.put("/{type_id}", response_model=schemas.BusTypeRead)
def update_bus_type(
    type_id: int, update: schemas.BusTypeUpdate, db: Session = Depends(get_db)
):
    obj = db.query(BusType).filter_by(type_id=type_id).first()
    if not obj:
        raise HTTPException(status_code=404, detail="BusType not found")
    for key, value in update.model_dump(exclude_unset=True).items():
        setattr(obj, key, value)
    db.commit()
    db.refresh(obj)
    return obj


@router.delete("/{type_id}", response_model=schemas.BusTypeRead)
def delete_bus_type(type_id: int, db: Session = Depends(get_db)):
    obj = db.query(BusType).filter_by(type_id=type_id).first()
    if obj:
        db.delete(obj)
        db.commit()
    return obj
