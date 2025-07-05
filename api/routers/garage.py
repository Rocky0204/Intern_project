from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session

from api import schemas
from api.database import get_db
from api.models import Garage

router = APIRouter(prefix="/api/garage", tags=["Garage"])


@router.post("", response_model=schemas.GarageRead)
def create_garage(obj_in: schemas.GarageCreate, db: Session = Depends(get_db)):
    obj = Garage(**obj_in.model_dump())
    db.add(obj)
    db.commit()
    db.refresh(obj)
    return obj


@router.get("/{garage_id}", response_model=schemas.GarageRead)
def read_garage(garage_id: int, db: Session = Depends(get_db)):
    obj = db.query(Garage).filter_by(garage_id=garage_id).first()
    if not obj:
        raise HTTPException(status_code=404, detail="Garage not found")
    return obj


@router.put("/{garage_id}", response_model=schemas.GarageRead)
def update_garage(
    garage_id: int, update: schemas.GarageUpdate, db: Session = Depends(get_db)
):
    obj = db.query(Garage).filter_by(garage_id=garage_id).first()
    if not obj:
        raise HTTPException(status_code=404, detail="Garage not found")
    for key, value in update.model_dump(exclude_unset=True).items():
        setattr(obj, key, value)
    db.commit()
    db.refresh(obj)
    return obj


@router.delete("/{garage_id}", response_model=schemas.GarageRead)
def delete_garage(garage_id: int, db: Session = Depends(get_db)):
    obj = db.query(Garage).filter_by(garage_id=garage_id).first()
    if obj:
        db.delete(obj)
        db.commit()
    return obj
