from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session

from api import schemas
from api.database import get_db
from api.models import VehicleJourney

router = APIRouter(prefix="/api/vehiclejourney", tags=["VehicleJourney"])


@router.post("", response_model=schemas.VehicleJourneyRead)
def create_vehiclejourney(
    obj_in: schemas.VehicleJourneyCreate, db: Session = Depends(get_db)
):
    obj = VehicleJourney(**obj_in.model_dump())
    db.add(obj)
    db.commit()
    db.refresh(obj)
    return obj


@router.get("/{vj_id}", response_model=schemas.VehicleJourneyRead)
def read_vehiclejourney(vj_id: int, db: Session = Depends(get_db)):
    obj = db.query(VehicleJourney).filter_by(vj_id=vj_id).first()
    if not obj:
        raise HTTPException(status_code=404, detail="VehicleJourney not found")
    return obj


@router.put("/{vj_id}", response_model=schemas.VehicleJourneyRead)
def update_vehiclejourney(
    vj_id: int, update: schemas.VehicleJourneyUpdate, db: Session = Depends(get_db)
):
    obj = db.query(VehicleJourney).filter_by(vj_id=vj_id).first()
    if not obj:
        raise HTTPException(status_code=404, detail="VehicleJourney not found")
    for key, value in update.model_dump(exclude_unset=True).items():
        setattr(obj, key, value)
    db.commit()
    db.refresh(obj)
    return obj


@router.delete("/{vj_id}", response_model=schemas.VehicleJourneyRead)
def delete_vehiclejourney(vj_id: int, db: Session = Depends(get_db)):
    obj = db.query(VehicleJourney).filter_by(vj_id=vj_id).first()
    if obj:
        db.delete(obj)
        db.commit()
    return obj
