from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session
from typing import List

from api.database import get_db
from api.models import Garage, Bus
from api.schemas import GarageCreate, GarageRead, GarageUpdate

router = APIRouter(prefix="/garages", tags=["garages"])


@router.post("/", response_model=GarageRead)
def create_garage(garage: GarageCreate, db: Session = Depends(get_db)):
    existing_garage = db.query(Garage).filter(Garage.name == garage.name).first()
    if existing_garage:
        raise HTTPException(
            status_code=400, detail="Garage with this name already exists"
        )

    db_garage = Garage(**garage.model_dump())
    db.add(db_garage)
    db.commit()
    db.refresh(db_garage)
    return db_garage


@router.get("/", response_model=List[GarageRead])
def read_garages(skip: int = 0, limit: int = 100, db: Session = Depends(get_db)):
    garages = db.query(Garage).offset(skip).limit(limit).all()
    return garages


@router.get("/{garage_id}", response_model=GarageRead)
def read_garage(garage_id: int, db: Session = Depends(get_db)):
    db_garage = db.query(Garage).filter(Garage.garage_id == garage_id).first()
    if db_garage is None:
        raise HTTPException(status_code=404, detail="Garage not found")
    return db_garage


@router.put("/{garage_id}", response_model=GarageRead)
def update_garage(garage_id: int, garage: GarageUpdate, db: Session = Depends(get_db)):
    db_garage = db.query(Garage).filter(Garage.garage_id == garage_id).first()
    if db_garage is None:
        raise HTTPException(status_code=404, detail="Garage not found")

    update_data = garage.model_dump(exclude_unset=True)

    if "name" in update_data:
        existing = (
            db.query(Garage)
            .filter(Garage.name == update_data["name"], Garage.garage_id != garage_id)
            .first()
        )
        if existing:
            raise HTTPException(
                status_code=400, detail="Garage with this name already exists"
            )

    for key, value in update_data.items():
        setattr(db_garage, key, value)

    db.commit()
    db.refresh(db_garage)
    return db_garage


@router.delete("/{garage_id}")
def delete_garage(garage_id: int, db: Session = Depends(get_db)):
    db_garage = db.query(Garage).filter(Garage.garage_id == garage_id).first()
    if db_garage is None:
        raise HTTPException(status_code=404, detail="Garage not found")

    if db.query(Bus).filter(Bus.garage_id == garage_id).first():
        raise HTTPException(
            status_code=400, detail="Cannot delete garage with assigned buses"
        )

    db.delete(db_garage)
    db.commit()
    return {"message": "Garage deleted successfully"}
