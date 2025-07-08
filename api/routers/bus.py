from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session
from typing import List

from api.database import get_db
from api.models import Bus
from api.schemas import BusCreate, BusRead, BusUpdate

router = APIRouter(prefix="/buses", tags=["buses"])


@router.post("/", response_model=BusRead)
def create_bus(bus: BusCreate, db: Session = Depends(get_db)):
    existing_bus = db.query(Bus).filter(Bus.reg_num == bus.reg_num).first()
    if existing_bus:
        raise HTTPException(
            status_code=400, detail="Bus with this registration number already exists"
        )

    db_bus = Bus(**bus.model_dump())
    db.add(db_bus)
    db.commit()
    db.refresh(db_bus)
    return db_bus


@router.get("/", response_model=List[BusRead])
def read_buses(skip: int = 0, limit: int = 100, db: Session = Depends(get_db)):
    buses = db.query(Bus).offset(skip).limit(limit).all()
    return buses


@router.get("/{bus_id}", response_model=BusRead)
def read_bus(bus_id: str, db: Session = Depends(get_db)):
    db_bus = db.query(Bus).filter(Bus.bus_id == bus_id).first()
    if db_bus is None:
        raise HTTPException(status_code=404, detail="Bus not found")
    return db_bus


@router.put("/{bus_id}", response_model=BusRead)
def update_bus(bus_id: str, bus: BusUpdate, db: Session = Depends(get_db)):
    db_bus = db.query(Bus).filter(Bus.bus_id == bus_id).first()
    if db_bus is None:
        raise HTTPException(status_code=404, detail="Bus not found")

    update_data = bus.model_dump(exclude_unset=True)

    if "registration_number" in update_data:
        update_data["reg_num"] = update_data.pop("registration_number")

    for key, value in update_data.items():
        setattr(db_bus, key, value)

    db.commit()
    db.refresh(db_bus)
    return db_bus


@router.delete("/{bus_id}")
def delete_bus(bus_id: str, db: Session = Depends(get_db)):
    db_bus = db.query(Bus).filter(Bus.bus_id == bus_id).first()
    if db_bus is None:
        raise HTTPException(status_code=404, detail="Bus not found")

    db.delete(db_bus)
    db.commit()
    return {"message": "Bus deleted successfully"}
