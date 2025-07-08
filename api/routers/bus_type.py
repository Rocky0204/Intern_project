from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session
from typing import List

from api.database import get_db
from api.models import BusType, Bus, Block
from api.schemas import BusTypeCreate, BusTypeRead, BusTypeUpdate

router = APIRouter(prefix="/bus-types", tags=["bus_types"])


@router.post("/", response_model=BusTypeRead)
def create_bus_type(bus_type: BusTypeCreate, db: Session = Depends(get_db)):
    existing_type = db.query(BusType).filter(BusType.name == bus_type.name).first()
    if existing_type:
        raise HTTPException(
            status_code=400, detail="Bus type with this name already exists"
        )

    db_bus_type = BusType(**bus_type.model_dump())
    db.add(db_bus_type)
    db.commit()
    db.refresh(db_bus_type)
    return db_bus_type


@router.get("/", response_model=List[BusTypeRead])
def read_bus_types(skip: int = 0, limit: int = 100, db: Session = Depends(get_db)):
    bus_types = db.query(BusType).offset(skip).limit(limit).all()
    return bus_types


@router.get("/{type_id}", response_model=BusTypeRead)
def read_bus_type(type_id: int, db: Session = Depends(get_db)):
    db_bus_type = db.query(BusType).filter(BusType.type_id == type_id).first()
    if db_bus_type is None:
        raise HTTPException(status_code=404, detail="Bus type not found")
    return db_bus_type


@router.put("/{type_id}", response_model=BusTypeRead)
def update_bus_type(
    type_id: int, bus_type: BusTypeUpdate, db: Session = Depends(get_db)
):
    db_bus_type = db.query(BusType).filter(BusType.type_id == type_id).first()
    if db_bus_type is None:
        raise HTTPException(status_code=404, detail="Bus type not found")

    update_data = bus_type.model_dump(exclude_unset=True)

    if "name" in update_data:
        existing = (
            db.query(BusType)
            .filter(BusType.name == update_data["name"], BusType.type_id != type_id)
            .first()
        )
        if existing:
            raise HTTPException(
                status_code=400, detail="Bus type with this name already exists"
            )

    for key, value in update_data.items():
        setattr(db_bus_type, key, value)

    db.commit()
    db.refresh(db_bus_type)
    return db_bus_type


@router.delete("/{type_id}")
def delete_bus_type(type_id: int, db: Session = Depends(get_db)):
    db_bus_type = db.query(BusType).filter(BusType.type_id == type_id).first()
    if db_bus_type is None:
        raise HTTPException(status_code=404, detail="Bus type not found")

    has_buses = db.query(Bus).filter(Bus.bus_type_id == type_id).first() is not None
    has_blocks = (
        db.query(Block).filter(Block.bus_type_id == type_id).first() is not None
    )

    if has_buses or has_blocks:
        raise HTTPException(
            status_code=400,
            detail="Cannot delete bus type with associated buses or blocks",
        )

    db.delete(db_bus_type)
    db.commit()
    return {"message": "Bus type deleted successfully"}
