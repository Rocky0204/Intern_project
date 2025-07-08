from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session
from typing import List

from api.database import get_db
from api.models import *
from api.schemas import *

router = APIRouter(prefix="/operators", tags=["operators"])


@router.post("/", response_model=OperatorRead)
def create_operator(operator: OperatorCreate, db: Session = Depends(get_db)):
    existing_operator = (
        db.query(Operator)
        .filter(Operator.operator_code == operator.operator_code)
        .first()
    )
    if existing_operator:
        raise HTTPException(
            status_code=400, detail="Operator with this code already exists"
        )

    db_operator = Operator(**operator.model_dump())
    db.add(db_operator)
    db.commit()
    db.refresh(db_operator)
    return db_operator


@router.get("/", response_model=List[OperatorRead])
def read_operators(skip: int = 0, limit: int = 100, db: Session = Depends(get_db)):
    operators = db.query(Operator).offset(skip).limit(limit).all()
    return operators


@router.get("/{operator_id}", response_model=OperatorRead)
def read_operator(operator_id: int, db: Session = Depends(get_db)):
    db_operator = db.query(Operator).filter(Operator.operator_id == operator_id).first()
    if db_operator is None:
        raise HTTPException(status_code=404, detail="Operator not found")
    return db_operator


@router.put("/{operator_id}", response_model=OperatorRead)
def update_operator(
    operator_id: int, operator: OperatorUpdate, db: Session = Depends(get_db)
):
    db_operator = db.query(Operator).filter(Operator.operator_id == operator_id).first()
    if db_operator is None:
        raise HTTPException(status_code=404, detail="Operator not found")

    update_data = operator.model_dump(exclude_unset=True)

    if "operator_code" in update_data:
        existing = (
            db.query(Operator)
            .filter(
                Operator.operator_code == update_data["operator_code"],
                Operator.operator_id != operator_id,
            )
            .first()
        )
        if existing:
            raise HTTPException(
                status_code=400, detail="Operator with this code already exists"
            )

    for key, value in update_data.items():
        setattr(db_operator, key, value)

    db.commit()
    db.refresh(db_operator)
    return db_operator


@router.delete("/{operator_id}")
def delete_operator(operator_id: int, db: Session = Depends(get_db)):
    db_operator = db.query(Operator).filter(Operator.operator_id == operator_id).first()
    if db_operator is None:
        raise HTTPException(status_code=404, detail="Operator not found")

    has_dependencies = False

    if db.query(Bus).filter(Bus.operator_id == operator_id).first():
        has_dependencies = True

    if db.query(Route).filter(Route.operator_id == operator_id).first():
        has_dependencies = True

    if db.query(Service).filter(Service.operator_id == operator_id).first():
        has_dependencies = True

    if db.query(Line).filter(Line.operator_id == operator_id).first():
        has_dependencies = True

    if db.query(Block).filter(Block.operator_id == operator_id).first():
        has_dependencies = True

    if (
        db.query(VehicleJourney)
        .filter(VehicleJourney.operator_id == operator_id)
        .first()
    ):
        has_dependencies = True

    if has_dependencies:
        raise HTTPException(
            status_code=400,
            detail="Cannot delete operator with associated buses, routes, services, lines, blocks, or vehicle journeys",
        )

    db.delete(db_operator)
    db.commit()
    return {"message": "Operator deleted successfully"}
