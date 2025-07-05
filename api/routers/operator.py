from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session

from api import schemas
from api.database import get_db
from api.models import Operator

router = APIRouter(prefix="/api/operator", tags=["Operator"])


@router.post("", response_model=schemas.OperatorRead)
def create_operator(obj_in: schemas.OperatorCreate, db: Session = Depends(get_db)):
    obj = Operator(**obj_in.model_dump())
    db.add(obj)
    db.commit()
    db.refresh(obj)
    return obj


@router.get("/{operator_id}", response_model=schemas.OperatorRead)
def read_operator(operator_id: int, db: Session = Depends(get_db)):
    obj = db.query(Operator).filter_by(operator_id=operator_id).first()
    if not obj:
        raise HTTPException(status_code=404, detail="Operator not found")
    return obj


@router.put("/{operator_id}", response_model=schemas.OperatorRead)
def update_operator(
    operator_id: int, update: schemas.OperatorUpdate, db: Session = Depends(get_db)
):
    obj = db.query(Operator).filter_by(operator_id=operator_id).first()
    if not obj:
        raise HTTPException(status_code=404, detail="Operator not found")
    for key, value in update.model_dump(exclude_unset=True).items():
        setattr(obj, key, value)
    db.commit()
    db.refresh(obj)
    return obj


@router.delete("/{operator_id}", response_model=schemas.OperatorRead)
def delete_operator(operator_id: int, db: Session = Depends(get_db)):
    obj = db.query(Operator).filter_by(operator_id=operator_id).first()
    if obj:
        db.delete(obj)
        db.commit()
    return obj
