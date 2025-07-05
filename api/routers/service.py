from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session

from api import schemas
from api.database import get_db
from api.models import Service

router = APIRouter(prefix="/api/service", tags=["Service"])


@router.post("", response_model=schemas.ServiceRead)
def create_service(obj_in: schemas.ServiceCreate, db: Session = Depends(get_db)):
    obj = Service(**obj_in.model_dump())
    db.add(obj)
    db.commit()
    db.refresh(obj)
    return obj


@router.get("/{service_id}", response_model=schemas.ServiceRead)
def read_service(service_id: int, db: Session = Depends(get_db)):
    obj = db.query(Service).filter_by(service_id=service_id).first()
    if not obj:
        raise HTTPException(status_code=404, detail="Service not found")
    return obj


@router.put("/{service_id}", response_model=schemas.ServiceRead)
def update_service(
    service_id: int, update: schemas.ServiceUpdate, db: Session = Depends(get_db)
):
    obj = db.query(Service).filter_by(service_id=service_id).first()
    if not obj:
        raise HTTPException(status_code=404, detail="Service not found")
    for key, value in update.model_dump(exclude_unset=True).items():
        setattr(obj, key, value)
    db.commit()
    db.refresh(obj)
    return obj


@router.delete("/{service_id}", response_model=schemas.ServiceRead)
def delete_service(service_id: int, db: Session = Depends(get_db)):
    obj = db.query(Service).filter_by(service_id=service_id).first()
    if obj:
        db.delete(obj)
        db.commit()
    return obj
