# api/routers/services.py
from fastapi import APIRouter, Depends, HTTPException, status # Import status
from sqlalchemy.orm import Session
from typing import List

from ..database import get_db
from ..models import Service, Operator, Line # Import Operator and Line
from ..schemas import ServiceCreate, ServiceRead, ServiceUpdate

router = APIRouter(
    prefix="/services",
    tags=["services"]
)

@router.post("/", response_model=ServiceRead)
def create_service(service: ServiceCreate, db: Session = Depends(get_db)):
    """
    Create a new service.
    """
    # Check if operator exists
    operator = db.query(Operator).filter(Operator.operator_id == service.operator_id).first()
    if not operator:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Operator with ID {service.operator_id} not found."
        )

    # Check if line exists
    line = db.query(Line).filter(Line.line_id == service.line_id).first()
    if not line:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Line with ID {service.line_id} not found."
        )

    db_service = Service(**service.model_dump())
    db.add(db_service)
    db.commit()
    db.refresh(db_service)
    return db_service

@router.get("/", response_model=List[ServiceRead])
def read_services(skip: int = 0, limit: int = 100, db: Session = Depends(get_db)):
    """
    Retrieve a list of services.
    """
    services = db.query(Service).offset(skip).limit(limit).all()
    return services

@router.get("/{service_id}", response_model=ServiceRead)
def read_service(service_id: int, db: Session = Depends(get_db)):
    """
    Retrieve a specific service by ID.
    """
    db_service = db.query(Service).filter(Service.service_id == service_id).first()
    if db_service is None:
        raise HTTPException(status_code=404, detail="Service not found")
    return db_service

@router.put("/{service_id}", response_model=ServiceRead)
def update_service(
    service_id: int, service: ServiceUpdate, db: Session = Depends(get_db)
):
    """
    Update a service.
    """
    db_service = db.query(Service).filter(Service.service_id == service_id).first()
    if db_service is None:
        raise HTTPException(status_code=404, detail="Service not found")
    
    update_data = service.model_dump(exclude_unset=True)
    for field, value in update_data.items():
        setattr(db_service, field, value)
    
    db.commit()
    db.refresh(db_service)
    return db_service

@router.delete("/{service_id}", response_model=dict)
def delete_service(service_id: int, db: Session = Depends(get_db)):
    """
    Delete a service.
    """
    db_service = db.query(Service).filter(Service.service_id == service_id).first()
    if db_service is None:
        raise HTTPException(status_code=404, detail="Service not found")
    
    db.delete(db_service)
    db.commit()
    return {"message": "Service deleted successfully"}