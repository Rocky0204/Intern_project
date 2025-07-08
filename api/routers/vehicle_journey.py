from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy.orm import Session
from sqlalchemy.exc import IntegrityError
from typing import List

from ..database import get_db
from ..models import (
    VehicleJourney,
    JourneyPattern,
    Block,
    Operator,
    Line,
    Service,
)
from ..schemas import VehicleJourneyCreate, VehicleJourneyRead, VehicleJourneyUpdate

router = APIRouter(prefix="/vehicle_journeys", tags=["vehicle_journeys"])


@router.post(
    "/", response_model=VehicleJourneyRead, status_code=status.HTTP_201_CREATED
)
def create_vehicle_journey(vj: VehicleJourneyCreate, db: Session = Depends(get_db)):
    journey_pattern = (
        db.query(JourneyPattern).filter(JourneyPattern.jp_id == vj.jp_id).first()
    )
    if not journey_pattern:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"JourneyPattern with ID {vj.jp_id} not found.",
        )

    block = db.query(Block).filter(Block.block_id == vj.block_id).first()
    if not block:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Block with ID {vj.block_id} not found.",
        )

    operator = db.query(Operator).filter(Operator.operator_id == vj.operator_id).first()
    if not operator:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Operator with ID {vj.operator_id} not found.",
        )

    line = db.query(Line).filter(Line.line_id == vj.line_id).first()
    if not line:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Line with ID {vj.line_id} not found.",
        )

    service = db.query(Service).filter(Service.service_id == vj.service_id).first()
    if not service:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Service with ID {vj.service_id} not found.",
        )

    db_vj = VehicleJourney(**vj.model_dump())
    try:
        db.add(db_vj)
        db.commit()
        db.refresh(db_vj)
        return db_vj
    except IntegrityError:
        db.rollback()
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Could not create vehicle journey due to a database integrity issue.",
        )


@router.get("/", response_model=List[VehicleJourneyRead])
def read_vehicle_journeys(
    skip: int = 0, limit: int = 100, db: Session = Depends(get_db)
):
    vehicle_journeys = db.query(VehicleJourney).offset(skip).limit(limit).all()
    return vehicle_journeys


@router.get("/{vj_id}", response_model=VehicleJourneyRead)
def read_vehicle_journey(vj_id: int, db: Session = Depends(get_db)):
    db_vj = db.query(VehicleJourney).filter(VehicleJourney.vj_id == vj_id).first()
    if db_vj is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND, detail="Vehicle journey not found"
        )
    return db_vj


@router.put("/{vj_id}", response_model=VehicleJourneyRead)
def update_vehicle_journey(
    vj_id: int, vj: VehicleJourneyUpdate, db: Session = Depends(get_db)
):
    db_vj = db.query(VehicleJourney).filter(VehicleJourney.vj_id == vj_id).first()
    if db_vj is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND, detail="Vehicle journey not found"
        )

    update_data = vj.model_dump(exclude_unset=True)

    if "jp_id" in update_data:
        journey_pattern = (
            db.query(JourneyPattern)
            .filter(JourneyPattern.jp_id == update_data["jp_id"])
            .first()
        )
        if not journey_pattern:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=f"JourneyPattern with ID {update_data['jp_id']} not found.",
            )

    if "block_id" in update_data:
        block = (
            db.query(Block).filter(Block.block_id == update_data["block_id"]).first()
        )
        if not block:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=f"Block with ID {update_data['block_id']} not found.",
            )

    if "operator_id" in update_data:
        operator = (
            db.query(Operator)
            .filter(Operator.operator_id == update_data["operator_id"])
            .first()
        )
        if not operator:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=f"Operator with ID {update_data['operator_id']} not found.",
            )

    if "line_id" in update_data:
        line = db.query(Line).filter(Line.line_id == update_data["line_id"]).first()
        if not line:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=f"Line with ID {update_data['line_id']} not found.",
            )

    if "service_id" in update_data:
        service = (
            db.query(Service)
            .filter(Service.service_id == update_data["service_id"])
            .first()
        )
        if not service:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=f"Service with ID {update_data['service_id']} not found.",
            )

    for field, value in update_data.items():
        setattr(db_vj, field, value)

    try:
        db.commit()
        db.refresh(db_vj)
        return db_vj
    except IntegrityError:
        db.rollback()
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Could not update vehicle journey due to a database integrity issue.",
        )


@router.delete("/{vj_id}", status_code=status.HTTP_204_NO_CONTENT)
def delete_vehicle_journey(vj_id: int, db: Session = Depends(get_db)):
    db_vj = db.query(VehicleJourney).filter(VehicleJourney.vj_id == vj_id).first()
    if db_vj is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND, detail="Vehicle journey not found"
        )

    try:
        db.delete(db_vj)
        db.commit()
        return {"message": "Vehicle journey deleted successfully"}
    except IntegrityError:
        db.rollback()
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Cannot delete vehicle journey due to existing dependencies (e.g., associated stop activities).",
        )
