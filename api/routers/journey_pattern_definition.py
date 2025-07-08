from fastapi import APIRouter, Depends, HTTPException, status 
from sqlalchemy.orm import Session
from typing import List
from datetime import time

from api.database import get_db
from api.models import JourneyPatternDefinition
from api.schemas import (
    JourneyPatternDefinitionCreate,
    JourneyPatternDefinitionRead,
    JourneyPatternDefinitionUpdate,
)

router = APIRouter(
    prefix="/journey_pattern_definitions",
    tags=["journey_pattern_definitions"],
    responses={404: {"description": "Not found"}},
)


@router.post("/", response_model=JourneyPatternDefinitionRead, status_code=status.HTTP_201_CREATED)
def create_journey_pattern_definition(
    definition: JourneyPatternDefinitionCreate, db: Session = Depends(get_db)
):
    db_definition = JourneyPatternDefinition(
        jp_id=getattr(definition, 'jp_id'),
        stop_point_id=getattr(definition, 'stop_point_atco_code'), 
        sequence=getattr(definition, 'sequence'),
        arrival_time=getattr(definition, 'arrival_time'),
        departure_time=getattr(definition, 'departure_time'),
    )
    db.add(db_definition)
    db.commit()
    db.refresh(db_definition)

    return {
        "jp_id": db_definition.jp_id,
        "stop_point_atco_code": db_definition.stop_point_id,
        "sequence": db_definition.sequence,
        "arrival_time": db_definition.arrival_time.isoformat(),
        "departure_time": db_definition.departure_time.isoformat(),
    }


@router.get("/", response_model=List[JourneyPatternDefinitionRead])
def read_journey_pattern_definitions(skip: int = 0, limit: int = 100, db: Session = Depends(get_db)):
    definitions = db.query(JourneyPatternDefinition).offset(skip).limit(limit).all()
    
    response_definitions = []
    for db_def in definitions:
        response_definitions.append({
            "jp_id": db_def.jp_id,
            "stop_point_atco_code": db_def.stop_point_id,
            "sequence": db_def.sequence,
            "arrival_time": db_def.arrival_time.isoformat(),
            "departure_time": db_def.departure_time.isoformat(),
        })
    return response_definitions


@router.get("/{jp_id}/{sequence}", response_model=JourneyPatternDefinitionRead)
def read_single_journey_pattern_definition(
    jp_id: int, sequence: int, db: Session = Depends(get_db)
):
    db_definition = (
        db.query(JourneyPatternDefinition)
        .filter(
            JourneyPatternDefinition.jp_id == jp_id,
            JourneyPatternDefinition.sequence == sequence,
        )
        .first()
    )
    if db_definition is None:
        raise HTTPException(status_code=404, detail="Journey pattern definition not found")
    
    return {
        "jp_id": db_definition.jp_id,
        "stop_point_atco_code": db_definition.stop_point_id,
        "sequence": db_definition.sequence,
        "arrival_time": db_definition.arrival_time.isoformat(),
        "departure_time": db_definition.departure_time.isoformat(),
    }


@router.put("/{jp_id}/{sequence}", response_model=JourneyPatternDefinitionRead)
def update_journey_pattern_definition(
    jp_id: int,
    sequence: int,
    definition: JourneyPatternDefinitionUpdate,
    db: Session = Depends(get_db)
):
    db_definition = (
        db.query(JourneyPatternDefinition)
        .filter(
            JourneyPatternDefinition.jp_id == jp_id,
            JourneyPatternDefinition.sequence == sequence,
        )
        .first()
    )
    if db_definition is None:
        raise HTTPException(status_code=404, detail="Journey pattern definition not found")

    stop_point_atco_code_val = getattr(definition, 'stop_point_atco_code', None)
    if stop_point_atco_code_val is not None:
        db_definition.stop_point_id = stop_point_atco_code_val
    
    arrival_time_val = getattr(definition, 'arrival_time', None)
    if arrival_time_val is not None:
        db_definition.arrival_time = arrival_time_val
    
    departure_time_val = getattr(definition, 'departure_time', None)
    if departure_time_val is not None:
        db_definition.departure_time = departure_time_val

    db.commit()
    db.refresh(db_definition)

    return {
        "jp_id": db_definition.jp_id,
        "stop_point_atco_code": db_definition.stop_point_id,
        "sequence": db_definition.sequence,
        "arrival_time": db_definition.arrival_time.isoformat(),
        "departure_time": db_definition.departure_time.isoformat(),
    }


@router.delete("/{jp_id}/{sequence}")
def delete_journey_pattern_definition(
    jp_id: int, sequence: int, db: Session = Depends(get_db)
):
    db_definition = (
        db.query(JourneyPatternDefinition)
        .filter(
            JourneyPatternDefinition.jp_id == jp_id,
            JourneyPatternDefinition.sequence == sequence,
        )
        .first()
    )
    if db_definition is None:
        raise HTTPException(status_code=404, detail="Journey pattern definition not found")
    db.delete(db_definition)
    db.commit()
    return {"message": "Journey pattern definition deleted successfully"}