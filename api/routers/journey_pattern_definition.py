from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session
from typing import List
from datetime import time # Ensure time is imported if used in router logic for direct object creation/comparison

from ..database import get_db
from ..models import JourneyPatternDefinition # Assuming JourneyPatternDefinition is the correct model
from ..schemas import (
    JourneyPatternDefinitionCreate,
    JourneyPatternDefinitionRead,
    JourneyPatternDefinitionUpdate
)

router = APIRouter(
    prefix="/journey_pattern_definitions",
    tags=["journey_pattern_definitions"],
    responses={404: {"description": "Not found"}},
)

@router.post("/", response_model=JourneyPatternDefinitionRead)
def create_journey_pattern_definition(
    definition: JourneyPatternDefinitionCreate,
    db: Session = Depends(get_db)
):
    # Manually map schema fields to model fields for creation
    # The schema uses 'stop_point_atco_code', but the model uses 'stop_point_id'
    db_definition = JourneyPatternDefinition(
        jp_id=definition.jp_id,
        stop_point_id=definition.stop_point_atco_code, # Correctly map to model's 'stop_point_id'
        sequence=definition.sequence,
        arrival_time=definition.arrival_time,
        departure_time=definition.departure_time
    )
    db.add(db_definition)
    db.commit()
    db.refresh(db_definition)

    # Return a dictionary that matches the response_model schema
    # The schema expects 'stop_point_atco_code', so map model's 'stop_point_id' back
    return {
        "jp_id": db_definition.jp_id,
        "stop_point_atco_code": db_definition.stop_point_id, # Map model's 'stop_point_id' to schema's 'stop_point_atco_code'
        "sequence": db_definition.sequence,
        "arrival_time": db_definition.arrival_time.isoformat(), # Ensure time is ISO 8601 string for JSON response
        "departure_time": db_definition.departure_time.isoformat() # Ensure time is ISO 8601 string for JSON response
    }

@router.get("/", response_model=List[JourneyPatternDefinitionRead])
def read_journey_pattern_definitions(
    jp_id: int = None,
    skip: int = 0,
    limit: int = 100,
    db: Session = Depends(get_db)
):
    query = db.query(JourneyPatternDefinition)
    if jp_id:
        query = query.filter(JourneyPatternDefinition.jp_id == jp_id)
    db_definitions = query.offset(skip).limit(limit).all()

    # Manually map each SQLAlchemy object to a dictionary matching the schema
    # This is necessary because of the 'stop_point_id' vs 'stop_point_atco_code' mismatch
    # and to ensure time objects are serialized as strings.
    response_list = []
    for db_def in db_definitions:
        response_list.append({
            "jp_id": db_def.jp_id,
            "stop_point_atco_code": db_def.stop_point_id, # Map model's 'stop_point_id' to schema's 'stop_point_atco_code'
            "sequence": db_def.sequence,
            "arrival_time": db_def.arrival_time.isoformat(),
            "departure_time": db_def.departure_time.isoformat()
        })
    return response_list

@router.put("/{jp_id}/{sequence}", response_model=JourneyPatternDefinitionRead)
def update_journey_pattern_definition(
    jp_id: int,
    sequence: int,
    definition: JourneyPatternDefinitionUpdate,
    db: Session = Depends(get_db)
):
    db_definition = db.query(JourneyPatternDefinition).filter(
        JourneyPatternDefinition.jp_id == jp_id,
        JourneyPatternDefinition.sequence == sequence
    ).first()

    if db_definition is None:
        raise HTTPException(status_code=404, detail="Journey pattern definition not found")

    # Update model fields, mapping from schema fields
    if definition.stop_point_atco_code is not None:
        db_definition.stop_point_id = definition.stop_point_atco_code # Map schema's 'stop_point_atco_code' to model's 'stop_point_id'
    if definition.sequence is not None:
        db_definition.sequence = definition.sequence
    if definition.arrival_time is not None:
        db_definition.arrival_time = definition.arrival_time
    if definition.departure_time is not None:
        db_definition.departure_time = definition.departure_time

    db.commit()
    db.refresh(db_definition)

    # Return a dictionary that matches the response_model schema
    return {
        "jp_id": db_definition.jp_id,
        "stop_point_atco_code": db_definition.stop_point_id, # Map model's 'stop_point_id' to schema's 'stop_point_atco_code'
        "sequence": db_definition.sequence,
        "arrival_time": db_definition.arrival_time.isoformat(),
        "departure_time": db_definition.departure_time.isoformat()
    }

@router.delete("/{jp_id}/{sequence}")
def delete_journey_pattern_definition(
    jp_id: int,
    sequence: int,
    db: Session = Depends(get_db)
):
    db_definition = db.query(JourneyPatternDefinition).filter(
        JourneyPatternDefinition.jp_id == jp_id,
        JourneyPatternDefinition.sequence == sequence
    ).first()

    if db_definition is None:
        raise HTTPException(status_code=404, detail="Journey pattern definition not found")

    db.delete(db_definition)
    db.commit()
    return {"message": "Journey pattern definition deleted successfully"}
