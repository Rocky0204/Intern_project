from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session
from typing import List

from ..database import get_db
from ..models import RouteDefinition # Assuming RouteDefinition is the correct model
from ..schemas import (
    RouteDefinitionCreate,
    RouteDefinitionRead,
    RouteDefinitionUpdate
)

router = APIRouter(
    prefix="/route_definitions",
    tags=["route_definitions"],
    responses={404: {"description": "Not found"}},
)

@router.post("/", response_model=RouteDefinitionRead)
def create_route_definition(
    definition: RouteDefinitionCreate,
    db: Session = Depends(get_db)
):
    # Manually map schema fields to model fields for creation
    # The schema uses 'stop_point_atco_code', but the model uses 'stop_point_id'
    db_definition = RouteDefinition(
        route_id=definition.route_id,
        stop_point_id=definition.stop_point_atco_code, # Correctly map to model's 'stop_point_id'
        sequence=definition.sequence
    )
    db.add(db_definition)
    db.commit()
    db.refresh(db_definition)

    # Return a dictionary that matches the response_model schema
    # The schema expects 'stop_point_atco_code', so map model's 'stop_point_id' back
    return {
        "route_id": db_definition.route_id,
        "stop_point_atco_code": db_definition.stop_point_id, # Map model's 'stop_point_id' to schema's 'stop_point_atco_code'
        "sequence": db_definition.sequence
    }

@router.get("/", response_model=List[RouteDefinitionRead])
def read_route_definitions(
    route_id: int = None,
    skip: int = 0,
    limit: int = 100,
    db: Session = Depends(get_db)
):
    query = db.query(RouteDefinition)
    if route_id:
        query = query.filter(RouteDefinition.route_id == route_id)
    db_definitions = query.offset(skip).limit(limit).all()

    # Manually map each SQLAlchemy object to a dictionary matching the schema
    # This is necessary because of the 'stop_point_id' vs 'stop_point_atco_code' mismatch
    response_list = []
    for db_def in db_definitions:
        response_list.append({
            "route_id": db_def.route_id,
            "stop_point_atco_code": db_def.stop_point_id, # Map model's 'stop_point_id' to schema's 'stop_point_atco_code'
            "sequence": db_def.sequence
        })
    return response_list

@router.put("/{route_id}/{sequence}", response_model=RouteDefinitionRead)
def update_route_definition(
    route_id: int,
    sequence: int,
    definition: RouteDefinitionUpdate,
    db: Session = Depends(get_db)
):
    db_definition = db.query(RouteDefinition).filter(
        RouteDefinition.route_id == route_id,
        RouteDefinition.sequence == sequence
    ).first()

    if db_definition is None:
        raise HTTPException(status_code=404, detail="Route definition not found")

    # Update model fields, mapping from schema fields
    if definition.stop_point_atco_code is not None:
        db_definition.stop_point_id = definition.stop_point_atco_code # Map schema's 'stop_point_atco_code' to model's 'stop_point_id'
    if definition.sequence is not None:
        db_definition.sequence = definition.sequence

    db.commit()
    db.refresh(db_definition)

    # Return a dictionary that matches the response_model schema
    return {
        "route_id": db_definition.route_id,
        "stop_point_atco_code": db_definition.stop_point_id, # Map model's 'stop_point_id' to schema's 'stop_point_atco_code'
        "sequence": db_definition.sequence
    }

@router.delete("/{route_id}/{sequence}")
def delete_route_definition(
    route_id: int,
    sequence: int,
    db: Session = Depends(get_db)
):
    db_definition = db.query(RouteDefinition).filter(
        RouteDefinition.route_id == route_id,
        RouteDefinition.sequence == sequence
    ).first()

    if db_definition is None:
        raise HTTPException(status_code=404, detail="Route definition not found")

    db.delete(db_definition)
    db.commit()
    return {"message": "Route definition deleted successfully"}
