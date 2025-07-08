
from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy.orm import Session
from typing import List

from ..database import get_db
from ..models import RouteDefinition, Route, StopPoint
from ..schemas import (
    RouteDefinitionCreate,
    RouteDefinitionRead,
    RouteDefinitionUpdate,
)

router = APIRouter(
    prefix="/route_definitions",
    tags=["Route Definitions"],
    responses={404: {"description": "Not found"}},
)


@router.post("/", response_model=RouteDefinitionRead, status_code=status.HTTP_201_CREATED)
def create_route_definition(
    definition: RouteDefinitionCreate, db: Session = Depends(get_db)
):
    route = db.query(Route).filter(Route.route_id == definition.route_id).first()
    if not route:
        raise HTTPException(status_code=404, detail=f"Route with ID {definition.route_id} not found")

    stop_point = db.query(StopPoint).filter(StopPoint.atco_code == definition.stop_point_id).first()
    if not stop_point:
        raise HTTPException(status_code=404, detail=f"Stop Point with ATCO Code {definition.stop_point_id} not found")

    existing_definition = (
        db.query(RouteDefinition)
        .filter(
            RouteDefinition.route_id == definition.route_id,
            RouteDefinition.stop_point_id == definition.stop_point_id,
            RouteDefinition.sequence == definition.sequence,
        )
        .first()
    )
    if existing_definition:
        raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail="Route definition with these keys already exists")


    db_definition = RouteDefinition(
        route_id=definition.route_id,
        stop_point_id=definition.stop_point_id, 
        sequence=definition.sequence,
    )
    db.add(db_definition)
    db.commit()
    db.refresh(db_definition)
    return db_definition


@router.get("/", response_model=List[RouteDefinitionRead])
def read_route_definitions(
    route_id: int = None, skip: int = 0, limit: int = 100, db: Session = Depends(get_db)
):
    query = db.query(RouteDefinition)
    if route_id:
        query = query.filter(RouteDefinition.route_id == route_id)
    definitions = query.offset(skip).limit(limit).all()
    
    response_list = []
    for definition in definitions:
        response_list.append({
            "route_id": definition.route_id,
            "stop_point_id": definition.stop_point_id,
            "sequence": definition.sequence
        })
    return response_list


@router.get("/{route_id}/{stop_point_id}/{sequence}", response_model=RouteDefinitionRead)
def read_single_route_definition(
    route_id: int, stop_point_id: int, sequence: int, db: Session = Depends(get_db)
):
    db_definition = (
        db.query(RouteDefinition)
        .filter(
            RouteDefinition.route_id == route_id,
            RouteDefinition.stop_point_id == stop_point_id,
            RouteDefinition.sequence == sequence,
        )
        .first()
    )
    if db_definition is None:
        raise HTTPException(status_code=404, detail="Route definition not found")
    return db_definition


@router.put("/{route_id}/{stop_point_id}/{sequence}", response_model=RouteDefinitionRead)
def update_route_definition(
    route_id: int,
    stop_point_id: int,
    sequence: int,
    definition_update: RouteDefinitionUpdate,
    db: Session = Depends(get_db),
):
    db_definition = (
        db.query(RouteDefinition)
        .filter(
            RouteDefinition.route_id == route_id,
            RouteDefinition.stop_point_id == stop_point_id,
            RouteDefinition.sequence == sequence,
        )
        .first()
    )

    if db_definition is None:
        raise HTTPException(status_code=404, detail="Route definition not found")

    if definition_update.stop_point_id is not None:
        new_stop_point = db.query(StopPoint).filter(StopPoint.atco_code == definition_update.stop_point_id).first()
        if not new_stop_point:
            raise HTTPException(status_code=404, detail=f"New Stop Point with ATCO Code {definition_update.stop_point_id} not found")
        db_definition.stop_point_id = definition_update.stop_point_id
    
    if definition_update.sequence is not None:
        db_definition.sequence = definition_update.sequence

    db.commit()
    db.refresh(db_definition)
    return db_definition


@router.delete("/{route_id}/{stop_point_id}/{sequence}", status_code=status.HTTP_204_NO_CONTENT)
def delete_route_definition(
    route_id: int, stop_point_id: int, sequence: int, db: Session = Depends(get_db)
):
    db_definition = (
        db.query(RouteDefinition)
        .filter(
            RouteDefinition.route_id == route_id,
            RouteDefinition.stop_point_id == stop_point_id,
            RouteDefinition.sequence == sequence,
        )
        .first()
    )

    if db_definition is None:
        raise HTTPException(status_code=404, detail="Route definition not found")

    db.delete(db_definition)
    db.commit()
    return {"message": "Route definition deleted successfully"}