from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session
from typing import List

from api.database import get_db
from api.models import Route, Operator, RouteDefinition, JourneyPattern
from api.schemas import RouteCreate, RouteRead, RouteUpdate

router = APIRouter(prefix="/routes", tags=["routes"])


@router.post("/", response_model=RouteRead)
def create_route(route: RouteCreate, db: Session = Depends(get_db)):
    db_operator = (
        db.query(Operator).filter(Operator.operator_id == route.operator_id).first()
    )
    if not db_operator:
        raise HTTPException(status_code=400, detail="Operator not found")

    db_route = Route(**route.model_dump())
    db.add(db_route)
    db.commit()
    db.refresh(db_route)
    return db_route


@router.get("/", response_model=List[RouteRead])
def read_routes(skip: int = 0, limit: int = 100, db: Session = Depends(get_db)):
    routes = db.query(Route).offset(skip).limit(limit).all()
    return routes


@router.get("/{route_id}", response_model=RouteRead)
def read_route(route_id: int, db: Session = Depends(get_db)):
    db_route = db.query(Route).filter(Route.route_id == route_id).first()
    if db_route is None:
        raise HTTPException(status_code=404, detail="Route not found")
    return db_route


@router.put("/{route_id}", response_model=RouteRead)
def update_route(route_id: int, route: RouteUpdate, db: Session = Depends(get_db)):
    db_route = db.query(Route).filter(Route.route_id == route_id).first()
    if db_route is None:
        raise HTTPException(status_code=404, detail="Route not found")

    update_data = route.model_dump(exclude_unset=True)

    if "operator_id" in update_data:
        db_operator = (
            db.query(Operator)
            .filter(Operator.operator_id == update_data["operator_id"])
            .first()
        )
        if not db_operator:
            raise HTTPException(status_code=400, detail="Operator not found")

    for key, value in update_data.items():
        setattr(db_route, key, value)

    db.commit()
    db.refresh(db_route)
    return db_route


@router.delete("/{route_id}")
def delete_route(route_id: int, db: Session = Depends(get_db)):
    db_route = db.query(Route).filter(Route.route_id == route_id).first()
    if db_route is None:
        raise HTTPException(status_code=404, detail="Route not found")

    has_definitions = (
        db.query(RouteDefinition).filter(RouteDefinition.route_id == route_id).first()
        is not None
    )
    has_journey_patterns = (
        db.query(JourneyPattern).filter(JourneyPattern.route_id == route_id).first()
        is not None
    )

    if has_definitions or has_journey_patterns:
        raise HTTPException(
            status_code=400,
            detail="Cannot delete route with existing definitions or journey patterns",
        )

    db.delete(db_route)
    db.commit()
    return {"message": "Route deleted successfully"}


@router.get("/{route_id}/definition", summary="Get route definition with stop points")
def get_route_definition(route_id: int, db: Session = Depends(get_db)):
    pass
