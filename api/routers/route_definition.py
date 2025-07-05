from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session

from api import schemas
from api.database import get_db
from api.models import RouteDefinition

router = APIRouter(prefix="/api/route_definition", tags=["RouteDefinition"])


@router.post("", response_model=schemas.RouteDefinitionRead)
def create_route_definition(
    obj_in: schemas.RouteDefinitionCreate, db: Session = Depends(get_db)
):
    obj = RouteDefinition(**obj_in.model_dump())
    db.add(obj)
    db.commit()
    db.refresh(obj)
    return obj


@router.get("/{sequence}/{route_id}", response_model=schemas.RouteDefinitionRead)
def read_route_definition(sequence: int, route_id: int, db: Session = Depends(get_db)):
    obj = (
        db.query(RouteDefinition)
        .filter_by(sequence=sequence, route_id=route_id)
        .first()
    )
    if not obj:
        raise HTTPException(status_code=404, detail="RouteDefinition not found")
    return obj


@router.put("/{sequence}/{route_id}", response_model=schemas.RouteDefinitionRead)
def update_route_definition(
    sequence: int,
    route_id: int,
    update: schemas.RouteDefinitionUpdate,
    db: Session = Depends(get_db),
):
    obj = (
        db.query(RouteDefinition)
        .filter_by(sequence=sequence, route_id=route_id)
        .first()
    )
    if not obj:
        raise HTTPException(status_code=404, detail="RouteDefinition not found")
    for key, value in update.model_dump(exclude_unset=True).items():
        setattr(obj, key, value)
    db.commit()
    db.refresh(obj)
    return obj


@router.delete("/{sequence}/{route_id}", response_model=schemas.RouteDefinitionRead)
def delete_route_definition(
    sequence: int, route_id: int, db: Session = Depends(get_db)
):
    obj = (
        db.query(RouteDefinition)
        .filter_by(sequence=sequence, route_id=route_id)
        .first()
    )
    if obj:
        db.delete(obj)
        db.commit()
    return obj
