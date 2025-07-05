from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session

from api import schemas
from api.database import get_db
from api.models import Route

router = APIRouter(prefix="/api/route", tags=["Route"])


@router.post("", response_model=schemas.RouteRead)
def create_route(obj_in: schemas.RouteCreate, db: Session = Depends(get_db)):
    obj = Route(**obj_in.model_dump())
    db.add(obj)
    db.commit()
    db.refresh(obj)
    return obj


@router.get("/{route_id}", response_model=schemas.RouteRead)
def read_route(route_id: int, db: Session = Depends(get_db)):
    obj = db.query(Route).filter_by(route_id=route_id).first()
    if not obj:
        raise HTTPException(status_code=404, detail="Route not found")
    return obj


@router.put("/{route_id}", response_model=schemas.RouteRead)
def update_route(
    route_id: int, update: schemas.RouteUpdate, db: Session = Depends(get_db)
):
    obj = db.query(Route).filter_by(route_id=route_id).first()
    if not obj:
        raise HTTPException(status_code=404, detail="Route not found")
    for key, value in update.model_dump(exclude_unset=True).items():
        setattr(obj, key, value)
    db.commit()
    db.refresh(obj)
    return obj


@router.delete("/{route_id}", response_model=schemas.RouteRead)
def delete_route(route_id: int, db: Session = Depends(get_db)):
    obj = db.query(Route).filter_by(route_id=route_id).first()
    if obj:
        db.delete(obj)
        db.commit()
    return obj
