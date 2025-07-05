from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session

from api import schemas
from api.database import get_db
from api.models import Line

router = APIRouter(prefix="/api/line", tags=["Line"])


@router.post("", response_model=schemas.LineRead)
def create_line(obj_in: schemas.LineCreate, db: Session = Depends(get_db)):
    obj = Line(**obj_in.model_dump())
    db.add(obj)
    db.commit()
    db.refresh(obj)
    return obj


@router.get("/{line_id}", response_model=schemas.LineRead)
def read_line(line_id: int, db: Session = Depends(get_db)):
    obj = db.query(Line).filter_by(line_id=line_id).first()
    if not obj:
        raise HTTPException(status_code=404, detail="Line not found")
    return obj


@router.put("/{line_id}", response_model=schemas.LineRead)
def update_line(
    line_id: int, update: schemas.LineUpdate, db: Session = Depends(get_db)
):
    obj = db.query(Line).filter_by(line_id=line_id).first()
    if not obj:
        raise HTTPException(status_code=404, detail="Line not found")
    for key, value in update.model_dump(exclude_unset=True).items():
        setattr(obj, key, value)
    db.commit()
    db.refresh(obj)
    return obj


@router.delete("/{line_id}", response_model=schemas.LineRead)
def delete_line(line_id: int, db: Session = Depends(get_db)):
    obj = db.query(Line).filter_by(line_id=line_id).first()
    if obj:
        db.delete(obj)
        db.commit()
    return obj
