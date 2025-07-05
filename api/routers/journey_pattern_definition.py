from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session

from api import schemas
from api.database import get_db
from api.models import JourneyPatternDefinition

router = APIRouter(
    prefix="/api/journeypattern_definition", tags=["JourneyPatternDefinition"]
)


@router.post("", response_model=schemas.JourneyPatternDefinitionRead)
def create_journeypattern_definition(
    obj_in: schemas.JourneyPatternDefinitionCreate, db: Session = Depends(get_db)
):
    obj = JourneyPatternDefinition(**obj_in.model_dump())
    db.add(obj)
    db.commit()
    db.refresh(obj)
    return obj


@router.get("/{sequence}/{jp_id}", response_model=schemas.JourneyPatternDefinitionRead)
def read_journeypattern_definition(
    sequence: int, jp_id: int, db: Session = Depends(get_db)
):
    obj = (
        db.query(JourneyPatternDefinition)
        .filter_by(sequence=sequence, jp_id=jp_id)
        .first()
    )
    if not obj:
        raise HTTPException(
            status_code=404, detail="JourneyPatternDefinition not found"
        )
    return obj


@router.put("/{sequence}/{jp_id}", response_model=schemas.JourneyPatternDefinitionRead)
def update_journeypattern_definition(
    sequence: int,
    jp_id: int,
    update: schemas.JourneyPatternDefinitionUpdate,
    db: Session = Depends(get_db),
):
    obj = (
        db.query(JourneyPatternDefinition)
        .filter_by(sequence=sequence, jp_id=jp_id)
        .first()
    )
    if not obj:
        raise HTTPException(
            status_code=404, detail="JourneyPatternDefinition not found"
        )
    for key, value in update.model_dump(exclude_unset=True).items():
        setattr(obj, key, value)
    db.commit()
    db.refresh(obj)
    return obj


@router.delete(
    "/{sequence}/{jp_id}", response_model=schemas.JourneyPatternDefinitionRead
)
def delete_journeypattern_definition(
    sequence: int, jp_id: int, db: Session = Depends(get_db)
):
    obj = (
        db.query(JourneyPatternDefinition)
        .filter_by(sequence=sequence, jp_id=jp_id)
        .first()
    )
    if obj:
        db.delete(obj)
        db.commit()
    return obj
