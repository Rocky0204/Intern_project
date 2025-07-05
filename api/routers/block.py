from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session

from api import schemas
from api.database import get_db
from api.models import Block

router = APIRouter(prefix="/api/block", tags=["Block"])


@router.post("", response_model=schemas.BlockRead)
def create_block(obj_in: schemas.BlockCreate, db: Session = Depends(get_db)):
    obj = Block(**obj_in.model_dump())
    db.add(obj)
    db.commit()
    db.refresh(obj)
    return obj


@router.get("/{block_id}", response_model=schemas.BlockRead)
def read_block(block_id: int, db: Session = Depends(get_db)):
    obj = db.query(Block).filter_by(block_id=block_id).first()
    if not obj:
        raise HTTPException(status_code=404, detail="Block not found")
    return obj


@router.put("/{block_id}", response_model=schemas.BlockRead)
def update_block(
    block_id: int, update: schemas.BlockUpdate, db: Session = Depends(get_db)
):
    obj = db.query(Block).filter_by(block_id=block_id).first()
    if not obj:
        raise HTTPException(status_code=404, detail="Block not found")
    for key, value in update.model_dump(exclude_unset=True).items():
        setattr(obj, key, value)
    db.commit()
    db.refresh(obj)
    return obj


@router.delete("/{block_id}", response_model=schemas.BlockRead)
def delete_block(block_id: int, db: Session = Depends(get_db)):
    obj = db.query(Block).filter_by(block_id=block_id).first()
    if obj:
        db.delete(obj)
        db.commit()
    return obj
