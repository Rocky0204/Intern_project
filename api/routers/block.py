# api/routers/block.py
from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy.orm import Session
from sqlalchemy.exc import IntegrityError
from typing import List

from ..database import get_db
from ..models import Block, Operator, BusType  # Import Block model and its dependencies
from ..schemas import BlockCreate, BlockRead, BlockUpdate

router = APIRouter(prefix="/blocks", tags=["blocks"])


@router.post("/", response_model=BlockRead, status_code=status.HTTP_201_CREATED)
def create_block(block: BlockCreate, db: Session = Depends(get_db)):
    """
    Create a new block.
    Requires existing operator_id and bus_type_id.
    """
    # Check if operator_id exists
    operator = (
        db.query(Operator).filter(Operator.operator_id == block.operator_id).first()
    )
    if not operator:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Operator with ID {block.operator_id} not found.",
        )

    # Check if bus_type_id exists
    bus_type = db.query(BusType).filter(BusType.type_id == block.bus_type_id).first()
    if not bus_type:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"BusType with ID {block.bus_type_id} not found.",
        )

    db_block = Block(**block.model_dump())
    try:
        db.add(db_block)
        db.commit()
        db.refresh(db_block)
        return db_block
    except IntegrityError:
        db.rollback()
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Could not create block due to a database integrity issue (e.g., duplicate name).",
        )


@router.get("/", response_model=List[BlockRead])
def read_blocks(skip: int = 0, limit: int = 100, db: Session = Depends(get_db)):
    """
    Retrieve a list of blocks.
    """
    blocks = db.query(Block).offset(skip).limit(limit).all()
    return blocks


@router.get("/{block_id}", response_model=BlockRead)
def read_block(block_id: int, db: Session = Depends(get_db)):
    """
    Retrieve a specific block by ID.
    """
    db_block = db.query(Block).filter(Block.block_id == block_id).first()
    if db_block is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND, detail="Block not found"
        )
    return db_block


@router.put("/{block_id}", response_model=BlockRead)
def update_block(block_id: int, block: BlockUpdate, db: Session = Depends(get_db)):
    """
    Update an existing block.
    """
    db_block = db.query(Block).filter(Block.block_id == block_id).first()
    if db_block is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND, detail="Block not found"
        )

    update_data = block.model_dump(exclude_unset=True)

    # Check for existence of foreign key dependencies if they are being updated
    if "operator_id" in update_data:
        operator = (
            db.query(Operator)
            .filter(Operator.operator_id == update_data["operator_id"])
            .first()
        )
        if not operator:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=f"Operator with ID {update_data['operator_id']} not found.",
            )

    if "bus_type_id" in update_data:
        bus_type = (
            db.query(BusType)
            .filter(BusType.type_id == update_data["bus_type_id"])
            .first()
        )
        if not bus_type:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=f"BusType with ID {update_data['bus_type_id']} not found.",
            )

    for field, value in update_data.items():
        setattr(db_block, field, value)

    try:
        db.commit()
        db.refresh(db_block)
        return db_block
    except IntegrityError:
        db.rollback()
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Could not update block due to a database integrity issue (e.g., duplicate name).",
        )


@router.delete("/{block_id}", status_code=status.HTTP_204_NO_CONTENT)
def delete_block(block_id: int, db: Session = Depends(get_db)):
    """
    Delete a block.
    """
    db_block = db.query(Block).filter(Block.block_id == block_id).first()
    if db_block is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND, detail="Block not found"
        )

    try:
        db.delete(db_block)
        db.commit()
        return {"message": "Block deleted successfully"}
    except IntegrityError:
        db.rollback()
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Cannot delete block due to existing dependencies (e.g., associated vehicle journeys).",
        )
