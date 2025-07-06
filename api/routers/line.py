# api/routers/line.py
from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy.orm import Session
from sqlalchemy.exc import IntegrityError
from typing import List

from ..database import get_db
from ..models import Line, Operator # Import Line model and its dependency Operator
from ..schemas import LineCreate, LineRead, LineUpdate

router = APIRouter(
    prefix="/lines",
    tags=["lines"]
)

@router.post("/", response_model=LineRead, status_code=status.HTTP_201_CREATED)
def create_line(line: LineCreate, db: Session = Depends(get_db)):
    """
    Create a new line.
    Requires an existing operator_id.
    """
    # Check if operator_id exists
    operator = db.query(Operator).filter(Operator.operator_id == line.operator_id).first()
    if not operator:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Operator with ID {line.operator_id} not found."
        )

    db_line = Line(**line.model_dump())
    try:
        db.add(db_line)
        db.commit()
        db.refresh(db_line)
        return db_line
    except IntegrityError:
        db.rollback()
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Could not create line due to a database integrity issue (e.g., duplicate line_name)."
        )

@router.get("/", response_model=List[LineRead])
def read_lines(skip: int = 0, limit: int = 100, db: Session = Depends(get_db)):
    """
    Retrieve a list of lines.
    """
    lines = db.query(Line).offset(skip).limit(limit).all()
    return lines

@router.get("/{line_id}", response_model=LineRead)
def read_line(line_id: int, db: Session = Depends(get_db)):
    """
    Retrieve a specific line by ID.
    """
    db_line = db.query(Line).filter(Line.line_id == line_id).first()
    if db_line is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Line not found")
    return db_line

@router.put("/{line_id}", response_model=LineRead)
def update_line(
    line_id: int, line: LineUpdate, db: Session = Depends(get_db)
):
    """
    Update an existing line.
    """
    db_line = db.query(Line).filter(Line.line_id == line_id).first()
    if db_line is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Line not found")
    
    update_data = line.model_dump(exclude_unset=True)

    # Check for existence of foreign key dependencies if they are being updated
    if "operator_id" in update_data:
        operator = db.query(Operator).filter(Operator.operator_id == update_data["operator_id"]).first()
        if not operator:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=f"Operator with ID {update_data['operator_id']} not found."
            )

    for field, value in update_data.items():
        setattr(db_line, field, value)
    
    try:
        db.commit()
        db.refresh(db_line)
        return db_line
    except IntegrityError:
        db.rollback()
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Could not update line due to a database integrity issue (e.g., duplicate line_name)."
        )

@router.delete("/{line_id}", status_code=status.HTTP_204_NO_CONTENT)
def delete_line(line_id: int, db: Session = Depends(get_db)):
    """
    Delete a line.
    """
    db_line = db.query(Line).filter(Line.line_id == line_id).first()
    if db_line is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Line not found")
    
    try:
        db.delete(db_line)
        db.commit()
        return {"message": "Line deleted successfully"}
    except IntegrityError:
        db.rollback()
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Cannot delete line due to existing dependencies (e.g., associated services, journey patterns, or vehicle journeys)."
        )
