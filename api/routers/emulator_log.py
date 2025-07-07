# api/routers/emulator_log.py

from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy.orm import Session
from typing import List, Optional
from datetime import datetime
import json

from api.database import get_db
from api.models import EmulatorLog
from api.schemas import EmulatorLogCreate, EmulatorLogRead, EmulatorLogUpdate, RunStatus, OptimizationDetailsRead


router = APIRouter(
    prefix="/emulator_logs",
    tags=["Emulator Logs"],
)

# Helper function to create EmulatorLogRead instance from a database object
def _create_emulator_log_read(db_log: EmulatorLog) -> EmulatorLogRead:
    """
    Constructs an EmulatorLogRead Pydantic model from an EmulatorLog SQLAlchemy object.
    Explicitly uses the optimization_details_dict hybrid property for the
    optimization_details field to ensure correct serialization.
    """
    return EmulatorLogRead(
        run_id=db_log.run_id,
        status=db_log.status,
        started_at=db_log.started_at,
        last_updated=db_log.last_updated,
        # IMPORTANT: Pass the dictionary from the hybrid property
        # Pydantic will then validate this dictionary against OptimizationDetailsRead schema
        optimization_details=db_log.optimization_details_dict 
    )

@router.post("/", response_model=EmulatorLogRead, status_code=status.HTTP_201_CREATED)
def create_emulator_log(log: EmulatorLogCreate, db: Session = Depends(get_db)):
    db_log = EmulatorLog(
        status=log.status,
        started_at=datetime.now(),
        last_updated=datetime.now(),
    )
    db.add(db_log)
    db.commit()
    db.refresh(db_log)
    
    # Use the helper function to return the correctly serialized Pydantic model
    return _create_emulator_log_read(db_log)


@router.get("/", response_model=List[EmulatorLogRead])
def read_emulator_logs(skip: int = 0, limit: int = 100, db: Session = Depends(get_db)):
    logs = db.query(EmulatorLog).offset(skip).limit(limit).all()
    
    # Map each SQLAlchemy model instance to EmulatorLogRead using the helper
    return [_create_emulator_log_read(db_log) for db_log in logs]


@router.get("/{run_id}", response_model=EmulatorLogRead)
def read_emulator_log(run_id: int, db: Session = Depends(get_db)):
    db_log = db.query(EmulatorLog).filter(EmulatorLog.run_id == run_id).first()
    if db_log is None:
        raise HTTPException(status_code=404, detail="Emulator log not found")
    
    # Use the helper function to return the correctly serialized Pydantic model
    return _create_emulator_log_read(db_log)

@router.put("/{run_id}", response_model=EmulatorLogRead)
def update_emulator_log(run_id: int, log_update: EmulatorLogUpdate, db: Session = Depends(get_db)):
    db_log = db.query(EmulatorLog).filter(EmulatorLog.run_id == run_id).first()
    if db_log is None:
        raise HTTPException(status_code=404, detail="Emulator log not found")

    if log_update.status is not None:
        db_log.status = log_update.status
    
    # This block ensures 'optimization_details' is handled correctly if provided in the update payload.
    if "optimization_details" in log_update.model_dump(exclude_unset=False):
        if log_update.optimization_details is not None:
            # Convert Pydantic model (OptimizationDetailsRead) to dictionary
            # before assigning to the hybrid property setter
            db_log.optimization_details_dict = log_update.optimization_details.model_dump()
        else:
            db_log.optimization_details_dict = None # Handles explicit setting to None
    
    db_log.last_updated = datetime.now()

    db.commit()
    db.refresh(db_log)
    
    # Use the helper function to return the correctly serialized Pydantic model
    return _create_emulator_log_read(db_log)

@router.delete("/{run_id}")
def delete_emulator_log(run_id: int, db: Session = Depends(get_db)):
    db_log = db.query(EmulatorLog).filter(EmulatorLog.run_id == run_id).first()
    if db_log is None:
        raise HTTPException(status_code=404, detail="Emulator log not found")
    db.delete(db_log)
    db.commit()
    # This endpoint returns a simple message, so no Pydantic model serialization needed here
    return {"message": "Emulator log deleted successfully"}