# api/routers/emulator_log.py
from datetime import datetime
from typing import List

from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session

from ..database import get_db
from ..models import EmulatorLog
from ..schemas import EmulatorLogCreate, EmulatorLogRead, EmulatorLogUpdate

router = APIRouter(
    prefix="/emulator_logs",
    tags=["emulator_logs"],
    responses={404: {"description": "Not found"}},
)


@router.post("/", response_model=EmulatorLogRead)
def create_emulator_log(
    emulator_log: EmulatorLogCreate, db: Session = Depends(get_db)
) -> EmulatorLogRead:
    """
    Create a new emulator log entry.
    """
    db_emulator_log = EmulatorLog(
        status=emulator_log.status.value,
        started_at=datetime.now(),
        last_updated=datetime.now(),
    )
    db.add(db_emulator_log)
    db.commit()
    db.refresh(db_emulator_log)
    return db_emulator_log


@router.get("/", response_model=List[EmulatorLogRead])
def read_emulator_logs(
    skip: int = 0, limit: int = 100, db: Session = Depends(get_db)
) -> List[EmulatorLogRead]:
    """
    Retrieve a list of emulator logs with pagination.
    """
    emulator_logs = db.query(EmulatorLog).offset(skip).limit(limit).all()
    return emulator_logs


@router.get("/{run_id}", response_model=EmulatorLogRead)
def read_emulator_log(run_id: int, db: Session = Depends(get_db)) -> EmulatorLogRead:
    """
    Retrieve a specific emulator log by run_id.
    """
    db_emulator_log = db.query(EmulatorLog).filter(EmulatorLog.run_id == run_id).first()
    if db_emulator_log is None:
        raise HTTPException(status_code=404, detail="Emulator log not found")
    return db_emulator_log


@router.put("/{run_id}", response_model=EmulatorLogRead)
def update_emulator_log(
    run_id: int, emulator_log: EmulatorLogUpdate, db: Session = Depends(get_db)
) -> EmulatorLogRead:
    """
    Update an existing emulator log.
    """
    db_emulator_log = db.query(EmulatorLog).filter(EmulatorLog.run_id == run_id).first()
    if db_emulator_log is None:
        raise HTTPException(status_code=404, detail="Emulator log not found")

    if emulator_log.status is not None:
        db_emulator_log.status = emulator_log.status.value
    db_emulator_log.last_updated = datetime.now()

    db.commit()
    db.refresh(db_emulator_log)
    return db_emulator_log


@router.delete("/{run_id}", response_model=dict)
def delete_emulator_log(run_id: int, db: Session = Depends(get_db)) -> dict:
    """
    Delete an emulator log by run_id.
    """
    db_emulator_log = db.query(EmulatorLog).filter(EmulatorLog.run_id == run_id).first()
    if db_emulator_log is None:
        raise HTTPException(status_code=404, detail="Emulator log not found")

    db.delete(db_emulator_log)
    db.commit()
    return {"message": "Emulator log deleted successfully"}
