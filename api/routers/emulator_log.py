from fastapi import APIRouter, Depends, HTTPException, status, Body
from sqlalchemy.orm import Session
from typing import List, Optional
from datetime import datetime, timezone
import json
import logging
from pydantic import BaseModel

from api.database import get_db
from api.models import EmulatorLog
from api.schemas import EmulatorLogCreate, EmulatorLogRead, EmulatorLogUpdate, RunStatus, OptimizationDetailsRead
from services.bus_simulation import BusEmulator

router = APIRouter(
    prefix="/emulator_logs",
    tags=["Emulator Logs"],
)

class SimulationParams(BaseModel):
    use_optimized_schedule: bool = True
    start_time_minutes: int = 0
    end_time_minutes: int = 1440
    optimization_details: Optional[OptimizationDetailsRead] = None

def _create_emulator_log_read(db_log: EmulatorLog) -> EmulatorLogRead:
    optimization_details_obj = None
    if db_log.optimization_details:
        try:
            optimization_details_data = json.loads(db_log.optimization_details)
            optimization_details_obj = OptimizationDetailsRead(**optimization_details_data)
        except (json.JSONDecodeError, TypeError) as e:
            logging.error(f"Failed to decode optimization_details JSON for run_id {db_log.run_id}: {e}")
            optimization_details_obj = OptimizationDetailsRead(
                status="ERROR",
                message="Failed to parse optimization details"
            )
    
    started_at_utc = db_log.started_at
    if started_at_utc is not None and started_at_utc.tzinfo is None:
        started_at_utc = started_at_utc.astimezone(timezone.utc)

    last_updated_utc = db_log.last_updated
    if last_updated_utc is not None and last_updated_utc.tzinfo is None:
        last_updated_utc = last_updated_utc.astimezone(timezone.utc)

    return EmulatorLogRead(
        run_id=db_log.run_id,
        status=RunStatus(db_log.status),
        started_at=started_at_utc,
        last_updated=last_updated_utc,
        optimization_details=optimization_details_obj
    )

@router.post("/", response_model=EmulatorLogRead, status_code=status.HTTP_201_CREATED)
def create_emulator_log(log: EmulatorLogCreate, db: Session = Depends(get_db)):
    db_log = EmulatorLog(
        status=log.status,
        started_at=datetime.now(),
        last_updated=datetime.now()
    )
    db.add(db_log)
    db.commit()
    db.refresh(db_log)
    return _create_emulator_log_read(db_log)

@router.get("/", response_model=List[EmulatorLogRead])
def read_emulator_logs(skip: int = 0, limit: int = 100, db: Session = Depends(get_db)):
    logs = db.query(EmulatorLog).offset(skip).limit(limit).all()
    return [_create_emulator_log_read(log) for log in logs]

@router.get("/{run_id}", response_model=EmulatorLogRead)
def read_emulator_log(run_id: int, db: Session = Depends(get_db)):
    db_log = db.query(EmulatorLog).filter(EmulatorLog.run_id == run_id).first()
    if not db_log:
        raise HTTPException(status_code=404, detail="Emulator log not found")
    return _create_emulator_log_read(db_log)

@router.patch("/{run_id}/run_simulation", response_model=EmulatorLogRead)
def update_emulator_log_and_run_simulation(
    run_id: int,
    params: SimulationParams = Body(...),
    db: Session = Depends(get_db)
):

    db_log = db.query(EmulatorLog).filter(EmulatorLog.run_id == run_id).first()
    if not db_log:
        raise HTTPException(status_code=404, detail="Emulator log not found")

    db_log.status = RunStatus.RUNNING.value
    db_log.last_updated = datetime.now()
    db.commit()
    db.refresh(db_log) 

    try:
        emulator = BusEmulator(
            db=db,
            use_optimized_schedule=params.use_optimized_schedule,
            start_time_minutes=params.start_time_minutes,
            end_time_minutes=params.end_time_minutes
        )
        simulation_result = emulator.run_simulation() 

        if simulation_result and simulation_result.get("status") == "Success":
            db_log.status = RunStatus.COMPLETED.value
            if "optimization_details" in simulation_result:
                db_log.optimization_details_dict = simulation_result["optimization_details"]
            else: 
                db_log.optimization_details_dict = {
                    "status": "Success",
                    "message": "Simulation completed successfully"
                }
        else:
            db_log.status = RunStatus.FAILED.value
            if simulation_result:
                db_log.optimization_details_dict = {
                    "status": "FAILED",
                    "message": str(simulation_result) 
                }
            else: 
                db_log.optimization_details_dict = {
                    "status": "FAILED",
                    "message": "Simulation returned no result."
                }

    except Exception as e:
        
        logging.exception(f"Simulation failed for run_id {run_id}: {e}")
        db_log.status = RunStatus.FAILED.value
        db_log.optimization_details_dict = {
            "status": "ERROR",
            "message": f"Simulation error: {str(e)}"
        }
    finally:
        db_log.last_updated = datetime.now()
        db.commit()
        db.refresh(db_log) 
    
    return _create_emulator_log_read(db_log)

@router.put("/{run_id}", response_model=EmulatorLogRead)
def update_emulator_log(
    run_id: int,
    log: EmulatorLogUpdate,
    db: Session = Depends(get_db)
):
    db_log = db.query(EmulatorLog).filter(EmulatorLog.run_id == run_id).first()
    if not db_log:
        raise HTTPException(status_code=404, detail="Emulator log not found")

    if log.status is not None:
        db_log.status = log.status

    if log.optimization_details is not None:
        current_details = db_log.optimization_details_dict or {}
        current_details.update(log.optimization_details.model_dump(exclude_unset=True))
        db_log.optimization_details_dict = current_details

    db_log.last_updated = datetime.now()
    db.commit()
    db.refresh(db_log)
    return _create_emulator_log_read(db_log)

@router.delete("/{run_id}")
def delete_emulator_log(run_id: int, db: Session = Depends(get_db)):
    db_log = db.query(EmulatorLog).filter(EmulatorLog.run_id == run_id).first()
    if not db_log:
        raise HTTPException(status_code=404, detail="Emulator log not found")
    db.delete(db_log)
    db.commit()
    return {"message": "Emulator log deleted successfully"}

