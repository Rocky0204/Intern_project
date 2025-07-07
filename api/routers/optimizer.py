# api/routers/optimizer.py

import logging
from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy.orm import Session
from datetime import datetime

from api.database import get_db
from services.frequency_optimiser import FrequencyOptimiser
from api.schemas import EmulatorLogRead, RunStatus
from api.models import EmulatorLog

router = APIRouter(
    prefix="/optimize",
    tags=["Optimization"],
    responses={404: {"description": "Not found"}},
)

logger = logging.getLogger(__name__)

@router.post("/run", response_model=EmulatorLogRead, status_code=status.HTTP_202_ACCEPTED)
async def run_frequency_optimization(
    num_slots: int = 24,
    slot_length: int = 60,
    layover: int = 15,
    min_demand_threshold: float = 1.0,
    min_frequency_trips_per_period: int = 1,
    min_frequency_period_minutes: int = 60,
    start_time_minutes: int = 0,
    db: Session = Depends(get_db)
):
    """
    Runs the bus frequency optimization.
    """
    logger.info("API: Received request to run frequency optimization.")

    # Create an initial log entry
    db_log_entry = EmulatorLog(
        status=RunStatus.RUNNING.value,
        started_at=datetime.now(),
        last_updated=datetime.now()
    )
    db.add(db_log_entry)
    db.commit()
    db.refresh(db_log_entry)
    logger.info(f"API: Log entry created for optimization run_id: {db_log_entry.run_id}")

    try:
        optimiser = FrequencyOptimiser(
            num_slots=num_slots,
            slot_length=slot_length,
            layover=layover,
            min_demand_threshold=min_demand_threshold,
            min_frequency_trips_per_period=min_frequency_trips_per_period,
            min_frequency_period_minutes=min_frequency_period_minutes,
        )

        optimiser.fit_data(db, start_time_minutes=start_time_minutes)
        
        optimization_result = optimiser.optimise_frequencies(db, start_time_minutes=start_time_minutes)
        
        # Debug logging
        logger.debug(f"Optimizer returned: {optimization_result}")
        
        # Update status based on optimization result
        if isinstance(optimization_result, dict) and optimization_result.get("status") in ["OPTIMAL", "FEASIBLE"]:
            db_log_entry.status = RunStatus.COMPLETED.value
            logger.info(f"API: Optimization run_id {db_log_entry.run_id} completed successfully.")
        else:
            db_log_entry.status = RunStatus.FAILED.value
            logger.error(f"API: Optimization run_id {db_log_entry.run_id} failed with result: {optimization_result}")
        
        db_log_entry.last_updated = datetime.now()
        db.commit()
        db.refresh(db_log_entry)
        
        return EmulatorLogRead.model_validate(db_log_entry)

    except Exception as e:
        logger.exception(f"API: An error occurred during frequency optimization run_id {db_log_entry.run_id}: {e}")
        db_log_entry.status = RunStatus.FAILED.value
        db_log_entry.last_updated = datetime.now()
        db.commit()
        db.refresh(db_log_entry)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"An error occurred during optimization: {e}"
        )