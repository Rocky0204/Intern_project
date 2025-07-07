# api/routers/bus_simulator_router.py

import logging
from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy.orm import Session
from datetime import datetime

# Adjust import paths for absolute imports within the project structure
from api.database import get_db
from services.bus_simulation import BusEmulator
from api.schemas import EmulatorLogRead, RunStatus # Import RunStatus from schemas

# Import EmulatorLog model from models.py
from api.models import EmulatorLog 

router = APIRouter(
    prefix="/simulate",
    tags=["Simulation"],
    responses={404: {"description": "Not found"}},
)

logger = logging.getLogger(__name__)

@router.post("/run", response_model=EmulatorLogRead, status_code=status.HTTP_202_ACCEPTED)
async def run_bus_simulation(
    use_optimized_schedule: bool = True,
    start_time_minutes: int = 0,
    end_time_minutes: int = 1440,
    db: Session = Depends(get_db)
):
    """
    Runs the bus simulation.

    Args:
        use_optimized_schedule (bool): Whether to use optimized schedules from DB (VehicleJourneys)
                                       or generate a random schedule.
        start_time_minutes (int): The simulation start time in minutes from midnight (0-1440).
        end_time_minutes (int): The simulation end time in minutes from midnight (0-1440).
        db (Session): Database session dependency.

    Returns:
        EmulatorLogRead: A log entry indicating the status of the simulation run.
    """
    logger.info("API: Received request to run bus simulation.")

    # Create an initial log entry for the simulation run in the database
    db_log_entry = EmulatorLog(
        status=RunStatus.RUNNING.value,
        started_at=datetime.now(),
        last_updated=datetime.now()
    )
    db.add(db_log_entry)
    db.commit()
    db.refresh(db_log_entry)
    logger.info(f"API: Log entry created for simulation run_id: {db_log_entry.run_id}")

    try:
        emulator = BusEmulator(
            db=db,
            use_optimized_schedule=use_optimized_schedule,
            start_time_minutes=start_time_minutes,
            end_time_minutes=end_time_minutes
        )

        simulation_result = emulator.run_simulation()

        # Update status based on simulation result
        if isinstance(simulation_result, dict) and simulation_result.get("status") == "Success":
            db_log_entry.status = RunStatus.COMPLETED.value
            logger.info(f"API: Simulation run_id {db_log_entry.run_id} completed successfully.")
        else:
            db_log_entry.status = RunStatus.FAILED.value
            logger.error(f"API: Simulation run_id {db_log_entry.run_id} failed with result: {simulation_result}")
        
        db_log_entry.last_updated = datetime.now()
        db.commit()
        # db.refresh(db_log_entry)
        
        # Return the EmulatorLogRead object
        return EmulatorLogRead.model_validate(db_log_entry)
        # return EmulatorLogRead.model_validate(db_log_entry)

    except Exception as e:
        logger.exception(f"API: An error occurred during bus simulation run_id {db_log_entry.run_id}: {e}")
        db_log_entry.status = RunStatus.FAILED.value
        db_log_entry.last_updated = datetime.now()
        db.commit()
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"An error occurred during simulation: {e}"
        )

