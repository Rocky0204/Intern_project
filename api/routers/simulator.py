# api/routers/simulator.py
import logging
from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy.orm import Session
from datetime import datetime, timezone # Import timezone for datetime conversion
import json # Import json for parsing
from typing import Dict, Any, Optional # Import Optional for type hinting

from api.database import get_db
from services.bus_simulation import BusEmulator
from api.schemas import EmulatorLogRead, RunStatus, OptimizationDetailsRead # Import OptimizationDetailsRead
from api.models import EmulatorLog

router = APIRouter(
    prefix="/simulate",
    tags=["Simulation"],
    responses={404: {"description": "Not found"}},
)

logger = logging.getLogger(__name__)

# Helper function to create EmulatorLogRead instance - Explicitly loading JSON
# This helper now explicitly maps all fields since EmulatorLogRead no longer uses from_attributes=True
def _create_emulator_log_read(db_log: EmulatorLog) -> EmulatorLogRead:
    """
    Helper function to construct an EmulatorLogRead schema object from a database EmulatorLog model.
    It specifically handles the deserialization of the 'optimization_details' JSON string.
    """
    optimization_details_obj = None
    # Access the raw string column directly and parse it
    if db_log.optimization_details:
        try:
            # Attempt to parse the JSON string into the Pydantic model
            optimization_details_data = json.loads(db_log.optimization_details)
            optimization_details_obj = OptimizationDetailsRead(**optimization_details_data)
        except json.JSONDecodeError as e:
            # Log an error if JSON decoding fails and provide a structured error message
            logging.error(f"Failed to decode optimization_details JSON for run_id {db_log.run_id}: {e}")
            optimization_details_obj = OptimizationDetailsRead(status="ERROR", message=f"Failed to parse optimization details: {e}")
    
    # IMPORTANT FIX: Convert the integer status from the database to the RunStatus enum.
    # Also, ensure datetimes are timezone-aware (UTC) if they are naive, to prevent validation issues.
    # Add checks for None before accessing .tzinfo to prevent AttributeError.
    started_at_utc = db_log.started_at
    if started_at_utc is not None and started_at_utc.tzinfo is None:
        started_at_utc = started_at_utc.astimezone(timezone.utc)

    last_updated_utc = db_log.last_updated
    if last_updated_utc is not None and last_updated_utc.tzinfo is None:
        last_updated_utc = last_updated_utc.astimezone(timezone.utc)

    return EmulatorLogRead(
        run_id=db_log.run_id,
        status=RunStatus(db_log.status), # Convert int to RunStatus enum
        started_at=started_at_utc,
        last_updated=last_updated_utc,
        optimization_details=optimization_details_obj,
    )


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
        use_optimized_schedule (bool): Whether to use optimized schedules from DB
        start_time_minutes (int): Simulation start time in minutes from midnight (0-1440)
        end_time_minutes (int): Simulation end time in minutes from midnight (0-1440)
        db (Session): Database session dependency

    Returns:
        EmulatorLogRead: A log entry indicating the status of the simulation run
    """
    logger.info("Starting bus simulation")

    # Create a new log entry with status QUEUED (or RUNNING as per current logic)
    # The status is set to RUNNING immediately as the simulation is about to start.
    db_log_entry = EmulatorLog(status=RunStatus.RUNNING.value)
    db.add(db_log_entry)
    db.commit()
    db.refresh(db_log_entry)

    try:
        # Initialize the BusEmulator with the provided parameters
        emulator = BusEmulator(
            db=db,
            use_optimized_schedule=use_optimized_schedule,
            start_time_minutes=start_time_minutes,
            end_time_minutes=end_time_minutes
        )

        # Run the simulation
        simulation_result = emulator.run_simulation()

        # Update the log entry based on the simulation result
        if simulation_result and simulation_result.get("status") == "Success":
            db_log_entry.status = RunStatus.COMPLETED.value
            if "optimization_details" in simulation_result:
                # Assign the dictionary directly to the hybrid property.
                # The setter for optimization_details_dict in models.py will handle JSON dumping.
                db_log_entry.optimization_details_dict = simulation_result["optimization_details"]
        else:
            # If simulation failed or returned an unexpected status
            db_log_entry.status = RunStatus.FAILED.value
            if simulation_result:
                # Store the simulation result as optimization details for debugging/logging
                db_log_entry.optimization_details_dict = {
                    "status": "FAILED",
                    "message": str(simulation_result)
                }

        db_log_entry.last_updated = datetime.now()
        db.commit()
        db.refresh(db_log_entry)

        # Return the updated log entry using the helper function for proper schema conversion
        return _create_emulator_log_read(db_log_entry)

    except Exception as e:
        # Catch any exceptions during the simulation process
        logger.exception(f"Simulation failed: {e}")
        db_log_entry.status = RunStatus.FAILED.value
        # Store the exception message in optimization details
        db_log_entry.optimization_details_dict = {
            "status": "ERROR",
            "message": str(e)
        }
        db_log_entry.last_updated = datetime.now()
        db.commit()
        db.refresh(db_log_entry)

        # Return the failed log entry using the helper function
        return _create_emulator_log_read(db_log_entry)

