import logging
from fastapi import APIRouter, Depends, status
from sqlalchemy.orm import Session
from datetime import datetime, timezone
import json

from api.database import get_db
from services.bus_simulation import BusEmulator
from api.schemas import EmulatorLogRead, RunStatus, OptimizationDetailsRead
from api.models import EmulatorLog

router = APIRouter(
    prefix="/simulate",
    tags=["Simulation"],
    responses={404: {"description": "Not found"}},
)

logger = logging.getLogger(__name__)


def _create_emulator_log_read(db_log: EmulatorLog) -> EmulatorLogRead:
    """
    Helper function to construct an EmulatorLogRead schema object from a database EmulatorLog model.
    It specifically handles the deserialization of the 'optimization_details' JSON string.
    """
    optimization_details_obj = None
    if db_log.optimization_details:
        try:
            optimization_details_data = json.loads(db_log.optimization_details)
            optimization_details_obj = OptimizationDetailsRead(
                **optimization_details_data
            )
        except json.JSONDecodeError as e:
            logging.error(
                f"Failed to decode optimization_details JSON for run_id {db_log.run_id}: {e}"
            )
            optimization_details_obj = OptimizationDetailsRead(
                status="ERROR", message=f"Failed to parse optimization details: {e}"
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
        optimization_details=optimization_details_obj,
    )


@router.post(
    "/run", response_model=EmulatorLogRead, status_code=status.HTTP_202_ACCEPTED
)
async def run_bus_simulation(
    use_optimized_schedule: bool = True,
    start_time_minutes: int = 0,
    end_time_minutes: int = 1440,
    db: Session = Depends(get_db),
):
    logger.info("Starting bus simulation")

    db_log_entry = EmulatorLog(status=RunStatus.RUNNING.value)
    db.add(db_log_entry)
    db.commit()
    db.refresh(db_log_entry)

    try:
        emulator = BusEmulator(
            db=db,
            use_optimized_schedule=use_optimized_schedule,
            start_time_minutes=start_time_minutes,
            end_time_minutes=end_time_minutes,
        )

        simulation_result = emulator.run_simulation()

        if simulation_result and simulation_result.get("status") == "Success":
            db_log_entry.status = RunStatus.COMPLETED.value
            if "optimization_details" in simulation_result:
                db_log_entry.optimization_details_dict = simulation_result[
                    "optimization_details"
                ]
        else:
            db_log_entry.status = RunStatus.FAILED.value
            if simulation_result:
                db_log_entry.optimization_details_dict = {
                    "status": "FAILED",
                    "message": str(simulation_result),
                }

        db_log_entry.last_updated = datetime.now()
        db.commit()
        db.refresh(db_log_entry)

        return _create_emulator_log_read(db_log_entry)

    except Exception as e:
        logger.exception(f"Simulation failed: {e}")
        db_log_entry.status = RunStatus.FAILED.value
        db_log_entry.optimization_details_dict = {"status": "ERROR", "message": str(e)}
        db_log_entry.last_updated = datetime.now()
        db.commit()
        db.refresh(db_log_entry)

        return _create_emulator_log_read(db_log_entry)
