import logging
import os
import sys  

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from api.database import get_db  
from services.bus_simulation import (
    BusEmulator,
)  
logging.basicConfig(level=logging.DEBUG, format="%(levelname)s:%(name)s:%(message)s")
logger = logging.getLogger(__name__)


def main():
    logger.info("Starting bus simulation runner script.")

    db_file = os.path.join(os.path.dirname(os.path.dirname(__file__)), "pluto.db")
    if not os.path.exists(db_file):
        logger.error(
            f"Database file '{db_file}' not found. Please run create_db.py first."
        )
        return

    db_session = None
    try:
        db_session = next(get_db())

        emulator = BusEmulator(db=db_session, use_optimized_schedule=True)
        simulation_return_status = emulator.run_simulation()

        logger.info(f"Simulation ended. Bus return status: {simulation_return_status}")

    except Exception as e:
        logger.error(
            f"An unexpected error occurred during simulation: {e}", exc_info=True
        )
        if db_session:
            db_session.rollback()  
    finally:
        if db_session:
            db_session.close()


if __name__ == "__main__":
    main()
