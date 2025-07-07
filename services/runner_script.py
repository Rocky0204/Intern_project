import logging
import os
import sys  # Added for path manipulation

# Add the project root to sys.path to enable absolute imports
# This assumes the script is located at your_project_root/services/runner_script.py
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from api.database import get_db  # Changed to absolute import
from services.bus_simulation import (
    BusEmulator,
)  # Changed to absolute import within services package

# Configure logging for detailed output
logging.basicConfig(level=logging.DEBUG, format="%(levelname)s:%(name)s:%(message)s")
logger = logging.getLogger(__name__)


def main():
    logger.info("Starting bus simulation runner script.")

    # Ensure the database file exists and is populated with dummy data
    # This assumes create_db.py has been run at least once,
    # or you run it before this script to ensure a fresh database.
    # Updated path to assume pluto.db is in the project root
    db_file = os.path.join(os.path.dirname(os.path.dirname(__file__)), "pluto.db")
    if not os.path.exists(db_file):
        logger.error(
            f"Database file '{db_file}' not found. Please run create_db.py first."
        )
        return

    db_session = None
    try:
        # Get a database session
        # Using next(get_db()) to get the session from the generator
        db_session = next(get_db())

        # Initialize the BusEmulator
        # Set use_optimized_schedule to True to enable the optimized scheduling logic.
        emulator = BusEmulator(db=db_session, use_optimized_schedule=True)

        # Run the simulation
        simulation_return_status = emulator.run_simulation()

        logger.info(f"Simulation ended. Bus return status: {simulation_return_status}")

    except Exception as e:
        logger.error(
            f"An unexpected error occurred during simulation: {e}", exc_info=True
        )
        if db_session:
            db_session.rollback()  # Rollback any pending changes on error
    finally:
        if db_session:
            db_session.close()


if __name__ == "__main__":
    main()
