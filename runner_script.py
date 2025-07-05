import logging
from sqlalchemy.orm import Session
from api.database import get_db, engine
from bus_simulation import BusEmulator
import os

# Configure logging for detailed output
logging.basicConfig(level=logging.DEBUG, format="%(levelname)s:%(name)s:%(message)s")
logger = logging.getLogger(__name__)

def main():
    logger.info("Starting bus simulation runner script.")

    # Ensure the database file exists and is populated with dummy data
    # This assumes create_db.py has been run at least once,
    # or you run it before this script to ensure a fresh database.
    db_file = "pluto.db"
    if not os.path.exists(db_file):
        logger.error(f"Database file '{db_file}' not found. Please run create_db.py first.")
        return

    db_session = None
    try:
        # Get a database session
        # Using next(get_db()) to get the session from the generator
        db_session = next(get_db())

        # Initialize the BusEmulator
        # Set use_optimized_schedule to False to ensure the emulator generates
        # and saves its own schedule to the database.
        emulator = BusEmulator(db=db_session, use_optimized_schedule=False)

        # Run the simulation
        simulation_return_status = emulator.run_simulation()

        logger.info(f"Simulation ended. Bus return status: {simulation_return_status}")

    except Exception as e:
        logger.error(f"An unexpected error occurred during simulation: {e}", exc_info=True)
        if db_session:
            db_session.rollback() # Rollback any pending changes on error
    finally:
        if db_session:
            db_session.close() # Always close the session
            logger.info("Database session closed.")
        logger.info("Runner script finished.")

if __name__ == "__main__":
    main()

