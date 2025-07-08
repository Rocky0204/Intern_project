import logging
import os
import sys

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

if __name__ == "__main__":
    try:
        # Add project root to sys.path
        project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        sys.path.insert(0, project_root)
        os.chdir(project_root)

        # Import AFTER modifying sys.path
        from api.database import get_db, engine
        from api.models import Base
        from scripts.insert_dummy_data import insert_data

        # Delete existing database file
        db_path = os.path.join(project_root, "pluto.db")
        if os.path.exists(db_path):
            os.remove(db_path)
            logger.info("Existing pluto.db removed.")

        # Create tables
        logger.info("Creating database tables...")
        Base.metadata.create_all(bind=engine)
        logger.info("Database tables created successfully.")

        # Insert dummy data
        with next(get_db()) as db:
            insert_data(db)

    except Exception as e:
        logger.error(f"An error occurred during database setup: {e}")
