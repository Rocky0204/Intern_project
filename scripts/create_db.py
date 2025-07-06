# create_db.py
import logging
import os
import sys
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

# Add project root to Python path
project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, project_root)

from api.database import DATABASE_URL, get_db, engine
from api.models import Base
from scripts.insert_dummy_data import insert_data  # Update path if needed

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

if __name__ == "__main__":
    try:
        # Set working directory to project root
        os.chdir(project_root)
        
        # 1. Delete existing database file
        db_path = os.path.join(project_root, "pluto.db")
        if os.path.exists(db_path):
            os.remove(db_path)
            logger.info("Existing pluto.db removed.")

        # 2. Create all tables defined in models.py
        logger.info("Creating database tables...")
        Base.metadata.create_all(bind=engine)
        logger.info("Database tables created successfully.")

        # 3. Insert dummy data
        with next(get_db()) as db:
            insert_data(db)

    except Exception as e:
        logger.error(f"An error occurred during database setup: {e}")