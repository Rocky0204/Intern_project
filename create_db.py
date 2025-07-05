# create_db.py
import logging
import os
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from api.database import DATABASE_URL, get_db, engine
from api.models import Base
from insert_dummy_data import insert_data

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

if __name__ == "__main__":
    try:
        # 1. Delete existing database file
        if os.path.exists("pluto.db"):
            os.remove("pluto.db")
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