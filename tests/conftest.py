# tests/conftest.py

import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

import pytest
from fastapi.testclient import TestClient
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import StaticPool
from api.models import *
from api.database import *
from api.main import app
# Define the URL for an in-memory SQLite database for testing
# This ensures that tests run against a fresh, temporary database each time
SQLALCHEMY_DATABASE_URL = "sqlite:///:memory:"

# Create a SQLAlchemy engine specifically for testing
# `check_same_thread=False` is needed for SQLite when using multiple threads (e.g., FastAPI's internal threadpool)
# `poolclass=StaticPool` is crucial for in-memory SQLite to ensure the same connection
# is used across requests within a single test, preventing issues with closing the in-memory DB.
engine = create_engine(
    SQLALCHEMY_DATABASE_URL,
    connect_args={"check_same_thread": False},
    poolclass=StaticPool,
)

# Create a SessionLocal class for testing purposes
# This session will be used to interact with the test database
TestingSessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

@pytest.fixture(scope="function")
def db_session():
    """
    Provides a clean, isolated database session for each test function.
    It creates all tables before a test and drops them after the test completes.
    This ensures that each test runs with a fresh database state.
    """
    # Create all database tables defined in Base.metadata
    Base.metadata.create_all(bind=engine)
    # Get a new session for the test
    db = TestingSessionLocal()
    try:
        yield db # Yield the session to the test function
    finally:
        # Close the session after the test
        db.close()
        # Drop all tables to clean up the database for the next test
        Base.metadata.drop_all(bind=engine)

@pytest.fixture(scope="function")
def client_with_db(db_session):
    """
    Provides a FastAPI TestClient that uses the isolated test database session.
    This fixture overrides the `get_db` dependency in the FastAPI app
    to ensure that all API calls during a test use the test database.
    """
    def override_get_db():
        """
        Dependency override function that yields the test database session.
        """
        try:
            yield db_session
        finally:
            # The session is closed by the db_session fixture's teardown,
            # but it's good practice to ensure it's handled here too if needed
            pass # db_session fixture handles the close()

    # Override the get_db dependency in the FastAPI application
    app.dependency_overrides[get_db] = override_get_db
    # Yield the TestClient instance
    yield TestClient(app)
    # Clear the dependency overrides after the test to prevent interference
    # with other tests or the actual application if it were to run later
    app.dependency_overrides.clear()
