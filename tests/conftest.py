# tests/conftest.py
import pytest
from fastapi.testclient import TestClient
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import StaticPool

from api.main import app
from api.database import get_db
from api.models import Base # Ensure Base is imported here for metadata operations


# Use an in-memory SQLite database for testing
TEST_DATABASE_URL = "sqlite:///:memory:"

@pytest.fixture(scope="session")
def test_engine():
    """Provides a SQLAlchemy engine for the test session."""
    engine = create_engine(
        TEST_DATABASE_URL,
        connect_args={"check_same_thread": False},
        poolclass=StaticPool, # Use StaticPool for in-memory SQLite to keep connection alive
    )
    # Create all tables once for the entire test session
    Base.metadata.create_all(bind=engine)
    yield engine
    # Drop all tables after the test session is complete
    Base.metadata.drop_all(bind=engine)

@pytest.fixture(scope="function")
def db_session(test_engine):
    """Provides a database session for each test function.
    
    Each test runs within its own transaction which is rolled back,
    ensuring a clean state for every test.
    """
    connection = test_engine.connect()
    transaction = connection.begin()
    
    # Use the session for the test
    SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=connection)
    session = SessionLocal()
    
    yield session
    
    session.close()
    transaction.rollback() # Rollback all changes made by the test
    connection.close()


@pytest.fixture(scope="function")
def client_with_db(db_session):
    """Provides a FastAPI test client with an overridden database dependency."""
    
    def override_get_db():
        yield db_session
    
    app.dependency_overrides[get_db] = override_get_db
    yield TestClient(app)
    app.dependency_overrides.clear() # Clear overrides after the test to prevent interference