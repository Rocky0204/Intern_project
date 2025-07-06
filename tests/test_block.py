# tests/test_block.py
import pytest
from fastapi import status
from fastapi.testclient import TestClient
from sqlalchemy import create_engine, event
from sqlalchemy.orm import sessionmaker, Session
from datetime import datetime
from enum import Enum # Changed from IntEnum to a regular Enum for string values

# Add these lines to fix module import paths
import os
import sys
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../')))


from api.main import app
from api.database import get_db
# Import all models that might be created or need cleanup in tests
from api.models import Base, EmulatorLog, StopArea, Demand, StopPoint, Operator, Line, Service, Bus, BusType, Route, JourneyPattern, VehicleJourney, Block, JourneyPatternDefinition, RouteDefinition, StopActivity, Garage
from api.schemas import BlockCreate, BlockRead, BlockUpdate, RunStatus # Import RunStatus from schemas

# --- Database Setup for Tests (self-contained, as requested) ---
SQLALCHEMY_DATABASE_URL = "sqlite:///:memory:"

engine = create_engine(
    SQLALCHEMY_DATABASE_URL,
    connect_args={"check_same_thread": False}
)

# Ensure foreign key enforcement for SQLite using an event listener
@event.listens_for(engine, "connect")
def set_sqlite_pragma(dbapi_connection, connection_record):
    cursor = dbapi_connection.cursor()
    cursor.execute("PRAGMA foreign_keys=ON;")
    cursor.close()

TestingSessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

@pytest.fixture(scope="module")
def setup_db():
    """Creates all tables before tests in this module run, and drops them afterwards."""
    Base.metadata.create_all(bind=engine)
    yield
    Base.metadata.drop_all(bind=engine)

@pytest.fixture(scope="function")
def db_session(setup_db):
    """
    Provides a transactional database session for each test function.
    Ensures a clean state for every test by rolling back changes.
    Cleans up all relevant tables before each test.
    """
    connection = engine.connect()
    transaction = connection.begin()
    session = TestingSessionLocal(bind=connection)

    # Comprehensive cleanup for all tables that might have data from fixtures or previous tests
    # Order matters for foreign key constraints (delete children before parents)
    session.query(EmulatorLog).delete()
    session.query(VehicleJourney).delete() # Blocks depend on VehicleJourneys
    session.query(Block).delete() # Block cleanup
    session.query(Demand).delete()
    session.query(StopActivity).delete()
    session.query(JourneyPatternDefinition).delete()
    session.query(JourneyPattern).delete()
    session.query(Bus).delete()
    session.query(RouteDefinition).delete()
    session.query(Route).delete()
    session.query(Service).delete()
    session.query(Line).delete()
    session.query(Operator).delete()
    session.query(BusType).delete()
    session.query(Garage).delete()
    session.query(StopPoint).delete()
    session.query(StopArea).delete()
    session.commit() # Commit the deletions to ensure a clean state before the test starts

    try:
        yield session
    finally:
        # Ensure rollback happens before closing the session
        transaction.rollback()
        session.close()
        connection.close()

@pytest.fixture(scope="function")
def client(db_session: Session):
    """Overrides the get_db dependency to use the test database session."""
    def override_get_db():
        try:
            yield db_session
        finally:
            pass
    
    app.dependency_overrides[get_db] = override_get_db
    with TestClient(app) as c:
        yield c
    app.dependency_overrides.clear()

# --- Reusable Test Data Fixtures ---

@pytest.fixture(scope="function")
def test_operator(db_session: Session):
    operator = Operator(operator_code="OP1", name="Test Operator")
    db_session.add(operator)
    db_session.commit()
    db_session.refresh(operator)
    return operator

@pytest.fixture(scope="function")
def test_bus_type(db_session: Session):
    bus_type = BusType(name="Standard", capacity=50)
    db_session.add(bus_type)
    db_session.commit()
    db_session.refresh(bus_type)
    return bus_type

@pytest.fixture(scope="function")
def test_block(db_session: Session, test_operator: Operator, test_bus_type: BusType):
    block = Block(
        name="Test Block 1",
        operator_id=test_operator.operator_id,
        bus_type_id=test_bus_type.type_id
    )
    db_session.add(block)
    db_session.commit()
    db_session.refresh(block)
    return block

# --- Test Functions for Block ---

def test_create_block(client: TestClient, test_operator: Operator, test_bus_type: BusType):
    block_data = {
        "name": "New Block",
        "operator_id": test_operator.operator_id,
        "bus_type_id": test_bus_type.type_id
    }
    response = client.post("/blocks/", json=block_data)
    assert response.status_code == status.HTTP_201_CREATED
    data = response.json()
    assert data["name"] == block_data["name"]
    assert data["operator_id"] == block_data["operator_id"]
    assert data["bus_type_id"] == block_data["bus_type_id"]
    assert "block_id" in data

def test_create_block_invalid_operator(client: TestClient, test_bus_type: BusType):
    block_data = {
        "name": "Invalid Operator Block",
        "operator_id": 99999, # Non-existent operator
        "bus_type_id": test_bus_type.type_id
    }
    response = client.post("/blocks/", json=block_data)
    assert response.status_code == status.HTTP_400_BAD_REQUEST
    assert "Operator with ID 99999 not found" in response.json()["detail"]

def test_create_block_invalid_bus_type(client: TestClient, test_operator: Operator):
    block_data = {
        "name": "Invalid Bus Type Block",
        "operator_id": test_operator.operator_id,
        "bus_type_id": 99999 # Non-existent bus type
    }
    response = client.post("/blocks/", json=block_data)
    assert response.status_code == status.HTTP_400_BAD_REQUEST
    assert "BusType with ID 99999 not found" in response.json()["detail"]

def test_read_block(client: TestClient, test_block: Block):
    response = client.get(f"/blocks/{test_block.block_id}")
    assert response.status_code == status.HTTP_200_OK
    data = response.json()
    assert data["block_id"] == test_block.block_id
    assert data["name"] == test_block.name

def test_read_blocks(client: TestClient, test_block: Block):
    response = client.get("/blocks/")
    assert response.status_code == status.HTTP_200_OK
    data = response.json()
    assert isinstance(data, list)
    assert len(data) >= 1
    assert any(block["block_id"] == test_block.block_id for block in data)

def test_update_block(client: TestClient, test_block: Block, test_operator: Operator, test_bus_type: BusType):
    update_data = {
        "name": "Updated Block Name",
        "operator_id": test_operator.operator_id, # Re-use existing or create new
        "bus_type_id": test_bus_type.type_id # Re-use existing or create new
    }
    response = client.put(f"/blocks/{test_block.block_id}", json=update_data)
    assert response.status_code == status.HTTP_200_OK
    data = response.json()
    assert data["name"] == update_data["name"]
    assert data["operator_id"] == update_data["operator_id"]
    assert data["bus_type_id"] == update_data["bus_type_id"]

def test_update_block_invalid_operator(client: TestClient, test_block: Block):
    update_data = {"operator_id": 99999} # Non-existent operator
    response = client.put(f"/blocks/{test_block.block_id}", json=update_data)
    assert response.status_code == status.HTTP_400_BAD_REQUEST
    assert "Operator with ID 99999 not found" in response.json()["detail"]

def test_update_block_invalid_bus_type(client: TestClient, test_block: Block):
    update_data = {"bus_type_id": 99999} # Non-existent bus type
    response = client.put(f"/blocks/{test_block.block_id}", json=update_data)
    assert response.status_code == status.HTTP_400_BAD_REQUEST
    assert "BusType with ID 99999 not found" in response.json()["detail"]

def test_delete_block(client: TestClient, db_session: Session, test_block: Block):
    block_id_to_delete = test_block.block_id
    response = client.delete(f"/blocks/{block_id_to_delete}")
    assert response.status_code == status.HTTP_204_NO_CONTENT

    # Verify in DB
    db_block = db_session.query(Block).filter(Block.block_id == block_id_to_delete).first()
    assert db_block is None

def test_read_nonexistent_block(client: TestClient):
    response = client.get("/blocks/99999")
    assert response.status_code == status.HTTP_404_NOT_FOUND

def test_update_nonexistent_block(client: TestClient):
    update_data = {"name": "Non Existent Update"}
    response = client.put("/blocks/99999", json=update_data)
    assert response.status_code == status.HTTP_404_NOT_FOUND

def test_delete_nonexistent_block(client: TestClient):
    response = client.delete("/blocks/99999")
    assert response.status_code == status.HTTP_404_NOT_FOUND
