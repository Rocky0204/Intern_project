# tests/test_demand.py
import pytest
from fastapi import status
from fastapi.testclient import TestClient
from sqlalchemy import create_engine, event
from sqlalchemy.orm import sessionmaker, Session
from datetime import time # Import time for time objects in tests

# Add these lines to fix module import paths
import os
import sys
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../')))


from api.main import app
from api.database import get_db
# Import all models that might be created or need cleanup in tests
from api.models import Base, StopArea, Demand, StopPoint, Operator, Line, Service, Bus, BusType, Route, JourneyPattern, VehicleJourney, Block, EmulatorLog, JourneyPatternDefinition, RouteDefinition, StopActivity, Garage
from api.schemas import DemandCreate, DemandRead, DemandUpdate, StopAreaCreate # Import StopAreaCreate for fixture setup

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
    session.query(Demand).delete() # Demand cleanup
    session.query(StopActivity).delete()
    session.query(VehicleJourney).delete()
    session.query(JourneyPatternDefinition).delete()
    session.query(JourneyPattern).delete()
    session.query(Block).delete()
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
        # Only rollback if the transaction is still active/valid
        if transaction.is_active:
            transaction.rollback()
        # Close the session and connection
        session.close()
        connection.close()

@pytest.fixture(scope="function")
def client(db_session: Session):
    """Overrides the get_db dependency to use the test database session."""
    def override_get_db():
        try:
            yield db_session
        finally:
            pass # The session is closed by the db_session fixture's finally block

    app.dependency_overrides[get_db] = override_get_db
    with TestClient(app) as c:
        yield c
    app.dependency_overrides.clear()

# --- Reusable Test Data Fixtures ---

@pytest.fixture(scope="function")
def test_stop_area_origin(db_session: Session):
    """Creates and returns a test StopArea for use as origin."""
    stop_area = StopArea(
        stop_area_code=101,
        admin_area_code="ADM_DEM_001",
        name="Demand Origin Area",
        is_terminal=True
    )
    db_session.add(stop_area)
    db_session.commit()
    db_session.refresh(stop_area)
    return stop_area

@pytest.fixture(scope="function")
def test_stop_area_destination(db_session: Session):
    """Creates and returns a test StopArea for use as destination."""
    stop_area = StopArea(
        stop_area_code=102,
        admin_area_code="ADM_DEM_002",
        name="Demand Destination Area",
        is_terminal=False
    )
    db_session.add(stop_area)
    db_session.commit()
    db_session.refresh(stop_area)
    return stop_area

@pytest.fixture(scope="function")
def test_demand(db_session: Session, test_stop_area_origin: StopArea, test_stop_area_destination: StopArea):
    """
    Creates and returns a test Demand entry.
    Note: This fixture now commits the demand entry. For tests specifically
    testing duplicate entries, they will need to create their own conflicting data.
    """
    demand_entry = Demand(
        origin=test_stop_area_origin.stop_area_code,
        destination=test_stop_area_destination.stop_area_code,
        count=10.5,
        start_time=time(8, 0, 0),
        end_time=time(9, 0, 0)
    )
    db_session.add(demand_entry)
    db_session.commit()
    db_session.refresh(demand_entry)
    return demand_entry

# Helper function to format time for URL paths
def format_time_for_url(t: time) -> str:
    return t.strftime("%H:%M:%S")

# --- Test Functions for Demand ---

def test_create_demand(client: TestClient, db_session: Session, test_stop_area_origin: StopArea, test_stop_area_destination: StopArea):
    demand_data = {
        "origin": test_stop_area_origin.stop_area_code,
        "destination": test_stop_area_destination.stop_area_code,
        "count": 15.0,
        "start_time": "09:00:00",
        "end_time": "10:00:00"
    }
    response = client.post("/demand/", json=demand_data)
    assert response.status_code == status.HTTP_201_CREATED
    data = response.json()
    assert data["origin"] == demand_data["origin"]
    assert data["destination"] == demand_data["destination"]
    assert data["count"] == demand_data["count"]
    assert data["start_time"] == demand_data["start_time"]
    assert data["end_time"] == demand_data["end_time"]

    # Verify in DB
    db_demand = db_session.query(Demand).filter(
        Demand.origin == demand_data["origin"],
        Demand.destination == demand_data["destination"],
        Demand.start_time == time(9, 0, 0),
        Demand.end_time == time(10, 0, 0)
    ).first()
    assert db_demand is not None
    assert db_demand.count == 15.0

def test_create_demand_duplicate_entry(client: TestClient, db_session: Session, test_stop_area_origin: StopArea, test_stop_area_destination: StopArea):
    # Define the data for the initial demand entry
    initial_demand_data = {
        "origin": test_stop_area_origin.stop_area_code,
        "destination": test_stop_area_destination.stop_area_code,
        "count": 10.0,
        "start_time": time(8, 0, 0),
        "end_time": time(9, 0, 0)
    }

    # Create the initial demand entry directly in the session
    initial_demand = Demand(
        origin=initial_demand_data["origin"],
        destination=initial_demand_data["destination"],
        count=initial_demand_data["count"],
        start_time=initial_demand_data["start_time"],
        end_time=initial_demand_data["end_time"]
    )
    db_session.add(initial_demand)
    db_session.commit() # Commit this initial entry
    db_session.expunge(initial_demand) # <--- IMPORTANT: Expunge the object from the session

    # Now attempt to create a duplicate via the API, using the original data dictionary
    duplicate_data = {
        "origin": initial_demand_data["origin"],
        "destination": initial_demand_data["destination"],
        "count": 20.0, # Count can be different, but PK is same
        "start_time": format_time_for_url(initial_demand_data["start_time"]), # Use data from dictionary
        "end_time": format_time_for_url(initial_demand_data["end_time"])     # Use data from dictionary
    }
    response = client.post("/demand/", json=duplicate_data)
    assert response.status_code == status.HTTP_409_CONFLICT
    assert "Demand entry with these origin, destination, start_time, and end_time already exists." in response.json()["detail"]

def test_create_demand_invalid_origin(client: TestClient, db_session: Session, test_stop_area_destination: StopArea):
    demand_data = {
        "origin": 99999, # Non-existent origin
        "destination": test_stop_area_destination.stop_area_code,
        "count": 5.0,
        "start_time": "11:00:00",
        "end_time": "12:00:00"
    }
    response = client.post("/demand/", json=demand_data)
    assert response.status_code == status.HTTP_400_BAD_REQUEST
    assert "Origin StopArea with code 99999 not found." in response.json()["detail"]

def test_create_demand_invalid_destination(client: TestClient, db_session: Session, test_stop_area_origin: StopArea):
    demand_data = {
        "origin": test_stop_area_origin.stop_area_code,
        "destination": 99999, # Non-existent destination
        "count": 7.0,
        "start_time": "13:00:00",
        "end_time": "14:00:00"
    }
    response = client.post("/demand/", json=demand_data)
    assert response.status_code == status.HTTP_400_BAD_REQUEST
    assert "Destination StopArea with code 99999 not found." in response.json()["detail"]

def test_read_demand(client: TestClient, test_demand: Demand):
    url = (
        f"/demand/{test_demand.origin}/"
        f"{test_demand.destination}/"
        f"{format_time_for_url(test_demand.start_time)}/"
        f"{format_time_for_url(test_demand.end_time)}"
    )
    response = client.get(url)
    assert response.status_code == status.HTTP_200_OK
    data = response.json()
    assert data["origin"] == test_demand.origin
    assert data["destination"] == test_demand.destination
    assert data["count"] == test_demand.count
    assert data["start_time"] == format_time_for_url(test_demand.start_time)
    assert data["end_time"] == format_time_for_url(test_demand.end_time)

def test_read_demands(client: TestClient, test_demand: Demand):
    response = client.get("/demand/")
    assert response.status_code == status.HTTP_200_OK
    data = response.json()
    assert isinstance(data, list)
    assert len(data) >= 1
    assert any(
        d["origin"] == test_demand.origin and
        d["destination"] == test_demand.destination and
        d["start_time"] == format_time_for_url(test_demand.start_time) and
        d["end_time"] == format_time_for_url(test_demand.end_time)
        for d in data
    )

def test_update_demand(client: TestClient, db_session: Session, test_demand: Demand):
    update_data = {"count": 25.5}
    url = (
        f"/demand/{test_demand.origin}/"
        f"{test_demand.destination}/"
        f"{format_time_for_url(test_demand.start_time)}/"
        f"{format_time_for_url(test_demand.end_time)}"
    )
    response = client.put(url, json=update_data)
    assert response.status_code == status.HTTP_200_OK
    data = response.json()
    assert data["count"] == update_data["count"]

    # Verify in DB
    db_demand = db_session.query(Demand).filter(
        Demand.origin == test_demand.origin,
        Demand.destination == test_demand.destination,
        Demand.start_time == test_demand.start_time,
        Demand.end_time == test_demand.end_time
    ).first()
    assert db_demand.count == update_data["count"]

def test_delete_demand(client: TestClient, db_session: Session, test_demand: Demand):
    url = (
        f"/demand/{test_demand.origin}/"
        f"{test_demand.destination}/"
        f"{format_time_for_url(test_demand.start_time)}/"
        f"{format_time_for_url(test_demand.end_time)}"
    )
    response = client.delete(url)
    assert response.status_code == status.HTTP_204_NO_CONTENT

    # Verify in DB
    db_demand = db_session.query(Demand).filter(
        Demand.origin == test_demand.origin,
        Demand.destination == test_demand.destination,
        Demand.start_time == test_demand.start_time,
        Demand.end_time == test_demand.end_time
    ).first()
    assert db_demand is None

def test_read_nonexistent_demand(client: TestClient):
    non_existent_origin = 99999
    non_existent_destination = 88888
    non_existent_start_time = "01:00:00"
    non_existent_end_time = "02:00:00"
    url = (
        f"/demand/{non_existent_origin}/"
        f"{non_existent_destination}/"
        f"{non_existent_start_time}/"
        f"{non_existent_end_time}"
    )
    response = client.get(url)
    assert response.status_code == status.HTTP_404_NOT_FOUND

def test_update_nonexistent_demand(client: TestClient):
    non_existent_origin = 99999
    non_existent_destination = 88888
    non_existent_start_time = "01:00:00"
    non_existent_end_time = "02:00:00"
    update_data = {"count": 100.0}
    url = (
        f"/demand/{non_existent_origin}/"
        f"{non_existent_destination}/"
        f"{non_existent_start_time}/"
        f"{non_existent_end_time}"
    )
    response = client.put(url, json=update_data)
    assert response.status_code == status.HTTP_404_NOT_FOUND

def test_delete_nonexistent_demand(client: TestClient):
    non_existent_origin = 99999
    non_existent_destination = 88888
    non_existent_start_time = "01:00:00"
    non_existent_end_time = "02:00:00"
    url = (
        f"/demand/{non_existent_origin}/"
        f"{non_existent_destination}/"
        f"{non_existent_start_time}/"
        f"{non_existent_end_time}"
    )
    response = client.delete(url)
    assert response.status_code == status.HTTP_404_NOT_FOUND
