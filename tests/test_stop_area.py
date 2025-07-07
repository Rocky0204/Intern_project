# tests/test_stop_area.py
import pytest
from fastapi import status
from fastapi.testclient import TestClient
from sqlalchemy import create_engine, event
from sqlalchemy.orm import sessionmaker, Session

# Add these lines to fix module import paths
import os
import sys

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "../")))


from api.main import app
from api.database import get_db

# Import all models that might be created or need cleanup in tests
# Added Garage to the import list
from api.models import (
    Base,
    StopArea,
    StopPoint,
    Operator,
    Line,
    Service,
    Bus,
    BusType,
    Route,
    JourneyPattern,
    VehicleJourney,
    Block,
    Demand,
    EmulatorLog,
    JourneyPatternDefinition,
    RouteDefinition,
    StopActivity,
    Garage,
)

# --- Database Setup for Tests (self-contained, as requested) ---
SQLALCHEMY_DATABASE_URL = "sqlite:///:memory:"

engine = create_engine(
    SQLALCHEMY_DATABASE_URL, connect_args={"check_same_thread": False}
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
    session.query(Demand).delete()
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
    session.query(Garage).delete()  # Now Garage is imported and should be recognized
    session.query(StopPoint).delete()
    session.query(StopArea).delete()  # Delete StopArea last as it's a parent
    session.commit()

    try:
        yield session
    finally:
        session.close()
        transaction.rollback()
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
def test_stop_area(db_session: Session):
    """Creates and returns a test StopArea."""
    stop_area = StopArea(
        stop_area_code=1001,
        admin_area_code="ADM001",
        name="Central Bus Station",
        is_terminal=True,
    )
    db_session.add(stop_area)
    db_session.commit()
    db_session.refresh(stop_area)
    return stop_area


# --- Test Functions for StopArea ---


def test_create_stop_area(client: TestClient, db_session: Session):
    stop_area_data = {
        "stop_area_code": 1002,
        "admin_area_code": "ADM002",
        "name": "North Side Terminal",
        "is_terminal": False,
    }
    response = client.post("/stop_areas/", json=stop_area_data)
    assert response.status_code == status.HTTP_201_CREATED
    data = response.json()
    assert data["stop_area_code"] == stop_area_data["stop_area_code"]
    assert data["admin_area_code"] == stop_area_data["admin_area_code"]
    assert data["name"] == stop_area_data["name"]
    assert data["is_terminal"] == stop_area_data["is_terminal"]

    # Verify in DB
    db_sa = db_session.query(StopArea).filter(StopArea.stop_area_code == 1002).first()
    assert db_sa is not None
    assert db_sa.name == "North Side Terminal"


def test_create_stop_area_duplicate_admin_area_code(
    client: TestClient, test_stop_area: StopArea
):
    duplicate_data = {
        "stop_area_code": 1003,
        "admin_area_code": test_stop_area.admin_area_code,  # Duplicate admin_area_code
        "name": "Another Terminal",
        "is_terminal": True,
    }
    response = client.post("/stop_areas/", json=duplicate_data)
    assert response.status_code == status.HTTP_409_CONFLICT
    assert "already exists" in response.json()["detail"]


def test_read_stop_area(client: TestClient, test_stop_area: StopArea):
    response = client.get(f"/stop_areas/{test_stop_area.stop_area_code}")
    assert response.status_code == status.HTTP_200_OK
    data = response.json()
    assert data["stop_area_code"] == test_stop_area.stop_area_code
    assert data["name"] == test_stop_area.name


def test_read_stop_areas(client: TestClient, test_stop_area: StopArea):
    response = client.get("/stop_areas/")
    assert response.status_code == status.HTTP_200_OK
    data = response.json()
    assert isinstance(data, list)
    assert len(data) >= 1
    assert any(sa["stop_area_code"] == test_stop_area.stop_area_code for sa in data)


def test_update_stop_area(
    client: TestClient, db_session: Session, test_stop_area: StopArea
):
    update_data = {"name": "Updated Central Station", "is_terminal": False}
    response = client.put(
        f"/stop_areas/{test_stop_area.stop_area_code}", json=update_data
    )
    assert response.status_code == status.HTTP_200_OK
    data = response.json()
    assert data["name"] == update_data["name"]
    assert data["is_terminal"] == update_data["is_terminal"]

    # Verify in DB
    db_sa = (
        db_session.query(StopArea)
        .filter(StopArea.stop_area_code == test_stop_area.stop_area_code)
        .first()
    )
    assert db_sa.name == update_data["name"]
    assert db_sa.is_terminal == update_data["is_terminal"]


def test_update_stop_area_duplicate_admin_area_code(
    client: TestClient, db_session: Session
):
    # Create a second stop area
    sa2 = StopArea(
        stop_area_code=1003,
        admin_area_code="ADM003",
        name="Temp Area",
        is_terminal=False,
    )
    db_session.add(sa2)
    db_session.commit()
    db_session.refresh(sa2)

    # Try to update test_stop_area with sa2's admin_area_code
    test_sa = (
        db_session.query(StopArea).filter(StopArea.stop_area_code == 1001).first()
    )  # Assuming 1001 is the ID of test_stop_area
    if not test_sa:  # If test_stop_area was not created by a fixture, create it here
        test_sa = StopArea(
            stop_area_code=1001,
            admin_area_code="ADM001",
            name="Central Bus Station",
            is_terminal=True,
        )
        db_session.add(test_sa)
        db_session.commit()
        db_session.refresh(test_sa)

    update_data = {
        "admin_area_code": sa2.admin_area_code
    }  # Try to use existing admin_area_code
    response = client.put(f"/stop_areas/{test_sa.stop_area_code}", json=update_data)
    assert response.status_code == status.HTTP_409_CONFLICT
    assert "already exists" in response.json()["detail"]


def test_delete_stop_area(
    client: TestClient, db_session: Session, test_stop_area: StopArea
):
    stop_area_code_to_delete = test_stop_area.stop_area_code
    response = client.delete(f"/stop_areas/{stop_area_code_to_delete}")
    assert (
        response.status_code == status.HTTP_204_NO_CONTENT
    )  # 204 No Content for successful deletion

    # Verify in DB
    db_sa = (
        db_session.query(StopArea)
        .filter(StopArea.stop_area_code == stop_area_code_to_delete)
        .first()
    )
    assert db_sa is None


def test_read_nonexistent_stop_area(client: TestClient):
    response = client.get("/stop_areas/99999")
    assert response.status_code == status.HTTP_404_NOT_FOUND


def test_update_nonexistent_stop_area(client: TestClient):
    update_data = {"name": "Non Existent Update"}
    response = client.put("/stop_areas/99999", json=update_data)
    assert response.status_code == status.HTTP_404_NOT_FOUND


def test_delete_nonexistent_stop_area(client: TestClient):
    response = client.delete("/stop_areas/99999")
    assert response.status_code == status.HTTP_404_NOT_FOUND


# Example of a test for deleting a stop area with dependencies (if you implement this logic)
# def test_delete_stop_area_with_dependencies(client: TestClient, db_session: Session, test_stop_area: StopArea):
#     # Create a StopPoint linked to test_stop_area
#     stop_point = StopPoint(
#         atco_code=2001,
#         name="Test Stop Point",
#         latitude=51.0,
#         longitude=0.0,
#         stop_area_code=test_stop_area.stop_area_code
#     )
#     db_session.add(stop_point)
#     db_session.commit()
#     db_session.refresh(stop_point)

#     response = client.delete(f"/stop_areas/{test_stop_area.stop_area_code}")
#     assert response.status_code == status.HTTP_400_BAD_REQUEST # Or 500 if DB error
#     assert "Cannot delete stop area due to existing dependencies" in response.json()["detail"] # Adjust message
