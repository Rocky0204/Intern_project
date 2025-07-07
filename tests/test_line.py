# tests/test_line.py
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
from api.models import (
    Base,
    EmulatorLog,
    StopArea,
    Demand,
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
    """
    connection = engine.connect()
    transaction = connection.begin()
    session = TestingSessionLocal(bind=connection)

    # Comprehensive cleanup for all tables that might have data from fixtures or previous tests
    # Order matters for foreign key constraints (delete children before parents)
    session.query(EmulatorLog).delete()
    session.query(StopActivity).delete()
    session.query(VehicleJourney).delete()
    session.query(JourneyPatternDefinition).delete()
    session.query(JourneyPattern).delete()
    session.query(Block).delete()
    session.query(Demand).delete()
    session.query(Bus).delete()
    session.query(RouteDefinition).delete()
    session.query(Route).delete()
    session.query(Service).delete()
    session.query(Line).delete()
    session.query(Operator).delete()
    session.query(BusType).delete()
    session.query(StopPoint).delete()
    session.query(StopArea).delete()
    session.query(Garage).delete()
    session.commit()  # Commit the deletions to ensure a clean state before the test starts

    try:
        yield session
    finally:
        # Only rollback if the transaction is still active/valid
        if transaction.is_active:  # Added this check
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
            # The session is closed by the db_session fixture's finally block
            pass

    app.dependency_overrides[get_db] = override_get_db
    with TestClient(app) as c:
        yield c
    app.dependency_overrides.clear()


# --- Reusable Test Data Fixtures for Line dependencies ---


@pytest.fixture(scope="function")
def test_operator(db_session: Session):
    operator = Operator(operator_code="OP1", name="Test Operator")
    db_session.add(operator)
    db_session.commit()  # Commit here to make it available for other fixtures
    db_session.refresh(operator)
    return operator


@pytest.fixture(scope="function")
def test_line(db_session: Session, test_operator: Operator):
    """
    Creates and returns a test Line entry.
    Note: This fixture now commits the line entry. For tests specifically
    testing duplicate entries, they will need to create their own conflicting data.
    """
    line = Line(line_name="Test Line 1", operator_id=test_operator.operator_id)
    db_session.add(line)
    db_session.commit()  # Commit here to make it available for other fixtures
    db_session.refresh(line)
    return line


# --- Test Functions for Line ---


def test_create_line(client: TestClient, test_operator: Operator):
    line_data = {"line_name": "New Line", "operator_id": test_operator.operator_id}
    response = client.post("/lines/", json=line_data)
    assert response.status_code == status.HTTP_201_CREATED
    data = response.json()
    assert data["line_name"] == line_data["line_name"]
    assert data["operator_id"] == line_data["operator_id"]
    assert "line_id" in data


def test_create_line_duplicate_name(
    client: TestClient, db_session: Session, test_operator: Operator
):
    # Create the initial line directly in the session, but don't use the test_line fixture
    # This avoids the SAWarning because we are explicitly creating the conflicting data here.
    initial_line = Line(
        line_name="Duplicate Line Name", operator_id=test_operator.operator_id
    )
    db_session.add(initial_line)
    db_session.commit()  # Commit this initial entry
    db_session.refresh(initial_line)

    # Now attempt to create a duplicate via the API
    line_data = {
        "line_name": "Duplicate Line Name",  # Duplicate name
        "operator_id": test_operator.operator_id,
    }
    response = client.post("/lines/", json=line_data)
    assert response.status_code == status.HTTP_400_BAD_REQUEST
    assert (
        "Could not create line due to a database integrity issue (e.g., duplicate line_name)."
        in response.json()["detail"]
    )


def test_create_line_invalid_operator(client: TestClient):
    line_data = {
        "line_name": "Line with Invalid Operator",
        "operator_id": 99999,  # Non-existent operator
    }
    response = client.post("/lines/", json=line_data)
    assert response.status_code == status.HTTP_400_BAD_REQUEST
    assert "Operator with ID 99999 not found" in response.json()["detail"]


def test_read_line(client: TestClient, test_line: Line):
    response = client.get(f"/lines/{test_line.line_id}")
    assert response.status_code == status.HTTP_200_OK
    data = response.json()
    assert data["line_id"] == test_line.line_id
    assert data["line_name"] == test_line.line_name


def test_read_lines(client: TestClient, test_line: Line):
    response = client.get("/lines/")
    assert response.status_code == status.HTTP_200_OK
    data = response.json()
    assert isinstance(data, list)
    assert len(data) >= 1
    assert any(line["line_id"] == test_line.line_id for line in data)


def test_update_line(client: TestClient, test_line: Line, test_operator: Operator):
    update_data = {
        "line_name": "Updated Line Name",
        "operator_id": test_operator.operator_id,  # Can update to another valid operator
    }
    response = client.put(f"/lines/{test_line.line_id}", json=update_data)
    assert response.status_code == status.HTTP_200_OK
    data = response.json()
    assert data["line_name"] == update_data["line_name"]
    assert data["operator_id"] == update_data["operator_id"]


def test_update_line_invalid_operator(client: TestClient, test_line: Line):
    update_data = {"operator_id": 99999}  # Non-existent operator
    response = client.put(f"/lines/{test_line.line_id}", json=update_data)
    assert response.status_code == status.HTTP_400_BAD_REQUEST
    assert "Operator with ID 99999 not found" in response.json()["detail"]


def test_delete_line(client: TestClient, db_session: Session, test_line: Line):
    line_id_to_delete = test_line.line_id
    response = client.delete(f"/lines/{line_id_to_delete}")
    assert response.status_code == status.HTTP_204_NO_CONTENT

    # Verify in DB
    db_line = db_session.query(Line).filter(Line.line_id == line_id_to_delete).first()
    assert db_line is None


def test_read_nonexistent_line(client: TestClient):
    response = client.get("/lines/99999")
    assert response.status_code == status.HTTP_404_NOT_FOUND


def test_update_nonexistent_line(client: TestClient):
    update_data = {"line_name": "Non Existent Update"}
    response = client.put("/lines/99999", json=update_data)
    assert response.status_code == status.HTTP_404_NOT_FOUND


def test_delete_nonexistent_line(client: TestClient):
    response = client.delete("/lines/99999")
    assert response.status_code == status.HTTP_404_NOT_FOUND
