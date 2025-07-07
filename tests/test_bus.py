import pytest
from fastapi.testclient import TestClient
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.exc import IntegrityError

from api.main import app
from api.models import Base
from api.database import get_db
from api.models import Bus, BusType, Garage, Operator

# Test database setup
SQLALCHEMY_DATABASE_URL = "sqlite:///./test.db"
engine = create_engine(
    SQLALCHEMY_DATABASE_URL, connect_args={"check_same_thread": False}
)
TestingSessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)


@pytest.fixture(scope="module")
def setup_db():
    # Create all tables
    Base.metadata.create_all(bind=engine)
    yield
    # Drop all tables
    Base.metadata.drop_all(bind=engine)


@pytest.fixture(scope="function")
def db_session(setup_db):
    # Start a new transaction for each test
    connection = engine.connect()
    transaction = connection.begin()
    session = TestingSessionLocal(bind=connection)

    # Create prerequisite data
    try:
        # Clear existing data first
        session.query(Bus).delete()
        session.query(BusType).delete()
        session.query(Garage).delete()
        session.query(Operator).delete()

        # Add fresh test data
        bus_type = BusType(type_id=1, name="Double Decker", capacity=80)
        garage = Garage(
            garage_id=1, name="Main Garage", capacity=50, latitude=51.5, longitude=-0.1
        )
        operator = Operator(operator_id=1, operator_code="ABC", name="ABC Buses")

        session.add_all([bus_type, garage, operator])
        session.commit()

        # Create test bus
        test_bus = Bus(
            bus_id="BUS001", reg_num="ABC123", bus_type_id=1, garage_id=1, operator_id=1
        )
        session.add(test_bus)
        session.commit()
    except IntegrityError:
        session.rollback()
        raise

    yield session

    # Rollback transaction after test
    session.close()
    transaction.rollback()
    connection.close()


@pytest.fixture(scope="function")
def client(db_session):
    # Dependency override
    def override_get_db():
        try:
            yield db_session
        finally:
            pass  # Don't close session here - handled in db_session fixture

    app.dependency_overrides[get_db] = override_get_db
    yield TestClient(app)
    app.dependency_overrides.clear()


def test_create_bus(client):
    response = client.post(
        "/buses/",
        json={
            "bus_id": "BUS002",
            "reg_num": "XYZ789",
            "bus_type_id": 1,
            "garage_id": 1,
            "operator_id": 1,
        },
    )
    assert response.status_code == 200
    data = response.json()
    assert data["bus_id"] == "BUS002"
    assert data["reg_num"] == "XYZ789"


def test_read_buses(client):
    response = client.get("/buses/")
    assert response.status_code == 200
    data = response.json()
    assert len(data) >= 1
    assert any(bus["bus_id"] == "BUS001" for bus in data)


def test_read_bus(client):
    response = client.get("/buses/BUS001")
    assert response.status_code == 200
    data = response.json()
    assert data["bus_id"] == "BUS001"
    assert data["reg_num"] == "ABC123"


def test_update_bus(client):
    response = client.put(
        "/buses/BUS001", json={"registration_number": "NEW123", "garage_id": 1}
    )
    assert response.status_code == 200
    data = response.json()
    assert data["reg_num"] == "NEW123"


def test_delete_bus(client):
    # First create a bus to delete
    client.post(
        "/buses/",
        json={
            "bus_id": "BUS003",
            "reg_num": "DEL123",
            "bus_type_id": 1,
            "garage_id": 1,
            "operator_id": 1,
        },
    )

    # Now delete it
    response = client.delete("/buses/BUS003")
    assert response.status_code == 200
    assert response.json()["message"] == "Bus deleted successfully"

    # Verify it's gone
    response = client.get("/buses/BUS003")
    assert response.status_code == 404


def test_duplicate_registration(client):
    response = client.post(
        "/buses/",
        json={
            "bus_id": "BUS004",
            "reg_num": "ABC123",  # Duplicate of BUS001's reg_num
            "bus_type_id": 1,
            "garage_id": 1,
            "operator_id": 1,
        },
    )
    assert response.status_code == 400
    assert "already exists" in response.json()["detail"]
