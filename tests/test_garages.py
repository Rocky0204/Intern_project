import pytest
from fastapi.testclient import TestClient
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from api.main import app
from api.models import Base, Garage, Bus
from api.database import get_db

# Test database setup
SQLALCHEMY_DATABASE_URL = "sqlite:///./test.db"
engine = create_engine(
    SQLALCHEMY_DATABASE_URL, connect_args={"check_same_thread": False}
)
TestingSessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)


@pytest.fixture(scope="module")
def setup_db():
    Base.metadata.create_all(bind=engine)
    yield
    Base.metadata.drop_all(bind=engine)


@pytest.fixture(scope="function")
def db_session(setup_db):
    connection = engine.connect()
    transaction = connection.begin()
    session = TestingSessionLocal(bind=connection)

    # Clear existing data
    session.query(Bus).delete()
    session.query(Garage).delete()
    session.commit()

    # Add test garage
    test_garage = Garage(
        garage_id=1, name="Main Garage", capacity=50, latitude=51.5, longitude=-0.1
    )
    session.add(test_garage)
    session.commit()

    yield session

    session.close()
    transaction.rollback()
    connection.close()


@pytest.fixture(scope="function")
def client(db_session):
    def override_get_db():
        try:
            yield db_session
        finally:
            pass

    app.dependency_overrides[get_db] = override_get_db
    yield TestClient(app)
    app.dependency_overrides.clear()


def test_create_garage(client):
    response = client.post(
        "/garages/",
        json={
            "name": "North Garage",
            "capacity": 30,
            "latitude": 51.6,
            "longitude": -0.2,
        },
    )
    assert response.status_code == 200
    data = response.json()
    assert data["name"] == "North Garage"
    assert data["capacity"] == 30


def test_read_garages(client):
    response = client.get("/garages/")
    assert response.status_code == 200
    data = response.json()
    assert len(data) >= 1
    assert any(g["name"] == "Main Garage" for g in data)


def test_read_garage(client):
    response = client.get("/garages/1")
    assert response.status_code == 200
    data = response.json()
    assert data["garage_id"] == 1
    assert data["name"] == "Main Garage"


def test_update_garage(client):
    response = client.put(
        "/garages/1", json={"name": "Updated Garage Name", "capacity": 60}
    )
    assert response.status_code == 200
    data = response.json()
    assert data["name"] == "Updated Garage Name"
    assert data["capacity"] == 60


def test_delete_garage(client, db_session):
    # First create a garage with no dependencies
    new_garage = Garage(
        garage_id=2, name="Temp Garage", capacity=10, latitude=0, longitude=0
    )
    db_session.add(new_garage)
    db_session.commit()

    # Now delete it
    response = client.delete("/garages/2")
    assert response.status_code == 200
    assert response.json()["message"] == "Garage deleted successfully"

    # Verify it's gone
    response = client.get("/garages/2")
    assert response.status_code == 404


def test_duplicate_garage_name(client):
    response = client.post(
        "/garages/",
        json={
            "name": "Main Garage",  # Duplicate of existing garage
            "capacity": 30,
            "latitude": 0,
            "longitude": 0,
        },
    )
    assert response.status_code == 400
    assert "already exists" in response.json()["detail"]


def test_delete_garage_with_buses(client, db_session):
    # First create a bus that depends on the garage
    from api.models import BusType, Operator

    # Create required dependencies
    bus_type = BusType(type_id=1, name="Double Decker", capacity=80)
    operator = Operator(operator_id=1, operator_code="OP1", name="Test Operator")
    db_session.add_all([bus_type, operator])
    db_session.commit()

    # Create a bus that depends on garage 1
    test_bus = Bus(
        bus_id="BUS001", reg_num="TEST001", bus_type_id=1, garage_id=1, operator_id=1
    )
    db_session.add(test_bus)
    db_session.commit()

    # Now try to delete the garage
    response = client.delete("/garages/1")
    assert response.status_code == 400
    assert "Cannot delete garage" in response.json()["detail"]

    # Verify garage still exists
    response = client.get("/garages/1")
    assert response.status_code == 200
