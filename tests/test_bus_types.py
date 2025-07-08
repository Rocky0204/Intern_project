import pytest
from fastapi.testclient import TestClient
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from api.main import app
from api.models import Base, BusType, Bus, Block
from api.database import get_db

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

    session.query(Bus).delete()
    session.query(Block).delete()
    session.query(BusType).delete()
    session.commit()

    test_type = BusType(type_id=1, name="Double Decker", capacity=80)
    session.add(test_type)
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


def test_create_bus_type(client):
    response = client.post("/bus-types/", json={"name": "Minibus", "capacity": 20})
    assert response.status_code == 200
    data = response.json()
    assert data["name"] == "Minibus"
    assert data["capacity"] == 20


def test_read_bus_types(client):
    response = client.get("/bus-types/")
    assert response.status_code == 200
    data = response.json()
    assert len(data) >= 1
    assert any(t["name"] == "Double Decker" for t in data)


def test_read_bus_type(client):
    response = client.get("/bus-types/1")
    assert response.status_code == 200
    data = response.json()
    assert data["type_id"] == 1
    assert data["name"] == "Double Decker"


def test_update_bus_type(client):
    response = client.put(
        "/bus-types/1", json={"name": "Updated Bus Type", "capacity": 85}
    )
    assert response.status_code == 200
    data = response.json()
    assert data["name"] == "Updated Bus Type"
    assert data["capacity"] == 85


def test_delete_bus_type(client, db_session):
    new_type = BusType(type_id=2, name="Temp Type", capacity=10)
    db_session.add(new_type)
    db_session.commit()

    response = client.delete("/bus-types/2")
    assert response.status_code == 200
    assert response.json()["message"] == "Bus type deleted successfully"

    response = client.get("/bus-types/2")
    assert response.status_code == 404


def test_duplicate_bus_type_name(client):
    response = client.post(
        "/bus-types/",
        json={
            "name": "Double Decker",  
            "capacity": 30,
        },
    )
    assert response.status_code == 400
    assert "already exists" in response.json()["detail"]


def test_delete_bus_type_with_dependencies(client, db_session):
    from api.models import Garage, Operator, Bus

    garage = Garage(
        garage_id=1, name="Test Garage", capacity=10, latitude=0, longitude=0
    )
    operator = Operator(operator_id=1, operator_code="OP1", name="Test Operator")
    db_session.add_all([garage, operator])
    db_session.commit()

    test_bus = Bus(
        bus_id="BUS001", reg_num="TEST001", bus_type_id=1, garage_id=1, operator_id=1
    )
    db_session.add(test_bus)
    db_session.commit()

    response = client.delete("/bus-types/1")
    assert response.status_code == 400
    assert "Cannot delete bus type" in response.json()["detail"]

    response = client.get("/bus-types/1")
    assert response.status_code == 200
