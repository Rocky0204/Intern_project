import pytest
from fastapi.testclient import TestClient
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from api.main import app
from api.models import Base
from api.database import get_db
from api.models import Operator

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
    session.query(Operator).delete()
    session.commit()

    # Add test operator
    test_operator = Operator(operator_id=1, operator_code="OP1", name="Test Operator")
    session.add(test_operator)
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


def test_create_operator(client):
    response = client.post(
        "/operators/", json={"operator_code": "OP2", "name": "Second Operator"}
    )
    assert response.status_code == 200
    data = response.json()
    assert data["operator_code"] == "OP2"
    assert data["name"] == "Second Operator"


def test_read_operators(client):
    response = client.get("/operators/")
    assert response.status_code == 200
    data = response.json()
    assert len(data) >= 1
    assert any(op["operator_code"] == "OP1" for op in data)


def test_read_operator(client):
    response = client.get("/operators/1")
    assert response.status_code == 200
    data = response.json()
    assert data["operator_id"] == 1
    assert data["operator_code"] == "OP1"


def test_update_operator(client):
    response = client.put(
        "/operators/1", json={"name": "Updated Operator Name", "operator_code": "OP1U"}
    )
    assert response.status_code == 200
    data = response.json()
    assert data["name"] == "Updated Operator Name"
    assert data["operator_code"] == "OP1U"


def test_delete_operator(client, db_session):
    # First create an operator with no dependencies
    new_op = Operator(operator_id=2, operator_code="OPD", name="To Delete")
    db_session.add(new_op)
    db_session.commit()

    # Now delete it
    response = client.delete("/operators/2")
    assert response.status_code == 200
    assert response.json()["message"] == "Operator deleted successfully"

    # Verify it's gone
    response = client.get("/operators/2")
    assert response.status_code == 404


def test_duplicate_operator_code(client):
    response = client.post(
        "/operators/",
        json={
            "operator_code": "OP1",  # Duplicate of existing operator
            "name": "Duplicate Operator",
        },
    )
    assert response.status_code == 400
    assert "already exists" in response.json()["detail"]


def test_delete_operator_with_dependencies(client, db_session):
    # First create a bus that depends on the operator
    from api.models import BusType, Garage, Bus

    # Create required dependencies
    bus_type = BusType(type_id=2, name="Mini Bus", capacity=20)
    garage = Garage(
        garage_id=2, name="Test Garage", capacity=10, latitude=0, longitude=0
    )
    db_session.add_all([bus_type, garage])
    db_session.commit()

    # Create a bus that depends on operator 1
    test_bus = Bus(
        bus_id="BUS001",
        reg_num="TEST001",
        bus_type_id=2,
        garage_id=2,
        operator_id=1,  # Depends on our test operator
    )
    db_session.add(test_bus)
    db_session.commit()

    # Now try to delete the operator
    response = client.delete("/operators/1")
    assert response.status_code == 400
    error_detail = response.json()["detail"].lower()
    assert "cannot delete operator" in error_detail
    assert any(
        word in error_detail for word in ["associated", "buses", "routes", "services"]
    )
