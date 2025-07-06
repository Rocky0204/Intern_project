# tests/test_services.py
import pytest
from fastapi import status
from fastapi.testclient import TestClient
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, Session

# Add these lines to fix module import paths
import os
import sys
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../')))


from api.main import app
from api.database import get_db
# Removed Bus, BusType from here as they are not directly used in service tests
from api.models import Base, Service, Operator, Line
from api.schemas import ServiceCreate, ServiceRead

# Test database setup
SQLALCHEMY_DATABASE_URL = "sqlite:///./test.db"
engine = create_engine(
    SQLALCHEMY_DATABASE_URL,
    connect_args={"check_same_thread": False}
)
TestingSessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

@pytest.fixture(scope="module")
def setup_db():
    """Sets up and tears down the test database for the module."""
    Base.metadata.create_all(bind=engine)
    yield
    Base.metadata.drop_all(bind=engine)

@pytest.fixture(scope="function")
def db_session(setup_db):
    """Provides a transactional database session for each test function."""
    connection = engine.connect()
    transaction = connection.begin()
    session = TestingSessionLocal(bind=connection)

    # Clean up data before each test to ensure isolation
    session.query(Service).delete()
    session.query(Line).delete()
    session.query(Operator).delete()
    # Commented out Bus and BusType deletion as they are not imported/used
    # session.query(Bus).delete()
    # session.query(BusType).delete()
    session.commit()

    try:
        yield session
    finally:
        session.close()
        transaction.rollback() # Rollback transaction to isolate tests
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
    app.dependency_overrides.clear() # Clear overrides after the test

# Your existing fixtures, now correctly using db_session
@pytest.fixture
def test_operator(db_session: Session):
    operator = Operator(operator_code="OP1", name="Test Operator")
    db_session.add(operator)
    db_session.commit()
    db_session.refresh(operator)
    return operator

@pytest.fixture
def test_line(db_session: Session, test_operator: Operator):
    line = Line(line_name="L1", operator_id=test_operator.operator_id)
    db_session.add(line)
    db_session.commit()
    db_session.refresh(line)
    return line

@pytest.fixture
def test_service(db_session: Session, test_operator: Operator, test_line: Line):
    service = Service(
        service_code="S1",
        name="Test Service",
        description="Test Description",
        operator_id=test_operator.operator_id,
        line_id=test_line.line_id
    )
    db_session.add(service)
    db_session.commit()
    db_session.refresh(service)
    return service

# Update your test functions to use the new `client` and `db_session` fixtures
def test_create_service(client: TestClient, db_session: Session, test_operator: Operator, test_line: Line):
    service_data = {
        "service_code": "S2",
        "name": "New Service",
        "description": "A newly created service",
        "operator_id": test_operator.operator_id,
        "line_id": test_line.line_id
    }
    response = client.post("/services/", json=service_data)
    assert response.status_code == status.HTTP_200_OK
    data = response.json()
    assert data["service_code"] == service_data["service_code"]
    assert data["name"] == service_data["name"]
    assert "service_id" in data

    # Verify the service is in the database
    db_service = db_session.query(Service).filter(Service.service_code == "S2").first()
    assert db_service is not None
    assert db_service.name == "New Service"

def test_read_service(client: TestClient, test_service: Service):
    response = client.get(f"/services/{test_service.service_id}")
    assert response.status_code == status.HTTP_200_OK
    data = response.json()
    assert data["service_code"] == test_service.service_code
    assert data["name"] == test_service.name

def test_read_services(client: TestClient, test_service: Service):
    response = client.get("/services/")
    assert response.status_code == status.HTTP_200_OK
    data = response.json()
    assert isinstance(data, list)
    assert len(data) > 0
    assert any(s["service_code"] == test_service.service_code for s in data)

def test_update_service(client: TestClient, db_session: Session, test_service: Service):
    update_data = {"name": "Updated Test Service", "description": "New description"}
    response = client.put(f"/services/{test_service.service_id}", json=update_data)
    assert response.status_code == status.HTTP_200_OK
    data = response.json()
    assert data["name"] == update_data["name"]
    assert data["description"] == update_data["description"]

    # Verify the update in the database
    db_service = db_session.query(Service).filter(Service.service_id == test_service.service_id).first()
    assert db_service.name == update_data["name"]
    assert db_service.description == update_data["description"]

def test_delete_service(client: TestClient, db_session: Session, test_service: Service):
    service_id_to_delete = test_service.service_id
    response = client.delete(f"/services/{service_id_to_delete}")
    assert response.status_code == status.HTTP_200_OK
    assert response.json() == {"message": "Service deleted successfully"}

    # Verify the service is deleted from the database
    db_service = db_session.query(Service).filter(Service.service_id == service_id_to_delete).first()
    assert db_service is None

def test_read_nonexistent_service(client: TestClient):
    non_existent_id = 99999
    response = client.get(f"/services/{non_existent_id}")
    assert response.status_code == status.HTTP_404_NOT_FOUND

def test_update_nonexistent_service(client: TestClient):
    non_existent_id = 99999
    update_data = {"name": "Updated Name"}
    response = client.put(f"/services/{non_existent_id}", json=update_data)
    assert response.status_code == status.HTTP_404_NOT_FOUND

def test_delete_nonexistent_service(client: TestClient):
    non_existent_id = 99999
    response = client.delete(f"/services/{non_existent_id}")
    assert response.status_code == status.HTTP_404_NOT_FOUND

def test_create_service_with_invalid_operator(client: TestClient, db_session: Session, test_line: Line):
    invalid_data = {
        "service_code": "S3",
        "name": "Invalid Service",
        "operator_id": 99999,  # Non-existent operator
        "line_id": test_line.line_id
    }
    
    response = client.post("/services/", json=invalid_data)
    assert response.status_code == status.HTTP_400_BAD_REQUEST 
    assert "Operator with ID 99999 not found." in response.json()["detail"]

def test_create_service_with_invalid_line(client: TestClient, db_session: Session, test_operator: Operator):
    invalid_data = {
        "service_code": "S4",
        "name": "Invalid Service",
        "operator_id": test_operator.operator_id,
        "line_id": 99999  # Non-existent line
    }
    
    response = client.post("/services/", json=invalid_data)
    assert response.status_code == status.HTTP_400_BAD_REQUEST
    assert "Line with ID 99999 not found." in response.json()["detail"]