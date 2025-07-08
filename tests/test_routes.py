import pytest
from fastapi.testclient import TestClient
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from api.main import app
from api.models import (
    Base,
    Route,
    Operator,
    RouteDefinition,
    JourneyPattern,
    StopArea,
    StopPoint,
)
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

    session.query(RouteDefinition).delete()
    session.query(JourneyPattern).delete()
    session.query(Route).delete()
    session.query(Operator).delete()
    session.query(StopPoint).delete()
    session.query(StopArea).delete()
    session.commit()

    test_operator = Operator(operator_id=1, operator_code="OP1", name="Test Operator")
    session.add(test_operator)
    session.commit()

    test_route = Route(
        route_id=1, name="Route 101", operator_id=1, description="Main city route"
    )
    session.add(test_route)
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


def test_create_route(client):
    response = client.post(
        "/routes/",
        json={"name": "Route 202", "operator_id": 1, "description": "Secondary route"},
    )
    assert response.status_code == 200
    data = response.json()
    assert data["name"] == "Route 202"
    assert data["operator_id"] == 1


def test_read_routes(client):
    response = client.get("/routes/")
    assert response.status_code == 200
    data = response.json()
    assert len(data) >= 1
    assert any(r["name"] == "Route 101" for r in data)


def test_read_route(client):
    response = client.get("/routes/1")
    assert response.status_code == 200
    data = response.json()
    assert data["route_id"] == 1
    assert data["name"] == "Route 101"


def test_update_route(client):
    response = client.put(
        "/routes/1",
        json={"name": "Updated Route 101", "description": "Updated description"},
    )
    assert response.status_code == 200
    data = response.json()
    assert data["name"] == "Updated Route 101"
    assert data["description"] == "Updated description"


def test_delete_route(client, db_session):
    new_route = Route(route_id=2, name="Temp Route", operator_id=1)
    db_session.add(new_route)
    db_session.commit()

    response = client.delete("/routes/2")
    assert response.status_code == 200
    assert response.json()["message"] == "Route deleted successfully"

    response = client.get("/routes/2")
    assert response.status_code == 404


def test_create_route_invalid_operator(client):
    response = client.post(
        "/routes/",
        json={
            "name": "Invalid Route",
            "operator_id": 999,
            "description": "Should fail",
        },
    )
    assert response.status_code == 400
    assert "Operator not found" in response.json()["detail"]


def test_delete_route_with_dependencies(client, db_session):
    stop_area = StopArea(
        stop_area_code=1, admin_area_code="A1", name="Test Area", is_terminal=True
    )
    stop_point = StopPoint(
        atco_code=1, name="Test Stop", latitude=0, longitude=0, stop_area_code=1
    )
    db_session.add_all([stop_area, stop_point])
    db_session.commit()

    route_def = RouteDefinition(route_id=1, stop_point_id=1, sequence=1)
    db_session.add(route_def)
    db_session.commit()

    response = client.delete("/routes/1")
    assert response.status_code == 400
    assert "Cannot delete route" in response.json()["detail"]

    response = client.get("/routes/1")
    assert response.status_code == 200
