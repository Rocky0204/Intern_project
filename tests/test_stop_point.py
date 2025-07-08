import pytest
from fastapi import status
from fastapi.testclient import TestClient
from sqlalchemy import create_engine, event
from sqlalchemy.orm import sessionmaker, Session

import os
import sys

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "../")))


from api.main import app
from api.database import get_db

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

SQLALCHEMY_DATABASE_URL = "sqlite:///:memory:"

engine = create_engine(
    SQLALCHEMY_DATABASE_URL, connect_args={"check_same_thread": False}
)


@event.listens_for(engine, "connect")
def set_sqlite_pragma(dbapi_connection, connection_record):
    cursor = dbapi_connection.cursor()
    cursor.execute("PRAGMA foreign_keys=ON;")
    cursor.close()


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
    session.query(Garage).delete()
    session.query(StopPoint).delete()
    session.query(StopArea).delete()
    session.commit()

    try:
        yield session
    finally:
        session.close()
        transaction.rollback()
        connection.close()


@pytest.fixture(scope="function")
def client(db_session: Session):
    def override_get_db():
        try:
            yield db_session
        finally:
            pass

    app.dependency_overrides[get_db] = override_get_db
    with TestClient(app) as c:
        yield c
    app.dependency_overrides.clear()


@pytest.fixture(scope="function")
def test_stop_area(db_session: Session):
    stop_area = StopArea(
        stop_area_code=1001,
        admin_area_code="ADM_SP_001",
        name="Test Stop Area for SP",
        is_terminal=True,
    )
    db_session.add(stop_area)
    db_session.commit()
    db_session.refresh(stop_area)
    return stop_area


@pytest.fixture(scope="function")
def test_stop_point(db_session: Session, test_stop_area: StopArea):
    """Creates and returns a test StopPoint."""
    stop_point = StopPoint(
        atco_code=2001,
        name="Test Stop Point 1",
        latitude=51.50,
        longitude=-0.10,
        stop_area_code=test_stop_area.stop_area_code,
    )
    db_session.add(stop_point)
    db_session.commit()
    db_session.refresh(stop_point)
    return stop_point


def test_create_stop_point(
    client: TestClient, db_session: Session, test_stop_area: StopArea
):
    stop_point_data = {
        "atco_code": 2002,
        "name": "New Stop Point",
        "latitude": 51.51,
        "longitude": -0.11,
        "stop_area_code": test_stop_area.stop_area_code,
    }
    response = client.post("/stop_points/", json=stop_point_data)
    assert response.status_code == status.HTTP_201_CREATED
    data = response.json()
    assert data["atco_code"] == stop_point_data["atco_code"]
    assert data["name"] == stop_point_data["name"]
    assert data["latitude"] == stop_point_data["latitude"]
    assert data["longitude"] == stop_point_data["longitude"]
    assert data["stop_area_code"] == stop_point_data["stop_area_code"]

    # Verify in DB
    db_sp = db_session.query(StopPoint).filter(StopPoint.atco_code == 2002).first()
    assert db_sp is not None
    assert db_sp.name == "New Stop Point"


def test_create_stop_point_invalid_stop_area(client: TestClient, db_session: Session):
    stop_point_data = {
        "atco_code": 2003,
        "name": "Invalid Stop Point",
        "latitude": 51.52,
        "longitude": -0.12,
        "stop_area_code": 99999,
    }
    response = client.post("/stop_points/", json=stop_point_data)
    assert response.status_code == status.HTTP_400_BAD_REQUEST
    assert "StopArea with code 99999 not found." in response.json()["detail"]


def test_read_stop_point(client: TestClient, test_stop_point: StopPoint):
    response = client.get(f"/stop_points/{test_stop_point.atco_code}")
    assert response.status_code == status.HTTP_200_OK
    data = response.json()
    assert data["atco_code"] == test_stop_point.atco_code
    assert data["name"] == test_stop_point.name


def test_read_stop_points(client: TestClient, test_stop_point: StopPoint):
    response = client.get("/stop_points/")
    assert response.status_code == status.HTTP_200_OK
    data = response.json()
    assert isinstance(data, list)
    assert len(data) >= 1
    assert any(sp["atco_code"] == test_stop_point.atco_code for sp in data)


def test_update_stop_point(
    client: TestClient,
    db_session: Session,
    test_stop_point: StopPoint,
    test_stop_area: StopArea,
):
    new_stop_area = StopArea(
        stop_area_code=1002,
        admin_area_code="ADM_SP_002",
        name="Another Stop Area for SP",
        is_terminal=False,
    )
    db_session.add(new_stop_area)
    db_session.commit()
    db_session.refresh(new_stop_area)

    update_data = {
        "name": "Updated Stop Point Name",
        "latitude": 51.55,
        "stop_area_code": new_stop_area.stop_area_code,
    }
    response = client.put(f"/stop_points/{test_stop_point.atco_code}", json=update_data)
    assert response.status_code == status.HTTP_200_OK
    data = response.json()
    assert data["name"] == update_data["name"]
    assert data["latitude"] == update_data["latitude"]
    assert data["stop_area_code"] == update_data["stop_area_code"]

    db_sp = (
        db_session.query(StopPoint)
        .filter(StopPoint.atco_code == test_stop_point.atco_code)
        .first()
    )
    assert db_sp.name == update_data["name"]
    assert db_sp.latitude == update_data["latitude"]
    assert db_sp.stop_area_code == update_data["stop_area_code"]


def test_update_stop_point_invalid_stop_area(
    client: TestClient, test_stop_point: StopPoint
):
    update_data = {"stop_area_code": 99999}
    response = client.put(f"/stop_points/{test_stop_point.atco_code}", json=update_data)
    assert response.status_code == status.HTTP_400_BAD_REQUEST
    assert "StopArea with code 99999 not found." in response.json()["detail"]


def test_delete_stop_point(
    client: TestClient, db_session: Session, test_stop_point: StopPoint
):
    atco_code_to_delete = test_stop_point.atco_code
    response = client.delete(f"/stop_points/{atco_code_to_delete}")
    assert response.status_code == status.HTTP_204_NO_CONTENT

    db_sp = (
        db_session.query(StopPoint)
        .filter(StopPoint.atco_code == atco_code_to_delete)
        .first()
    )
    assert db_sp is None


def test_read_nonexistent_stop_point(client: TestClient):
    response = client.get("/stop_points/99999")
    assert response.status_code == status.HTTP_404_NOT_FOUND


def test_update_nonexistent_stop_point(client: TestClient):
    update_data = {"name": "Non Existent Update"}
    response = client.put("/stop_points/99999", json=update_data)
    assert response.status_code == status.HTTP_404_NOT_FOUND


def test_delete_nonexistent_stop_point(client: TestClient):
    response = client.delete("/stop_points/99999")
    assert response.status_code == status.HTTP_404_NOT_FOUND
