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

    db_sa = db_session.query(StopArea).filter(StopArea.stop_area_code == 1002).first()
    assert db_sa is not None
    assert db_sa.name == "North Side Terminal"


def test_create_stop_area_duplicate_admin_area_code(
    client: TestClient, test_stop_area: StopArea
):
    duplicate_data = {
        "stop_area_code": 1003,
        "admin_area_code": test_stop_area.admin_area_code, 
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
    sa2 = StopArea(
        stop_area_code=1003,
        admin_area_code="ADM003",
        name="Temp Area",
        is_terminal=False,
    )
    db_session.add(sa2)
    db_session.commit()
    db_session.refresh(sa2)

    test_sa = (
        db_session.query(StopArea).filter(StopArea.stop_area_code == 1001).first()
    )  
    if not test_sa:  
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
    }  
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
    )  

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
