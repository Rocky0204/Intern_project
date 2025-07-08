import pytest
from fastapi import status
from fastapi.testclient import TestClient
from sqlalchemy import create_engine, event
from sqlalchemy.orm import sessionmaker, Session
from datetime import time
import os
import sys

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "../")))


from api.main import app
from api.database import get_db

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
    session.commit()

    try:
        yield session
    finally:
        transaction.rollback()

        session.close()
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
def test_operator(db_session: Session):
    operator = Operator(operator_code="OP1", name="Test Operator")
    db_session.add(operator)
    db_session.commit()
    db_session.refresh(operator)
    return operator


@pytest.fixture(scope="function")
def test_line(db_session: Session, test_operator: Operator):
    line = Line(line_name="L1", operator_id=test_operator.operator_id)
    db_session.add(line)
    db_session.commit()
    db_session.refresh(line)
    return line


@pytest.fixture(scope="function")
def test_service(db_session: Session, test_operator: Operator, test_line: Line):
    service = Service(
        service_code="S1",
        name="Test Service",
        description="Test Description",
        operator_id=test_operator.operator_id,
        line_id=test_line.line_id,
    )
    db_session.add(service)
    db_session.commit()
    db_session.refresh(service)
    return service


@pytest.fixture(scope="function")
def test_route(db_session: Session, test_operator: Operator):
    route = Route(name="Test Route", operator_id=test_operator.operator_id)
    db_session.add(route)
    db_session.commit()
    db_session.refresh(route)
    return route


@pytest.fixture(scope="function")
def test_journey_pattern(
    db_session: Session,
    test_route: Route,
    test_service: Service,
    test_line: Line,
    test_operator: Operator,
):
    jp = JourneyPattern(
        jp_code="JP1",
        name="Test Journey Pattern 1",
        route_id=test_route.route_id,
        service_id=test_service.service_id,
        line_id=test_line.line_id,
        operator_id=test_operator.operator_id,
    )
    db_session.add(jp)
    db_session.commit()
    db_session.refresh(jp)
    return jp


@pytest.fixture(scope="function")
def test_bus_type(db_session: Session):
    bus_type = BusType(name="Standard", capacity=50)
    db_session.add(bus_type)
    db_session.commit()
    db_session.refresh(bus_type)
    return bus_type


@pytest.fixture(scope="function")
def test_block(db_session: Session, test_operator: Operator, test_bus_type: BusType):
    block = Block(
        name="Test Block 1",
        operator_id=test_operator.operator_id,
        bus_type_id=test_bus_type.type_id,
    )
    db_session.add(block)
    db_session.commit()
    db_session.refresh(block)
    return block


@pytest.fixture(scope="function")
def test_vehicle_journey(
    db_session: Session,
    test_journey_pattern: JourneyPattern,
    test_block: Block,
    test_operator: Operator,
    test_line: Line,
    test_service: Service,
):
    vj = VehicleJourney(
        departure_time=time(8, 0, 0),
        dayshift=1,
        jp_id=test_journey_pattern.jp_id,
        block_id=test_block.block_id,
        operator_id=test_operator.operator_id,
        line_id=test_line.line_id,
        service_id=test_service.service_id,
    )
    db_session.add(vj)
    db_session.commit()
    db_session.refresh(vj)
    return vj


def test_create_vehicle_journey(
    client: TestClient,
    test_journey_pattern: JourneyPattern,
    test_block: Block,
    test_operator: Operator,
    test_line: Line,
    test_service: Service,
):
    vj_data = {
        "departure_time": "09:00:00",
        "dayshift": 2,
        "jp_id": test_journey_pattern.jp_id,
        "block_id": test_block.block_id,
        "operator_id": test_operator.operator_id,
        "line_id": test_line.line_id,
        "service_id": test_service.service_id,
    }
    response = client.post("/vehicle_journeys/", json=vj_data)
    assert response.status_code == status.HTTP_201_CREATED
    data = response.json()
    assert data["departure_time"] == vj_data["departure_time"]
    assert data["dayshift"] == vj_data["dayshift"]
    assert data["jp_id"] == vj_data["jp_id"]
    assert data["block_id"] == vj_data["block_id"]
    assert data["operator_id"] == vj_data["operator_id"]
    assert data["line_id"] == vj_data["line_id"]
    assert data["service_id"] == vj_data["service_id"]
    assert "vj_id" in data


def test_create_vehicle_journey_invalid_jp_id(
    client: TestClient,
    test_block: Block,
    test_operator: Operator,
    test_line: Line,
    test_service: Service,
):
    vj_data = {
        "departure_time": "10:00:00",
        "dayshift": 1,
        "jp_id": 99999,
        "block_id": test_block.block_id,
        "operator_id": test_operator.operator_id,
        "line_id": test_line.line_id,
        "service_id": test_service.service_id,
    }
    response = client.post("/vehicle_journeys/", json=vj_data)
    assert response.status_code == status.HTTP_400_BAD_REQUEST
    assert "JourneyPattern with ID 99999 not found." in response.json()["detail"]


def test_create_vehicle_journey_invalid_block_id(
    client: TestClient,
    test_journey_pattern: JourneyPattern,
    test_operator: Operator,
    test_line: Line,
    test_service: Service,
):
    vj_data = {
        "departure_time": "10:00:00",
        "dayshift": 1,
        "jp_id": test_journey_pattern.jp_id,
        "block_id": 99999,
        "operator_id": test_operator.operator_id,
        "line_id": test_line.line_id,
        "service_id": test_service.service_id,
    }
    response = client.post("/vehicle_journeys/", json=vj_data)
    assert response.status_code == status.HTTP_400_BAD_REQUEST
    assert "Block with ID 99999 not found." in response.json()["detail"]


def test_read_vehicle_journey(client: TestClient, test_vehicle_journey: VehicleJourney):
    response = client.get(f"/vehicle_journeys/{test_vehicle_journey.vj_id}")
    assert response.status_code == status.HTTP_200_OK
    data = response.json()
    assert data["vj_id"] == test_vehicle_journey.vj_id
    assert data["departure_time"] == test_vehicle_journey.departure_time.isoformat()
    assert data["dayshift"] == test_vehicle_journey.dayshift


def test_read_vehicle_journeys(
    client: TestClient, test_vehicle_journey: VehicleJourney
):
    response = client.get("/vehicle_journeys/")
    assert response.status_code == status.HTTP_200_OK
    data = response.json()
    assert isinstance(data, list)
    assert len(data) >= 1
    assert any(vj["vj_id"] == test_vehicle_journey.vj_id for vj in data)


def test_update_vehicle_journey(
    client: TestClient,
    test_vehicle_journey: VehicleJourney,
    test_journey_pattern: JourneyPattern,
    test_block: Block,
    test_operator: Operator,
    test_line: Line,
    test_service: Service,
):
    update_data = {
        "departure_time": "11:30:00",
        "dayshift": 3,
        "jp_id": test_journey_pattern.jp_id,
        "block_id": test_block.block_id,
        "operator_id": test_operator.operator_id,
        "line_id": test_line.line_id,
        "service_id": test_service.service_id,
    }
    response = client.put(
        f"/vehicle_journeys/{test_vehicle_journey.vj_id}", json=update_data
    )
    assert response.status_code == status.HTTP_200_OK
    data = response.json()
    assert data["departure_time"] == update_data["departure_time"]
    assert data["dayshift"] == update_data["dayshift"]


def test_update_vehicle_journey_invalid_jp_id(
    client: TestClient, test_vehicle_journey: VehicleJourney
):
    update_data = {"jp_id": 99999}
    response = client.put(
        f"/vehicle_journeys/{test_vehicle_journey.vj_id}", json=update_data
    )
    assert response.status_code == status.HTTP_400_BAD_REQUEST
    assert "JourneyPattern with ID 99999 not found." in response.json()["detail"]


def test_delete_vehicle_journey(
    client: TestClient, db_session: Session, test_vehicle_journey: VehicleJourney
):
    vj_id_to_delete = test_vehicle_journey.vj_id
    response = client.delete(f"/vehicle_journeys/{vj_id_to_delete}")
    assert response.status_code == status.HTTP_204_NO_CONTENT
    db_vj = (
        db_session.query(VehicleJourney)
        .filter(VehicleJourney.vj_id == vj_id_to_delete)
        .first()
    )
    assert db_vj is None


def test_read_nonexistent_vehicle_journey(client: TestClient):
    response = client.get("/vehicle_journeys/99999")
    assert response.status_code == status.HTTP_404_NOT_FOUND


def test_update_nonexistent_vehicle_journey(client: TestClient):
    update_data = {"departure_time": "12:00:00"}
    response = client.put("/vehicle_journeys/99999", json=update_data)
    assert response.status_code == status.HTTP_404_NOT_FOUND


def test_delete_nonexistent_vehicle_journey(client: TestClient):
    response = client.delete("/vehicle_journeys/99999")
    assert response.status_code == status.HTTP_404_NOT_FOUND
