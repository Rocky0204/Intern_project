import pytest
from fastapi.testclient import TestClient
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, Session
from sqlalchemy.pool import StaticPool
from datetime import datetime, time, timezone

from api.models import (
    Base,
    EmulatorLog,
    Demand,
    StopArea,
    StopPoint,
    BusType,
    Operator,
    Garage,
    Bus,
    Route,
    RouteDefinition,
)
from api.schemas import RunStatus
from api.main import app
from api.database import get_db

TEST_DATABASE_URL = "sqlite:///:memory:"


@pytest.fixture(scope="function")
def test_db_session():
    engine = create_engine(
        TEST_DATABASE_URL,
        connect_args={"check_same_thread": False},
        poolclass=StaticPool,
    )
    Base.metadata.create_all(bind=engine)

    connection = engine.connect()
    transaction = connection.begin()

    SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
    db = SessionLocal()

    try:
        yield db
    finally:
        db.close()
        transaction.rollback()
        connection.close()
        Base.metadata.drop_all(bind=engine)


@pytest.fixture(scope="function")
def client_with_db(test_db_session: Session):
    def override_get_db():
        try:
            yield test_db_session
        finally:
            pass

    app.dependency_overrides[get_db] = override_get_db
    with TestClient(app) as client:
        yield client
    app.dependency_overrides.clear()


def _populate_db_for_simulation(db: Session):
    db.query(Demand).delete()
    db.query(RouteDefinition).delete()
    db.query(Route).delete()
    db.query(Bus).delete()
    db.query(StopPoint).delete()
    db.query(StopArea).delete()
    db.query(Garage).delete()
    db.query(Operator).delete()
    db.query(BusType).delete()
    db.commit()

    operator = Operator(operator_id=1, operator_code="OP1", name="Operator One")
    garage = Garage(
        garage_id=1, name="Central Garage", latitude=0.0, longitude=0.0, capacity=100
    )
    bus_type = BusType(type_id=1, name="Standard Bus", capacity=50)
    bus = Bus(
        bus_id="bus1", reg_num="REG123", bus_type_id=1, garage_id=1, operator_id=1
    )

    db.add_all([operator, garage, bus_type, bus])
    db.commit()

    stop_area1 = StopArea(
        stop_area_code=1, admin_area_code="ADM1", name="Area 1", is_terminal=True
    )  # is_terminal=True for depot
    stop_area2 = StopArea(stop_area_code=2, admin_area_code="ADM2", name="Area 2")
    db.add_all([stop_area1, stop_area2])
    db.commit()

    stop1 = StopPoint(
        atco_code=1001, name="Stop A", latitude=51.5, longitude=0.0, stop_area_code=1
    )
    stop2 = StopPoint(
        atco_code=1002, name="Stop B", latitude=51.6, longitude=0.1, stop_area_code=1
    )
    db.add_all([stop1, stop2])
    db.commit()

    route = Route(route_id=1, name="Route 1", operator_id=1)
    db.add(route)
    db.commit()

    route_def1 = RouteDefinition(route_id=1, stop_point_id=1001, sequence=1)
    route_def2 = RouteDefinition(route_id=1, stop_point_id=1002, sequence=2)
    db.add_all([route_def1, route_def2])
    db.commit()

    demand1 = Demand(
        origin=1001,
        destination=1002,
        count=10,
        start_time=time(8, 0),
        end_time=time(9, 0),
    )
    demand2 = Demand(
        origin=1002,
        destination=1001,
        count=5,
        start_time=time(9, 30),
        end_time=time(10, 30),
    )
    db.add_all([demand1, demand2])
    db.commit()


def test_create_emulator_log(client_with_db: TestClient, test_db_session: Session):
    test_db_session.query(EmulatorLog).delete()
    test_db_session.commit()

    response = client_with_db.post(
        "/emulator_logs/", json={"status": RunStatus.RUNNING.value}
    )
    assert response.status_code == 201
    assert response.json()["status"] == RunStatus.RUNNING.value

    assert test_db_session.query(EmulatorLog).count() == 1
    assert "run_id" in response.json()
    assert "started_at" in response.json()
    assert "last_updated" in response.json()


def test_update_emulator_log_and_run_simulation_patch_exception(
    client_with_db: TestClient, test_db_session: Session, mocker
):
    _populate_db_for_simulation(test_db_session)

    db_log = EmulatorLog(
        status=RunStatus.QUEUED.value, started_at=datetime.now(timezone.utc)
    )
    test_db_session.add(db_log)
    test_db_session.commit()
    test_db_session.refresh(db_log)

    mock_bus_emulator_class = mocker.patch(
        "api.routers.emulator_log.BusEmulator", autospec=True
    )
    mock_emulator_instance = mock_bus_emulator_class.return_value
    mock_emulator_instance.run_simulation.side_effect = Exception(
        "Test error during simulation"
    )

    response = client_with_db.patch(
        f"/emulator_logs/{db_log.run_id}/run_simulation",
        json={
            "use_optimized_schedule": True,
            "start_time_minutes": 0,
            "end_time_minutes": 1440,
        },
    )

    assert response.status_code == 200
    assert response.json()["status"] == RunStatus.FAILED.value
    assert response.json()["optimization_details"]["status"] == "ERROR"
    assert (
        "Test error during simulation"
        in response.json()["optimization_details"]["message"]
    )

    updated_db_log = (
        test_db_session.query(EmulatorLog)
        .filter(EmulatorLog.run_id == db_log.run_id)
        .first()
    )
    assert updated_db_log.status == RunStatus.FAILED.value
    assert updated_db_log.optimization_details_dict["status"] == "ERROR"
    assert (
        "Test error during simulation"
        in updated_db_log.optimization_details_dict["message"]
    )


def test_update_emulator_log_and_run_simulation_patch_success(
    client_with_db: TestClient, test_db_session: Session, mocker
):
    _populate_db_for_simulation(test_db_session)

    db_log = EmulatorLog(
        status=RunStatus.QUEUED.value, started_at=datetime.now(timezone.utc)
    )
    test_db_session.add(db_log)
    test_db_session.commit()
    test_db_session.refresh(db_log)

    mock_bus_emulator_class = mocker.patch(
        "api.routers.emulator_log.BusEmulator", autospec=True
    )
    mock_emulator_instance = mock_bus_emulator_class.return_value
    mock_emulator_instance.run_simulation.return_value = {
        "status": "Success",
        "optimization_details": {
            "status": "Success",
            "message": "Simulation ran perfectly",
            "total_buses": 5,
        },
    }

    response = client_with_db.patch(
        f"/emulator_logs/{db_log.run_id}/run_simulation",
        json={
            "use_optimized_schedule": True,
            "start_time_minutes": 0,
            "end_time_minutes": 1440,
        },
    )

    assert response.status_code == 200
    assert response.json()["status"] == RunStatus.COMPLETED.value
    assert response.json()["optimization_details"]["status"] == "Success"
    assert (
        response.json()["optimization_details"]["message"] == "Simulation ran perfectly"
    )

    updated_db_log = (
        test_db_session.query(EmulatorLog)
        .filter(EmulatorLog.run_id == db_log.run_id)
        .first()
    )
    assert updated_db_log.status == RunStatus.COMPLETED.value
    assert updated_db_log.optimization_details_dict["status"] == "Success"
    assert (
        updated_db_log.optimization_details_dict["message"]
        == "Simulation ran perfectly"
    )
    assert updated_db_log.optimization_details_dict.get("total_buses") == 5


def test_read_emulator_logs(client_with_db: TestClient, test_db_session: Session):
    test_db_session.query(EmulatorLog).delete()
    test_db_session.commit()

    log1 = EmulatorLog(
        status=RunStatus.COMPLETED.value,
        started_at=datetime.now(timezone.utc),
        last_updated=datetime.now(timezone.utc),
    )
    log2 = EmulatorLog(
        status=RunStatus.FAILED.value,
        started_at=datetime.now(timezone.utc),
        last_updated=datetime.now(timezone.utc),
    )
    test_db_session.add_all([log1, log2])
    test_db_session.commit()
    test_db_session.refresh(log1)
    test_db_session.refresh(log2)

    response = client_with_db.get("/emulator_logs/")
    assert response.status_code == 200
    logs_data = response.json()
    assert len(logs_data) == 2
    assert logs_data[0]["status"] == RunStatus.COMPLETED.value
    assert logs_data[1]["status"] == RunStatus.FAILED.value


def test_read_emulator_log_by_id(client_with_db: TestClient, test_db_session: Session):
    test_db_session.query(EmulatorLog).delete()
    test_db_session.commit()

    log = EmulatorLog(
        status=RunStatus.RUNNING.value,
        started_at=datetime.now(timezone.utc),
        last_updated=datetime.now(timezone.utc),
    )
    test_db_session.add(log)
    test_db_session.commit()
    test_db_session.refresh(log)

    response = client_with_db.get(f"/emulator_logs/{log.run_id}")
    assert response.status_code == 200
    log_data = response.json()
    assert log_data["run_id"] == log.run_id
    assert log_data["status"] == RunStatus.RUNNING.value


def test_read_emulator_log_not_found(client_with_db: TestClient):
    response = client_with_db.get("/emulator_logs/9999")
    assert response.status_code == 404
    assert response.json()["detail"] == "Emulator log not found"


def test_update_emulator_log_status(
    client_with_db: TestClient, test_db_session: Session
):
    test_db_session.query(EmulatorLog).delete()
    test_db_session.commit()

    log = EmulatorLog(
        status=RunStatus.QUEUED.value,
        started_at=datetime.now(timezone.utc),
        last_updated=datetime.now(timezone.utc),
    )
    test_db_session.add(log)
    test_db_session.commit()
    test_db_session.refresh(log)

    response = client_with_db.put(
        f"/emulator_logs/{log.run_id}", json={"status": RunStatus.COMPLETED.value}
    )
    assert response.status_code == 200
    assert response.json()["status"] == RunStatus.COMPLETED.value

    updated_log = (
        test_db_session.query(EmulatorLog)
        .filter(EmulatorLog.run_id == log.run_id)
        .first()
    )
    assert updated_log.status == RunStatus.COMPLETED.value


def test_update_emulator_log_optimization_details(
    client_with_db: TestClient, test_db_session: Session
):
    test_db_session.query(EmulatorLog).delete()
    test_db_session.commit()

    log = EmulatorLog(
        status=RunStatus.RUNNING.value,
        started_at=datetime.now(timezone.utc),
        last_updated=datetime.now(timezone.utc),
    )
    test_db_session.add(log)
    test_db_session.commit()
    test_db_session.refresh(log)

    new_details = {
        "status": "OPTIMAL",
        "total_passengers_served": 500,
        "schedule": [{"bus": "B1", "time": "10:00"}],
    }
    response = client_with_db.put(
        f"/emulator_logs/{log.run_id}", json={"optimization_details": new_details}
    )
    assert response.status_code == 200
    assert response.json()["optimization_details"]["status"] == "OPTIMAL"
    assert response.json()["optimization_details"]["total_passengers_served"] == 500

    updated_log = (
        test_db_session.query(EmulatorLog)
        .filter(EmulatorLog.run_id == log.run_id)
        .first()
    )
    assert updated_log.optimization_details_dict["status"] == "OPTIMAL"
    assert updated_log.optimization_details_dict["total_passengers_served"] == 500


def test_delete_emulator_log(client_with_db: TestClient, test_db_session: Session):
    test_db_session.query(EmulatorLog).delete()
    test_db_session.commit()

    log = EmulatorLog(
        status=RunStatus.COMPLETED.value,
        started_at=datetime.now(timezone.utc),
        last_updated=datetime.now(timezone.utc),
    )
    test_db_session.add(log)
    test_db_session.commit()
    test_db_session.refresh(log)

    response = client_with_db.delete(f"/emulator_logs/{log.run_id}")
    assert response.status_code == 200
    assert response.json()["message"] == "Emulator log deleted successfully"
    assert test_db_session.query(EmulatorLog).count() == 0


def test_delete_emulator_log_not_found(client_with_db: TestClient):
    response = client_with_db.delete("/emulator_logs/9999")
    assert response.status_code == 404
    assert response.json()["detail"] == "Emulator log not found"
