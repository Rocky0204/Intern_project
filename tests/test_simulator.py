import pytest
from fastapi.testclient import TestClient
from sqlalchemy.orm import Session
from datetime import datetime, timezone 
from unittest.mock import patch, MagicMock 
from api.main import app
from api.models import EmulatorLog
from api.schemas import RunStatus
from api.database import get_db 


@pytest.fixture
def mock_db_session(mocker):
    return mocker.MagicMock(spec=Session)

@pytest.fixture
def mock_bus_emulator(mocker):
   
    return mocker.patch("api.routers.simulator.BusEmulator")

@pytest.fixture
def client_with_mock_db(mock_db_session: MagicMock): 
    def override_get_db():
        yield mock_db_session

    app.dependency_overrides[get_db] = override_get_db
    with TestClient(app) as client:
        yield client
    app.dependency_overrides.clear()


def test_run_simulation_success(client_with_mock_db: TestClient, mock_bus_emulator, mock_db_session):
    mock_emulator_instance = mock_bus_emulator.return_value 
    mock_emulator_instance.run_simulation.return_value = {
        "status": "Success",
        "optimization_details": {"message": "Optimization successful", "total_passengers_served": 100}
    }
    
    db_mock_instance = mock_db_session

    def mock_refresh_side_effect(obj: EmulatorLog):
        obj.run_id = 1 
        obj.started_at = datetime.now(timezone.utc) 
        obj.last_updated = datetime.now(timezone.utc) 

    db_mock_instance.add.return_value = None
    db_mock_instance.commit.return_value = None
    db_mock_instance.refresh.side_effect = mock_refresh_side_effect 

    test_log = EmulatorLog(
        run_id=1,
        status=RunStatus.COMPLETED.value,
        started_at=datetime.now(timezone.utc), 
        last_updated=datetime.now(timezone.utc)
    )
    test_log.optimization_details_dict = {"message": "Optimization successful", "total_passengers_served": 100}
    db_mock_instance.query.return_value.filter.return_value.first.return_value = test_log


    response = client_with_mock_db.post(
        "/simulate/run",
        json={"use_optimized_schedule": True} 
    )
    
    assert response.status_code == 202
    assert response.json()["status"] == RunStatus.COMPLETED.value 
    assert "optimization_details" in response.json()
    assert response.json()["optimization_details"]["message"] == "Optimization successful"
    assert response.json()["optimization_details"]["total_passengers_served"] == 100


def test_run_simulation_failure(client_with_mock_db: TestClient, mock_bus_emulator, mock_db_session):
   
    mock_emulator_instance = mock_bus_emulator.return_value
    mock_emulator_instance.run_simulation.return_value = {
        "status": "Failed",
        "error": "Simulation error details"
    }

    db_mock_instance = mock_db_session

    def mock_refresh_side_effect(obj: EmulatorLog):
        obj.run_id = 1
        obj.started_at = datetime.now(timezone.utc)
        obj.last_updated = datetime.now(timezone.utc)

    db_mock_instance.add.return_value = None
    db_mock_instance.commit.return_value = None
    db_mock_instance.refresh.side_effect = mock_refresh_side_effect 

    test_log = EmulatorLog(
        run_id=1,
        status=RunStatus.FAILED.value, 
        started_at=datetime.now(timezone.utc),
        last_updated=datetime.now(timezone.utc)
    )
    test_log.optimization_details_dict = {
        "status": "FAILED", 
        "message": "{'status': 'Failed', 'error': 'Simulation error details'}"
    }
    db_mock_instance.query.return_value.filter.return_value.first.return_value = test_log


    response = client_with_mock_db.post("/simulate/run")

    assert response.status_code == 202
    assert response.json()["status"] == RunStatus.FAILED.value
    assert response.json()["optimization_details"]["status"] == "FAILED" 
    assert "Simulation error details" in response.json()["optimization_details"]["message"]


def test_run_simulation_exception(client_with_mock_db: TestClient, mock_bus_emulator, mock_db_session):
    
    mock_emulator_instance = mock_bus_emulator.return_value
    mock_emulator_instance.run_simulation.side_effect = Exception("Deliberate simulation error")

    db_mock_instance = mock_db_session

    def mock_refresh_side_effect(obj: EmulatorLog):
        obj.run_id = 1
        obj.started_at = datetime.now(timezone.utc)
        obj.last_updated = datetime.now(timezone.utc)

    db_mock_instance.add.return_value = None
    db_mock_instance.commit.return_value = None
    db_mock_instance.refresh.side_effect = mock_refresh_side_effect 

    test_log = EmulatorLog(
        run_id=1,
        status=RunStatus.FAILED.value,
        started_at=datetime.now(timezone.utc),
        last_updated=datetime.now(timezone.utc)
    )
    test_log.optimization_details_dict = {
        "status": "ERROR", 
        "message": "Deliberate simulation error"
    }
    db_mock_instance.query.return_value.filter.return_value.first.return_value = test_log

    response = client_with_mock_db.post("/simulate/run")

    assert response.status_code == 202
    assert response.json()["status"] == RunStatus.FAILED.value
    assert response.json()["optimization_details"]["status"] == "ERROR" 
    assert "Deliberate simulation error" in response.json()["optimization_details"]["message"]

