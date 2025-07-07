# tests/test_emulator_log.py
import pytest
from fastapi.testclient import TestClient
from sqlalchemy.orm import Session
from datetime import datetime
import json # Ensure json is imported for json.dumps in test_read_emulator_logs

from api.models import EmulatorLog, Base
from api.schemas import RunStatus, OptimizationDetailsRead


def test_create_emulator_log(client_with_db: TestClient, db_session: Session):
    test_data = {"status": RunStatus.RUNNING.value}
    response = client_with_db.post("/emulator_logs/", json=test_data)
    assert response.status_code == 201
    data = response.json()
    assert data["status"] == RunStatus.RUNNING.value
    assert "run_id" in data
    assert "started_at" in data
    assert "last_updated" in data
    assert data["optimization_details"] is None

    db_log = db_session.query(EmulatorLog).filter(EmulatorLog.run_id == data["run_id"]).first()
    assert db_log is not None
    assert db_log.status == RunStatus.RUNNING.value
    assert db_log.optimization_details_dict is None


def test_read_emulator_logs(client_with_db: TestClient, db_session: Session):
    db_log1 = EmulatorLog(status=RunStatus.RUNNING, started_at=datetime.now(), last_updated=datetime.now())
    # Ensure json.dumps is used when setting optimization_details for the model
    db_log2 = EmulatorLog(status=RunStatus.COMPLETED, started_at=datetime.now(), last_updated=datetime.now(),
                          optimization_details=json.dumps({"status": "OPTIMAL", "message": "Success"}))
    db_session.add_all([db_log1, db_log2])
    db_session.commit()

    response = client_with_db.get("/emulator_logs/")
    assert response.status_code == 200
    data = response.json()
    assert len(data) == 2
    
    log1_data = next((item for item in data if item["run_id"] == db_log1.run_id), None)
    log2_data = next((item for item in data if item["run_id"] == db_log2.run_id), None)

    assert log1_data is not None
    assert log1_data["status"] == RunStatus.RUNNING.value
    assert log1_data["optimization_details"] is None

    assert log2_data is not None
    assert log2_data["status"] == RunStatus.COMPLETED.value
    # UPDATED ASSERTION: Expect all optional fields from OptimizationDetailsRead to be None if not set
    assert log2_data["optimization_details"] == {
        "status": "OPTIMAL",
        "message": "Success",
        "total_passengers_served": None,
        "schedule": None,
        "solver_runtime_ms": None,
        "solver_iterations": None,
        "buses_assigned_summary": None,
    }


def test_read_single_emulator_log(client_with_db: TestClient, db_session: Session):
    db_log = EmulatorLog(status=RunStatus.RUNNING, started_at=datetime.now(), last_updated=datetime.now())
    db_session.add(db_log)
    db_session.commit()
    db_session.refresh(db_log)
    run_id = db_log.run_id

    response = client_with_db.get(f"/emulator_logs/{run_id}")
    assert response.status_code == 200
    data = response.json()
    assert data["run_id"] == run_id
    assert data["status"] == RunStatus.RUNNING.value
    assert data["optimization_details"] is None


def test_update_emulator_log(client_with_db: TestClient, db_session: Session):
    db_log = EmulatorLog(status=RunStatus.RUNNING, started_at=datetime.now(), last_updated=datetime.now())
    db_session.add(db_log)
    db_session.commit()
    db_session.refresh(db_log)
    run_id = db_log.run_id

    update_payload = {
        "status": RunStatus.COMPLETED.value,
        "optimization_details": {"status": "OPTIMAL", "total_passengers_served": 100}
    }
    response = client_with_db.put(
        f"/emulator_logs/{run_id}",
        json=update_payload,
    )
    assert response.status_code == 200
    data = response.json()
    assert data["status"] == RunStatus.COMPLETED.value
    # UPDATED ASSERTION: Expect all optional fields from OptimizationDetailsRead
    assert data["optimization_details"] == {
        "status": "OPTIMAL",
        "total_passengers_served": 100,
        "message": None,
        "schedule": None,
        "solver_runtime_ms": None,
        "solver_iterations": None,
        "buses_assigned_summary": None,
    }

    # Verify directly from DB
    updated_db_log = db_session.query(EmulatorLog).filter(EmulatorLog.run_id == run_id).first()
    assert updated_db_log is not None # Defensive check
    assert updated_db_log.status == RunStatus.COMPLETED.value
    assert updated_db_log.optimization_details_dict is not None # Ensure it's not None before subscripting
    assert updated_db_log.optimization_details_dict["status"] == "OPTIMAL"
    assert updated_db_log.optimization_details_dict["total_passengers_served"] == 100


def test_delete_emulator_log(client_with_db: TestClient, db_session: Session):
    db_log = EmulatorLog(status=RunStatus.RUNNING, started_at=datetime.now(), last_updated=datetime.now())
    db_session.add(db_log)
    db_session.commit()
    db_session.refresh(db_log)
    run_id = db_log.run_id

    response = client_with_db.delete(f"/emulator_logs/{run_id}")
    assert response.status_code == 200
    assert response.json()["message"] == "Emulator log deleted successfully"

    deleted_db_log = db_session.query(EmulatorLog).filter(EmulatorLog.run_id == run_id).first()
    assert deleted_db_log is None