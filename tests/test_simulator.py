# tests/test_simulator.py
import pytest
from fastapi.testclient import TestClient
from sqlalchemy.orm import Session
from datetime import datetime, timezone # Import timezone for consistent datetime handling
from unittest.mock import patch, MagicMock # Import MagicMock for type hinting

from api.main import app
from api.models import EmulatorLog
from api.schemas import RunStatus
from api.database import get_db # Import get_db to override it for tests


@pytest.fixture
def mock_db_session(mocker):
    """
    Fixture to provide a mocked SQLAlchemy Session.
    Mocker is used to create a MagicMock object that simulates a database session.
    """
    return mocker.MagicMock(spec=Session)

@pytest.fixture
def mock_bus_emulator(mocker):
    """
    Fixture to mock the BusEmulator class as it's imported in the simulator router.
    This prevents the tests from actually running the simulation logic within BusEmulator's __init__.
    """
    # IMPORTANT: Patch BusEmulator where it's imported in simulator.py
    return mocker.patch("api.routers.simulator.BusEmulator")

@pytest.fixture
def client_with_mock_db(mock_db_session: MagicMock): # Add type hint for clarity
    """
    Provides a TestClient that uses the mock database session.
    This overrides the get_db dependency in the FastAPI app for testing purposes.
    """
    def override_get_db():
        # This mock_db_session here IS the MagicMock object yielded by the fixture
        yield mock_db_session

    app.dependency_overrides[get_db] = override_get_db
    with TestClient(app) as client:
        yield client
    # Clear overrides after the test to ensure other tests are not affected
    app.dependency_overrides.clear()


def test_run_simulation_success(client_with_mock_db: TestClient, mock_bus_emulator, mock_db_session):
    """
    Tests the /simulate/run endpoint for a successful simulation scenario.
    """
    # Setup mock for BusEmulator instance and its run_simulation method
    # mock_bus_emulator is now the MagicMock representing the BusEmulator class
    mock_emulator_instance = mock_bus_emulator.return_value # This is the mock instance returned when BusEmulator() is called
    mock_emulator_instance.run_simulation.return_value = {
        "status": "Success",
        "optimization_details": {"message": "Optimization successful", "total_passengers_served": 100}
    }
    
    # Explicitly get the mock database session instance
    db_mock_instance = mock_db_session

    # Define a side effect for db_mock_instance.refresh
    # This function will be called when db.refresh(db_log_entry) is executed in simulator.py
    def mock_refresh_side_effect(obj: EmulatorLog):
        # Simulate the database populating run_id and timestamps
        # These values will be used when _create_emulator_log_read is called
        obj.run_id = 1 # Assign a mock run_id
        obj.started_at = datetime.now(timezone.utc) # Ensure it's timezone-aware for Pydantic
        obj.last_updated = datetime.now(timezone.utc) # Ensure it's timezone-aware for Pydantic

    # Configure the mock_db_session to return the test_log when queried
    db_mock_instance.add.return_value = None
    db_mock_instance.commit.return_value = None
    db_mock_instance.refresh.side_effect = mock_refresh_side_effect 

    # The test_log is primarily for mocking query results if the API were to fetch an existing log.
    # For the object created *inside* run_bus_simulation, the refresh side_effect handles population.
    # Still, ensure test_log is well-formed for consistency.
    test_log = EmulatorLog(
        run_id=1,
        status=RunStatus.COMPLETED.value,
        started_at=datetime.now(timezone.utc), # Use timezone-aware datetime here too
        last_updated=datetime.now(timezone.utc) # Use timezone-aware datetime here too
    )
    test_log.optimization_details_dict = {"message": "Optimization successful", "total_passengers_served": 100}
    db_mock_instance.query.return_value.filter.return_value.first.return_value = test_log


    # Test the API endpoint
    response = client_with_mock_db.post(
        "/simulate/run",
        json={"use_optimized_schedule": True} # Example body, though optional for this test
    )
    
    # Assertions to verify the response
    assert response.status_code == 202
    assert response.json()["status"] == RunStatus.COMPLETED.value # Now this should pass
    assert "optimization_details" in response.json()
    assert response.json()["optimization_details"]["message"] == "Optimization successful"
    assert response.json()["optimization_details"]["total_passengers_served"] == 100


def test_run_simulation_failure(client_with_mock_db: TestClient, mock_bus_emulator, mock_db_session):
    """
    Tests the /simulate/run endpoint for a simulation failure scenario.
    """
    # Setup mock for BusEmulator instance to return a "Failed" status
    mock_emulator_instance = mock_bus_emulator.return_value
    mock_emulator_instance.run_simulation.return_value = {
        "status": "Failed", # This status will be used by the simulator's logic
        "error": "Simulation error details"
    }

    # Explicitly get the mock database session instance
    db_mock_instance = mock_db_session

    # Define a side effect for db_mock_instance.refresh
    def mock_refresh_side_effect(obj: EmulatorLog):
        obj.run_id = 1
        obj.started_at = datetime.now(timezone.utc)
        obj.last_updated = datetime.now(timezone.utc)

    # Configure the mock_db_session
    db_mock_instance.add.return_value = None
    db_mock_instance.commit.return_value = None
    db_mock_instance.refresh.side_effect = mock_refresh_side_effect 

    test_log = EmulatorLog(
        run_id=1,
        status=RunStatus.FAILED.value, # This will be the initial status in the test log
        started_at=datetime.now(timezone.utc),
        last_updated=datetime.now(timezone.utc)
    )
    # The simulator's logic will set optimization_details_dict based on simulation_result
    # So, we expect the output to reflect the mock_emulator_instance.run_simulation.return_value
    test_log.optimization_details_dict = {
        "status": "FAILED", # This should now be "FAILED" as per the mock_emulator_instance.run_simulation
        "message": "{'status': 'Failed', 'error': 'Simulation error details'}"
    }
    db_mock_instance.query.return_value.filter.return_value.first.return_value = test_log


    # Test the API endpoint
    response = client_with_mock_db.post("/simulate/run")

    # Assertions to verify the response for a failed simulation
    assert response.status_code == 202
    assert response.json()["status"] == RunStatus.FAILED.value
    assert response.json()["optimization_details"]["status"] == "FAILED" # This should now pass
    assert "Simulation error details" in response.json()["optimization_details"]["message"]


def test_run_simulation_exception(client_with_mock_db: TestClient, mock_bus_emulator, mock_db_session):
    """
    Tests the /simulate/run endpoint when an unexpected exception occurs during simulation.
    """
    # Setup mock for BusEmulator instance to raise an exception
    mock_emulator_instance = mock_bus_emulator.return_value
    mock_emulator_instance.run_simulation.side_effect = Exception("Deliberate simulation error")

    # Explicitly get the mock database session instance
    db_mock_instance = mock_db_session

    # Define a side effect for db_mock_instance.refresh
    def mock_refresh_side_effect(obj: EmulatorLog):
        obj.run_id = 1
        obj.started_at = datetime.now(timezone.utc)
        obj.last_updated = datetime.now(timezone.utc)

    # Configure the mock_db_session
    db_mock_instance.add.return_value = None
    db_mock_instance.commit.return_value = None
    db_mock_instance.refresh.side_effect = mock_refresh_side_effect 

    test_log = EmulatorLog(
        run_id=1,
        status=RunStatus.FAILED.value,
        started_at=datetime.now(timezone.utc),
        last_updated=datetime.now(timezone.utc)
    )
    # The simulator's exception handling will set optimization_details_dict
    test_log.optimization_details_dict = {
        "status": "ERROR", # This will be "ERROR" due to the exception handling in simulator.py
        "message": "Deliberate simulation error"
    }
    db_mock_instance.query.return_value.filter.return_value.first.return_value = test_log

    # Test the API endpoint
    response = client_with_mock_db.post("/simulate/run")

    # Assertions to verify the response for an exception during simulation
    assert response.status_code == 202
    assert response.json()["status"] == RunStatus.FAILED.value
    assert response.json()["optimization_details"]["status"] == "ERROR" # This should now pass
    assert "Deliberate simulation error" in response.json()["optimization_details"]["message"] # This should now pass

