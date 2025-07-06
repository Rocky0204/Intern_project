import pytest
from fastapi.testclient import TestClient
from sqlalchemy.orm import Session
from datetime import time # Import time for time objects

from api.main import app
from api.models import JourneyPattern, JourneyPatternDefinition
from api.schemas import (
    JourneyPatternDefinitionCreate,
    JourneyPatternDefinitionRead,
    JourneyPatternDefinitionUpdate
)

# Import the client_with_db and db_session fixtures from conftest.py
from .conftest import client_with_db, db_session

def test_create_journey_pattern_definition(client_with_db: TestClient, db_session: Session):
    """
    Tests the creation of a new journey pattern definition via the API.
    Handles the 'stop_point_atco_code' vs 'stop_point_id' mismatch.
    """
    # Create a parent JourneyPattern as a dependency for the definition
    jp_data = {
        "jp_code": "JP_DEF_PARENT_CREATE",
        "line_id": 1,
        "route_id": 1,
        "service_id": 1,
        "operator_id": 1,
        "name": "Parent Journey Pattern for Definition Create"
    }
    db_jp = JourneyPattern(**jp_data)
    db_session.add(db_jp)
    db_session.commit()
    db_session.refresh(db_jp)
    jp_id = db_jp.jp_id

    # Data to send to the FastAPI endpoint (uses stop_point_atco_code as per schema)
    test_data_api = {
        "jp_id": jp_id,
        "stop_point_atco_code": 1001, # Use stop_point_atco_code for API interaction
        "sequence": 1,
        "arrival_time": "08:00:00",
        "departure_time": "08:02:00"
    }

    response = client_with_db.post("/journey_pattern_definitions/", json=test_data_api)
    assert response.status_code == 200
    data = response.json()

    # Assertions on the API response (will contain stop_point_atco_code)
    assert data["jp_id"] == test_data_api["jp_id"]
    assert data["sequence"] == test_data_api["sequence"]
    assert data["stop_point_atco_code"] == test_data_api["stop_point_atco_code"] # Assert on atco_code
    assert data["arrival_time"] == test_data_api["arrival_time"]
    assert data["departure_time"] == test_data_api["departure_time"]

    # Verify creation by querying the database directly.
    # When querying the DB directly, the model uses 'stop_point_id'.
    db_def = db_session.query(JourneyPatternDefinition).filter(
        JourneyPatternDefinition.jp_id == jp_id,
        JourneyPatternDefinition.sequence == 1
    ).first()
    assert db_def is not None
    assert db_def.stop_point_id == test_data_api["stop_point_atco_code"] # Compare with atco_code from input
    assert db_def.arrival_time == time.fromisoformat(test_data_api["arrival_time"])
    assert db_def.departure_time == time.fromisoformat(test_data_api["departure_time"])


def test_read_journey_pattern_definitions(client_with_db: TestClient, db_session: Session):
    """
    Tests retrieving a list of journey pattern definitions, with and without filtering by jp_id.
    Ensures correct field names are used for direct DB insertion and API response assertion.
    """
    # Create parent JourneyPattern
    jp_data_1 = {
        "jp_code": "JP_DEF_PARENT_READ_1",
        "line_id": 1, "route_id": 1, "service_id": 1, "operator_id": 1,
        "name": "Parent for Read 1"
    }
    db_jp_1 = JourneyPattern(**jp_data_1)
    db_session.add(db_jp_1)
    db_session.commit()
    db_session.refresh(db_jp_1)
    jp_id_1 = db_jp_1.jp_id

    # Create definitions for jp_id_1 using datetime.time objects and 'stop_point_id' for direct DB insertion
    def_data_1_1 = {"jp_id": jp_id_1, "stop_point_id": 101, "sequence": 1, "arrival_time": time(9, 0, 0), "departure_time": time(9, 1, 0)}
    def_data_1_2 = {"jp_id": jp_id_1, "stop_point_id": 102, "sequence": 2, "arrival_time": time(9, 2, 0), "departure_time": time(9, 3, 0)}
    db_session.add(JourneyPatternDefinition(**def_data_1_1))
    db_session.add(JourneyPatternDefinition(**def_data_1_2))
    db_session.commit()

    # Create another parent JourneyPattern
    jp_data_2 = {
        "jp_code": "JP_DEF_PARENT_READ_2",
        "line_id": 2, "route_id": 2, "service_id": 2, "operator_id": 2,
        "name": "Parent for Read 2"
    }
    db_jp_2 = JourneyPattern(**jp_data_2)
    db_session.add(db_jp_2)
    db_session.commit()
    db_session.refresh(db_jp_2)
    jp_id_2 = db_jp_2.jp_id

    # Create definitions for jp_id_2 using datetime.time objects and 'stop_point_id' for direct DB insertion
    def_data_2_1 = {"jp_id": jp_id_2, "stop_point_id": 201, "sequence": 1, "arrival_time": time(10, 0, 0), "departure_time": time(10, 1, 0)}
    db_session.add(JourneyPatternDefinition(**def_data_2_1))
    db_session.commit()

    # Test reading all definitions
    response = client_with_db.get("/journey_pattern_definitions/")
    assert response.status_code == 200
    data = response.json()
    assert isinstance(data, list)
    # The number of definitions might vary based on other tests, so ensure at least what we added
    assert len(data) >= 3

    # Test reading definitions filtered by jp_id
    response_filtered = client_with_db.get(f"/journey_pattern_definitions/?jp_id={jp_id_1}")
    assert response_filtered.status_code == 200
    filtered_data = response_filtered.json()
    assert isinstance(filtered_data, list)
    assert len(filtered_data) == 2
    assert all(d["jp_id"] == jp_id_1 for d in filtered_data)
    assert any(d["sequence"] == 1 for d in filtered_data)
    assert any(d["sequence"] == 2 for d in filtered_data)
    # Verify time formats in the response are strings (as FastAPI serializes them)
    assert filtered_data[0]["arrival_time"] == "09:00:00"
    # Assert on 'stop_point_atco_code' from the API response
    assert filtered_data[0]["stop_point_atco_code"] == 101


def test_update_journey_pattern_definition(client_with_db: TestClient, db_session: Session):
    """
    Tests updating an existing journey pattern definition.
    Handles the 'stop_point_atco_code' vs 'stop_point_id' mismatch.
    """
    # Create parent JourneyPattern
    jp_data = {
        "jp_code": "JP_DEF_PARENT_UPDATE",
        "line_id": 3, "route_id": 3, "service_id": 3, "operator_id": 3,
        "name": "Parent for Update"
    }
    db_jp = JourneyPattern(**jp_data)
    db_session.add(db_jp)
    db_session.commit()
    db_session.refresh(db_jp)
    jp_id = db_jp.jp_id

    # Create the definition to be updated (using time objects and 'stop_point_id' for direct DB insertion)
    original_def_data = {
        "jp_id": jp_id,
        "stop_point_id": 2001,
        "sequence": 1,
        "arrival_time": time(11, 0, 0),
        "departure_time": time(11, 2, 0)
    }
    db_def = JourneyPatternDefinition(**original_def_data)
    db_session.add(db_def)
    db_session.commit()
    db_session.refresh(db_def)
    sequence = db_def.sequence

    # Data for update via API (uses stop_point_atco_code as per schema)
    update_data_api = {"arrival_time": "11:01:00", "stop_point_atco_code": 2002} # Use atco_code for API
    response = client_with_db.put(
        f"/journey_pattern_definitions/{jp_id}/{sequence}",
        json=update_data_api
    )
    assert response.status_code == 200
    data = response.json()

    # Assertions on the API response (will contain stop_point_atco_code)
    assert data["jp_id"] == jp_id
    assert data["sequence"] == sequence
    assert data["arrival_time"] == update_data_api["arrival_time"]
    assert data["stop_point_atco_code"] == update_data_api["stop_point_atco_code"] # Assert on atco_code

    # Verify update by querying the database directly
    updated_db_def = db_session.query(JourneyPatternDefinition).filter(
        JourneyPatternDefinition.jp_id == jp_id,
        JourneyPatternDefinition.sequence == sequence
    ).first()
    assert updated_db_def is not None
    # Compare with time object and 'stop_point_id' from DB
    assert updated_db_def.arrival_time == time.fromisoformat(update_data_api["arrival_time"])
    assert updated_db_def.stop_point_id == update_data_api["stop_point_atco_code"] # Compare with atco_code from input


def test_delete_journey_pattern_definition(client_with_db: TestClient, db_session: Session):
    """
    Tests deleting a journey pattern definition.
    Ensures time objects and 'stop_point_id' are used for direct DB insertion.
    """
    # Create parent JourneyPattern
    jp_data = {
        "jp_code": "JP_DEF_PARENT_DELETE",
        "line_id": 4, "route_id": 4, "service_id": 4, "operator_id": 4,
        "name": "Parent for Delete"
    }
    db_jp = JourneyPattern(**jp_data)
    db_session.add(db_jp)
    db_session.commit()
    db_session.refresh(db_jp)
    jp_id = db_jp.jp_id

    # Create the definition to be deleted (using time objects and 'stop_point_id' for direct DB insertion)
    def_data = {
        "jp_id": jp_id,
        "stop_point_id": 3001,
        "sequence": 1,
        "arrival_time": time(12, 0, 0),
        "departure_time": time(12, 2, 0)
    }
    db_def = JourneyPatternDefinition(**def_data)
    db_session.add(db_def)
    db_session.commit()
    db_session.refresh(db_def)
    sequence = db_def.sequence

    response = client_with_db.delete(f"/journey_pattern_definitions/{jp_id}/{sequence}")
    assert response.status_code == 200
    assert response.json()["message"] == "Journey pattern definition deleted successfully"

    # Verify deletion by attempting to retrieve from the database
    deleted_db_def = db_session.query(JourneyPatternDefinition).filter(
        JourneyPatternDefinition.jp_id == jp_id,
        JourneyPatternDefinition.sequence == sequence
    ).first()
    assert deleted_db_def is None

    # Verify deletion by attempting to retrieve via API
    response = client_with_db.get(f"/journey_pattern_definitions/?jp_id={jp_id}")
    assert response.status_code == 200
    data = response.json()
    assert len(data) == 0 # No definitions should be found for this jp_id after deletion
