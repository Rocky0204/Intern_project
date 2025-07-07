# tests/test_journey_pattern_definition.py
from fastapi.testclient import TestClient
from sqlalchemy.orm import Session
from datetime import time  # Import time for time objects

from api.models import JourneyPattern, JourneyPatternDefinition

# Import the client_with_db and db_session fixtures from conftest.py


def test_create_journey_pattern_definition(
    client_with_db: TestClient, db_session: Session
):
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
        "name": "Parent Journey Pattern for Definition Create",
    }
    db_jp = JourneyPattern(**jp_data)
    db_session.add(db_jp)
    db_session.commit()
    db_session.refresh(db_jp)
    jp_id = db_jp.jp_id

    # Data to send to the FastAPI endpoint (uses stop_point_atco_code as per schema)
    test_data_api = {
        "jp_id": jp_id,
        "stop_point_atco_code": 1001,
        "sequence": 1,
        "arrival_time": "10:00:00",  # Send as string
        "departure_time": "10:05:00",  # Send as string
    }

    response = client_with_db.post("/journey_pattern_definitions/", json=test_data_api)
    assert response.status_code == 201
    data = response.json()
    assert data["jp_id"] == jp_id
    assert data["stop_point_atco_code"] == 1001
    assert data["sequence"] == 1
    assert data["arrival_time"] == "10:00:00"
    assert data["departure_time"] == "10:05:00"


def test_read_journey_pattern_definitions(
    client_with_db: TestClient, db_session: Session
):
    """
    Tests retrieving multiple journey pattern definitions.
    """
    # Create a parent JourneyPattern
    jp_data = {
        "jp_code": "JP_DEF_PARENT_READ_ALL",
        "line_id": 2,
        "route_id": 2,
        "service_id": 2,
        "operator_id": 2,
        "name": "Parent Journey Pattern for Definition Read All",
    }
    db_jp = JourneyPattern(**jp_data)
    db_session.add(db_jp)
    db_session.commit()
    db_session.refresh(db_jp)
    jp_id = db_jp.jp_id

    # Create test data directly in the database (using time objects and 'stop_point_id')
    def_data1 = {
        "jp_id": jp_id,
        "stop_point_id": 2001,
        "sequence": 1,
        "arrival_time": time(9, 0, 0),
        "departure_time": time(9, 2, 0),
    }
    db_def1 = JourneyPatternDefinition(**def_data1)
    db_session.add(db_def1)

    def_data2 = {
        "jp_id": jp_id,
        "stop_point_id": 2002,
        "sequence": 2,
        "arrival_time": time(9, 5, 0),
        "departure_time": time(9, 7, 0),
    }
    db_def2 = JourneyPatternDefinition(**def_data2)
    db_session.add(db_def2)
    db_session.commit()
    db_session.refresh(db_def1)
    db_session.refresh(db_def2)

    response = client_with_db.get("/journey_pattern_definitions/")
    assert response.status_code == 200
    data = response.json()
    assert len(data) >= 2  # Account for other tests potentially adding data

    # Find the definitions created by this test for assertion
    found_def1 = next(
        (d for d in data if d["jp_id"] == jp_id and d["sequence"] == 1), None
    )
    found_def2 = next(
        (d for d in data if d["jp_id"] == jp_id and d["sequence"] == 2), None
    )

    assert found_def1 is not None
    assert found_def1["stop_point_atco_code"] == 2001
    assert found_def1["arrival_time"] == "09:00:00"
    assert found_def1["departure_time"] == "09:02:00"

    assert found_def2 is not None
    assert found_def2["stop_point_atco_code"] == 2002
    assert found_def2["arrival_time"] == "09:05:00"
    assert found_def2["departure_time"] == "09:07:00"


def test_read_single_journey_pattern_definition(
    client_with_db: TestClient, db_session: Session
):
    """
    Tests retrieving a single journey pattern definition by its composite key.
    """
    # Create a parent JourneyPattern
    jp_data = {
        "jp_code": "JP_DEF_PARENT_READ_SINGLE",
        "line_id": 3,
        "route_id": 3,
        "service_id": 3,
        "operator_id": 3,
        "name": "Parent Journey Pattern for Definition Read Single",
    }
    db_jp = JourneyPattern(**jp_data)
    db_session.add(db_jp)
    db_session.commit()
    db_session.refresh(db_jp)
    jp_id = db_jp.jp_id

    # Create test data directly in the database (using time objects and 'stop_point_id')
    def_data = {
        "jp_id": jp_id,
        "stop_point_id": 2001,
        "sequence": 1,
        "arrival_time": time(11, 0, 0),
        "departure_time": time(11, 5, 0),
    }
    db_def = JourneyPatternDefinition(**def_data)
    db_session.add(db_def)
    db_session.commit()
    db_session.refresh(db_def)
    sequence = db_def.sequence

    response = client_with_db.get(f"/journey_pattern_definitions/{jp_id}/{sequence}")
    assert response.status_code == 200
    data = response.json()
    assert data["jp_id"] == jp_id
    assert data["stop_point_atco_code"] == 2001
    assert data["sequence"] == sequence
    assert data["arrival_time"] == "11:00:00"
    assert data["departure_time"] == "11:05:00"


def test_update_journey_pattern_definition(
    client_with_db: TestClient, db_session: Session
):
    """
    Tests updating an existing journey pattern definition.
    """
    # Create a parent JourneyPattern
    jp_data = {
        "jp_code": "JP_DEF_PARENT_UPDATE",
        "line_id": 4,
        "route_id": 4,
        "service_id": 4,
        "operator_id": 4,
        "name": "Parent Journey Pattern for Definition Update",
    }
    db_jp = JourneyPattern(**jp_data)
    db_session.add(db_jp)
    db_session.commit()
    db_session.refresh(db_jp)
    jp_id = db_jp.jp_id

    # Create the definition to be updated
    def_data = {
        "jp_id": jp_id,
        "stop_point_id": 3001,
        "sequence": 1,
        "arrival_time": time(12, 0, 0),
        "departure_time": time(12, 2, 0),
    }
    db_def = JourneyPatternDefinition(**def_data)
    db_session.add(db_def)
    db_session.commit()
    db_session.refresh(db_def)
    sequence = db_def.sequence

    update_data = {
        "stop_point_atco_code": 3002,  # Update this field
        "arrival_time": "12:05:00",  # Update this field
    }

    response = client_with_db.put(
        f"/journey_pattern_definitions/{jp_id}/{sequence}", json=update_data
    )
    assert response.status_code == 200
    data = response.json()
    assert data["jp_id"] == jp_id
    assert data["stop_point_atco_code"] == 3002
    assert data["sequence"] == sequence
    assert data["arrival_time"] == "12:05:00"
    assert data["departure_time"] == "12:02:00"

    # Verify update in the database
    updated_db_def = (
        db_session.query(JourneyPatternDefinition)
        .filter(
            JourneyPatternDefinition.jp_id == jp_id,
            JourneyPatternDefinition.sequence == sequence,
        )
        .first()
    )
    assert updated_db_def is not None
    assert updated_db_def.stop_point_id == 3002
    assert updated_db_def.arrival_time == time(12, 5, 0)
    assert updated_db_def.departure_time == time(12, 2, 0)


def test_delete_journey_pattern_definition(
    client_with_db: TestClient, db_session: Session
):
    """
    Tests deleting a journey pattern definition.
    """
    # Create a parent JourneyPattern
    jp_data = {
        "jp_code": "JP_DEF_PARENT_DELETE",
        "line_id": 5,
        "route_id": 5,
        "service_id": 5,
        "operator_id": 5,
        "name": "Parent Journey Pattern for Definition Delete",
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
        "departure_time": time(12, 2, 0),
    }
    db_def = JourneyPatternDefinition(**def_data)
    db_session.add(db_def)
    db_session.commit()
    db_session.refresh(db_def)
    sequence = db_def.sequence

    response = client_with_db.delete(f"/journey_pattern_definitions/{jp_id}/{sequence}")
    assert response.status_code == 200
    assert (
        response.json()["message"] == "Journey pattern definition deleted successfully"
    )

    # Verify deletion by attempting to retrieve from the database
    deleted_db_def = (
        db_session.query(JourneyPatternDefinition)
        .filter(
            JourneyPatternDefinition.jp_id == jp_id,
            JourneyPatternDefinition.sequence == sequence,
        )
        .first()
    )
    assert deleted_db_def is None