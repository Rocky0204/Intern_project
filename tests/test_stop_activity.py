from fastapi.testclient import TestClient
from sqlalchemy.orm import Session
from datetime import time

from api.models import StopPoint, VehicleJourney, StopActivity
import pytest

# Assuming client_with_db and db_session are provided by conftest.py


def test_create_stop_activity(client_with_db: TestClient, db_session: Session):
    """
    Tests the creation of a new stop activity via the API.
    """
    # Create required parent objects in the database
    stop_point_data = {
        "atco_code": 100008,
        "name": "Test Stop Point SA Create",
        "latitude": 52.2,
        "longitude": 0.8,
        "stop_area_code": 1,
    }
    db_stop_point = db_session.query(StopPoint).filter_by(atco_code=stop_point_data["atco_code"]).first()
    if not db_stop_point:
        db_stop_point = StopPoint(**stop_point_data)
        db_session.add(db_stop_point)
        db_session.commit()
        db_session.refresh(db_stop_point)
    stop_point_id = db_stop_point.atco_code

    # Create a dummy VehicleJourney if vj_id is used
    vj_data = {
        "departure_time": time(7, 0, 0),
        "dayshift": 1,
        "jp_id": 1,
        "block_id": 1,
        "operator_id": 1,
        "line_id": 1,
        "service_id": 1,
    }
    db_vj = db_session.query(VehicleJourney).filter_by(jp_id=vj_data["jp_id"], block_id=vj_data["block_id"]).first()
    if not db_vj:
        db_vj = VehicleJourney(**vj_data)
        db_session.add(db_vj)
        db_session.commit()
        db_session.refresh(db_vj)
    vj_id = db_vj.vj_id

    test_data = {
        "activity_type": "arrival",
        "activity_time": "08:30:00", # Send as string
        "pax_count": 15,
        "stop_point_id": stop_point_id, # Use stop_point_id
        "vj_id": vj_id,
    }

    response = client_with_db.post("/stop_activities/", json=test_data)
    assert response.status_code == 201

    response_data = response.json()
    assert "activity_id" in response_data
    assert response_data["activity_type"] == "arrival"
    assert response_data["activity_time"] == "08:30:00"
    assert response_data["pax_count"] == 15
    assert response_data["stop_point_id"] == stop_point_id
    assert response_data["vj_id"] == vj_id

    # Verify directly from the database
    db_activity = db_session.query(StopActivity).filter_by(activity_id=response_data["activity_id"]).first()
    assert db_activity is not None
    assert db_activity.activity_type == "arrival"
    assert db_activity.activity_time == time(8, 30, 0) # Stored as time object
    assert db_activity.pax_count == 15
    assert db_activity.stop_point_id == stop_point_id
    assert db_activity.vj_id == vj_id


def test_read_stop_activities(client_with_db: TestClient, db_session: Session):
    """
    Tests retrieving all stop activities and filtering by stop_point_id.
    """
    # Create required parent objects in the database
    stop_point_data_1 = {
        "atco_code": 100009,
        "name": "Test Stop Point SA Read 1",
        "latitude": 52.3,
        "longitude": 0.9,
        "stop_area_code": 1,
    }
    db_stop_point_1 = db_session.query(StopPoint).filter_by(atco_code=stop_point_data_1["atco_code"]).first()
    if not db_stop_point_1:
        db_stop_point_1 = StopPoint(**stop_point_data_1)
        db_session.add(db_stop_point_1)
        db_session.commit()
        db_session.refresh(db_stop_point_1)
    stop_point_id_1 = db_stop_point_1.atco_code

    stop_point_data_2 = {
        "atco_code": 100010,
        "name": "Test Stop Point SA Read 2",
        "latitude": 52.4,
        "longitude": 1.0,
        "stop_area_code": 1,
    }
    db_stop_point_2 = db_session.query(StopPoint).filter_by(atco_code=stop_point_data_2["atco_code"]).first()
    if not db_stop_point_2:
        db_stop_point_2 = StopPoint(**stop_point_data_2)
        db_session.add(db_stop_point_2)
        db_session.commit()
        db_session.refresh(db_stop_point_2)
    stop_point_id_2 = db_stop_point_2.atco_code

    vj_data = {
        "departure_time": time(7, 30, 0),
        "dayshift": 1,
        "jp_id": 2,
        "block_id": 1,
        "operator_id": 1,
        "line_id": 1,
        "service_id": 1,
    }
    db_vj = db_session.query(VehicleJourney).filter_by(jp_id=vj_data["jp_id"], block_id=vj_data["block_id"]).first()
    if not db_vj:
        db_vj = VehicleJourney(**vj_data)
        db_session.add(db_vj)
        db_session.commit()
        db_session.refresh(db_vj)
    vj_id = db_vj.vj_id

    # Create test activities directly in the database
    act_1 = StopActivity(
        activity_type="departure",
        activity_time=time(9, 0, 0),
        pax_count=20,
        stop_point_id=stop_point_id_1,
        vj_id=vj_id,
    )
    act_2 = StopActivity(
        activity_type="arrival",
        activity_time=time(9, 10, 0),
        pax_count=10,
        stop_point_id=stop_point_id_2,
        vj_id=vj_id,
    )
    db_session.add_all([act_1, act_2])
    db_session.commit()

    # Test retrieving all activities
    response = client_with_db.get("/stop_activities/")
    assert response.status_code == 200
    data = response.json()
    assert len(data) >= 2

    # Validate structure and types of returned data
    for item in data:
        assert "activity_id" in item
        assert "activity_type" in item
        assert "activity_time" in item
        time.fromisoformat(item["activity_time"]) # Check if it's a valid ISO 8601 string
        assert "pax_count" in item
        assert "stop_point_id" in item
        assert "vj_id" in item

    # Test filtering by stop_point_id
    response = client_with_db.get(f"/stop_activities/?stop_point_id={stop_point_id_1}")
    assert response.status_code == 200
    data = response.json()
    assert len(data) == 1
    assert data[0]["stop_point_id"] == stop_point_id_1
    assert data[0]["activity_time"] == "09:00:00"


def test_read_single_stop_activity(client_with_db: TestClient, db_session: Session):
    """
    Tests retrieving a single stop activity by its ID.
    """
    # Create required parent objects in the database
    stop_point_data = {
        "atco_code": 100011,
        "name": "Test Stop Point SA Single",
        "latitude": 52.5,
        "longitude": 1.1,
        "stop_area_code": 1,
    }
    db_stop_point = db_session.query(StopPoint).filter_by(atco_code=stop_point_data["atco_code"]).first()
    if not db_stop_point:
        db_stop_point = StopPoint(**stop_point_data)
        db_session.add(db_stop_point)
        db_session.commit()
        db_session.refresh(db_stop_point)
    stop_point_id = db_stop_point.atco_code

    vj_data = {
        "departure_time": time(8, 0, 0),
        "dayshift": 1,
        "jp_id": 3,
        "block_id": 1,
        "operator_id": 1,
        "line_id": 1,
        "service_id": 1,
    }
    db_vj = db_session.query(VehicleJourney).filter_by(jp_id=vj_data["jp_id"], block_id=vj_data["block_id"]).first()
    if not db_vj:
        db_vj = VehicleJourney(**vj_data)
        db_session.add(db_vj)
        db_session.commit()
        db_session.refresh(db_vj)
    vj_id = db_vj.vj_id

    # Create a test activity directly in the database
    activity_data = {
        "activity_type": "boarding",
        "activity_time": time(10, 0, 0),
        "pax_count": 5,
        "stop_point_id": stop_point_id,
        "vj_id": vj_id,
    }
    db_activity = StopActivity(**activity_data)
    db_session.add(db_activity)
    db_session.commit()
    db_session.refresh(db_activity)

    response = client_with_db.get(f"/stop_activities/{db_activity.activity_id}")
    assert response.status_code == 200
    data = response.json()
    assert data["activity_id"] == db_activity.activity_id
    assert data["activity_type"] == "boarding"
    assert data["activity_time"] == "10:00:00"
    assert data["pax_count"] == 5
    assert data["stop_point_id"] == stop_point_id
    assert data["vj_id"] == vj_id

    # Test for non-existent activity
    response = client_with_db.get("/stop_activities/99999")
    assert response.status_code == 404


def test_update_stop_activity(client_with_db: TestClient, db_session: Session):
    """
    Tests updating an existing stop activity.
    """
    # Create required parent objects in the database
    stop_point_data_orig = {
        "atco_code": 100012,
        "name": "Test Stop Point SA Update Orig",
        "latitude": 52.6,
        "longitude": 1.2,
        "stop_area_code": 1,
    }
    db_stop_point_orig = db_session.query(StopPoint).filter_by(atco_code=stop_point_data_orig["atco_code"]).first()
    if not db_stop_point_orig:
        db_stop_point_orig = StopPoint(**stop_point_data_orig)
        db_session.add(db_stop_point_orig)
        db_session.commit()
        db_session.refresh(db_stop_point_orig)
    stop_point_id_orig = db_stop_point_orig.atco_code

    stop_point_data_new = {
        "atco_code": 100013,
        "name": "Test Stop Point SA Update New",
        "latitude": 52.7,
        "longitude": 1.3,
        "stop_area_code": 1,
    }
    db_stop_point_new = db_session.query(StopPoint).filter_by(atco_code=stop_point_data_new["atco_code"]).first()
    if not db_stop_point_new:
        db_stop_point_new = StopPoint(**stop_point_data_new)
        db_session.add(db_stop_point_new)
        db_session.commit()
        db_session.refresh(db_stop_point_new)
    stop_point_id_new = db_stop_point_new.atco_code

    vj_data_orig = {
        "departure_time": time(8, 30, 0),
        "dayshift": 1,
        "jp_id": 4,
        "block_id": 1,
        "operator_id": 1,
        "line_id": 1,
        "service_id": 1,
    }
    db_vj_orig = db_session.query(VehicleJourney).filter_by(jp_id=vj_data_orig["jp_id"], block_id=vj_data_orig["block_id"]).first()
    if not db_vj_orig:
        db_vj_orig = VehicleJourney(**vj_data_orig)
        db_session.add(db_vj_orig)
        db_session.commit()
        db_session.refresh(db_vj_orig)
    vj_id_orig = db_vj_orig.vj_id

    vj_data_new = {
        "departure_time": time(9, 30, 0),
        "dayshift": 1,
        "jp_id": 5,
        "block_id": 1,
        "operator_id": 1,
        "line_id": 1,
        "service_id": 1,
    }
    db_vj_new = db_session.query(VehicleJourney).filter_by(jp_id=vj_data_new["jp_id"], block_id=vj_data_new["block_id"]).first()
    if not db_vj_new:
        db_vj_new = VehicleJourney(**vj_data_new)
        db_session.add(db_vj_new)
        db_session.commit()
        db_session.refresh(db_vj_new)
    vj_id_new = db_vj_new.vj_id


    # Create the activity to be updated
    activity_data = {
        "activity_type": "alighting",
        "activity_time": time(11, 0, 0),
        "pax_count": 8,
        "stop_point_id": stop_point_id_orig,
        "vj_id": vj_id_orig,
    }
    db_activity = StopActivity(**activity_data)
    db_session.add(db_activity)
    db_session.commit()
    db_session.refresh(db_activity)

    update_data = {
        "activity_type": "boarding",
        "activity_time": "11:15:00",
        "pax_count": 12,
        "stop_point_id": stop_point_id_new, # Update stop_point_id
        "vj_id": vj_id_new, # Update vj_id
    }

    response = client_with_db.put(
        f"/stop_activities/{db_activity.activity_id}", json=update_data
    )
    assert response.status_code == 200
    data = response.json()
    assert data["activity_id"] == db_activity.activity_id
    assert data["activity_type"] == "boarding"
    assert data["activity_time"] == "11:15:00"
    assert data["pax_count"] == 12
    assert data["stop_point_id"] == stop_point_id_new
    assert data["vj_id"] == vj_id_new

    # Verify update directly from the database
    updated_db_activity = db_session.query(StopActivity).filter_by(activity_id=db_activity.activity_id).first()
    assert updated_db_activity.activity_type == "boarding"
    assert updated_db_activity.activity_time == time(11, 15, 0)
    assert updated_db_activity.pax_count == 12
    assert updated_db_activity.stop_point_id == stop_point_id_new
    assert updated_db_activity.vj_id == vj_id_new


def test_delete_stop_activity(client_with_db: TestClient, db_session: Session):
    """
    Tests deleting a stop activity.
    """
    # Create required parent objects in the database
    stop_point_data = {
        "atco_code": 100014,
        "name": "Test Stop Point SA Delete",
        "latitude": 52.8,
        "longitude": 1.4,
        "stop_area_code": 1,
    }
    db_stop_point = db_session.query(StopPoint).filter_by(atco_code=stop_point_data["atco_code"]).first()
    if not db_stop_point:
        db_stop_point = StopPoint(**stop_point_data)
        db_session.add(db_stop_point)
        db_session.commit()
        db_session.refresh(db_stop_point)
    stop_point_id = db_stop_point.atco_code

    vj_data = {
        "departure_time": time(9, 0, 0),
        "dayshift": 1,
        "jp_id": 6,
        "block_id": 1,
        "operator_id": 1,
        "line_id": 1,
        "service_id": 1,
    }
    db_vj = db_session.query(VehicleJourney).filter_by(jp_id=vj_data["jp_id"], block_id=vj_data["block_id"]).first()
    if not db_vj:
        db_vj = VehicleJourney(**vj_data)
        db_session.add(db_vj)
        db_session.commit()
        db_session.refresh(db_vj)
    vj_id = db_vj.vj_id

    # Create the activity to be deleted
    activity_data = {
        "activity_type": "boarding",
        "activity_time": time(12, 0, 0),
        "pax_count": 10,
        "stop_point_id": stop_point_id,
        "vj_id": vj_id,
    }
    db_activity = StopActivity(**activity_data)
    db_session.add(db_activity)
    db_session.commit()
    db_session.refresh(db_activity)

    response = client_with_db.delete(f"/stop_activities/{db_activity.activity_id}")
    assert response.status_code == 204 # Expect 204 No Content for successful deletion

    # Verify deletion by attempting to retrieve from the database
    deleted_db_activity = db_session.query(StopActivity).filter_by(activity_id=db_activity.activity_id).first()
    assert deleted_db_activity is None

    # Test deleting a non-existent activity
    response = client_with_db.delete("/stop_activities/99999")
    assert response.status_code == 404