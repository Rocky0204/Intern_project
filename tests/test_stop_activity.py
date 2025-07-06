import pytest
from fastapi.testclient import TestClient
from sqlalchemy.orm import Session
from datetime import time

from api.main import app
from api.models import (
    StopArea, StopPoint, Operator, Line, Service, Route,
    JourneyPattern, BusType, Block, VehicleJourney, StopActivity
)
from api.schemas import (
    StopActivityCreate,
    StopActivityRead,
    StopActivityUpdate
)

from .conftest import client_with_db, db_session

def setup_all_parent_entities(db_session: Session, index: int = 0):
    """
    Helper function to create all necessary parent entities for StopActivity tests
    with unique IDs based on the provided index.
    """
    # 1. Create Operator
    op_data = {"operator_code": f"OP{100 + index}", "name": f"Operator {index}"}
    db_operator = Operator(**op_data)
    db_session.add(db_operator)
    db_session.commit()
    db_session.refresh(db_operator)

    # 2. Create Line
    line_data = {"line_name": f"Line {200 + index}", "operator_id": db_operator.operator_id}
    db_line = Line(**line_data)
    db_session.add(db_line)
    db_session.commit()
    db_session.refresh(db_line)

    # 3. Create Service
    service_data = {
        "service_code": f"SVC{300 + index}",
        "name": f"Service {index}",
        "operator_id": db_operator.operator_id,
        "line_id": db_line.line_id
    }
    db_service = Service(**service_data)
    db_session.add(db_service)
    db_session.commit()
    db_session.refresh(db_service)

    # 4. Create Route
    route_data = {
        "name": f"Route {400 + index}",
        "operator_id": db_operator.operator_id,
        "description": f"Route desc {index}"
    }
    db_route = Route(**route_data)
    db_session.add(db_route)
    db_session.commit()
    db_session.refresh(db_route)

    # 5. Create JourneyPattern
    jp_data = {
        "jp_code": f"JP{500 + index}",
        "line_id": db_line.line_id,
        "route_id": db_route.route_id,
        "service_id": db_service.service_id,
        "operator_id": db_operator.operator_id,
        "name": f"JP {index}"
    }
    db_jp = JourneyPattern(**jp_data)
    db_session.add(db_jp)
    db_session.commit()
    db_session.refresh(db_jp)

    # 6. Create BusType
    bus_type_data = {"name": f"BusType {600 + index}", "capacity": 50 + index}
    db_bus_type = BusType(**bus_type_data)
    db_session.add(db_bus_type)
    db_session.commit()
    db_session.refresh(db_bus_type)

    # 7. Create Block
    block_data = {
        "name": f"Block {700 + index}",
        "operator_id": db_operator.operator_id,
        "bus_type_id": db_bus_type.type_id
    }
    db_block = Block(**block_data)
    db_session.add(db_block)
    db_session.commit()
    db_session.refresh(db_block)

    # 8. Create VehicleJourney
    vj_data = {
        "departure_time": time(8, 0, 0),
        "dayshift": 1,
        "jp_id": db_jp.jp_id,
        "block_id": db_block.block_id,
        "operator_id": db_operator.operator_id,
        "line_id": db_line.line_id,
        "service_id": db_service.service_id
    }
    db_vj = VehicleJourney(**vj_data)
    db_session.add(db_vj)
    db_session.commit()
    db_session.refresh(db_vj)

    # 9. Create StopArea
    stop_area_data = {
        "stop_area_code": 8000 + index,
        "admin_area_code": f"SA{80 + index}",
        "name": f"Stop Area {index}",
        "is_terminal": True
    }
    db_stop_area = StopArea(**stop_area_data)
    db_session.add(db_stop_area)
    db_session.commit()
    db_session.refresh(db_stop_area)

    # 10. Create StopPoint
    stop_point_data = {
        "atco_code": 9000 + index, # This will be the stop_point_id in StopActivity
        "name": f"Stop Point {index}",
        "latitude": 51.0 + (index * 0.001),
        "longitude": -0.5 + (index * 0.001),
        "stop_area_code": db_stop_area.stop_area_code
    }
    db_stop_point = StopPoint(**stop_point_data)
    db_session.add(db_stop_point)
    db_session.commit()
    db_session.refresh(db_stop_point)

    return db_vj.vj_id, db_stop_point.atco_code # Return vj_id and stop_point_atco_code


def test_create_stop_activity(client_with_db: TestClient, db_session: Session):
    """
    Tests the creation of a new stop activity via the API.
    Handles 'atco_code' vs 'stop_point_id' mapping and time formats.
    """
    vj_id, stop_point_atco_code = setup_all_parent_entities(db_session, index=0)

    # Data to send to the FastAPI endpoint (time as ISO 8601 strings, atco_code for stop_point)
    test_data_api = {
        "activity_type": "boarding",
        "activity_time": "08:15:00",
        "pax_count": 5,
        "atco_code": stop_point_atco_code, # Use atco_code for API interaction
        "vj_id": vj_id
    }

    response = client_with_db.post("/stop_activities/", json=test_data_api)
    assert response.status_code == 200
    data = response.json()

    # Assertions on the API response (time will be ISO 8601 strings, atco_code will be present)
    assert data["activity_type"] == test_data_api["activity_type"]
    assert data["activity_time"] == test_data_api["activity_time"]
    assert data["pax_count"] == test_data_api["pax_count"]
    assert data["atco_code"] == test_data_api["atco_code"]
    assert data["vj_id"] == test_data_api["vj_id"]
    assert "activity_id" in data

    # Verify creation by querying the database directly
    db_activity = db_session.query(StopActivity).filter(
        StopActivity.activity_id == data["activity_id"]
    ).first()
    assert db_activity is not None
    assert db_activity.stop_point_id == test_data_api["atco_code"] # Compare with atco_code from input
    assert db_activity.activity_time == time.fromisoformat(test_data_api["activity_time"])


def test_read_stop_activities(client_with_db: TestClient, db_session: Session):
    """
    Tests retrieving a list of stop activities, with and without filtering by vj_id.
    """
    vj_id_1, stop_point_atco_code_1 = setup_all_parent_entities(db_session, index=1)
    vj_id_2, stop_point_atco_code_2 = setup_all_parent_entities(db_session, index=2)

    # Create activities for vj_id_1 using 'stop_point_id' for direct DB insertion
    act_data_1_1 = {
        "activity_type": "boarding", "activity_time": time(9, 0, 0), "pax_count": 10,
        "stop_point_id": stop_point_atco_code_1, "vj_id": vj_id_1
    }
    act_data_1_2 = {
        "activity_type": "alighting", "activity_time": time(9, 5, 0), "pax_count": 5,
        "stop_point_id": stop_point_atco_code_1 + 1, "vj_id": vj_id_1
    }
    db_session.add(StopActivity(**act_data_1_1))
    db_session.add(StopActivity(**act_data_1_2))
    db_session.commit()

    # Create activities for vj_id_2
    act_data_2_1 = {
        "activity_type": "boarding", "activity_time": time(10, 0, 0), "pax_count": 15,
        "stop_point_id": stop_point_atco_code_2, "vj_id": vj_id_2
    }
    db_session.add(StopActivity(**act_data_2_1))
    db_session.commit()

    # Test reading all activities
    response = client_with_db.get("/stop_activities/")
    assert response.status_code == 200
    data = response.json()
    assert isinstance(data, list)
    assert len(data) >= 3 # At least 3 activities created

    # Test reading activities filtered by vj_id
    response_filtered = client_with_db.get(f"/stop_activities/?vj_id={vj_id_1}")
    assert response_filtered.status_code == 200
    filtered_data = response_filtered.json()
    assert isinstance(filtered_data, list)
    assert len(filtered_data) == 2
    assert all(d["vj_id"] == vj_id_1 for d in filtered_data)
    assert filtered_data[0]["activity_time"] == "09:00:00"
    assert filtered_data[0]["atco_code"] == stop_point_atco_code_1


def test_read_single_stop_activity(client_with_db: TestClient, db_session: Session):
    """
    Tests retrieving a single stop activity by its ID.
    """
    vj_id, stop_point_atco_code = setup_all_parent_entities(db_session, index=3)

    # Create the activity to be read
    act_data = {
        "activity_type": "boarding", "activity_time": time(11, 0, 0), "pax_count": 20,
        "stop_point_id": stop_point_atco_code, "vj_id": vj_id
    }
    db_activity = StopActivity(**act_data)
    db_session.add(db_activity)
    db_session.commit()
    db_session.refresh(db_activity)
    activity_id = db_activity.activity_id

    response = client_with_db.get(f"/stop_activities/{activity_id}")
    assert response.status_code == 200
    data = response.json()

    assert data["activity_id"] == activity_id
    assert data["activity_type"] == act_data["activity_type"]
    assert data["activity_time"] == act_data["activity_time"].isoformat()
    assert data["pax_count"] == act_data["pax_count"]
    assert data["atco_code"] == act_data["stop_point_id"]
    assert data["vj_id"] == act_data["vj_id"]


def test_update_stop_activity(client_with_db: TestClient, db_session: Session):
    """
    Tests updating an existing stop activity.
    """
    vj_id, stop_point_atco_code = setup_all_parent_entities(db_session, index=4)

    # Create the activity to be updated
    original_act_data = {
        "activity_type": "boarding", "activity_time": time(12, 0, 0), "pax_count": 25,
        "stop_point_id": stop_point_atco_code, "vj_id": vj_id
    }
    db_activity = StopActivity(**original_act_data)
    db_session.add(db_activity)
    db_session.commit()
    db_session.refresh(db_activity)
    activity_id = db_activity.activity_id

    # Data for update via API (time as ISO 8601 string, atco_code for stop_point)
    update_data_api = {
        "activity_type": "alighting",
        "activity_time": "12:05:00",
        "pax_count": 10,
        "atco_code": stop_point_atco_code + 5 # Update stop_point_id via atco_code
    }
    response = client_with_db.put(
        f"/stop_activities/{activity_id}",
        json=update_data_api
    )
    assert response.status_code == 200
    data = response.json()

    # Assertions on the API response
    assert data["activity_id"] == activity_id
    assert data["activity_type"] == update_data_api["activity_type"]
    assert data["activity_time"] == update_data_api["activity_time"]
    assert data["pax_count"] == update_data_api["pax_count"]
    assert data["atco_code"] == update_data_api["atco_code"]

    # Verify update by querying the database directly
    updated_db_activity = db_session.query(StopActivity).filter(
        StopActivity.activity_id == activity_id
    ).first()
    assert updated_db_activity is not None
    assert updated_db_activity.activity_type == update_data_api["activity_type"]
    assert updated_db_activity.activity_time == time.fromisoformat(update_data_api["activity_time"])
    assert updated_db_activity.pax_count == update_data_api["pax_count"]
    assert updated_db_activity.stop_point_id == update_data_api["atco_code"]


def test_delete_stop_activity(client_with_db: TestClient, db_session: Session):
    """
    Tests deleting a stop activity.
    """
    vj_id, stop_point_atco_code = setup_all_parent_entities(db_session, index=5)

    # Create the activity to be deleted
    def_data = {
        "activity_type": "boarding", "activity_time": time(13, 0, 0), "pax_count": 30,
        "stop_point_id": stop_point_atco_code, "vj_id": vj_id
    }
    db_activity = StopActivity(**def_data)
    db_session.add(db_activity)
    db_session.commit()
    db_session.refresh(db_activity)
    activity_id = db_activity.activity_id

    response = client_with_db.delete(f"/stop_activities/{activity_id}")
    assert response.status_code == 200
    assert response.json()["message"] == "Stop activity deleted successfully"

    # Verify deletion by attempting to retrieve from the database
    deleted_db_activity = db_session.query(StopActivity).filter(
        StopActivity.activity_id == activity_id
    ).first()
    assert deleted_db_activity is None

    # Verify deletion by attempting to retrieve via API
    response = client_with_db.get(f"/stop_activities/{activity_id}")
    assert response.status_code == 404
