from fastapi.testclient import TestClient
from sqlalchemy.orm import Session

from api.models import (
    Route,
    StopPoint,
    RouteDefinition,
    StopArea,
)  # Import necessary models

# Import the client_with_db and db_session fixtures from conftest.py


def setup_parent_entities(db_session: Session, index: int = 0):  # Added index parameter
    """
    Helper function to create necessary parent entities (Route, StopArea, StopPoint)
    for RouteDefinition tests. Uses an index to ensure unique IDs.
    """
    # Create a StopArea first
    stop_area_data = {
        "stop_area_code": 9000 + index,  # Make unique using index
        "admin_area_code": f"AA{90 + index}",  # Make unique using index
        "name": f"Test Stop Area for Route Def {index}",
        "is_terminal": True,
    }
    db_stop_area = StopArea(**stop_area_data)
    db_session.add(db_stop_area)
    db_session.commit()
    db_session.refresh(db_stop_area)

    # Create a StopPoint
    stop_point_data = {
        "atco_code": 10001 + index,  # Make unique using index
        "name": f"Test Stop Point for Route Def {index}",
        "latitude": 51.5 + (index * 0.001),  # Slightly vary latitude for uniqueness
        "longitude": -0.1 + (index * 0.001),  # Slightly vary longitude for uniqueness
        "stop_area_code": db_stop_area.stop_area_code,
    }
    db_stop_point = StopPoint(**stop_point_data)
    db_session.add(db_stop_point)
    db_session.commit()
    db_session.refresh(db_stop_point)

    # Create a Route
    route_data = {
        "name": f"Test Route for Definition {index}",
        "operator_id": 1,  # Assuming operator_id 1 exists or is not strictly validated
        "description": f"A test route {index}",
    }
    db_route = Route(**route_data)
    db_session.add(db_route)
    db_session.commit()
    db_session.refresh(db_route)

    return (
        db_route.route_id,
        db_stop_point.atco_code,
    )  # Return route_id and stop_point_id


def test_create_route_definition(client_with_db: TestClient, db_session: Session):
    """
    Tests the creation of a new route definition via the API.
    Handles the 'stop_point_atco_code' vs 'stop_point_id' mapping.
    """
    route_id, stop_point_atco_code = setup_parent_entities(
        db_session, index=0
    )  # Pass index

    # Data to send to the FastAPI endpoint (uses stop_point_atco_code as per schema)
    test_data_api = {
        "route_id": route_id,
        "stop_point_atco_code": stop_point_atco_code,  # Use stop_point_atco_code for API interaction
        "sequence": 1,
    }

    response = client_with_db.post("/route_definitions/", json=test_data_api)
    assert response.status_code == 200
    data = response.json()

    # Assertions on the API response (will contain stop_point_atco_code)
    assert data["route_id"] == test_data_api["route_id"]
    assert data["sequence"] == test_data_api["sequence"]
    assert data["stop_point_atco_code"] == test_data_api["stop_point_atco_code"]

    # Verify creation by querying the database directly
    # When querying the DB directly, the model uses 'stop_point_id'.
    db_def = (
        db_session.query(RouteDefinition)
        .filter(RouteDefinition.route_id == route_id, RouteDefinition.sequence == 1)
        .first()
    )
    assert db_def is not None
    assert (
        db_def.stop_point_id == test_data_api["stop_point_atco_code"]
    )  # Compare with atco_code from input


def test_read_route_definitions(client_with_db: TestClient, db_session: Session):
    """
    Tests retrieving a list of route definitions, with and without filtering by route_id.
    Ensures correct field names are used for direct DB insertion and API response assertion.
    """
    route_id_1, stop_point_atco_code_1 = setup_parent_entities(
        db_session, index=1
    )  # Pass index 1
    route_id_2, stop_point_atco_code_2 = setup_parent_entities(
        db_session, index=2
    )  # Pass index 2

    # Create definitions for route_id_1 using 'stop_point_id' for direct DB insertion
    def_data_1_1 = {
        "route_id": route_id_1,
        "stop_point_id": stop_point_atco_code_1,
        "sequence": 1,
    }
    def_data_1_2 = {
        "route_id": route_id_1,
        "stop_point_id": stop_point_atco_code_1 + 1,
        "sequence": 2,
    }  # Use a different stop point for sequence 2
    db_session.add(RouteDefinition(**def_data_1_1))
    db_session.add(RouteDefinition(**def_data_1_2))
    db_session.commit()

    # Create definitions for route_id_2 using 'stop_point_id' for direct DB insertion
    def_data_2_1 = {
        "route_id": route_id_2,
        "stop_point_id": stop_point_atco_code_2,
        "sequence": 1,
    }
    db_session.add(RouteDefinition(**def_data_2_1))
    db_session.commit()

    # Test reading all definitions
    response = client_with_db.get("/route_definitions/")
    assert response.status_code == 200
    data = response.json()
    assert isinstance(data, list)
    assert len(data) >= 3  # At least 3 definitions created across two routes

    # Test reading definitions filtered by route_id
    response_filtered = client_with_db.get(f"/route_definitions/?route_id={route_id_1}")
    assert response_filtered.status_code == 200
    filtered_data = response_filtered.json()
    assert isinstance(filtered_data, list)
    assert len(filtered_data) == 2
    assert all(d["route_id"] == route_id_1 for d in filtered_data)
    assert any(d["sequence"] == 1 for d in filtered_data)
    assert any(d["sequence"] == 2 for d in filtered_data)
    # Assert on 'stop_point_atco_code' from the API response
    assert filtered_data[0]["stop_point_atco_code"] == stop_point_atco_code_1


def test_update_route_definition(client_with_db: TestClient, db_session: Session):
    """
    Tests updating an existing route definition.
    Handles the 'stop_point_atco_code' vs 'stop_point_id' mapping.
    """
    route_id, stop_point_atco_code = setup_parent_entities(
        db_session, index=3
    )  # Pass index

    # Create the definition to be updated (using 'stop_point_id' for direct DB insertion)
    original_def_data = {
        "route_id": route_id,
        "stop_point_id": stop_point_atco_code,
        "sequence": 1,
    }
    db_def = RouteDefinition(**original_def_data)
    db_session.add(db_def)
    db_session.commit()
    db_session.refresh(db_def)
    sequence = db_def.sequence

    # Data for update via API (uses stop_point_atco_code as per schema)
    update_data_api = {
        "stop_point_atco_code": stop_point_atco_code + 10
    }  # Use atco_code for API
    response = client_with_db.put(
        f"/route_definitions/{route_id}/{sequence}", json=update_data_api
    )
    assert response.status_code == 200
    data = response.json()

    # Assertions on the API response (will contain stop_point_atco_code)
    assert data["route_id"] == route_id
    assert data["sequence"] == sequence
    assert data["stop_point_atco_code"] == update_data_api["stop_point_atco_code"]

    # Verify update by querying the database directly
    updated_db_def = (
        db_session.query(RouteDefinition)
        .filter(
            RouteDefinition.route_id == route_id, RouteDefinition.sequence == sequence
        )
        .first()
    )
    assert updated_db_def is not None
    assert (
        updated_db_def.stop_point_id == update_data_api["stop_point_atco_code"]
    )  # Compare with atco_code from input


def test_delete_route_definition(client_with_db: TestClient, db_session: Session):
    """
    Tests deleting a route definition.
    """
    route_id, stop_point_atco_code = setup_parent_entities(
        db_session, index=4
    )  # Pass index

    # Create the definition to be deleted (using 'stop_point_id' for direct DB insertion)
    def_data = {
        "route_id": route_id,
        "stop_point_id": stop_point_atco_code,
        "sequence": 1,
    }
    db_def = RouteDefinition(**def_data)
    db_session.add(db_def)
    db_session.commit()
    db_session.refresh(db_def)
    sequence = db_def.sequence

    response = client_with_db.delete(f"/route_definitions/{route_id}/{sequence}")
    assert response.status_code == 200
    assert response.json()["message"] == "Route definition deleted successfully"

    # Verify deletion by attempting to retrieve from the database
    deleted_db_def = (
        db_session.query(RouteDefinition)
        .filter(
            RouteDefinition.route_id == route_id, RouteDefinition.sequence == sequence
        )
        .first()
    )
    assert deleted_db_def is None

    # Verify deletion by attempting to retrieve via API
    response = client_with_db.get(f"/route_definitions/?route_id={route_id}")
    assert response.status_code == 200
    data = response.json()
    assert (
        len(data) == 0
    )  # No definitions should be found for this route_id after deletion
