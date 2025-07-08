from fastapi.testclient import TestClient
from sqlalchemy.orm import Session
from datetime import time

from api.models import Route, StopPoint, RouteDefinition
import pytest


def test_create_route_definition(client_with_db: TestClient, db_session: Session):
   
    route_data = {"name": "Test Route for RD", "operator_id": 1}
    db_route = db_session.query(Route).filter_by(name=route_data["name"]).first()
    if not db_route:
        db_route = Route(**route_data)
        db_session.add(db_route)
        db_session.commit()
        db_session.refresh(db_route)
    route_id = db_route.route_id

    stop_point_data = {
        "atco_code": 100001,
        "name": "Test Stop Point RD",
        "latitude": 51.5,
        "longitude": 0.1,
        "stop_area_code": 1,
    }
    db_stop_point = db_session.query(StopPoint).filter_by(atco_code=stop_point_data["atco_code"]).first()
    if not db_stop_point:
        db_stop_point = StopPoint(**stop_point_data)
        db_session.add(db_stop_point)
        db_session.commit()
        db_session.refresh(db_stop_point)
    stop_point_id = db_stop_point.atco_code


    test_data = {
        "route_id": route_id,
        "stop_point_id": stop_point_id, 
        "sequence": 1,
    }

    response = client_with_db.post("/route_definitions/", json=test_data)
    assert response.status_code == 201 

    response_data = response.json()
    assert response_data["route_id"] == route_id
    assert response_data["stop_point_id"] == stop_point_id
    assert response_data["sequence"] == 1

    db_definition = (
        db_session.query(RouteDefinition)
        .filter_by(
            route_id=route_id,
            stop_point_id=stop_point_id,
            sequence=1
        )
        .first()
    )
    assert db_definition is not None
    assert db_definition.route_id == route_id
    assert db_definition.stop_point_id == stop_point_id
    assert db_definition.sequence == 1

    response = client_with_db.post("/route_definitions/", json=test_data)
    assert response.status_code == 409


def test_read_route_definitions(client_with_db: TestClient, db_session: Session):
    
    route_data_1 = {"name": "Test Route All 1", "operator_id": 1}
    db_route_1 = db_session.query(Route).filter_by(name=route_data_1["name"]).first()
    if not db_route_1:
        db_route_1 = Route(**route_data_1)
        db_session.add(db_route_1)
        db_session.commit()
        db_session.refresh(db_route_1)
    route_id_1 = db_route_1.route_id

    route_data_2 = {"name": "Test Route All 2", "operator_id": 1}
    db_route_2 = db_session.query(Route).filter_by(name=route_data_2["name"]).first()
    if not db_route_2:
        db_route_2 = Route(**route_data_2)
        db_session.add(db_route_2)
        db_session.commit()
        db_session.refresh(db_route_2)
    route_id_2 = db_route_2.route_id

    stop_point_data_1 = {
        "atco_code": 100002,
        "name": "Test Stop Point All 1",
        "latitude": 51.6,
        "longitude": 0.2,
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
        "atco_code": 100003,
        "name": "Test Stop Point All 2",
        "latitude": 51.7,
        "longitude": 0.3,
        "stop_area_code": 1,
    }
    db_stop_point_2 = db_session.query(StopPoint).filter_by(atco_code=stop_point_data_2["atco_code"]).first()
    if not db_stop_point_2:
        db_stop_point_2 = StopPoint(**stop_point_data_2)
        db_session.add(db_stop_point_2)
        db_session.commit()
        db_session.refresh(db_stop_point_2)
    stop_point_id_2 = db_stop_point_2.atco_code

    def_1 = RouteDefinition(route_id=route_id_1, stop_point_id=stop_point_id_1, sequence=1)
    def_2 = RouteDefinition(route_id=route_id_1, stop_point_id=stop_point_id_2, sequence=2)
    def_3 = RouteDefinition(route_id=route_id_2, stop_point_id=stop_point_id_1, sequence=1)
    db_session.add_all([def_1, def_2, def_3])
    db_session.commit()

    response = client_with_db.get("/route_definitions/")
    assert response.status_code == 200
    data = response.json()
    assert len(data) >= 3 
    for item in data:
        assert "route_id" in item
        assert "stop_point_id" in item
        assert "sequence" in item
        assert isinstance(item["route_id"], int)
        assert isinstance(item["stop_point_id"], int)
        assert isinstance(item["sequence"], int)

    response = client_with_db.get(f"/route_definitions/?route_id={route_id_1}")
    assert response.status_code == 200
    data = response.json()
    assert len(data) == 2
    assert all(item["route_id"] == route_id_1 for item in data)


def test_read_single_route_definition(client_with_db: TestClient, db_session: Session):
    
    route_data = {"name": "Test Route Single", "operator_id": 1}
    db_route = db_session.query(Route).filter_by(name=route_data["name"]).first()
    if not db_route:
        db_route = Route(**route_data)
        db_session.add(db_route)
        db_session.commit()
        db_session.refresh(db_route)
    route_id = db_route.route_id

    stop_point_data = {
        "atco_code": 100004,
        "name": "Test Stop Point Single",
        "latitude": 51.8,
        "longitude": 0.4,
        "stop_area_code": 1,
    }
    db_stop_point = db_session.query(StopPoint).filter_by(atco_code=stop_point_data["atco_code"]).first()
    if not db_stop_point:
        db_stop_point = StopPoint(**stop_point_data)
        db_session.add(db_stop_point)
        db_session.commit()
        db_session.refresh(db_stop_point)
    stop_point_id = db_stop_point.atco_code

    def_data = {
        "route_id": route_id,
        "stop_point_id": stop_point_id,
        "sequence": 1,
    }
    db_def = RouteDefinition(**def_data)
    db_session.add(db_def)
    db_session.commit()
    db_session.refresh(db_def)

    response = client_with_db.get(
        f"/route_definitions/{route_id}/{stop_point_id}/{db_def.sequence}"
    )
    assert response.status_code == 200
    data = response.json()
    assert data["route_id"] == route_id
    assert data["stop_point_id"] == stop_point_id
    assert data["sequence"] == db_def.sequence

    response = client_with_db.get(
        f"/route_definitions/{route_id}/{stop_point_id}/999"
    )
    assert response.status_code == 404


def test_update_route_definition(client_with_db: TestClient, db_session: Session):
  
    route_data = {"name": "Test Route Update", "operator_id": 1}
    db_route = db_session.query(Route).filter_by(name=route_data["name"]).first()
    if not db_route:
        db_route = Route(**route_data)
        db_session.add(db_route)
        db_session.commit()
        db_session.refresh(db_route)
    route_id = db_route.route_id

    stop_point_data_orig = {
        "atco_code": 100005,
        "name": "Test Stop Point Update Orig",
        "latitude": 51.9,
        "longitude": 0.5,
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
        "atco_code": 100006,
        "name": "Test Stop Point Update New",
        "latitude": 52.0,
        "longitude": 0.6,
        "stop_area_code": 1,
    }
    db_stop_point_new = db_session.query(StopPoint).filter_by(atco_code=stop_point_data_new["atco_code"]).first()
    if not db_stop_point_new:
        db_stop_point_new = StopPoint(**stop_point_data_new)
        db_session.add(db_stop_point_new)
        db_session.commit()
        db_session.refresh(db_stop_point_new)
    stop_point_id_new = db_stop_point_new.atco_code

    def_data = {
        "route_id": route_id,
        "stop_point_id": stop_point_id_orig,
        "sequence": 1,
    }
    db_def = RouteDefinition(**def_data)
    db_session.add(db_def)
    db_session.commit()
    db_session.refresh(db_def)

    update_data = {"stop_point_id": stop_point_id_new}
    response = client_with_db.put(
        f"/route_definitions/{route_id}/{stop_point_id_orig}/{db_def.sequence}",
        json=update_data,
    )
    assert response.status_code == 200
    data = response.json()
    assert data["route_id"] == route_id
    assert data["stop_point_id"] == stop_point_id_new
    assert data["sequence"] == db_def.sequence

    updated_db_def = (
        db_session.query(RouteDefinition)
        .filter_by(
            route_id=route_id,
            stop_point_id=stop_point_id_new, 
            sequence=db_def.sequence
        )
        .first()
    )
    assert updated_db_def.stop_point_id == stop_point_id_new


    update_data_seq = {"sequence": 2}
    response_seq = client_with_db.put(
        f"/route_definitions/{route_id}/{stop_point_id_new}/{db_def.sequence}", 
        json=update_data_seq,
    )
    assert response_seq.status_code == 200
    data_seq = response_seq.json()
    assert data_seq["sequence"] == 2


    response = client_with_db.put(
        f"/route_definitions/{route_id}/{stop_point_id_new}/999",
        json={"sequence": 3},
    )
    assert response.status_code == 404


def test_delete_route_definition(client_with_db: TestClient, db_session: Session):

    route_data = {"name": "Test Route Delete", "operator_id": 1}
    db_route = db_session.query(Route).filter_by(name=route_data["name"]).first()
    if not db_route:
        db_route = Route(**route_data)
        db_session.add(db_route)
        db_session.commit()
        db_session.refresh(db_route)
    route_id = db_route.route_id

    stop_point_data = {
        "atco_code": 100007,
        "name": "Test Stop Point Delete",
        "latitude": 52.1,
        "longitude": 0.7,
        "stop_area_code": 1,
    }
    db_stop_point = db_session.query(StopPoint).filter_by(atco_code=stop_point_data["atco_code"]).first()
    if not db_stop_point:
        db_stop_point = StopPoint(**stop_point_data)
        db_session.add(db_stop_point)
        db_session.commit()
        db_session.refresh(db_stop_point)
    stop_point_id = db_stop_point.atco_code

    def_data = {
        "route_id": route_id,
        "stop_point_id": stop_point_id,
        "sequence": 1,
    }
    db_def = RouteDefinition(**def_data)
    db_session.add(db_def)
    db_session.commit()
    db_session.refresh(db_def)

    response = client_with_db.delete(
        f"/route_definitions/{route_id}/{stop_point_id}/{db_def.sequence}"
    )
    assert response.status_code == 204 

    deleted_db_def = (
        db_session.query(RouteDefinition)
        .filter_by(
            route_id=route_id,
            stop_point_id=stop_point_id,
            sequence=db_def.sequence
        )
        .first()
    )
    assert deleted_db_def is None

    response = client_with_db.delete(
        f"/route_definitions/{route_id}/{stop_point_id}/999"
    )
    assert response.status_code == 404