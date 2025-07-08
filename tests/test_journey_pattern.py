from fastapi.testclient import TestClient
from sqlalchemy.orm import Session
from api.models import JourneyPattern


def test_create_journey_pattern(client_with_db: TestClient, db_session: Session):
    test_data = {
        "jp_code": "JP001_CREATE",
        "line_id": 1,
        "route_id": 1,
        "service_id": 1,
        "operator_id": 1,
        "name": "Morning Commute",
    }

    response = client_with_db.post("/journey_patterns/", json=test_data)
    assert response.status_code == 200
    data = response.json()

    assert data["jp_code"] == test_data["jp_code"]
    assert "jp_id" in data
    assert data["name"] == test_data["name"]

    db_jp = (
        db_session.query(JourneyPattern)
        .filter(JourneyPattern.jp_code == test_data["jp_code"])
        .first()
    )
    assert db_jp is not None
    assert db_jp.jp_code == test_data["jp_code"]
    assert db_jp.name == test_data["name"]


def test_read_journey_patterns(client_with_db: TestClient, db_session: Session):
    db_jp = JourneyPattern(
        **{
            "jp_code": "JP002_LIST",
            "line_id": 2,
            "route_id": 2,
            "service_id": 2,
            "operator_id": 2,
            "name": "Evening Commute List",
        }
    )
    db_session.add(db_jp)
    db_session.commit()
    db_session.refresh(db_jp)

    response = client_with_db.get("/journey_patterns/")
    assert response.status_code == 200
    data = response.json()

    assert isinstance(data, list)
    assert len(data) > 0
    assert any(jp["jp_code"] == "JP002_LIST" for jp in data)


def test_read_single_journey_pattern(client_with_db: TestClient, db_session: Session):
    db_jp = JourneyPattern(
        **{
            "jp_code": "JP003_SINGLE",
            "line_id": 3,
            "route_id": 3,
            "service_id": 3,
            "operator_id": 3,
            "name": "Midday Single",
        }
    )
    db_session.add(db_jp)
    db_session.commit()
    db_session.refresh(db_jp)
    jp_id = db_jp.jp_id

    response = client_with_db.get(f"/journey_patterns/{jp_id}")
    assert response.status_code == 200
    data = response.json()

    assert data["jp_id"] == jp_id
    assert data["jp_code"] == "JP003_SINGLE"
    assert data["name"] == "Midday Single"


def test_update_journey_pattern(client_with_db: TestClient, db_session: Session):
    db_jp = JourneyPattern(
        **{
            "jp_code": "JP004_UPDATE",
            "line_id": 4,
            "route_id": 4,
            "service_id": 4,
            "operator_id": 4,
            "name": "Original Name",
        }
    )
    db_session.add(db_jp)
    db_session.commit()
    db_session.refresh(db_jp)
    jp_id = db_jp.jp_id

    update_data = {"name": "Updated Pattern Name"}
    response = client_with_db.put(f"/journey_patterns/{jp_id}", json=update_data)
    assert response.status_code == 200
    data = response.json()

    assert data["jp_id"] == jp_id
    assert data["name"] == update_data["name"]

    updated_db_jp = (
        db_session.query(JourneyPattern).filter(JourneyPattern.jp_id == jp_id).first()
    )
    assert updated_db_jp is not None
    assert updated_db_jp.name == update_data["name"]


def test_delete_journey_pattern(client_with_db: TestClient, db_session: Session):
    db_jp = JourneyPattern(
        **{
            "jp_code": "JP005_DELETE",
            "line_id": 5,
            "route_id": 5,
            "service_id": 5,
            "operator_id": 5,
            "name": "To Be Deleted",
        }
    )
    db_session.add(db_jp)
    db_session.commit()
    db_session.refresh(db_jp)
    jp_id = db_jp.jp_id

    response = client_with_db.delete(f"/journey_patterns/{jp_id}")
    assert response.status_code == 200
    assert response.json()["message"] == "Journey pattern deleted successfully"

    deleted_db_jp = (
        db_session.query(JourneyPattern).filter(JourneyPattern.jp_id == jp_id).first()
    )
    assert deleted_db_jp is None

    response = client_with_db.get(f"/journey_patterns/{jp_id}")
    assert response.status_code == 404
