# tests/test_emulator_log.py
import pytest
from fastapi.testclient import TestClient
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import StaticPool

from api.main import app
from api.database import get_db
from api.models import Base, EmulatorLog
from api.schemas import RunStatus

# Setup the test database with StaticPool to maintain connection
TEST_DATABASE_URL = "sqlite:///:memory:"
engine = create_engine(
    TEST_DATABASE_URL,
    connect_args={"check_same_thread": False},
    poolclass=StaticPool,  # Maintains a single connection for all tests
)

# Create all tables before any tests run
Base.metadata.create_all(bind=engine)

TestingSessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)


# Override the get_db dependency
def override_get_db():
    db = TestingSessionLocal()
    try:
        yield db
    finally:
        db.close()


app.dependency_overrides[get_db] = override_get_db

client = TestClient(app)


@pytest.fixture(autouse=True)
def clean_db():
    # Clean up database before each test
    with TestingSessionLocal() as db:
        db.query(EmulatorLog).delete()
        db.commit()
    yield
    # No cleanup needed after test since we're using in-memory DB


def test_create_emulator_log():
    response = client.post(
        "/emulator_logs/",
        json={"status": RunStatus.RUNNING.value},
    )
    assert response.status_code == 200
    data = response.json()
    assert data["status"] == RunStatus.RUNNING.value
    assert "run_id" in data
    assert "started_at" in data


def test_read_emulator_logs():
    # Create test data directly in DB to avoid API dependency
    with TestingSessionLocal() as db:
        db_log = EmulatorLog(status=RunStatus.RUNNING)
        db.add(db_log)
        db.commit()
        db.refresh(db_log)

    response = client.get("/emulator_logs/")
    assert response.status_code == 200
    data = response.json()
    assert isinstance(data, list)
    assert len(data) > 0
    assert data[0]["status"] == RunStatus.RUNNING.value


def test_read_single_emulator_log():
    # Create test data directly
    with TestingSessionLocal() as db:
        db_log = EmulatorLog(status=RunStatus.RUNNING)
        db.add(db_log)
        db.commit()
        db.refresh(db_log)
        run_id = db_log.run_id

    response = client.get(f"/emulator_logs/{run_id}")
    assert response.status_code == 200
    data = response.json()
    assert data["run_id"] == run_id
    assert data["status"] == RunStatus.RUNNING.value


def test_update_emulator_log():
    # Create test data directly
    with TestingSessionLocal() as db:
        db_log = EmulatorLog(status=RunStatus.RUNNING)
        db.add(db_log)
        db.commit()
        db.refresh(db_log)
        run_id = db_log.run_id

    response = client.put(
        f"/emulator_logs/{run_id}",
        json={"status": RunStatus.COMPLETED.value},
    )
    assert response.status_code == 200
    data = response.json()
    assert data["status"] == RunStatus.COMPLETED.value


def test_delete_emulator_log():
    # Create test data directly
    with TestingSessionLocal() as db:
        db_log = EmulatorLog(status=RunStatus.RUNNING)
        db.add(db_log)
        db.commit()
        db.refresh(db_log)
        run_id = db_log.run_id

    response = client.delete(f"/emulator_logs/{run_id}")
    assert response.status_code == 200
    assert response.json() == {"message": "Emulator log deleted successfully"}

    # Verify deletion
    response = client.get(f"/emulator_logs/{run_id}")
    assert response.status_code == 404


def test_invalid_run_id():
    response = client.get("/emulator_logs/9999")
    assert response.status_code == 404
    assert response.json()["detail"] == "Emulator log not found"
