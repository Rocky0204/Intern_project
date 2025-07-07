# tests/test_bus_simulator.py

import pytest
from fastapi.testclient import TestClient
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, Session
from datetime import time, datetime, timedelta

# Import the main FastAPI app
from api.main import app

# Import database dependency and models
from api.database import get_db
from api.models import (
    Base, StopArea, StopPoint, BusType, Bus, Demand, Route, RouteDefinition,
    Operator, Line, Service, JourneyPattern, Block, VehicleJourney, EmulatorLog, Garage,
    JourneyPatternDefinition
)
from api.schemas import EmulatorLogRead, RunStatus

# In-memory SQLite database for testing
SQLALCHEMY_DATABASE_URL = "sqlite:///./test_pluto_simulator.db" # Use a different DB for simulator tests

# Create a new engine for testing
engine = create_engine(
    SQLALCHEMY_DATABASE_URL, connect_args={"check_same_thread": False}
)
TestingSessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

# Fixture to provide a database session for tests
@pytest.fixture(scope="function", name="db_session")
def db_session_fixture():
    """
    Fixture that provides a database session for testing.
    It creates tables before tests and drops them after each test to ensure isolation.
    """
    Base.metadata.create_all(bind=engine)  # Create tables
    connection = engine.connect()
    transaction = connection.begin()
    session = TestingSessionLocal(bind=connection)

    try:
        yield session
    finally:
        session.close()
        transaction.rollback()  # Rollback changes after test
        connection.close()
        Base.metadata.drop_all(bind=engine) # Drop tables after each test

# Fixture to provide a TestClient for FastAPI
@pytest.fixture(name="client")
def client_fixture(db_session: Session):
    """
    Fixture that provides a TestClient for FastAPI,
    with the database dependency overridden to use the test session.
    """
    def override_get_db():
        try:
            yield db_session
        finally:
            db_session.close()

    app.dependency_overrides[get_db] = override_get_db
    with TestClient(app) as test_client:
        yield test_client
    app.dependency_overrides.clear()

def setup_simulator_test_data(db: Session, include_vjs: bool = False):
    """
    Sets up dummy data required for the bus simulation.
    Optionally includes VehicleJourneys for optimized schedule testing.
    """
    # Clear existing data to ensure a clean state for each test
    for table in reversed(Base.metadata.sorted_tables):
        db.execute(table.delete())
    db.commit()

    # Create Operator
    op1 = Operator(operator_code="OP1", name="Test Operator 1")
    db.add(op1)
    db.flush()

    # Create Line
    line1 = Line(line_name="Test Line 1", operator_id=op1.operator_id)
    db.add(line1)
    db.flush()

    # Create Service
    service1 = Service(service_code="SVC1", name="Test Service 1", operator_id=op1.operator_id, line_id=line1.line_id)
    db.add(service1)
    db.flush()

    # Create StopAreas
    sa1 = StopArea(stop_area_code=1001, admin_area_code="ADM001", name="Central Station", is_terminal=True)
    sa2 = StopArea(stop_area_code=1002, admin_area_code="ADM002", name="Downtown Plaza", is_terminal=False)
    sa3 = StopArea(stop_area_code=1003, admin_area_code="ADM003", name="Uptown Market", is_terminal=True)
    db.add_all([sa1, sa2, sa3])
    db.flush()

    # Create StopPoints (linked to StopAreas)
    sp1 = StopPoint(atco_code=1, name="Stop A", latitude=51.5, longitude=-0.1, stop_area_code=sa1.stop_area_code)
    sp2 = StopPoint(atco_code=2, name="Stop B", latitude=51.51, longitude=-0.11, stop_area_code=sa2.stop_area_code)
    sp3 = StopPoint(atco_code=3, name="Stop C", latitude=51.52, longitude=-0.12, stop_area_code=sa3.stop_area_code)
    db.add_all([sp1, sp2, sp3])
    db.flush()

    # Create BusTypes
    bt_small = BusType(name="Small Bus", capacity=20)
    bt_large = BusType(name="Large Bus", capacity=50)
    db.add_all([bt_small, bt_large])
    db.flush()

    # Create Garage
    garage1 = Garage(name="Main Depot", capacity=100, latitude=51.49, longitude=-0.15)
    db.add(garage1)
    db.flush()

    # Create Buses
    bus1 = Bus(bus_id="B001", reg_num="REG1001", bus_type_id=bt_small.type_id, garage_id=garage1.garage_id, operator_id=op1.operator_id)
    bus2 = Bus(bus_id="B002", reg_num="REG1002", bus_type_id=bt_large.type_id, garage_id=garage1.garage_id, operator_id=op1.operator_id)
    db.add_all([bus1, bus2])
    db.flush()

    # Create Routes
    route1 = Route(name="Route A-C", operator_id=op1.operator_id, description="Route from A to C")
    route2 = Route(name="Route C-A", operator_id=op1.operator_id, description="Route from C to A")
    db.add_all([route1, route2])
    db.flush()

    # Create RouteDefinitions
    rd1_1 = RouteDefinition(route_id=route1.route_id, sequence=1, stop_point_id=sp1.atco_code)
    rd1_2 = RouteDefinition(route_id=route1.route_id, sequence=2, stop_point_id=sp2.atco_code)
    rd1_3 = RouteDefinition(route_id=route1.route_id, sequence=3, stop_point_id=sp3.atco_code)
    db.add_all([rd1_1, rd1_2, rd1_3])

    rd2_1 = RouteDefinition(route_id=route2.route_id, sequence=1, stop_point_id=sp3.atco_code)
    rd2_2 = RouteDefinition(route_id=route2.route_id, sequence=2, stop_point_id=sp2.atco_code)
    rd2_3 = RouteDefinition(route_id=route2.route_id, sequence=3, stop_point_id=sp1.atco_code)
    db.add_all([rd2_1, rd2_2, rd2_3])
    db.flush()

    # Create JourneyPatterns
    jp1 = JourneyPattern(
        jp_id=1, jp_code="JP001", name="JP A to C",
        route_id=route1.route_id, service_id=service1.service_id,
        line_id=line1.line_id, operator_id=op1.operator_id
    )
    jp2 = JourneyPattern(
        jp_id=2, jp_code="JP002", name="JP C to A",
        route_id=route2.route_id, service_id=service1.service_id,
        line_id=line1.line_id, operator_id=op1.operator_id
    )
    db.add_all([jp1, jp2])
    db.flush()

    # Create JourneyPatternDefinitions
    jpd1_1 = JourneyPatternDefinition(jp_id=jp1.jp_id, sequence=1, stop_point_id=sp1.atco_code, arrival_time=time(7,0), departure_time=time(7,0))
    jpd1_2 = JourneyPatternDefinition(jp_id=jp1.jp_id, sequence=2, stop_point_id=sp2.atco_code, arrival_time=time(7,15), departure_time=time(7,15))
    jpd1_3 = JourneyPatternDefinition(jp_id=jp1.jp_id, sequence=3, stop_point_id=sp3.atco_code, arrival_time=time(7,30), departure_time=time(7,30))
    db.add_all([jpd1_1, jpd1_2, jpd1_3])

    jpd2_1 = JourneyPatternDefinition(jp_id=jp2.jp_id, sequence=1, stop_point_id=sp3.atco_code, arrival_time=time(8,0), departure_time=time(8,0))
    jpd2_2 = JourneyPatternDefinition(jp_id=jp2.jp_id, sequence=2, stop_point_id=sp2.atco_code, arrival_time=time(8,15), departure_time=time(8,15))
    jpd2_3 = JourneyPatternDefinition(jp_id=jp2.jp_id, sequence=3, stop_point_id=sp1.atco_code, arrival_time=time(8,30), departure_time=time(8,30))
    db.add_all([jpd2_1, jpd2_2, jpd2_3])
    db.flush()

    # Create Demands
    demand1 = Demand(origin=sa1.stop_area_code, destination=sa3.stop_area_code, count=15.0, start_time=time(7, 45), end_time=time(8, 15))
    demand2 = Demand(origin=sa3.stop_area_code, destination=sa1.stop_area_code, count=25.0, start_time=time(8, 45), end_time=time(9, 15))
    db.add_all([demand1, demand2])
    db.commit()

    if include_vjs:
        # Create a block first
        block1 = Block(
            block_id=1,
            name="Test Block 1",
            operator_id=op1.operator_id,
            bus_type_id=bt_small.type_id  # Add this line to specify a bus type
        )
        db.add(block1)
        db.flush()

        # Create VehicleJourneys with block_id
        vj1 = VehicleJourney(
            vj_id=101,
            jp_id=jp1.jp_id,
            departure_time=time(7, 0),
            dayshift=0,
            block_id=block1.block_id,  # Add block_id
            operator_id=op1.operator_id,
            line_id=line1.line_id,
            service_id=service1.service_id
        )
        vj2 = VehicleJourney(
            vj_id=102,
            jp_id=jp2.jp_id,
            departure_time=time(8, 0),
            dayshift=0,
            block_id=block1.block_id,  # Add block_id
            operator_id=op1.operator_id,
            line_id=line1.line_id,
            service_id=service1.service_id
        )
        db.add_all([vj1, vj2])
        db.commit()


def test_run_simulation_success_optimized(client: TestClient, db_session: Session):
    """
    Tests the successful execution of the bus simulation API using an optimized schedule.
    """
    setup_simulator_test_data(db_session, include_vjs=True)

    response = client.post(
        "/simulate/run",
        params={
            "use_optimized_schedule": True,
            "start_time_minutes": 0,
            "end_time_minutes": 600 # Run for 10 hours
        }
    )

    assert response.status_code == 202
    log_entry = EmulatorLogRead(**response.json())

    assert log_entry.run_id is not None
    assert log_entry.status == RunStatus.COMPLETED # Expect COMPLETED for success
    assert log_entry.started_at is not None
    assert log_entry.last_updated is not None


def test_run_simulation_success_random(client: TestClient, db_session: Session):
    """
    Tests the successful execution of the bus simulation API using a randomly generated schedule.
    """
    setup_simulator_test_data(db_session, include_vjs=False) # Do not include VJs for random schedule

    response = client.post(
        "/simulate/run",
        params={
            "use_optimized_schedule": False, # Use random schedule
            "start_time_minutes": 0,
            "end_time_minutes": 600 # Run for 10 hours
        }
    )

    assert response.status_code == 202
    log_entry = EmulatorLogRead(**response.json())

    assert log_entry.run_id is not None
    assert log_entry.status == RunStatus.COMPLETED # Expect COMPLETED for success
    assert log_entry.started_at is not None
    assert log_entry.last_updated is not None


def test_run_simulation_no_data(client: TestClient, db_session: Session):
    """Tests the bus simulation API when no data is available."""
    # Ensure database is empty
    for table in reversed(Base.metadata.sorted_tables):
        db_session.execute(table.delete())
    db_session.commit()

    response = client.post(
        "/simulate/run",
        params={
            "use_optimized_schedule": True,
            "start_time_minutes": 0,
            "end_time_minutes": 60
        }
    )

    # Expect 500 when essential data is missing
    assert response.status_code == 500
    error_detail = response.json()
    assert "detail" in error_detail
    assert "No stop points" in error_detail["detail"]