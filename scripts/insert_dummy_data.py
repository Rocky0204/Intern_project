import logging
from datetime import time
from sqlalchemy.orm import Session

from api.database import get_db
from api.models import (
    Bus,
    BusType,
    Demand,
    Route,
    StopArea,
    StopPoint,
    RouteDefinition,
    Operator,
    Line,
    Block,
    JourneyPattern,
    Service,
    Garage,
)

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def insert_data(db: Session):
    logger.info("Inserting dummy data into the database...")
    # 1. Operator
    op1 = Operator(operator_code="OP1", name="City Bus Services")
    db.add(op1)
    db.flush()

    # 2. Garage
    garage1 = Garage(name="Main Depot", capacity=100, latitude=51.6, longitude=0.2)
    db.add(garage1)
    db.flush()

    # 3. BusType
    bus_type_small = BusType(name="Small Bus", capacity=30)
    bus_type_large = BusType(name="Large Bus", capacity=60)
    db.add_all([bus_type_small, bus_type_large])
    db.flush()

    # 4. StopArea
    sa1 = StopArea(
        stop_area_code=1,
        admin_area_code="ADM1",
        name="Central Station",
        is_terminal=True,
    )
    sa2 = StopArea(
        stop_area_code=2,
        admin_area_code="ADM2",
        name="Market Square",
        is_terminal=False,
    )
    sa3 = StopArea(
        stop_area_code=3, admin_area_code="ADM3", name="Bus Depot", is_terminal=True
    )
    db.add_all([sa1, sa2, sa3])
    db.flush()

    # 5. StopPoint
    sp1 = StopPoint(
        atco_code=1001,
        name="Stop 1A",
        latitude=51.5074,
        longitude=0.1278,
        stop_area_code=sa1.stop_area_code,
    )
    sp2 = StopPoint(
        atco_code=1002,
        name="Stop 1B",
        latitude=51.5080,
        longitude=0.1280,
        stop_area_code=sa1.stop_area_code,
    )
    sp3 = StopPoint(
        atco_code=2001,
        name="Stop 2A",
        latitude=51.5100,
        longitude=0.1300,
        stop_area_code=sa2.stop_area_code,
    )
    sp4 = StopPoint(
        atco_code=3001,
        name="Stop 3A",
        latitude=51.5200,
        longitude=0.1400,
        stop_area_code=sa3.stop_area_code,
    )
    db.add_all([sp1, sp2, sp3, sp4])
    db.flush()

    # 6. Route
    route1 = Route(
        name="City Centre Loop",
        description="Loop through city centre",
        operator_id=op1.operator_id,
    )
    route2 = Route(
        name="Suburban Link",
        description="Connects city to suburbs",
        operator_id=op1.operator_id,
    )
    db.add_all([route1, route2])
    db.flush()

    # 7. RouteDefinition
    rd1 = RouteDefinition(
        route_id=route1.route_id, sequence=1, stop_point_id=sp1.atco_code
    )
    rd2 = RouteDefinition(
        route_id=route1.route_id, sequence=2, stop_point_id=sp2.atco_code
    )
    rd3 = RouteDefinition(
        route_id=route2.route_id, sequence=1, stop_point_id=sp1.atco_code
    )
    rd4 = RouteDefinition(
        route_id=route2.route_id, sequence=2, stop_point_id=sp4.atco_code
    )
    rd5 = RouteDefinition(
        route_id=route2.route_id, sequence=3, stop_point_id=sp3.atco_code
    )
    db.add_all([rd1, rd2, rd3, rd4, rd5])
    db.flush()

    # 8. Line
    line1 = Line(line_name="Line 1", operator_id=op1.operator_id)
    line2 = Line(line_name="Line 2", operator_id=op1.operator_id)
    db.add_all([line1, line2])
    db.flush()

    # 9. Service
    service1 = Service(
        service_code="SER001",
        name="Morning Express",
        description="Morning peak service",
        operator_id=op1.operator_id,
        line_id=line1.line_id,
    )
    db.add(service1)
    db.flush()

    # 10. JourneyPattern
    jp1 = JourneyPattern(
        jp_code="JP001",
        name="Central Loop AM",
        route_id=route1.route_id,
        service_id=service1.service_id,
        line_id=line1.line_id,
        operator_id=op1.operator_id,
    )
    db.add(jp1)
    db.flush()

    # 11. Bus
    bus1 = Bus(
        bus_id="B001",
        reg_num="XYZ123",
        bus_type_id=bus_type_small.type_id,
        garage_id=garage1.garage_id,
        operator_id=op1.operator_id,
    )
    bus2 = Bus(
        bus_id="B002",
        reg_num="ABC456",
        bus_type_id=bus_type_large.type_id,
        garage_id=garage1.garage_id,
        operator_id=op1.operator_id,
    )
    db.add_all([bus1, bus2])
    db.flush()

    # 12. Block
    block1 = Block(
        name="Morning Block",
        operator_id=op1.operator_id,
        bus_type_id=bus_type_small.type_id,
    )
    block2 = Block(
        name="Evening Block",
        operator_id=op1.operator_id,
        bus_type_id=bus_type_large.type_id,
    )
    db.add_all([block1, block2])
    db.flush()

    # 13. Demand
    demand1 = Demand(
        origin=sa1.stop_area_code,
        destination=sa3.stop_area_code,
        count=50.0,
        start_time=time(8, 0),
        end_time=time(9, 0),
    )
    demand2 = Demand(
        origin=sa3.stop_area_code,
        destination=sa1.stop_area_code,
        count=70.0,
        start_time=time(17, 0),
        end_time=time(18, 0),
    )
    demand3 = Demand(
        origin=sa1.stop_area_code,
        destination=sa2.stop_area_code,
        count=20.0,
        start_time=time(9, 0),
        end_time=time(10, 0),
    )
    db.add_all([demand1, demand2, demand3])

    db.commit()
    logger.info("Dummy data inserted successfully!")


if __name__ == "__main__":
    try:
        import os

        if os.path.exists("pluto.db"):
            os.remove("pluto.db")
            logger.info("Existing pluto.db removed.")
        from api.database import get_db

        with next(get_db()) as db:
            insert_data(db)
    except Exception as e:
        logger.error(f"An error occurred during dummy data insertion: {e}")
        import traceback

        traceback.print_exc()
