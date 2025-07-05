# api/models.py
from datetime import time
from enum import IntEnum

from sqlalchemy import DateTime, ForeignKey, func, SmallInteger, String
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, relationship


class Base(DeclarativeBase):
    pass


class StopArea(Base):
    __tablename__ = "stop_area"

    stop_area_code: Mapped[int] = mapped_column(primary_key=True)
    admin_area_code: Mapped[str] = mapped_column(String(15), unique=True)
    name: Mapped[str] = mapped_column(String(100))
    is_terminal: Mapped[bool] = mapped_column(default=True)

    stop_points: Mapped[list["StopPoint"]] = relationship(back_populates="stop_area")


class StopPoint(Base):
    __tablename__ = "stop_point"

    atco_code: Mapped[int] = mapped_column(primary_key=True)
    name: Mapped[str] = mapped_column(String(100))
    latitude: Mapped[float]
    longitude: Mapped[float]

    stop_area: Mapped["StopArea"] = relationship(back_populates="stop_points")
    stop_area_code: Mapped[int] = mapped_column(ForeignKey("stop_area.stop_area_code"))

    route_definitions: Mapped[list["RouteDefinition"]] = relationship(
        back_populates="stop_point"
    )
    journey_pattern_definitions: Mapped[list["JourneyPatternDefinition"]] = relationship(
        back_populates="stop_point"
    )
    stop_activities: Mapped[list["StopActivity"]] = relationship(
        back_populates="stop_point"
    )


class Operator(Base):
    __tablename__ = "operator"

    operator_id: Mapped[int] = mapped_column(primary_key=True)
    operator_code: Mapped[str] = mapped_column(String(10), unique=True)
    name: Mapped[str] = mapped_column(String(100))

    routes: Mapped[list["Route"]] = relationship(back_populates="operator")
    lines: Mapped[list["Line"]] = relationship(back_populates="operator")
    services: Mapped[list["Service"]] = relationship(back_populates="operator")
    buses: Mapped[list["Bus"]] = relationship(back_populates="operator")
    blocks: Mapped[list["Block"]] = relationship(back_populates="operator")
    journey_patterns: Mapped[list["JourneyPattern"]] = relationship(
        back_populates="operator"
    )
    vehicle_journeys: Mapped[list["VehicleJourney"]] = relationship(
        back_populates="operator"
    )


class Garage(Base):
    __tablename__ = "garage"

    garage_id: Mapped[int] = mapped_column(primary_key=True)
    name: Mapped[str] = mapped_column(String(100))
    capacity: Mapped[int] = mapped_column(SmallInteger)
    latitude: Mapped[float]
    longitude: Mapped[float]

    buses: Mapped[list["Bus"]] = relationship(back_populates="garage")


class BusType(Base):
    __tablename__ = "bus_type"

    type_id: Mapped[int] = mapped_column(primary_key=True)
    name: Mapped[str] = mapped_column(String(50), unique=True)
    capacity: Mapped[int] = mapped_column(SmallInteger)

    buses: Mapped[list["Bus"]] = relationship(back_populates="bus_type")
    blocks: Mapped[list["Block"]] = relationship(back_populates="bus_type")


class Route(Base):
    __tablename__ = "route"

    route_id: Mapped[int] = mapped_column(primary_key=True)
    name: Mapped[str] = mapped_column(String(100))
    description: Mapped[str] = mapped_column(String(255), nullable=True)

    operator: Mapped["Operator"] = relationship(back_populates="routes")
    operator_id: Mapped[int] = mapped_column(ForeignKey("operator.operator_id"))

    route_definitions: Mapped[list["RouteDefinition"]] = relationship(
        back_populates="route"
    )
    journey_patterns: Mapped[list["JourneyPattern"]] = relationship(
        back_populates="route"
    )


class Line(Base):
    __tablename__ = "line"

    line_id: Mapped[int] = mapped_column(primary_key=True)
    line_name: Mapped[str] = mapped_column(String(50), unique=True)

    operator: Mapped["Operator"] = relationship(back_populates="lines")
    operator_id: Mapped[int] = mapped_column(ForeignKey("operator.operator_id"))

    services: Mapped[list["Service"]] = relationship(back_populates="line")
    journey_patterns: Mapped[list["JourneyPattern"]] = relationship(
        back_populates="line"
    )
    vehicle_journeys: Mapped[list["VehicleJourney"]] = relationship(
        back_populates="line"
    )


class Service(Base):
    __tablename__ = "service"

    service_id: Mapped[int] = mapped_column(primary_key=True)
    service_code: Mapped[str] = mapped_column(String(10), unique=True)
    name: Mapped[str] = mapped_column(String(100))
    description: Mapped[str] = mapped_column(String(255), nullable=True)

    operator: Mapped["Operator"] = relationship(back_populates="services")
    operator_id: Mapped[int] = mapped_column(ForeignKey("operator.operator_id"))

    line: Mapped["Line"] = relationship(back_populates="services")
    line_id: Mapped[int] = mapped_column(ForeignKey("line.line_id"))

    journey_patterns: Mapped[list["JourneyPattern"]] = relationship(
        back_populates="service"
    )
    vehicle_journeys: Mapped[list["VehicleJourney"]] = relationship(
        back_populates="service"
    )


class JourneyPattern(Base):
    __tablename__ = "journey_pattern"

    jp_id: Mapped[int] = mapped_column(primary_key=True)
    jp_code: Mapped[str] = mapped_column(String(10), unique=True)
    name: Mapped[str] = mapped_column(String(100))

    route: Mapped["Route"] = relationship(back_populates="journey_patterns")
    route_id: Mapped[int] = mapped_column(ForeignKey("route.route_id"))

    service: Mapped["Service"] = relationship(back_populates="journey_patterns")
    service_id: Mapped[int] = mapped_column(ForeignKey("service.service_id"))

    line: Mapped["Line"] = relationship(back_populates="journey_patterns")
    line_id: Mapped[int] = mapped_column(ForeignKey("line.line_id"))

    operator: Mapped["Operator"] = relationship(back_populates="journey_patterns")
    operator_id: Mapped[int] = mapped_column(ForeignKey("operator.operator_id"))

    journey_pattern_definitions: Mapped[list["JourneyPatternDefinition"]] = relationship(
        back_populates="journey_pattern"
    )
    vehicle_journeys: Mapped[list["VehicleJourney"]] = relationship(
        back_populates="journey_pattern"
    )


class JourneyPatternDefinition(Base):
    __tablename__ = "journey_pattern_definition"

    jp_id: Mapped[int] = mapped_column(ForeignKey("journey_pattern.jp_id"), primary_key=True)
    sequence: Mapped[int] = mapped_column(primary_key=True)

    journey_pattern: Mapped["JourneyPattern"] = relationship(back_populates="journey_pattern_definitions")

    stop_point: Mapped["StopPoint"] = relationship(back_populates="journey_pattern_definitions")
    stop_point_id: Mapped[int] = mapped_column(ForeignKey("stop_point.atco_code"))

    arrival_time: Mapped[time]
    departure_time: Mapped[time]


class RouteDefinition(Base):
    __tablename__ = "route_definition"

    route_id: Mapped[int] = mapped_column(ForeignKey("route.route_id"), primary_key=True)
    sequence: Mapped[int] = mapped_column(primary_key=True)

    route: Mapped["Route"] = relationship(back_populates="route_definitions")

    stop_point: Mapped["StopPoint"] = relationship(back_populates="route_definitions")
    stop_point_id: Mapped[int] = mapped_column(ForeignKey("stop_point.atco_code"))


class Block(Base):
    __tablename__ = "block"

    block_id: Mapped[int] = mapped_column(primary_key=True)
    name: Mapped[str] = mapped_column(String(100))

    operator: Mapped["Operator"] = relationship(back_populates="blocks")
    operator_id: Mapped[int] = mapped_column(ForeignKey("operator.operator_id"))

    bus_type: Mapped["BusType"] = relationship(back_populates="blocks")
    bus_type_id: Mapped[int] = mapped_column(ForeignKey("bus_type.type_id"))

    vehicle_journeys: Mapped[list["VehicleJourney"]] = relationship(back_populates="block")


class VehicleJourney(Base):
    __tablename__ = "vehicle_journey"

    vj_id: Mapped[int] = mapped_column(primary_key=True)
    departure_time: Mapped[time]
    dayshift: Mapped[int] = mapped_column(SmallInteger)

    journey_pattern: Mapped["JourneyPattern"] = relationship(back_populates="vehicle_journeys")
    jp_id: Mapped[int] = mapped_column(ForeignKey("journey_pattern.jp_id"))

    block: Mapped["Block"] = relationship(back_populates="vehicle_journeys")
    block_id: Mapped[int] = mapped_column(ForeignKey("block.block_id"))

    operator: Mapped["Operator"] = relationship(back_populates="vehicle_journeys")
    operator_id: Mapped[int] = mapped_column(ForeignKey("operator.operator_id"))

    line: Mapped["Line"] = relationship(back_populates="vehicle_journeys")
    line_id: Mapped[int] = mapped_column(ForeignKey("line.line_id"))

    # ADDED THIS: Relationship and Foreign Key for Service
    service: Mapped["Service"] = relationship(back_populates="vehicle_journeys")
    service_id: Mapped[int] = mapped_column(ForeignKey("service.service_id"))

    stop_activities: Mapped[list["StopActivity"]] = relationship(back_populates="vehicle_journey")


class StopActivity(Base):
    __tablename__ = "stop_activity"

    activity_id: Mapped[int] = mapped_column(primary_key=True)
    activity_time: Mapped[time]
    activity_type: Mapped[str] = mapped_column(String(20))
    pax_count: Mapped[int]

    stop_point: Mapped["StopPoint"] = relationship(back_populates="stop_activities")
    stop_point_id: Mapped[int] = mapped_column(ForeignKey("stop_point.atco_code"))

    vehicle_journey: Mapped["VehicleJourney"] = relationship(back_populates="stop_activities")
    vj_id: Mapped[int] = mapped_column(ForeignKey("vehicle_journey.vj_id"))


class Bus(Base):
    __tablename__ = "bus"

    bus_id: Mapped[str] = mapped_column(String(50), primary_key=True)
    reg_num: Mapped[str] = mapped_column(String(20), unique=True)

    garage: Mapped["Garage"] = relationship(back_populates="buses")
    garage_id: Mapped[int] = mapped_column(ForeignKey("garage.garage_id"))

    operator: Mapped["Operator"] = relationship(back_populates="buses")
    operator_id: Mapped[int] = mapped_column(ForeignKey("operator.operator_id"))

    bus_type: Mapped["BusType"] = relationship(back_populates="buses")
    bus_type_id: Mapped[int] = mapped_column(ForeignKey("bus_type.type_id"))


class Demand(Base):
    __tablename__ = "demand"

    origin: Mapped[int] = mapped_column(
        ForeignKey("stop_area.stop_area_code"), primary_key=True
    )
    destination: Mapped[int] = mapped_column(
        ForeignKey("stop_area.stop_area_code"), primary_key=True
    )

    count: Mapped[float]

    start_time: Mapped[time] = mapped_column(primary_key=True)
    end_time: Mapped[time] = mapped_column(primary_key=True)


class RunStatus(IntEnum):
    running = 1
    success = 2
    failed = 3


class EmulatorLog(Base):
    __tablename__ = "emulator_log"

    run_id: Mapped[int] = mapped_column(primary_key=True)
    status: Mapped[RunStatus]

    started_at = mapped_column(DateTime(timezone=True), default=func.now())
    last_updated = mapped_column(DateTime(timezone=True), onupdate=func.now())