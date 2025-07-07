# api/models.py
from datetime import time, datetime
from enum import IntEnum
import json
from sqlalchemy import DateTime, ForeignKey, func, SmallInteger, String, JSON
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, relationship
from typing import Optional, List, Dict, Any
from sqlalchemy import Column, Integer, String, DateTime, Time, ForeignKey, UniqueConstraint
from sqlalchemy.ext.hybrid import hybrid_property
from sqlalchemy.ext.declarative import declarative_base # <--- THIS LINE IS CRUCIAL


Base = declarative_base()

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
    jp_definitions: Mapped[list["JourneyPatternDefinition"]] = relationship(
        back_populates="stop_point"
    )
    stop_activities: Mapped[list["StopActivity"]] = relationship(
        back_populates="stop_point"
    )


class Garage(Base):
    __tablename__ = "garage"

    garage_id: Mapped[int] = mapped_column(primary_key=True)
    name: Mapped[str] = mapped_column(String(100), unique=True)
    capacity: Mapped[int]
    latitude: Mapped[float]
    longitude: Mapped[float]

    buses: Mapped[list["Bus"]] = relationship(back_populates="garage")


class Operator(Base):
    __tablename__ = "operator"

    operator_id: Mapped[int] = mapped_column(primary_key=True)
    operator_code: Mapped[str] = mapped_column(String(10), unique=True)
    name: Mapped[str] = mapped_column(String(100))

    buses: Mapped[list["Bus"]] = relationship(back_populates="operator")
    routes: Mapped[list["Route"]] = relationship(back_populates="operator")
    services: Mapped[list["Service"]] = relationship(back_populates="operator")
    lines: Mapped[list["Line"]] = relationship(back_populates="operator")
    journey_patterns: Mapped[list["JourneyPattern"]] = relationship(
        back_populates="operator"
    )
    blocks: Mapped[list["Block"]] = relationship(back_populates="operator")
    vehicle_journeys: Mapped[list["VehicleJourney"]] = relationship(
        back_populates="operator"
    )


class BusType(Base):
    __tablename__ = "bus_type"

    type_id: Mapped[int] = mapped_column(primary_key=True)
    name: Mapped[str] = mapped_column(String(50), unique=True)
    capacity: Mapped[int]

    buses: Mapped[list["Bus"]] = relationship(back_populates="bus_type")
    blocks: Mapped[list["Block"]] = relationship(back_populates="bus_type")


class Route(Base):
    __tablename__ = "route"

    route_id: Mapped[int] = mapped_column(primary_key=True)
    name: Mapped[str] = mapped_column(String(100))
    description: Mapped[Optional[str]] = mapped_column(String(255))

    operator: Mapped["Operator"] = relationship(back_populates="routes")
    operator_id: Mapped[int] = mapped_column(ForeignKey("operator.operator_id"))

    route_definitions: Mapped[list["RouteDefinition"]] = relationship(
        back_populates="route"
    )
    journey_patterns: Mapped[list["JourneyPattern"]] = relationship(
        back_populates="route"
    )


class Service(Base):
    __tablename__ = "service"

    service_id: Mapped[int] = mapped_column(primary_key=True)
    service_code: Mapped[str] = mapped_column(String(20), unique=True)
    name: Mapped[str] = mapped_column(String(100))
    description: Mapped[Optional[str]] = mapped_column(String(255))

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


class JourneyPattern(Base):
    __tablename__ = "journey_pattern"

    jp_id: Mapped[int] = mapped_column(primary_key=True)
    jp_code: Mapped[str] = mapped_column(String(20), unique=True)
    name: Mapped[Optional[str]] = mapped_column(String(100))

    line: Mapped["Line"] = relationship(back_populates="journey_patterns")
    line_id: Mapped[int] = mapped_column(ForeignKey("line.line_id"))

    route: Mapped["Route"] = relationship(back_populates="journey_patterns")
    route_id: Mapped[int] = mapped_column(ForeignKey("route.route_id"))

    service: Mapped["Service"] = relationship(back_populates="journey_patterns")
    service_id: Mapped[int] = mapped_column(ForeignKey("service.service_id"))

    operator: Mapped["Operator"] = relationship(back_populates="journey_patterns")
    operator_id: Mapped[int] = mapped_column(ForeignKey("operator.operator_id"))

    jp_definitions: Mapped[list["JourneyPatternDefinition"]] = relationship(
        back_populates="journey_pattern"
    )
    vehicle_journeys: Mapped[list["VehicleJourney"]] = relationship(
        back_populates="journey_pattern"
    )


class StopActivity(Base):
    __tablename__ = "stop_activity"

    activity_id: Mapped[int] = mapped_column(primary_key=True)
    activity_type: Mapped[str] = mapped_column(String(50))
    activity_time: Mapped[time]
    pax_count: Mapped[int]

    stop_point: Mapped["StopPoint"] = relationship(back_populates="stop_activities")
    stop_point_id: Mapped[int] = mapped_column(ForeignKey("stop_point.atco_code"))

    vehicle_journey: Mapped["VehicleJourney"] = relationship(
        back_populates="stop_activities"
    )
    vj_id: Mapped[int] = mapped_column(ForeignKey("vehicle_journey.vj_id"))


class JourneyPatternDefinition(Base):
    __tablename__ = "journey_pattern_definition"

    jp_id: Mapped[int] = mapped_column(ForeignKey("journey_pattern.jp_id"), primary_key=True)
    stop_point_id: Mapped[int] = mapped_column(ForeignKey("stop_point.atco_code"), primary_key=True)
    sequence: Mapped[int] = mapped_column(SmallInteger, primary_key=True)
    arrival_time: Mapped[time]
    departure_time: Mapped[time]

    journey_pattern: Mapped["JourneyPattern"] = relationship(
        back_populates="jp_definitions"
    )
    stop_point: Mapped["StopPoint"] = relationship(back_populates="jp_definitions")


class RouteDefinition(Base):
    __tablename__ = "route_definition"

    route_id: Mapped[int] = mapped_column(ForeignKey("route.route_id"), primary_key=True)
    stop_point_id: Mapped[int] = mapped_column(ForeignKey("stop_point.atco_code"), primary_key=True)
    sequence: Mapped[int] = mapped_column(SmallInteger, primary_key=True)

    route: Mapped["Route"] = relationship(back_populates="route_definitions")
    stop_point: Mapped["StopPoint"] = relationship(back_populates="route_definitions")


class Block(Base):
    __tablename__ = "block"

    block_id: Mapped[int] = mapped_column(primary_key=True)
    name: Mapped[str] = mapped_column(String(100), unique=True)

    operator: Mapped["Operator"] = relationship(back_populates="blocks")
    operator_id: Mapped[int] = mapped_column(ForeignKey("operator.operator_id"))

    bus_type: Mapped["BusType"] = relationship(back_populates="blocks")
    bus_type_id: Mapped[int] = mapped_column(ForeignKey("bus_type.type_id"))

    vehicle_journeys: Mapped[list["VehicleJourney"]] = relationship(
        back_populates="block"
    )


class VehicleJourney(Base):
    __tablename__ = "vehicle_journey"

    vj_id: Mapped[int] = mapped_column(primary_key=True)
    departure_time: Mapped[time]
    dayshift: Mapped[int]

    journey_pattern: Mapped["JourneyPattern"] = relationship(
        back_populates="vehicle_journeys"
    )
    jp_id: Mapped[int] = mapped_column(ForeignKey("journey_pattern.jp_id"))

    block: Mapped["Block"] = relationship(back_populates="vehicle_journeys")
    block_id: Mapped[int] = mapped_column(ForeignKey("block.block_id"))

    operator: Mapped["Operator"] = relationship(back_populates="vehicle_journeys")
    operator_id: Mapped[int] = mapped_column(ForeignKey("operator.operator_id"))

    line: Mapped["Line"] = relationship(back_populates="vehicle_journeys")
    line_id: Mapped[int] = mapped_column(ForeignKey("line.line_id"))

    service: Mapped["Service"] = relationship(back_populates="vehicle_journeys")
    service_id: Mapped[int] = mapped_column(ForeignKey("service.service_id"))

    stop_activities: Mapped[list["StopActivity"]] = relationship(
        back_populates="vehicle_journey"
    )


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


# ──────────────── EmulatorLog ────────────────
class RunStatus(IntEnum):
    QUEUED = 0
    RUNNING = 1
    COMPLETED = 2
    FAILED = 3


class EmulatorLog(Base):
    __tablename__ = "emulator_log"

    run_id: Mapped[int] = mapped_column(Integer, primary_key=True, index=True)
    status: Mapped[int] = mapped_column(Integer) # Stores RunStatus enum value
    started_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.now)
    last_updated: Mapped[datetime] = mapped_column(DateTime, default=datetime.now, onupdate=datetime.now)
    optimization_details: Mapped[Optional[str]] = mapped_column(String, nullable=True) # Stores JSON string

    # Hybrid property to convert the JSON string to a dictionary and vice-versa
    @hybrid_property
    def optimization_details_dict(self) -> Optional[Dict[str, Any]]:
        if self.optimization_details:
            try:
                return json.loads(self.optimization_details)
            except json.JSONDecodeError:
                # Handle cases where the string might not be valid JSON
                return None
        return None

    @optimization_details_dict.setter
    def optimization_details_dict(self, value: Optional[Dict[str, Any]]):
        if value is not None:
            # Ensure the value is a dictionary before dumping
            if isinstance(value, dict):
                self.optimization_details = json.dumps(value)
            else:
                # If not a dict, set to None or handle error as appropriate
                self.optimization_details = None
        else:
            self.optimization_details = None
