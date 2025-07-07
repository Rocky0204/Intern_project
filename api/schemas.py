# api/schemas.py
from datetime import datetime, time
from enum import IntEnum
from pydantic import BaseModel, ConfigDict
from typing import Dict, Any, List, Optional


# ──────────────── Bus ────────────────
class BusBase(BaseModel):
    bus_id: str
    bus_type_id: int
    garage_id: int
    operator_id: int


class BusCreate(BusBase):
    reg_num: str


class BusRead(BusBase):
    reg_num: str
    model_config = ConfigDict(from_attributes=True)


class BusUpdate(BaseModel):
    registration_number: Optional[str] = None
    garage_id: Optional[int] = None
    operator_id: Optional[int] = None
    bus_type_id: Optional[int] = None


# ───── OPERATOR ─────
class OperatorBase(BaseModel):
    operator_code: str
    name: str


class OperatorCreate(OperatorBase):
    pass


class OperatorRead(OperatorBase):
    operator_id: int
    model_config = ConfigDict(from_attributes=True)


class OperatorUpdate(BaseModel):
    operator_code: Optional[str] = None
    name: Optional[str] = None


# ───── GARAGE ─────
class GarageBase(BaseModel):
    name: str
    capacity: int
    latitude: float
    longitude: float


class GarageCreate(GarageBase):
    pass


class GarageRead(GarageBase):
    garage_id: int
    model_config = ConfigDict(from_attributes=True)


class GarageUpdate(BaseModel):
    name: Optional[str] = None
    capacity: Optional[int] = None
    latitude: Optional[float] = None
    longitude: Optional[float] = None


# ───── BusType ─────
class BusTypeBase(BaseModel):
    name: str
    capacity: int


class BusTypeCreate(BusTypeBase):
    pass


class BusTypeRead(BusTypeBase):
    type_id: int
    model_config = ConfigDict(from_attributes=True)


class BusTypeUpdate(BaseModel):
    name: Optional[str] = None
    capacity: Optional[int] = None


# ───── StopArea ─────
class StopAreaBase(BaseModel):
    stop_area_code: int
    admin_area_code: str
    name: str
    is_terminal: bool


class StopAreaCreate(StopAreaBase):
    pass


class StopAreaRead(StopAreaBase):
    model_config = ConfigDict(from_attributes=True)


class StopAreaUpdate(BaseModel):
    admin_area_code: Optional[str] = None
    name: Optional[str] = None
    is_terminal: Optional[bool] = None


# ───── StopPoint ─────
class StopPointBase(BaseModel):
    atco_code: int
    name: str
    latitude: float
    longitude: float
    stop_area_code: int


class StopPointCreate(StopPointBase):
    pass


class StopPointRead(StopPointBase):
    model_config = ConfigDict(from_attributes=True)


class StopPointUpdate(BaseModel):
    name: Optional[str] = None
    latitude: Optional[float] = None
    longitude: Optional[float] = None
    stop_area_code: Optional[int] = None


# ───── Route ─────
class RouteBase(BaseModel):
    name: str
    operator_id: int
    description: Optional[str] = None


class RouteCreate(RouteBase):
    pass


class RouteRead(RouteBase):
    route_id: int
    model_config = ConfigDict(from_attributes=True)


class RouteUpdate(BaseModel):
    name: Optional[str] = None
    operator_id: Optional[int] = None
    description: Optional[str] = None


# ───── Service ─────
class ServiceBase(BaseModel):
    service_code: str
    name: str
    operator_id: int
    line_id: int
    description: Optional[str] = None


class ServiceCreate(ServiceBase):
    pass


class ServiceRead(ServiceBase):
    service_id: int
    model_config = ConfigDict(from_attributes=True)


class ServiceUpdate(BaseModel):
    service_code: Optional[str] = None
    name: Optional[str] = None
    operator_id: Optional[int] = None
    line_id: Optional[int] = None
    description: Optional[str] = None


# ───── Line ─────
class LineBase(BaseModel):
    line_name: str
    operator_id: int


class LineCreate(LineBase):
    pass


class LineRead(LineBase):
    line_id: int
    model_config = ConfigDict(from_attributes=True)


class LineUpdate(BaseModel):
    line_name: Optional[str] = None
    operator_id: Optional[int] = None


# ───── JourneyPattern ─────
class JourneyPatternBase(BaseModel):
    jp_code: str
    line_id: int
    route_id: int
    service_id: int
    operator_id: int
    name: Optional[str] = None


class JourneyPatternCreate(JourneyPatternBase):
    pass


class JourneyPatternRead(JourneyPatternBase):
    jp_id: int
    model_config = ConfigDict(from_attributes=True)


class JourneyPatternUpdate(BaseModel):
    jp_code: Optional[str] = None
    line_id: Optional[int] = None
    route_id: Optional[int] = None
    service_id: Optional[int] = None
    operator_id: Optional[int] = None
    name: Optional[str] = None


# ───── StopActivity ─────
class StopActivityBase(BaseModel):
    activity_type: str
    activity_time: time
    pax_count: int
    stop_point_id: int # Changed from atco_code to stop_point_id to match model
    vj_id: int


class StopActivityCreate(StopActivityBase):
    pass


class StopActivityRead(StopActivityBase):
    activity_id: int
    model_config = ConfigDict(from_attributes=True)


class StopActivityUpdate(BaseModel):
    activity_type: Optional[str] = None
    activity_time: Optional[time] = None
    pax_count: Optional[int] = None
    stop_point_id: Optional[int] = None
    vj_id: Optional[int] = None


# ──────────────── JourneyPatternDefinition ────────────────
class JourneyPatternDefinitionBase(BaseModel):
    jp_id: int
    stop_point_atco_code: int
    sequence: int
    arrival_time: time
    departure_time: time


class JourneyPatternDefinitionCreate(JourneyPatternDefinitionBase):
    pass


class JourneyPatternDefinitionRead(JourneyPatternDefinitionBase):
    model_config = ConfigDict(from_attributes=True)


class JourneyPatternDefinitionUpdate(BaseModel):
    # jp_id and sequence are part of the composite primary key and passed in URL,
    # so they should not be in the request body for updates.
    stop_point_atco_code: Optional[int] = None
    arrival_time: Optional[time] = None
    departure_time: Optional[time] = None

# ───── RouteDefinition ─────
class RouteDefinitionBase(BaseModel):
    route_id: int
    stop_point_id: int # Changed from stop_point_atco_code to stop_point_id
    sequence: int


class RouteDefinitionCreate(RouteDefinitionBase):
    pass


class RouteDefinitionRead(RouteDefinitionBase):
    model_config = ConfigDict(from_attributes=True)


class RouteDefinitionUpdate(BaseModel):
    stop_point_id: Optional[int] = None
    sequence: Optional[int] = None


# ──────────────── Block ────────────────
class BlockBase(BaseModel):
    name: str
    operator_id: int
    bus_type_id: int


class BlockCreate(BlockBase):
    pass


class BlockRead(BlockBase):
    block_id: int
    model_config = ConfigDict(from_attributes=True)


class BlockUpdate(BaseModel):
    name: Optional[str] = None
    operator_id: Optional[int] = None
    bus_type_id: Optional[int] = None


# ──────────────── VehicleJourney ────────────────
class VehicleJourneyBase(BaseModel):
    departure_time: time
    dayshift: int
    jp_id: int
    block_id: int
    operator_id: int
    line_id: int
    service_id: int


class VehicleJourneyCreate(VehicleJourneyBase):
    pass


class VehicleJourneyRead(VehicleJourneyBase):
    vj_id: int
    model_config = ConfigDict(from_attributes=True)


class VehicleJourneyUpdate(BaseModel):
    departure_time: Optional[time] = None
    dayshift: Optional[int] = None
    jp_id: Optional[int] = None
    block_id: Optional[int] = None
    operator_id: Optional[int] = None
    line_id: Optional[int] = None
    service_id: Optional[int] = None


# ──────────────── Demand ────────────────
class DemandBase(BaseModel):
    origin: int
    destination: int
    count: float
    start_time: time
    end_time: time


class DemandCreate(DemandBase):
    pass


class DemandRead(DemandBase):
    model_config = ConfigDict(from_attributes=True)


class DemandUpdate(BaseModel):
    count: Optional[float] = None


# ──────────────── EmulatorLog ────────────────
class RunStatus(IntEnum):
    QUEUED = 0
    RUNNING = 1
    COMPLETED = 2
    FAILED = 3


class EmulatorLogBase(BaseModel):
    status: RunStatus


# New schema for detailed optimization results, with all fields optional
class OptimizationDetailsRead(BaseModel):
    status: Optional[str] = None  # e.g., "OPTIMAL", "FEASIBLE", "ERROR"
    message: Optional[str] = None # For error messages or success messages
    total_passengers_served: Optional[int] = None
    schedule: Optional[List[Dict[str, Any]]] = None # Assuming schedule is a list of dicts

    # Fields that were complained about as "required" in previous errors
    solver_runtime_ms: Optional[float] = None
    solver_iterations: Optional[int] = None
    buses_assigned_summary: Optional[Dict[str, Any]] = None

    model_config = ConfigDict(from_attributes=True)


class EmulatorLogCreate(EmulatorLogBase):
    # For creation, status is the only required field from the client.
    # started_at, last_updated, and optimization_details are handled by the server.
    pass


class EmulatorLogRead(EmulatorLogBase):
    run_id: int
    started_at: datetime
    last_updated: datetime
    # Now explicitly link to the new OptimizationDetailsRead schema
    optimization_details: Optional[OptimizationDetailsRead] = None
    model_config = ConfigDict(from_attributes=True)


class EmulatorLogUpdate(BaseModel):
    status: Optional[RunStatus] = None
    # Use the new Optional OptimizationDetailsRead schema for updates too
    optimization_details: Optional[OptimizationDetailsRead] = None