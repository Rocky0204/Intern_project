# api/schemas.py
from datetime import *
from enum import IntEnum  # Needed for RunStatus if it's used in schemas
from pydantic import BaseModel, ConfigDict


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
    # Note: 'capacity' was in your original BusUpdate schema but not in the model.
    # It's removed here to align with models.py.
    # If you need to update capacity, it should be added to models.py Bus model.
    registration_number: str | None = None  # Maps to reg_num in model
    garage_id: int | None = None
    operator_id: int | None = None
    bus_type_id: int | None = None  # Added for completeness if bus_type can be updated


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
    name: str | None = None
    operator_code: str | None = (
        None  # Added for completeness if operator_code can be updated
    )


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
    name: str | None = None
    capacity: int | None = None
    latitude: float | None = None
    longitude: float | None = None


# ───── BusType ─────
class BusTypeBase(BaseModel):
    name: str
    capacity: int  # Only fields present in models.py


class BusTypeCreate(BusTypeBase):
    pass


class BusTypeRead(BusTypeBase):
    type_id: int
    model_config = ConfigDict(from_attributes=True)


class BusTypeUpdate(BaseModel):
    name: str | None = None
    capacity: int | None = None


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
    admin_area_code: str | None = None
    name: str | None = None
    is_terminal: bool | None = None


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
    name: str | None = None
    latitude: float | None = None
    longitude: float | None = None
    stop_area_code: int | None = None


# ───── Route ─────
class RouteBase(BaseModel):
    # Note: models.py has 'name', 'description', 'operator_id'.
    # Your original schema had 'route_code', 'name', 'operator_id'.
    # Aligning with models.py for creation. If 'route_code' is needed, add it to models.py.
    name: str
    operator_id: int
    description: str | None = None  # From models.py, nullable


class RouteCreate(RouteBase):
    pass


class RouteRead(RouteBase):
    route_id: int
    model_config = ConfigDict(from_attributes=True)


class RouteUpdate(BaseModel):
    name: str | None = None
    operator_id: int | None = None
    description: str | None = None  # For updating description


# ───── Service ─────
class ServiceBase(BaseModel):
    service_code: str
    name: str
    operator_id: int
    line_id: int  # Added based on models.py relationship
    description: str | None = None  # Added for consistency with model and read schema


class ServiceCreate(ServiceBase):
    pass


class ServiceRead(ServiceBase):
    service_id: int
    # description: str | None = None  # Already in ServiceBase, no need to redefine
    model_config = ConfigDict(from_attributes=True)


class ServiceUpdate(BaseModel):
    service_code: str | None = None
    name: str | None = None
    operator_id: int | None = None
    line_id: int | None = None  # For updating line_id
    description: str | None = None  # Ensure this is present for updates


# ───── Line ─────
class LineBase(BaseModel):
    line_name: str
    # Note: models.py has operator_id, not service_id. Aligning with models.py.
    operator_id: int


class LineCreate(LineBase):
    pass


class LineRead(LineBase):
    line_id: int
    model_config = ConfigDict(from_attributes=True)


class LineUpdate(BaseModel):
    line_name: str | None = None
    operator_id: int | None = None  # For updating operator_id


# ───── JourneyPattern ─────
class JourneyPatternBase(BaseModel):
    jp_code: str
    line_id: int
    # Added based on models.py relationships
    route_id: int
    service_id: int
    operator_id: int
    name: str | None = None  # From models.py, nullable


class JourneyPatternCreate(JourneyPatternBase):
    pass


class JourneyPatternRead(JourneyPatternBase):
    jp_id: int
    model_config = ConfigDict(from_attributes=True)


class JourneyPatternUpdate(BaseModel):
    jp_code: str | None = None
    line_id: int | None = None
    route_id: int | None = None
    service_id: int | None = None
    operator_id: int | None = None
    name: str | None = None


# ───── StopActivity ─────
class StopActivityBase(BaseModel):
    # Aligned with models.py requirements for creation
    activity_type: str
    activity_time: time
    pax_count: int
    atco_code: int  # Maps to stop_point_id in model
    vj_id: int


class StopActivityCreate(StopActivityBase):
    pass


class StopActivityRead(StopActivityBase):
    activity_id: int
    model_config = ConfigDict(from_attributes=True)


class StopActivityUpdate(BaseModel):
    activity_type: str | None = None
    activity_time: time | None = None
    pax_count: int | None = None
    atco_code: int | None = None  # Maps to stop_point_id in model
    vj_id: int | None = None


# ───── JourneyPatternDefinition ─────
class JourneyPatternDefinitionBase(BaseModel):
    jp_id: int
    stop_point_atco_code: int  # Maps to stop_point_id in model
    sequence: int
    # Note: models.py has arrival_time and departure_time, not stop_activity_id or distance_from_start.
    arrival_time: time
    departure_time: time


class JourneyPatternDefinitionCreate(JourneyPatternDefinitionBase):
    pass


class JourneyPatternDefinitionRead(JourneyPatternDefinitionBase):
    model_config = ConfigDict(from_attributes=True)


class JourneyPatternDefinitionUpdate(BaseModel):
    stop_point_atco_code: int | None = None
    sequence: int | None = None
    arrival_time: time | None = None
    departure_time: time | None = None


# ───── RouteDefinition ─────
class RouteDefinitionBase(BaseModel):
    route_id: int
    stop_point_atco_code: int  # Maps to stop_point_id in model
    sequence: int
    # Note: models.py does not have distance_from_start.


class RouteDefinitionCreate(RouteDefinitionBase):
    pass


class RouteDefinitionRead(RouteDefinitionBase):
    model_config = ConfigDict(from_attributes=True)


class RouteDefinitionUpdate(BaseModel):
    stop_point_atco_code: int | None = None
    sequence: int | None = None


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
    name: str | None = None
    operator_id: int | None = None
    bus_type_id: int | None = None


# ──────────────── VehicleJourney ────────────────
class VehicleJourneyBase(BaseModel):
    departure_time: time
    dayshift: int
    jp_id: int
    block_id: int
    operator_id: int
    line_id: int
    service_id: int  # Added based on models.py relationship


class VehicleJourneyCreate(VehicleJourneyBase):
    pass


class VehicleJourneyRead(VehicleJourneyBase):
    vj_id: int
    model_config = ConfigDict(from_attributes=True)


class VehicleJourneyUpdate(BaseModel):
    departure_time: time | None = None
    dayshift: int | None = None
    jp_id: int | None = None
    block_id: int | None = None
    operator_id: int | None = None
    line_id: int | None = None
    service_id: int | None = None


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
    count: float | None = None


# ──────────────── EmulatorLog ────────────────
# Define RunStatus here if it's not globally available or imported in schemas
class RunStatus(IntEnum):
    QUEUED = 0
    RUNNING = 1
    COMPLETED = 2
    FAILED = 3


class EmulatorLogBase(BaseModel):
    status: RunStatus  # Use the IntEnum type directly


class EmulatorLogCreate(EmulatorLogBase):
    # No timestamp/started_at/last_updated here, as they are server-generated
    pass


class EmulatorLogRead(EmulatorLogBase):
    run_id: int  # Corrected from log_id to run_id
    started_at: datetime  # Corrected from timestamp to started_at, and type to datetime
    last_updated: datetime | None = None  # Added last_updated as per model snippet
    model_config = ConfigDict(from_attributes=True)


class EmulatorLogUpdate(BaseModel):
    status: RunStatus | None = None  # Allow updating status
    # last_updated is generally updated by the server on modification, not client
