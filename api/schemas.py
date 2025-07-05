# api/schemas.py
from datetime import time

from pydantic import BaseModel, ConfigDict

# Removed: from api.models import StopActivity (no longer needed for direct type hinting)


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
    registration_number: str | None = None
    capacity: int | None = None
    garage_id: int | None = None


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
    short_name: str
    speed_limit: int
    capacity: int


class BusTypeCreate(BusTypeBase):
    pass


class BusTypeRead(BusTypeBase):
    type_id: int
    model_config = ConfigDict(from_attributes=True)


class BusTypeUpdate(BaseModel):
    name: str | None = None
    short_name: str | None = None
    speed_limit: int | None = None
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
    route_code: str
    name: str
    operator_id: int


class RouteCreate(RouteBase):
    pass


class RouteRead(RouteBase):
    route_id: int
    model_config = ConfigDict(from_attributes=True)


class RouteUpdate(BaseModel):
    route_code: str | None = None
    name: str | None = None
    operator_id: int | None = None


# ───── RouteDefinition ─────
class RouteDefinitionBase(BaseModel):
    route_id: int
    stop_point_atco_code: int
    sequence: int
    distance_from_start: float


class RouteDefinitionCreate(RouteDefinitionBase):
    pass


class RouteDefinitionRead(RouteDefinitionBase):
    model_config = ConfigDict(from_attributes=True)


class RouteDefinitionUpdate(BaseModel):
    stop_point_atco_code: int | None = None
    sequence: int | None = None
    distance_from_start: float | None = None


# ───── Service ─────
class ServiceBase(BaseModel):
    service_code: str
    name: str
    operator_id: int


class ServiceCreate(ServiceBase):
    pass


class ServiceRead(ServiceBase):
    service_id: int
    model_config = ConfigDict(from_attributes=True)


class ServiceUpdate(BaseModel):
    service_code: str | None = None
    name: str | None = None
    operator_id: int | None = None


# ───── Line ─────
class LineBase(BaseModel):
    line_name: str
    service_id: int


class LineCreate(LineBase):
    pass


class LineRead(LineBase):
    line_id: int
    model_config = ConfigDict(from_attributes=True)


class LineUpdate(BaseModel):
    line_name: str | None = None
    service_id: int | None = None


# ───── JourneyPattern ─────
class JourneyPatternBase(BaseModel):
    jp_code: str
    line_id: int


class JourneyPatternCreate(JourneyPatternBase):
    pass


class JourneyPatternRead(JourneyPatternBase):
    jp_id: int
    model_config = ConfigDict(from_attributes=True)


class JourneyPatternUpdate(BaseModel):
    jp_code: str | None = None
    line_id: int | None = None


# ───── StopActivity (New Pydantic Schemas) ─────
class StopActivityBase(BaseModel):
    activity_type: str
    atco_code: int
    order: int

class StopActivityCreate(StopActivityBase):
    pass

class StopActivityRead(StopActivityBase):
    stop_activity_id: int
    model_config = ConfigDict(from_attributes=True)

class StopActivityUpdate(BaseModel):
    activity_type: str | None = None
    atco_code: int | None = None
    order: int | None = None


# ───── JourneyPatternDefinition ─────
class JourneyPatternDefinitionBase(BaseModel):
    jp_id: int
    stop_point_atco_code: int
    stop_activity_id: int # Changed from stop_activity: StopActivity
    sequence: int
    distance_from_start: float


class JourneyPatternDefinitionCreate(JourneyPatternDefinitionBase):
    pass


class JourneyPatternDefinitionRead(JourneyPatternDefinitionBase):
    model_config = ConfigDict(from_attributes=True)


class JourneyPatternDefinitionUpdate(BaseModel):
    stop_point_atco_code: int | None = None
    stop_activity_id: int | None = None # Changed from stop_activity: StopActivity
    sequence: int | None = None
    distance_from_start: float | None = None


# ──────────────── VehicleJourney ────────────────
class VehicleJourneyBase(BaseModel):
    departure_time: time
    dayshift: int
    jp_id: int
    block_id: int
    operator_id: int
    line_id: int


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