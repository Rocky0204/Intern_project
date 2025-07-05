from fastapi import APIRouter

from . import (
    block,
    bus,
    bus_type,
    garage,
    journey_pattern,
    journey_pattern_definition,
    line,
    operator,
    route,
    route_definition,
    service,
    stop_area,
    stop_point,
    vehicle_journey,
)

all_routers: list[APIRouter] = [
    operator.router,
    garage.router,
    bus_type.router,
    stop_area.router,
    route.router,

    bus.router,
    block.router,
    stop_point.router,
    route_definition.router,
    service.router,
    line.router,
    journey_pattern.router,
    journey_pattern_definition.router,
    vehicle_journey.router,
]
