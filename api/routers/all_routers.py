# api/routers/all_routers.py

# Import all individual routers
from . import (
    bus,
    bus_type,
    block,  # Ensure this is imported
    demand,
    emulator_log,
    garage,
    journey_pattern,
    journey_pattern_definition,
    line,
    operator,
    route,
    route_definition,
    service,
    stop_activity,
    stop_area,
    stop_point,
    vehicle_journey,
)

# Create a list of all routers
all_routers = [
    bus.router,
    bus_type.router,
    block.router,  # Ensure this router is included in the list
    demand.router,
    emulator_log.router,
    garage.router,
    journey_pattern.router,
    journey_pattern_definition.router,
    line.router,
    operator.router,
    route.router,
    route_definition.router,
    service.router,
    stop_activity.router,
    stop_area.router,
    stop_point.router,
    vehicle_journey.router,
]
