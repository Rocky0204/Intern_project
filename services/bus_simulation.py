import logging
import random
from datetime import datetime, timedelta
import pandas as pd
from typing import Optional
import copy
import math
import os
import sys
import collections

# Add the project root to sys.path to enable absolute imports
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

# --- Database Imports ---
from sqlalchemy.orm import Session, joinedload
from api.models import (
    StopPoint,
    BusType,
    Bus as DBBus,
    Demand,
    Route,
    RouteDefinition,
    JourneyPattern,
    VehicleJourney,
    Block,
    Operator,
    Line,
    Service,
)

# Configure logging for better output
logging.basicConfig(level=logging.DEBUG, format="%(levelname)s:%(name)s:%(message)s")
logger = logging.getLogger(__name__)

# --- Helper Functions for Dynamic Calculations ---


def haversine_distance(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    """
    Calculate the distance between two points on Earth using the Haversine formula.
    Returns distance in kilometers.
    """
    R = 6371  # Radius of Earth in kilometers

    lat1_rad = math.radians(lat1)
    lon1_rad = math.radians(lon1)
    lat2_rad = math.radians(lat2)
    lon2_rad = math.radians(lon2)

    dlon = lon2_rad - lon1_rad
    dlat = lat2_rad - lat1_rad

    a = (
        math.sin(dlat / 2) ** 2
        + math.cos(lat1_rad) * math.cos(lat2_rad) * math.sin(dlon / 2) ** 2
    )
    c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))

    distance = R * c
    return distance


def is_rush_hour(current_minutes_from_midnight: int) -> bool:
    """
    Determines if the current time falls within defined rush hour periods.
    Morning rush: 07:00 - 09:00 (420-540 minutes)
    Evening rush: 16:00 - 18:00 (960-1080 minutes)
    """
    morning_rush_start = 7 * 60
    morning_rush_end = 9 * 60

    evening_rush_start = 16 * 60
    evening_rush_end = 18 * 60

    if (morning_rush_start <= current_minutes_from_midnight <= morning_rush_end) or (
        evening_rush_start <= current_minutes_from_midnight <= evening_rush_end
    ):
        return True
    return False


def format_time(total_minutes_from_midnight: int) -> str:
    """
    Formats a total number of minutes from midnight into an HH:MM string.
    Used for human-readable timestamps in simulation logs.
    """
    sim_datetime = datetime(2025, 6, 10, 0, 0, 0) + timedelta(
        minutes=total_minutes_from_midnight
    )
    return sim_datetime.strftime("%H:%M")


# --- Simulation Classes ---


class Passenger:
    """
    Represents an individual passenger in the simulation.
    Tracks their journey from arrival at origin to alighting at destination.
    """

    _id_counter = 0

    def __init__(
        self, origin_stop_id: int, destination_stop_id: int, arrival_time_at_stop: int
    ):
        Passenger._id_counter += 1
        self.id = Passenger._id_counter
        self.origin_stop_id = origin_stop_id
        self.destination_stop_id = destination_stop_id
        self.arrival_time_at_stop = (
            arrival_time_at_stop  # Time they arrive at their origin stop
        )
        self.board_time: Optional[int] = None  # Time they board a bus
        self.alight_time: Optional[int] = None  # Time they alight from a bus

    @property
    def wait_time(self) -> Optional[int]:
        """Calculates the time a passenger waited for a bus."""
        if self.board_time is not None and self.arrival_time_at_stop is not None:
            return self.board_time - self.arrival_time_at_stop
        return None

    @property
    def travel_time(self) -> Optional[int]:
        """Calculates the time a passenger spent traveling on a bus."""
        if self.alight_time is not None and self.board_time is not None:
            return self.alight_time - self.board_time
        return None

    @property
    def total_trip_time(self) -> Optional[int]:
        """Calculates the total time from arrival at origin to alighting at destination."""
        if self.alight_time is not None and self.arrival_time_at_stop is not None:
            return self.alight_time - self.arrival_time_at_stop
        return None

    def __repr__(self) -> str:
        return f"Passenger(ID: {self.id}, From: {self.origin_stop_id}, To: {self.destination_stop_id}, Arrived: {format_time(self.arrival_time_at_stop)})"


class Stop:
    """
    Represents a bus stop in the simulation.
    Manages waiting passengers at this stop.
    """

    def __init__(self, stop_id: int, name: str):
        self.stop_id = stop_id
        self.name = name
        self.waiting_passengers: collections.deque[Passenger] = collections.deque()
        self.last_bus_arrival_time = -float("inf")

    def add_passenger(self, passenger: Passenger):
        """
        Adds a passenger object to the waiting queue at this stop.
        """
        self.waiting_passengers.append(passenger)

    def get_waiting_passengers_count(self) -> int:
        """
        Returns the current number of passengers waiting at the stop.
        """
        return len(self.waiting_passengers)

    def __repr__(self) -> str:
        return f"Stop(ID: {self.stop_id}, Name: {self.name})"


class Bus:
    """
    Represents a bus vehicle in the simulation.
    Manages its state, passengers onboard, and records its schedule/events.
    """

    def __init__(
        self,
        bus_id: str,
        capacity: int,
        depot_stop_id: int,
        initial_internal_time: int,
        overcrowding_factor: float = 1.0,
        db_registration: Optional[str] = None,
    ):
        self.bus_id = bus_id
        self.capacity = capacity
        self.overcrowding_factor = overcrowding_factor
        self.onboard_passengers: list[Passenger] = []
        self.current_stop_id = depot_stop_id
        self.initial_start_point = depot_stop_id
        self.current_route = None
        self.route_index = 0
        self.schedule = []
        self.current_time = initial_internal_time
        self.db_registration = db_registration

        self.add_event_to_schedule(
            self.current_time,
            f"Bus {self.bus_id} initialized at depot",
            depot_stop_id,
            0,
            0,
            0,
            0,
            "N/A",
            "INITIALIZED",
        )
        self.miles_traveled = 0
        self.fuel_consumed = 0
        self.is_en_route = False
        self.time_to_next_stop = 0
        self.total_route_duration_minutes = 0
        self.passenger_boarded_count = 0
        self.passenger_alighted_count = 0
        self.last_stop_arrival_time = self.current_time
        self.destination_stop_id_on_segment: Optional[int] = (
            None  # New attribute to store target stop when en route
        )

    def add_event_to_schedule(
        self,
        time: int,
        event_description: str,
        stop_id: Optional[int],
        passengers_onboard: int,
        passengers_waiting: int,
        boarded_count: int,
        alighted_count: int,
        direction: str,
        status: str,
    ):
        """
        Records an event in the bus's schedule.
        """
        self.schedule.append(
            (
                time,
                event_description,
                stop_id,
                passengers_onboard,
                passengers_waiting,
                boarded_count,
                alighted_count,
                direction,
                status,
            )
        )

    def start_route(self, route: "SimRoute", global_simulation_time: int):
        """
        Initiates a bus on a new route.
        """
        self.current_route = route
        self.current_direction = "Outbound"
        self.route_index = 0
        self.is_en_route = False  # Bus is at the start stop, not yet moving
        self.time_to_next_stop = 0
        self.total_route_duration_minutes = route.total_outbound_route_time_minutes
        self.last_stop_arrival_time = global_simulation_time
        self.add_event_to_schedule(
            global_simulation_time,
            f"Start Route {route.route_id}",
            self.current_stop_id,
            len(self.onboard_passengers),
            0,
            0,
            0,
            "N/A",
            "AT_STOP",  # Status is AT_STOP when starting a route
        )

    def move_to_next_stop(self, global_simulation_time: int):
        """
        Initiates the bus movement to the next stop on its current route.
        Sets is_en_route and time_to_next_stop.
        Stores the destination stop ID for later update upon arrival.
        """
        if not self.current_route:
            logger.warning(f"Bus {self.bus_id} has no route assigned to move on.")
            self.is_en_route = False
            return

        try:
            current_stop_idx = self.current_route.stops_ids_in_order.index(
                self.current_stop_id
            )
        except ValueError:
            logger.error(
                f"Bus {self.bus_id} is at {self.current_stop_id} which is not on its current route {self.current_route.route_id}. Cannot move to next stop."
            )
            self.is_en_route = False
            return

        if current_stop_idx < len(self.current_route.stops_ids_in_order) - 1:
            next_stop_idx = current_stop_idx + 1
            next_stop_id = self.current_route.stops_ids_in_order[
                next_stop_idx
            ]  # This is the destination for this segment
            self.destination_stop_id_on_segment = next_stop_id  # Store the target stop

            total_segments = len(self.current_route.stops_ids_in_order) - 1
            if total_segments > 0:
                self.time_to_next_stop = (
                    self.current_route.total_outbound_route_time_minutes
                    / total_segments
                )
            else:
                self.time_to_next_stop = 0

            self.is_en_route = True
            # Log the departure, not arrival yet
            logger.info(
                f"Time {format_time(global_simulation_time)}: Bus {self.bus_id} en route from {self.current_stop_id} to {next_stop_id}. Expected arrival in {self.time_to_next_stop:.1f} minutes."
            )

            # Add event for starting movement
            self.add_event_to_schedule(
                global_simulation_time,
                f"Departed {self.current_stop_id} towards {next_stop_id}",
                self.current_stop_id,  # Log current stop as departure point
                len(self.onboard_passengers),
                0,
                0,
                0,  # Boarded/alighted counts are for arrival at stop, not departure
                self.current_direction,
                "EN_ROUTE_DEPARTURE",
            )

        else:
            # This case means it's already at the last stop, so no further movement on this route.
            logger.info(
                f"Time {format_time(global_simulation_time)}: Bus {self.bus_id} is already at the end of route {self.current_route.route_id} at {self.current_stop_id} and cannot move further on this route."
            )
            self.is_en_route = False  # Ensure it's not en route if it can't move
            self.time_to_next_stop = 0
            # Do NOT set current_route = None here, let the main simulation loop handle route completion.

    def alight_passengers(self, current_time: int, stop: Stop) -> list[Passenger]:
        """
        Simulates passengers alighting from the bus at the current stop.
        Returns a list of alighted Passenger objects.
        """
        alighted_this_stop = []
        remaining_onboard = []
        self.passenger_alighted_count = 0

        for passenger in self.onboard_passengers:
            if passenger.destination_stop_id == stop.stop_id:
                passenger.alight_time = current_time
                alighted_this_stop.append(passenger)
                self.passenger_alighted_count += 1
                logger.info(
                    f"Time {format_time(current_time)}: Passenger {passenger.id} alighted from Bus {self.bus_id} at {stop.stop_id}."
                )
            else:
                remaining_onboard.append(passenger)

        self.onboard_passengers = remaining_onboard
        return alighted_this_stop

    def board_passengers(self, current_time: int, stop: Stop) -> int:
        """
        Simulates passengers boarding the bus from the current stop's waiting queue.
        Considers bus capacity and overcrowding factor.
        """
        boarded_count = 0
        max_onboard_with_overcrowding = int(self.capacity * self.overcrowding_factor)

        remaining_stops_on_current_route = []
        if self.current_route:
            try:
                current_stop_idx = self.current_route.stops_ids_in_order.index(
                    self.current_stop_id
                )
                remaining_stops_on_current_route = (
                    self.current_route.stops_ids_in_order[current_stop_idx + 1 :]
                )
            except ValueError:
                logger.warning(
                    f"Bus {self.bus_id} current stop {self.current_stop_id} not found in its current route {self.current_route.route_id}. Cannot determine remaining stops for boarding."
                )
                remaining_stops_on_current_route = []

        passengers_to_requeue = collections.deque()
        while (
            stop.waiting_passengers
            and len(self.onboard_passengers) < max_onboard_with_overcrowding
        ):
            passenger = stop.waiting_passengers.popleft()

            if passenger.destination_stop_id in remaining_stops_on_current_route:
                passenger.board_time = current_time
                self.onboard_passengers.append(passenger)
                boarded_count += 1
                logger.info(
                    f"Time {format_time(current_time)}: Passenger {passenger.id} boarded Bus {self.bus_id} at {stop.stop_id} (Dest: {passenger.destination_stop_id})."
                )
            else:
                passengers_to_requeue.append(passenger)

        stop.waiting_passengers.extendleft(reversed(passengers_to_requeue))
        self.passenger_boarded_count = boarded_count
        return boarded_count


class SimRoute:
    """
    Represents a bus route with its sequence of stops and total travel time.
    This is the simulation's internal representation of a route.
    """

    def __init__(
        self,
        route_id: int,
        stops_ids_in_order: list[int],
        total_outbound_route_time_minutes: int,
    ):
        self.route_id = route_id
        self.stops_ids_in_order = stops_ids_in_order
        self.total_outbound_route_time_minutes = total_outbound_route_time_minutes

        if len(stops_ids_in_order) > 1:
            self.segment_time = total_outbound_route_time_minutes / (
                len(stops_ids_in_order) - 1
            )
        else:
            self.segment_time = 0

    def __repr__(self) -> str:
        return f"SimRoute(ID: {self.route_id}, Stops: {self.stops_ids_in_order})"


class OptimizedScheduleManager:
    """
    Generates an optimized schedule for buses based on demand.
    Uses a greedy approach to assign buses to routes with the highest waiting passengers.
    """

    def __init__(
        self,
        stops: dict,
        routes: dict,
        buses: dict,
        all_raw_demands: list,
        start_time_minutes: int,
        end_time_minutes: int,
        config: dict,
        stop_points_data: dict,
    ):  # Added stop_points_data
        self.stops = {
            k: copy.deepcopy(v) for k, v in stops.items()
        }  # Deep copy stops for isolated demand tracking
        self.routes = routes
        self.buses = buses
        self.all_raw_demands = all_raw_demands
        self.start_time_minutes = start_time_minutes
        self.end_time_minutes = end_time_minutes
        self.config = config
        self.stop_points_data = stop_points_data  # Store stop_points_data

        self.estimated_demand = self._estimate_future_demand()
        self.bus_availability = {bus_id: start_time_minutes for bus_id in buses.keys()}
        self.bus_current_locations = {
            bus_id: buses[bus_id].initial_start_point for bus_id in buses.keys()
        }

    def _estimate_future_demand(self) -> collections.defaultdict:
        """
        Estimates future passenger demand at each stop based on raw demand data.
        Returns: defaultdict(lambda: defaultdict(int)) where
                 estimated_demand[stop_id][time_minute] = passenger_count
        This version concentrates demand at the exact arrival_time.
        """
        estimated_demand = collections.defaultdict(lambda: collections.defaultdict(int))
        for demand_record in self.all_raw_demands:
            origin_id = demand_record["origin"]
            arrival_time = demand_record["arrival_time"]
            count = demand_record["count"]
            if self.start_time_minutes <= arrival_time <= self.end_time_minutes:
                estimated_demand[origin_id][arrival_time] += count
        return estimated_demand

    def _calculate_dead_run_time(
        self, from_stop_id: int, to_stop_id: int, current_time_minutes: int
    ) -> int:
        """
        Calculates the travel time for a dead run between two stops.
        Considers distance and applies a traffic factor if it's rush hour.
        Returns time in minutes (rounded up).
        """
        from_coords = self.stop_points_data.get(from_stop_id)
        to_coords = self.stop_points_data.get(to_stop_id)

        if not from_coords or not to_coords:
            logger.error(
                f"Optimizer: Cannot calculate dead run time: Missing coordinates for stops ({from_stop_id}, {to_stop_id}). Using default time."
            )
            return 5  # Fallback to default if coordinates are missing

        distance_km = haversine_distance(
            from_coords[0],
            from_coords[1],  # lat1, lon1
            to_coords[0],
            to_coords[1],  # lat2, lon2
        )

        base_speed_kmph = self.config["dead_run_travel_rate_km_per_hour"]

        if base_speed_kmph <= 0:
            logger.error(
                "Optimizer: Dead run speed configured as zero or negative. Using default time."
            )
            return 5

        time_hours = distance_km / base_speed_kmph
        time_minutes = time_hours * 60

        traffic_factor = 1.0
        if is_rush_hour(current_time_minutes):
            traffic_factor = 1.5
            logger.debug(
                f"Optimizer: Rush hour detected at {format_time(current_time_minutes)}. Applying traffic factor {traffic_factor}."
            )

        return max(1, int(math.ceil(time_minutes * traffic_factor)))

    def _calculate_potential_passengers_served(
        self,
        route_obj: "SimRoute",
        current_bus_capacity: int,
        temp_stop_queues: dict,
        current_start_stop_id: int,
    ) -> tuple[int, list]:
        """
        Estimates how many *new* passengers would be served by a bus taking this route,
        starting from current_start_stop_id, considering its capacity and passenger destinations.
        Returns a tuple: (served_count, list_of_served_passenger_objects).
        """
        served_count = 0
        served_passenger_objects = []
        simulated_onboard_count = 0
        max_onboard_with_overcrowding = int(
            current_bus_capacity * self.config["overcrowding_factor"]
        )

        try:
            start_stop_index = route_obj.stops_ids_in_order.index(current_start_stop_id)
        except ValueError:
            return 0, []

        # Create a deep copy of the relevant part of temp_stop_queues for simulation
        # This is crucial to avoid modifying the actual queues during hypothetical calculations
        sim_temp_stop_queues = {
            stop_id: collections.deque(list(queue))
            for stop_id, queue in temp_stop_queues.items()
        }

        for stop_idx_on_route in range(
            start_stop_index, len(route_obj.stops_ids_in_order)
        ):
            current_stop_id = route_obj.stops_ids_in_order[stop_idx_on_route]

            if current_stop_id in sim_temp_stop_queues:
                passengers_at_this_stop = sim_temp_stop_queues[current_stop_id]

                # Temporarily store passengers that are not boarded in this simulation step
                temp_passengers_not_boarded_at_this_stop = collections.deque()

                while passengers_at_this_stop:
                    p_sim = passengers_at_this_stop.popleft()

                    if (
                        p_sim.destination_stop_id
                        in route_obj.stops_ids_in_order[stop_idx_on_route + 1 :]
                    ):
                        if simulated_onboard_count < max_onboard_with_overcrowding:
                            served_count += 1
                            served_passenger_objects.append(p_sim)
                            simulated_onboard_count += 1
                        else:
                            temp_passengers_not_boarded_at_this_stop.append(
                                p_sim
                            )  # Bus full, put back
                    else:
                        temp_passengers_not_boarded_at_this_stop.append(
                            p_sim
                        )  # Not going on this route, put back

                sim_temp_stop_queues[current_stop_id].extend(
                    temp_passengers_not_boarded_at_this_stop
                )

        return served_count, served_passenger_objects

    def generate_optimized_schedule(self) -> collections.defaultdict:
        """
        Generates an optimized schedule for all buses, prioritizing routes that serve
        the most waiting passengers.
        """
        bus_schedules_optimized = collections.defaultdict(list)

        # Initialize internal state for optimizer
        # Use a deep copy for temp_stops_with_passengers to allow modification without affecting original
        temp_stops_with_passengers = {
            stop_id: collections.deque() for stop_id in self.stops.keys()
        }

        # Populate initial demand into temp_stops for the optimizer's view
        sorted_initial_demands = sorted(
            list(self.all_raw_demands), key=lambda p: p["arrival_time"]
        )

        # Keep track of which passenger IDs have been 'served' by the optimizer's schedule
        # This prevents the same passenger from being counted multiple times across different scheduled trips
        optimizer_served_passenger_ids = set()

        # Main scheduling loop
        while True:
            # First, process any new demands that have arrived up to the current earliest available bus time
            earliest_available_time = min(self.bus_availability.values())

            # Add new dynamic demands to temp_stops_with_passengers
            newly_arrived_demands = []
            while (
                sorted_initial_demands
                and sorted_initial_demands[0]["arrival_time"] <= earliest_available_time
            ):
                demand = sorted_initial_demands.pop(0)  # Pop from the front
                for _ in range(int(demand["count"])):
                    # Only add if not already served by a previous schedule decision
                    p = Passenger(
                        demand["origin"], demand["destination"], demand["arrival_time"]
                    )
                    if p.id not in optimizer_served_passenger_ids:
                        temp_stops_with_passengers[demand["origin"]].append(p)
                newly_arrived_demands.append(demand)

            # Find the next bus to schedule
            next_bus_id_to_schedule = None
            earliest_bus_available_time = (
                self.end_time_minutes + 1
            )  # Initialize with a time outside the window

            for bus_id, availability_time in self.bus_availability.items():
                if availability_time <= earliest_bus_available_time:
                    earliest_bus_available_time = availability_time
                    next_bus_id_to_schedule = bus_id

            if (
                next_bus_id_to_schedule is None
                or earliest_bus_available_time > self.end_time_minutes
            ):
                # No more buses to schedule within the time window
                break

            bus_obj = self.buses[next_bus_id_to_schedule]
            current_bus_location = self.bus_current_locations[next_bus_id_to_schedule]

            best_trip_score = -1  # Maximize passengers served
            best_trip_details = None
            actual_served_passengers_for_best_trip = []  # To store the actual passenger objects for removal

            # Iterate through all possible routes to find the best one for this bus
            for route_id, route_obj in self.routes.items():
                if not route_obj.stops_ids_in_order:
                    continue  # Skip empty routes

                potential_start_stop_id = route_obj.stops_ids_in_order[0]

                # Calculate dead run time to the start of this potential route
                dead_run_duration = 0
                if current_bus_location != potential_start_stop_id:
                    dead_run_duration = self._calculate_dead_run_time(
                        current_bus_location,
                        potential_start_stop_id,
                        earliest_bus_available_time,
                    )

                # The earliest this trip can depart is when the bus is available AND it arrives at the start stop
                potential_departure_time = (
                    earliest_bus_available_time + dead_run_duration
                )

                # Only consider trips that can start within the simulation window
                if potential_departure_time > self.end_time_minutes:
                    continue

                # Calculate how many passengers this trip could potentially serve
                passengers_served_by_this_trip, served_passenger_objects = (
                    self._calculate_potential_passengers_served(
                        route_obj,
                        bus_obj.capacity,
                        temp_stops_with_passengers,
                        potential_start_stop_id,
                    )
                )

                # Prioritize trips that serve more passengers
                if passengers_served_by_this_trip > best_trip_score:
                    best_trip_score = passengers_served_by_this_trip
                    best_trip_details = {
                        "route_id": route_id,
                        "departure_time_minutes": potential_departure_time,
                        "start_stop_id": potential_start_stop_id,
                        "dead_run_duration": dead_run_duration,
                    }
                    actual_served_passengers_for_best_trip = served_passenger_objects
                # Tie-breaking: if scores are equal, prefer routes that start sooner
                elif (
                    passengers_served_by_this_trip == best_trip_score
                    and best_trip_details
                ):
                    if (
                        potential_departure_time
                        < best_trip_details["departure_time_minutes"]
                    ):
                        best_trip_details = {
                            "route_id": route_id,
                            "departure_time_minutes": potential_departure_time,
                            "start_stop_id": potential_start_stop_id,
                            "dead_run_duration": dead_run_duration,
                        }
                        actual_served_passengers_for_best_trip = (
                            served_passenger_objects
                        )

            # If a beneficial trip was found (serving > 0 passengers)
            if best_trip_details and best_trip_score > 0:
                route_obj = self.routes[best_trip_details["route_id"]]
                trip_end_time = (
                    best_trip_details["departure_time_minutes"]
                    + route_obj.total_outbound_route_time_minutes
                )

                # Only schedule if the trip ends within the simulation window
                if trip_end_time <= self.end_time_minutes:
                    layover_duration = random.randint(
                        self.config["min_layover_minutes"],
                        self.config["max_layover_minutes"],
                    )

                    bus_schedules_optimized[next_bus_id_to_schedule].append(
                        {
                            "route_id": best_trip_details["route_id"],
                            "departure_time_minutes": best_trip_details[
                                "departure_time_minutes"
                            ],
                            "layover_duration": layover_duration,
                        }
                    )
                    self.bus_availability[next_bus_id_to_schedule] = (
                        trip_end_time + layover_duration
                    )
                    self.bus_current_locations[next_bus_id_to_schedule] = (
                        route_obj.stops_ids_in_order[-1]
                    )  # Bus ends at last stop of route

                    # Mark the served passengers as "served" in the optimizer's tracking set
                    for p_served in actual_served_passengers_for_best_trip:
                        optimizer_served_passenger_ids.add(p_served.id)

                    # Remove the served passengers from the temporary stop queues
                    for stop_id in temp_stops_with_passengers.keys():
                        passengers_to_keep = collections.deque()
                        while temp_stops_with_passengers[stop_id]:
                            p = temp_stops_with_passengers[stop_id].popleft()
                            if (
                                p.id not in optimizer_served_passenger_ids
                            ):  # Keep only unserved passengers
                                passengers_to_keep.append(p)
                        temp_stops_with_passengers[stop_id] = passengers_to_keep

                    logger.debug(
                        f"Optimizer: Bus {next_bus_id_to_schedule} scheduled for Route {best_trip_details['route_id']} at {format_time(best_trip_details['departure_time_minutes'])}. Served {best_trip_score} passengers. Next available: {format_time(self.bus_availability[next_bus_id_to_schedule])}"
                    )
                else:
                    # Trip ends too late, advance bus availability to end of simulation to prevent re-evaluating
                    self.bus_availability[next_bus_id_to_schedule] = (
                        self.end_time_minutes + 1
                    )
            else:
                # No beneficial trip found for this bus at this time.
                # Advance its availability slightly to avoid infinite looping on this bus.
                # This also allows other buses to be considered or new demand to arrive.
                self.bus_availability[next_bus_id_to_schedule] += self.config.get(
                    "scheduling_interval_minutes", 5
                )
                # If the bus is now available beyond the simulation end, mark it as done
                if (
                    self.bus_availability[next_bus_id_to_schedule]
                    > self.end_time_minutes
                ):
                    self.bus_availability[next_bus_id_to_schedule] = (
                        self.end_time_minutes + 1
                    )

            # Check if there's any remaining demand or any bus can still be scheduled
            remaining_demand_exists = (
                any(len(q) > 0 for q in temp_stops_with_passengers.values())
                or len(sorted_initial_demands) > 0
            )
            any_bus_can_be_scheduled = any(
                avail_time <= self.end_time_minutes
                for avail_time in self.bus_availability.values()
            )

            if not remaining_demand_exists and not any_bus_can_be_scheduled:
                break  # All demand served or no more buses can run

        # Sort schedules by departure time
        for bus_id in bus_schedules_optimized:
            bus_schedules_optimized[bus_id].sort(
                key=lambda x: x["departure_time_minutes"]
            )

        return bus_schedules_optimized


class BusEmulator:
    """
    The main simulation engine for bus operations.
    Manages buses, stops, passengers, and time progression.
    Can run based on an optimized schedule or generate its own.
    Directly connects to the database for data loading and saving.
    """

    def __init__(
        self,
        db: Session,
        use_optimized_schedule: bool = False,
        start_time_minutes: int = 0,
        end_time_minutes: int = 1440,
        config: Optional[dict] = None,
    ):
        self.db = db
        self.use_optimized_schedule = use_optimized_schedule
        self.current_time = start_time_minutes
        self.total_passengers_completed_trip = 0
        self.total_passengers_waiting = 0

        self.start_time_minutes = start_time_minutes
        self.end_time_minutes = end_time_minutes

        self.config = config or self._load_default_config()

        self.stops = {}
        self.routes = {}
        self.all_raw_demands = []
        self.buses = {}
        self.bus_types_map = {}
        self.db_buses_map = {}
        self.stop_points_data = {}  # Added stop_points_data initialization

        self.bus_schedules_planned = collections.defaultdict(list)
        self.initial_bus_schedules_for_db_save = collections.defaultdict(
            list
        )  # Will be populated after _plan_schedules

        self.pending_passengers: collections.deque[Passenger] = collections.deque()
        self.completed_passengers: list[Passenger] = []

        # New: Cumulative passenger data at stops
        self.cumulative_stop_data = collections.defaultdict(
            lambda: {
                "arrived": 0,
                "boarded": 0,
                "alighted": 0,
                "name": "",  # To store stop name for reporting
            }
        )

        self._load_data_from_db()
        self._prepare_initial_passengers()
        self._initialize_buses()
        self._plan_schedules()

        self._perform_initial_bus_positioning()

    def _load_default_config(self) -> dict:
        """
        Loads default configuration values.
        """
        logger.info("Loading default simulation configuration.")
        return {
            "avg_stop_time_per_passenger": 3,
            "passenger_wait_threshold": 1,
            "dead_run_travel_rate_km_per_hour": 30,
            "overcrowding_factor": 1.2,
            "min_trips_per_bus": 2,
            "max_trips_per_bus": 5,
            "min_layover_minutes": 5,
            "max_layover_minutes": 15,
            "scheduling_interval_minutes": 5,  # New config for optimizer time step
        }

    def _load_data_from_db(self):
        """
        Loads all necessary initial data for the simulation directly from the database.
        Includes enhanced error handling and joinedload for performance.
        """
        logger.info("Loading simulation data from database...")

        # 1. Load Stop Points
        db_stop_points = self.db.query(StopPoint).all()
        if not db_stop_points:
            logger.error(
                "No stop points found in DB. Simulation cannot proceed without stops."
            )
            raise ValueError("No stop points in database.")
        self.stops = {
            sp.atco_code: Stop(sp.atco_code, sp.name) for sp in db_stop_points
        }
        self.stop_points_data = {
            sp.atco_code: (sp.latitude, sp.longitude) for sp in db_stop_points
        }  # Populated stop_points_data
        # Initialize cumulative_stop_data with stop names
        for sp in db_stop_points:
            self.cumulative_stop_data[sp.atco_code]["name"] = sp.name

        # Ensure default depot exists and is a valid stop point
        self.default_depot_id = db_stop_points[0].atco_code if db_stop_points else None
        if self.default_depot_id is None or self.default_depot_id not in self.stops:
            logger.error(
                "No valid default depot stop point found. Simulation cannot proceed."
            )
            raise ValueError("No valid default depot stop point in database.")
        logger.info(f"Loaded {len(self.stops)} stop points.")

        # 2. Load Bus Types
        db_bus_types = self.db.query(BusType).all()
        if not db_bus_types:
            logger.error("No bus types found in DB. Cannot initialize buses.")
            raise ValueError("No bus types in database.")
        self.bus_types_map = {bt.type_id: bt for bt in db_bus_types}
        logger.info(f"Loaded {len(self.bus_types_map)} bus types.")

        # 3. Load individual Bus instances (DBBus) with their associated BusType and Garage
        db_buses = (
            self.db.query(DBBus)
            .options(joinedload(DBBus.bus_type), joinedload(DBBus.garage))
            .all()
        )
        if not db_buses:
            logger.error("No individual buses found in DB. Cannot run simulation.")
            raise ValueError("No individual buses in database.")
        self.db_buses_map = {db_bus.bus_id: db_bus for db_bus in db_buses}
        logger.info(f"Loaded {len(self.db_buses_map)} individual buses.")

        # 4. Load Routes and Route Definitions with joinedload for efficiency
        db_routes = (
            self.db.query(Route)
            .options(
                joinedload(Route.route_definitions).joinedload(
                    RouteDefinition.stop_point
                )
            )
            .all()
        )
        if not db_routes:
            logger.warning("No routes found in DB. Simulation will not schedule trips.")

        for route_db in db_routes:
            stops_ids_in_order = []
            total_outbound_route_time_minutes = 0

            sorted_route_defs = sorted(
                route_db.route_definitions, key=lambda rd: rd.sequence
            )
            if not sorted_route_defs:
                logger.warning(
                    f"Route {route_db.route_id} has no defined stop points. Skipping this route."
                )
                continue

            for i, rd in enumerate(sorted_route_defs):
                if rd.stop_point:
                    stops_ids_in_order.append(rd.stop_point.atco_code)
                else:
                    logger.warning(
                        f"Route Definition {rd.route_def_id} for Route {route_db.route_id} has a missing StopPoint. Skipping this route definition."
                    )
                    continue

                if i < len(sorted_route_defs) - 1:
                    segment_travel_time = 5
                    total_outbound_route_time_minutes += segment_travel_time

            if len(stops_ids_in_order) > 1:
                self.routes[route_db.route_id] = SimRoute(
                    route_db.route_id,
                    stops_ids_in_order,
                    total_outbound_route_time_minutes,
                )
                logger.debug(
                    f"Loaded Route {route_db.route_id} with stops: {stops_ids_in_order}"
                )  # Added debug log
            else:
                logger.warning(
                    f"Route {route_db.route_id} only has {len(stops_ids_in_order)} stop(s). Skipping as it cannot form a valid trip."
                )

        logger.info(f"Loaded {len(self.routes)} routes and their definitions.")

        # 5. Load Demand Records
        db_demands = self.db.query(Demand).all()
        self.all_raw_demands = []
        for d in db_demands:
            origin_sp_id = self._get_stop_area_representative_stop_point(d.origin)
            destination_sp_id = self._get_stop_area_representative_stop_point(
                d.destination
            )

            if origin_sp_id is None or destination_sp_id is None:
                logger.warning(
                    f"Demand origin/destination StopArea ({d.origin}, {d.destination}) could not be mapped to StopPoints. Skipping this demand record."
                )
                continue

            if origin_sp_id not in self.stops:
                logger.warning(
                    f"Demand for origin {origin_sp_id} references unknown stop. Skipping."
                )
                continue
            if destination_sp_id not in self.stops:
                logger.warning(
                    f"Demand for destination {destination_sp_id} references unknown stop. Skipping."
                )
                continue

            self.all_raw_demands.append(
                {
                    "origin": origin_sp_id,
                    "destination": destination_sp_id,
                    "count": d.count,
                    "arrival_time": d.start_time.hour * 60 + d.start_time.minute,
                }
            )
        logger.info(f"Loaded {len(self.all_raw_demands)} demand records.")
        logger.info("Simulation data loading complete.")

    def _get_stop_area_representative_stop_point(
        self, stop_area_code: int
    ) -> Optional[int]:
        """Helper to find a representative stop point for a given stop area code."""
        stop_point = (
            self.db.query(StopPoint)
            .filter(StopPoint.stop_area_code == stop_area_code)
            .first()
        )
        return stop_point.atco_code if stop_point else None

    def _initialize_buses(self):
        """
        Initializes Bus objects for the simulation based on loaded DBBus data.
        Generates simulator-internal IDs (e.g., S1, L2) and stores DB registration.
        """
        sim_bus_id_counter_by_type = collections.defaultdict(lambda: 1)

        for db_reg, db_bus_obj in self.db_buses_map.items():
            bus_type = self.bus_types_map.get(db_bus_obj.bus_type_id)
            if not bus_type:
                logger.error(
                    f"Bus {db_reg} has unknown bus type ID {db_bus_obj.bus_type_id}. Skipping."
                )
                continue

            sim_bus_id = (
                f"{bus_type.name[0].upper()}{sim_bus_id_counter_by_type[bus_type.name]}"
            )
            sim_bus_id_counter_by_type[bus_type.name] += 1

            depot_id = self.default_depot_id
            if depot_id is None or depot_id not in self.stops:
                logger.error(
                    f"Bus {db_reg} has no suitable depot stop point. Skipping."
                )
                continue

            self.buses[sim_bus_id] = Bus(
                bus_id=sim_bus_id,
                capacity=bus_type.capacity,
                depot_stop_id=depot_id,
                initial_internal_time=self.start_time_minutes,
                overcrowding_factor=self.config["overcrowding_factor"],
                db_registration=db_reg,
            )
        logger.info(f"Initialized {len(self.buses)} simulation Bus objects.")

    def _plan_schedules(self):
        """
        Determines the schedule for each bus: either from optimized VehicleJourneys
        or by generating a random schedule.
        """
        self.bus_schedules_planned = collections.defaultdict(list)

        if self.use_optimized_schedule:
            logger.info(
                "Generating optimized schedules using OptimizedScheduleManager..."
            )
            optimizer = OptimizedScheduleManager(
                stops=self.stops,
                routes=self.routes,
                buses=self.buses,
                all_raw_demands=self.all_raw_demands,
                start_time_minutes=self.start_time_minutes,
                end_time_minutes=self.end_time_minutes,
                config=self.config,
                stop_points_data=self.stop_points_data,  # Passed stop_points_data to optimizer
            )
            self.bus_schedules_planned = optimizer.generate_optimized_schedule()
            logger.info(
                f"Optimized schedules generated for {len(self.bus_schedules_planned)} buses."
            )
            if not self.bus_schedules_planned:
                logger.warning(
                    "No optimized schedules were generated. Falling back to random schedules."
                )
                self._generate_random_schedules()
        else:
            logger.info("Generating random schedules for buses...")
            self._generate_random_schedules()

        self.initial_bus_schedules_for_db_save = copy.deepcopy(
            self.bus_schedules_planned
        )

    def _estimate_future_demand(self) -> collections.defaultdict:
        """
        Estimates future passenger demand at each stop based on raw demand data.
        Returns: defaultdict(lambda: defaultdict(int)) where
                 estimated_demand[stop_id][time_minute] = passenger_count
        """
        estimated_demand = collections.defaultdict(lambda: collections.defaultdict(int))
        # Populate estimated_demand based on self.all_raw_demands
        for demand_record in self.all_raw_demands:
            origin_id = demand_record["origin"]
            arrival_time = demand_record["arrival_time"]
            count = demand_record["count"]
            # This version concentrates demand at the exact arrival_time.
            if self.start_time_minutes <= arrival_time <= self.end_time_minutes:
                estimated_demand[origin_id][arrival_time] += count
        return estimated_demand

    def _generate_random_schedules(self):
        """
        Generates a simple random schedule for each bus, but with a bias towards
        stops with higher estimated future demand.
        """
        logger.info("Generating demand-aware random schedules for buses...")

        MIN_TRIPS_PER_BUS = self.config["min_trips_per_bus"]
        MAX_TRIPS_PER_BUS = self.config["max_trips_per_bus"]
        MIN_LAYOVER_MINUTES = self.config["min_layover_minutes"]
        MAX_LAYOVER_MINUTES = self.config["max_layover_minutes"]

        estimated_demand = self._estimate_future_demand()

        for bus_id, bus in self.buses.items():
            possible_routes = []
            for route_id, route in self.routes.items():
                if (
                    route.stops_ids_in_order
                    and route.stops_ids_in_order[0] == bus.initial_start_point
                ):
                    possible_routes.append(route_id)

            if not possible_routes:
                logger.error(
                    f"CRITICAL: No suitable route found starting at depot {bus.initial_start_point} for Bus {bus_id}. Cannot schedule this bus."
                )
                continue

            last_trip_end_time = self.start_time_minutes

            for i in range(MAX_TRIPS_PER_BUS):
                layover_duration = random.randint(
                    MIN_LAYOVER_MINUTES, MAX_LAYOVER_MINUTES
                )
                min_departure_for_current_trip = last_trip_end_time + layover_duration

                max_departure_for_current_trip = self.end_time_minutes - (5 * 60)

                if min_departure_for_current_trip > max_departure_for_current_trip:
                    logger.debug(
                        f"Bus {bus_id}: No more time slots for additional trips. Breaking after {i} trips."
                    )
                    break

                candidate_routes_with_demand = []
                for route_id in possible_routes:
                    route_obj = self.routes.get(route_id)
                    if not route_obj:
                        continue
                    start_stop_id = route_obj.stops_ids_in_order[0]

                    demand_at_start_stop = 0
                    for t_offset in range(-30, 31):
                        check_time = min_departure_for_current_trip + t_offset
                        if (
                            self.start_time_minutes
                            <= check_time
                            <= self.end_time_minutes
                        ):
                            demand_at_start_stop += estimated_demand[start_stop_id][
                                check_time
                            ]

                    candidate_routes_with_demand.append(
                        (demand_at_start_stop, route_id)
                    )

                candidate_routes_with_demand.sort(key=lambda x: x[0], reverse=True)

                if not candidate_routes_with_demand:
                    logger.debug(
                        f"Bus {bus_id}: No routes with estimated demand found. Breaking."
                    )
                    break

                top_routes_for_selection = [
                    r[1]
                    for r in candidate_routes_with_demand[
                        : min(3, len(candidate_routes_with_demand))
                    ]
                ]

                if all(
                    r[0] == 0
                    for r in candidate_routes_with_demand[
                        : min(3, len(candidate_routes_with_demand))
                    ]
                ):
                    route_id = random.choice(possible_routes)
                else:
                    route_id = random.choice(top_routes_for_selection)

                route_obj = self.routes.get(route_id)
                if not route_obj:
                    continue

                departure_time_minutes = random.randint(
                    min_departure_for_current_trip, max_departure_for_current_trip
                )

                self.bus_schedules_planned[bus_id].append(
                    {
                        "route_id": route_id,
                        "layover_duration": layover_duration,
                        "departure_time_minutes": departure_time_minutes,
                    }
                )
                logger.info(
                    f"Bus {bus_id} scheduled trip {i + 1}: Route {route_id}, Departure {format_time(departure_time_minutes)} (Demand-aware)."
                )

                last_trip_end_time = (
                    departure_time_minutes + route_obj.total_outbound_route_time_minutes
                )

            self.bus_schedules_planned[bus_id].sort(
                key=lambda x: x["departure_time_minutes"]
            )

    def _calculate_dead_run_time(
        self, from_stop_id: int, to_stop_id: int, current_time_minutes: int
    ) -> int:
        """
        Calculates the travel time for a dead run between two stops.
        Considers distance and applies a traffic factor if it's rush hour.
        Returns time in minutes (rounded up).
        """
        from_stop_db = (
            self.db.query(StopPoint).filter_by(atco_code=from_stop_id).first()
        )
        to_stop_db = self.db.query(StopPoint).filter_by(atco_code=to_stop_id).first()

        if not from_stop_db or not to_stop_db:
            logger.error(
                f"Cannot calculate dead run time: One or both stops ({from_stop_id}, {to_stop_id}) not found in DB."
            )
            return 5

        if (
            from_stop_db.latitude is None
            or from_stop_db.longitude is None
            or to_stop_db.latitude is None
            or to_stop_db.longitude is None
        ):
            logger.warning(
                f"Missing lat/lon for stops {from_stop_id} or {to_stop_id}. Using default dead run time."
            )
            return 5

        distance_km = haversine_distance(
            from_stop_db.latitude,
            from_stop_db.longitude,
            to_stop_db.latitude,
            to_stop_db.longitude,
        )

        base_speed_kmph = self.config["dead_run_travel_rate_km_per_hour"]

        if base_speed_kmph <= 0:
            logger.error(
                "Dead run speed configured as zero or negative. Using default time."
            )
            return 5

        time_hours = distance_km / base_speed_kmph
        time_minutes = time_hours * 60

        traffic_factor = 1.0
        if is_rush_hour(current_time_minutes):
            traffic_factor = 1.5
            logger.debug(
                f"Rush hour detected at {format_time(current_time_minutes)}. Applying traffic factor {traffic_factor}."
            )

        return max(1, int(math.ceil(time_minutes * traffic_factor)))

    def _perform_initial_bus_positioning(self):
        """
        Positions each bus at the start of its first scheduled route,
        accounting for dead runs from depot if necessary, and sets its initial time.
        """
        for bus_id, bus in self.buses.items():
            if not self.bus_schedules_planned.get(bus_id):
                logger.warning(
                    f"Bus {bus_id} has no schedule planned. It remains at its depot {bus.initial_start_point}."
                )
                continue

            first_schedule_entry = self.bus_schedules_planned[bus_id][0]
            route_id = first_schedule_entry["route_id"]
            route = self.routes.get(route_id)

            if not route:
                logger.error(
                    f"Route {route_id} not found for Bus {bus_id}'s first scheduled trip. Cannot position bus."
                )
                continue

            first_route_stop_id = route.stops_ids_in_order[0]

            scheduled_departure_time = first_schedule_entry.get(
                "departure_time_minutes", self.start_time_minutes
            )

            if bus.current_stop_id != first_route_stop_id:
                travel_time_needed = self._calculate_dead_run_time(
                    bus.current_stop_id, first_route_stop_id, bus.current_time
                )

                dead_run_arrival_time = bus.current_time + travel_time_needed

                final_dead_run_arrival_time = max(
                    dead_run_arrival_time, scheduled_departure_time
                )

                for t in range(
                    int(bus.current_time) + 1, int(final_dead_run_arrival_time) + 1
                ):
                    bus.add_event_to_schedule(
                        t,
                        f"Dead Run En Route from {bus.initial_start_point} to {first_route_stop_id}",
                        None,
                        len(bus.onboard_passengers),
                        0,
                        0,
                        0,
                        "N/A",
                        "EN_ROUTE_DEAD_RUN",
                    )

                bus.current_time = final_dead_run_arrival_time
                bus.current_stop_id = first_route_stop_id
                bus.add_event_to_schedule(
                    bus.current_time,
                    f"Arrived at first route stop {first_route_stop_id} (Dead Run)",
                    first_route_stop_id,
                    len(bus.onboard_passengers),
                    self.stops[first_route_stop_id].get_waiting_passengers_count(),
                    0,
                    0,
                    "N/A",
                    "AT_STOP_DEAD_RUN",
                )
                logger.info(
                    f"Bus {bus_id} dead ran from {bus.initial_start_point} to {first_route_stop_id}, arriving at {format_time(bus.current_time)}."
                )
            else:
                if bus.current_time < scheduled_departure_time:
                    bus.current_time = scheduled_departure_time
                    bus.add_event_to_schedule(
                        bus.current_time,
                        f"Ready at {bus.current_stop_id} (Scheduled Departure)",
                        bus.current_stop_id,
                        len(bus.onboard_passengers),
                        self.stops[bus.current_stop_id].get_waiting_passengers_count(),
                        0,
                        0,
                        "N/A",
                        "READY_AT_START",
                    )
                logger.info(
                    f"Bus {bus_id} is already at its first route stop {first_route_stop_id} and ready for departure at {format_time(bus.current_time)}."
                )

            self.bus_schedules_planned[bus_id][0]["departure_time_minutes"] = (
                bus.current_time
            )

    def _prepare_initial_passengers(self):
        """
        Initializes passenger objects from raw demand data and populates
        either `self.stops[].waiting_passengers` for immediate arrivals
        or `self.pending_passengers` for future arrivals.
        """
        Passenger._id_counter = 0

        all_arrival_times = []

        for demand in self.all_raw_demands:
            origin_stop_id = demand["origin"]
            destination_stop_id = demand["destination"]
            count = int(demand["count"])
            passenger_arrival_time = demand.get("arrival_time")

            if passenger_arrival_time is None:
                logger.warning(f"Demand missing 'arrival_time': {demand}. Skipping.")
                continue

            if origin_stop_id not in self.stops:
                logger.warning(
                    f"Origin stop {origin_stop_id} for demand not found in loaded stops. Skipping demand."
                )
                continue
            if destination_stop_id not in self.stops:
                logger.warning(
                    f"Destination stop {destination_stop_id} for demand not found in loaded stops. Skipping demand."
                )
                continue

            for _ in range(count):
                new_passenger = Passenger(
                    origin_stop_id, destination_stop_id, passenger_arrival_time
                )
                if new_passenger.arrival_time_at_stop <= self.start_time_minutes:
                    self.stops[origin_stop_id].add_passenger(new_passenger)
                    # Update cumulative arrived count
                    self.cumulative_stop_data[origin_stop_id]["arrived"] += 1
                else:
                    self.pending_passengers.append(new_passenger)
                all_arrival_times.append(new_passenger.arrival_time_at_stop)

        self.pending_passengers = collections.deque(
            sorted(list(self.pending_passengers), key=lambda p: p.arrival_time_at_stop)
        )

        logger.info(f"Prepared {Passenger._id_counter} initial passenger demands.")

        if (
            self.start_time_minutes == 0
            and self.end_time_minutes == 1440
            and all_arrival_times
        ):
            self.start_time_minutes = min(all_arrival_times)
            self.end_time_minutes = max(all_arrival_times) + 120
            logger.info(
                f"Dynamic simulation window set: {format_time(self.start_time_minutes)} to {format_time(self.end_time_minutes)}"
            )
        elif not all_arrival_times:
            logger.warning(
                "No passenger demands with arrival times found. Simulation window will default to 24 hours if not explicitly set."
            )

    def _process_dynamic_demands(self, current_time: int):
        """
        Checks for and adds new passenger demands whose arrival time matches the current simulation time.
        """
        while (
            self.pending_passengers
            and self.pending_passengers[0].arrival_time_at_stop <= current_time
        ):
            passenger = self.pending_passengers.popleft()
            if passenger.origin_stop_id in self.stops:
                self.stops[passenger.origin_stop_id].add_passenger(passenger)
                # Update cumulative arrived count
                self.cumulative_stop_data[passenger.origin_stop_id]["arrived"] += 1
                logger.debug(
                    f"Time {format_time(current_time)}: Passenger {passenger.id} dynamically arrived at stop {passenger.origin_stop_id}."
                )
            else:
                logger.warning(
                    f"Passenger {passenger.id} requested arrival at unknown stop {passenger.origin_stop_id}. Skipping."
                )

    def _get_stop_by_id(self, stop_id: int) -> Optional[Stop]:
        """Helper to retrieve a Stop object by its ID."""
        return self.stops.get(stop_id)

    def _get_route_by_id(self, route_id: int) -> Optional["SimRoute"]:
        """Helper to retrieve a SimRoute object by its ID."""
        return self.routes.get(route_id)

    def _return_bus_to_depot(self, bus: "Bus", global_simulation_time: int):
        """
        Simulates a bus instantly returning to its initial depot after all trips are done.
        """
        if bus.current_stop_id == bus.initial_start_point:
            logger.info(
                f"Bus {bus.bus_id} is already at its depot {bus.initial_start_point}."
            )
            return

        logger.info(
            f"Time {format_time(global_simulation_time)}: Bus {bus.bus_id} returning to depot {bus.initial_start_point}."
        )

        bus.current_time = global_simulation_time
        bus.current_stop_id = bus.initial_start_point
        bus.is_en_route = False

        bus.add_event_to_schedule(
            bus.current_time,
            f"Returned to Depot {bus.initial_start_point}",
            bus.initial_start_point,
            len(bus.onboard_passengers),
            0,
            0,
            0,
            "N/A",
            "AT_DEPOT",
        )
        logger.info(
            f"Time {format_time(bus.current_time)}: Bus {bus.bus_id} successfully returned to depot {bus.initial_start_point}."
        )

    def export_all_bus_schedules_to_separate_csvs(self):
        """
        Exports each bus's internal schedule to a separate CSV file.
        """
        for bus_id, bus in self.buses.items():
            if bus.schedule:
                df = pd.DataFrame(
                    bus.schedule,
                    columns=[
                        "Time",
                        "Event",
                        "Stop ID",
                        "Passengers Onboard",
                        "Passengers Waiting",
                        "Boarded",
                        "Alighted",
                        "Direction",
                        "Status",
                    ],
                )
                df["Time"] = df["Time"].apply(format_time)
                file_name = f"bus_schedule_{bus_id}.csv"
                df.to_csv(file_name, index=False)
                logger.info(f"Schedule for Bus {bus_id} exported to {file_name}")
            else:
                logger.info(f"No schedule to export for Bus {bus_id}.")

    def _calculate_passenger_metrics(self):
        """
        Calculates and logs overall passenger metrics from completed trips.
        """
        total_completed_trips = len(self.completed_passengers)
        if total_completed_trips == 0:
            logger.info("\n===== Passenger Metrics =====")
            logger.info("No passengers completed trips during the simulation.")
            return

        total_wait_time = sum(
            p.wait_time for p in self.completed_passengers if p.wait_time is not None
        )
        total_travel_time = sum(
            p.travel_time
            for p in self.completed_passengers
            if p.travel_time is not None
        )
        # Corrected line: Changed 'p.total_total_trip_time' to 'p.total_trip_time'
        total_total_trip_time = sum(
            p.total_trip_time
            for p in self.completed_passengers
            if p.total_trip_time is not None
        )

        avg_wait_time = total_wait_time / total_completed_trips
        avg_travel_time = total_travel_time / total_completed_trips
        avg_total_trip_time = total_total_trip_time / total_completed_trips

        logger.info("\n===== Passenger Metrics =====")
        logger.info(f"Total passengers who completed trips: {total_completed_trips}")
        logger.info(f"Average passenger wait time: {avg_wait_time:.2f} minutes")
        logger.info(f"Average passenger travel time: {avg_travel_time:.2f} minutes")
        logger.info(
            f"Average total trip time (arrival at origin to alighting at destination): {avg_total_trip_time:.2f} minutes"
        )

    def _report_cumulative_stop_data(self):
        """
        Reports cumulative passenger activity at each stop.
        """
        logger.info("\n===== Cumulative Passenger Activity at Stops =====")
        for stop_id, data in self.cumulative_stop_data.items():
            stop_name = data.get("name", f"Unknown Stop ({stop_id})")
            logger.info(f"Stop: {stop_name} (ID: {stop_id})")
            logger.info(f"  Passengers Arrived: {data['arrived']}")
            logger.info(f"  Passengers Boarded: {data['boarded']}")
            logger.info(f"  Passengers Alighted: {data['alighted']}")
        logger.info("================================================")

    def _save_emulator_schedule_to_db(self):
        """
        Saves the emulator-generated schedule (VehicleJourneys, JourneyPatterns, Blocks)
        to the database.
        """
        logger.info("Saving emulator-generated schedule to database...")
        # Clean up old data to prevent accumulation during debugging/multiple runs
        # This is an aggressive cleanup and might not be desired in a production system.
        # For debugging and ensuring fresh state, it's useful.
        logger.warning(
            "Performing aggressive cleanup of old VehicleJourneys, JourneyPatterns, and Blocks for debugging."
        )
        self.db.query(VehicleJourney).delete()
        self.db.query(JourneyPattern).delete()
        self.db.query(Block).delete()
        self.db.commit()
        logger.warning("Aggressive cleanup complete.")

        # Fetch or create default Operator, Line, Service
        default_operator = (
            self.db.query(Operator).filter_by(operator_code="DEFAULT").first()
        )
        if not default_operator:
            default_operator = Operator(
                operator_code="DEFAULT", name="Default Operator"
            )
            self.db.add(default_operator)
            self.db.flush()  # Flush to get operator_id
            logger.debug(f"Created Default Operator: {default_operator.operator_id}")
        else:
            logger.debug(
                f"Default operator already exists: {default_operator.operator_id}"
            )

        default_line = self.db.query(Line).filter_by(line_name="DEFAULT_LINE").first()
        if not default_line:
            default_line = Line(
                line_name="DEFAULT_LINE", operator_id=default_operator.operator_id
            )
            self.db.add(default_line)
            self.db.flush()
            logger.debug(f"Created Default Line: {default_line.line_id}")
        else:
            logger.debug(f"Default line already exists: {default_line.line_id}")

        default_service = (
            self.db.query(Service).filter_by(service_code="DEFAULT_SERVICE").first()
        )
        if not default_service:
            default_service = Service(
                service_code="DEFAULT_SERVICE",
                name="Default Service",
                operator_id=default_operator.operator_id,
                line_id=default_line.line_id,
            )
            self.db.add(default_service)
            self.db.flush()
            logger.debug(f"Created Default Service: {default_service.service_id}")
        else:
            logger.debug(
                f"Default service already exists: {default_service.service_id}"
            )

        total_vjs_saved = 0
        logger.debug(f"total_vjs_saved initialized to: {total_vjs_saved}")

        simulation_run_timestamp = datetime.now().strftime("%Y%m%d%H%M%S%f")
        logger.debug(f"Simulation run timestamp: {simulation_run_timestamp}")

        logger.debug(
            f"Bus schedules planned for saving: {self.initial_bus_schedules_for_db_save}"
        )

        for sim_bus_id, trips in self.initial_bus_schedules_for_db_save.items():
            db_bus_obj = self.buses[sim_bus_id].db_registration
            assigned_db_bus = self.db.query(DBBus).filter_by(bus_id=db_bus_obj).first()
            if not assigned_db_bus:
                logger.error(
                    f"DBBus with registration {db_bus_obj} not found for simulator bus {sim_bus_id}. Cannot save VJs for this bus."
                )
                continue

            for trip in trips:
                route_id = trip["route_id"]
                departure_time_minutes = trip["departure_time_minutes"]

                # Create JourneyPattern
                jp_code = f"EMU_JP_{sim_bus_id}_R{route_id}_T{departure_time_minutes}_{simulation_run_timestamp}"
                assigned_jp = JourneyPattern(
                    jp_code=jp_code,
                    line_id=default_line.line_id,
                    route_id=route_id,
                    service_id=default_service.service_id,
                    operator_id=default_operator.operator_id,
                    name=f"Emulator Generated JP for {sim_bus_id} Route {route_id}",
                )
                self.db.add(assigned_jp)
                self.db.flush()  # Flush to get jp_id
                logger.debug(f"Generated jp_code: {jp_code}")
                logger.debug(f"Created JourneyPattern: {assigned_jp.jp_id}")

                # Create Block
                block_name = f"EMU_BLOCK_{sim_bus_id}_R{route_id}_T{departure_time_minutes}_{simulation_run_timestamp}"
                assigned_block = Block(
                    name=block_name,
                    operator_id=default_operator.operator_id,
                    bus_type_id=assigned_db_bus.bus_type_id,  # Use the bus's actual type
                )
                self.db.add(assigned_block)
                self.db.flush()
                logger.debug(f"Generated block_name: {block_name}")
                logger.debug(f"Created Block: {assigned_block.block_id}")

                departure_time_obj = (
                    datetime.min + timedelta(minutes=departure_time_minutes)
                ).time()

                new_vj = VehicleJourney(
                    departure_time=departure_time_obj,
                    dayshift=1,
                    jp_id=assigned_jp.jp_id,
                    block_id=assigned_block.block_id,
                    operator_id=default_operator.operator_id,
                    line_id=default_line.line_id,
                    service_id=default_service.service_id,
                    # Removed assigned_bus_id from constructor
                )
                # Explicitly assign the DBBus object to the relationship attribute after instantiation
                new_vj.assigned_bus_obj = assigned_db_bus

                self.db.add(new_vj)
                # Flush after adding new_vj to ensure it gets an ID and is tracked by the session
                self.db.flush()
                logger.info(
                    f"Saved generated VehicleJourney for Bus {sim_bus_id} (DB Reg: {assigned_db_bus.bus_id}) on Route {route_id} at {format_time(departure_time_minutes)}. VJ ID: {new_vj.vj_id}"
                )
                total_vjs_saved += 1
                logger.debug(
                    f"total_vjs_saved incremented to: {total_vjs_saved}"
                )  # Debug log

        logger.debug(
            f"Final total_vjs_saved before commit/rollback: {total_vjs_saved}"
        )  # Debug log
        if total_vjs_saved > 0:
            self.db.commit()
            logger.info(
                f"Emulator-generated schedule saved to database successfully. Total {total_vjs_saved} new VJs added."
            )
        else:
            self.db.rollback()  # Rollback if no new VJs were added
            logger.warning(
                "No new VehicleJourneys were generated or saved to the database."
            )

    def check_bus_return_to_start(self) -> dict:
        """
        Checks if all buses returned to their initial start point.
        Returns a dictionary with status and details.
        """
        all_returned = True
        details = {}
        for bus_id, bus in self.buses.items():
            if bus.current_stop_id != bus.initial_start_point:
                all_returned = False
                details[bus_id] = (
                    f"Did not return to depot. Current stop: {bus.current_stop_id}, Expected: {bus.initial_start_point}"
                )
            else:
                details[bus_id] = "Returned to depot successfully."

        return {
            "status": "Success" if all_returned else "Warning",
            "message": "All buses returned to their initial start points."
            if all_returned
            else "Some buses did not return to their initial start points.",
            "details": details,
        }

    def run_simulation(self) -> dict:
        """
        Runs the discrete-event simulation minute by minute.
        """
        logger.info(
            f"===== Starting Bus Simulation ({format_time(self.start_time_minutes)} to {format_time(self.end_time_minutes)}) ====="
        )

        self.current_time = self.start_time_minutes

        while self.current_time <= self.end_time_minutes:
            # --- 1. Process dynamically arriving passengers ---
            self._process_dynamic_demands(self.current_time)

            # Create a list of buses to process to avoid dictionary modification issues
            buses_to_process_this_minute = list(self.buses.values())

            # --- 2. Process buses ---
            for bus in buses_to_process_this_minute:
                # Ensure bus's internal time is aligned with global simulation time
                if bus.current_time > self.current_time:
                    continue  # Bus is ahead, wait for it in a future minute

                bus.current_time = self.current_time  # Align bus time

                # If bus is currently en route, update its movement
                if bus.is_en_route:
                    bus.time_to_next_stop -= 1
                    if bus.time_to_next_stop <= 0:
                        # Bus has arrived at its destination stop for this segment
                        bus.time_to_next_stop = 0  # Ensure it's exactly 0

                        # Update current_stop_id to the stop it just arrived at
                        if (
                            bus.current_route
                            and bus.destination_stop_id_on_segment is not None
                        ):
                            bus.current_stop_id = bus.destination_stop_id_on_segment
                            bus.destination_stop_id_on_segment = None  # Reset
                        else:
                            logger.error(
                                f"Bus {bus.bus_id} arrived but its destination_stop_id_on_segment was not set or current_route is missing. Halting its movement."
                            )
                            bus.is_en_route = False  # Halt movement due to error
                            continue  # Skip this bus

                        bus.is_en_route = False  # Bus is now AT a stop

                        current_stop_obj = self._get_stop_by_id(bus.current_stop_id)
                        current_stop_passengers_waiting = (
                            current_stop_obj.get_waiting_passengers_count()
                            if current_stop_obj
                            else 0
                        )

                        bus.add_event_to_schedule(
                            self.current_time,
                            f"Arrived At Stop {bus.current_stop_id}",
                            bus.current_stop_id,
                            len(bus.onboard_passengers),
                            current_stop_passengers_waiting,
                            0,
                            0,
                            bus.current_direction,
                            "AT_STOP",
                        )
                        logger.info(
                            f"Time {format_time(self.current_time)}: Bus {bus.bus_id} arrived at {bus.current_stop_id}."
                        )

                        # DO NOT `continue` here. Fall through to process alighting/boarding and next action.

                    else:  # Bus is still en route, not arrived yet
                        continue  # Nothing else to do for it this minute, move to next bus

                # At this point, the bus is AT A STOP (`bus.is_en_route` is False)
                # This applies to buses that just arrived, or were already waiting at a stop.
                current_stop = self._get_stop_by_id(bus.current_stop_id)
                if not current_stop:
                    logger.error(
                        f"Bus {bus.bus_id} is at an unknown stop ID: {bus.current_stop_id}. Halting its simulation."
                    )
                    continue

                # 2a. Alight passengers at the current stop
                alighted_passengers = bus.alight_passengers(
                    self.current_time, current_stop
                )
                self.completed_passengers.extend(alighted_passengers)
                self.cumulative_stop_data[current_stop.stop_id]["alighted"] += (
                    bus.passenger_alighted_count
                )

                # 2b. Board passengers at the current stop
                boarded_count = bus.board_passengers(self.current_time, current_stop)
                self.cumulative_stop_data[current_stop.stop_id]["boarded"] += (
                    boarded_count
                )

                # Update the last AT_STOP event in schedule with boarding/alighting info
                if bus.schedule and bus.schedule[-1][8] == "AT_STOP":
                    last_event = list(bus.schedule[-1])
                    last_event[3] = len(
                        bus.onboard_passengers
                    )  # Passengers onboard after alighting/boarding
                    last_event[4] = (
                        current_stop.get_waiting_passengers_count()
                    )  # Passengers waiting after boarding
                    last_event[5] = boarded_count
                    last_event[6] = bus.passenger_alighted_count
                    bus.schedule[-1] = tuple(last_event)

                # 2c. Determine next action for the bus (continue route, start new trip, or return to depot)

                # If the bus is currently on an active route (meaning it's at an intermediate stop)
                if bus.current_route:
                    # Check if it's the last stop of the current route
                    if bus.current_stop_id == bus.current_route.stops_ids_in_order[-1]:
                        logger.info(
                            f"Time {format_time(self.current_time)}: Bus {bus.bus_id} completed route {bus.current_route.route_id} at {bus.current_stop_id}."
                        )
                        bus.current_route = None  # Mark route as completed
                        # Now, the bus has no current route, so it will fall through to the next 'if' block
                        # to check for the next scheduled trip or return to depot.
                    else:
                        # It's an intermediate stop, so immediately move to the next stop on the current route
                        bus.move_to_next_stop(self.current_time)
                        continue  # Bus is now en route, done processing for this bus in this minute

                # If bus has no current route (e.g., just finished a trip or waiting for first trip)
                # Check for next scheduled trip and if it's time to depart
                if self.bus_schedules_planned.get(bus.bus_id):
                    next_scheduled_trip = self.bus_schedules_planned[bus.bus_id][0]
                    scheduled_departure_time = next_scheduled_trip[
                        "departure_time_minutes"
                    ]
                    next_route_id = next_scheduled_trip["route_id"]
                    next_route = self._get_route_by_id(next_route_id)

                    if not next_route:
                        logger.error(
                            f"Scheduled route {next_route_id} not found for Bus {bus.bus_id}. Skipping its future trips."
                        )
                        self.bus_schedules_planned[bus.bus_id].pop(0)
                        continue

                    # Handle dead run to the start of the next route
                    if bus.current_stop_id != next_route.stops_ids_in_order[0]:
                        target_stop_id = next_route.stops_ids_in_order[0]
                        dead_run_duration = self._calculate_dead_run_time(
                            bus.current_stop_id, target_stop_id, self.current_time
                        )
                        dead_run_arrival_time = self.current_time + dead_run_duration

                        final_arrival_at_next_start_point = max(
                            dead_run_arrival_time, scheduled_departure_time
                        )

                        for t in range(
                            self.current_time + 1, final_arrival_at_next_start_point + 1
                        ):
                            bus.add_event_to_schedule(
                                t,
                                f"Dead Run En Route from {bus.current_stop_id} to {target_stop_id}",
                                None,
                                len(bus.onboard_passengers),
                                0,
                                0,
                                0,
                                "N/A",
                                "EN_ROUTE_DEAD_RUN",
                            )
                        bus.current_time = final_arrival_at_next_start_point
                        bus.current_stop_id = target_stop_id
                        bus.add_event_to_schedule(
                            bus.current_time,
                            f"Arrived at next scheduled route start {target_stop_id} (Dead Run)",
                            target_stop_id,
                            len(bus.onboard_passengers),
                            self.stops[target_stop_id].get_waiting_passengers_count(),
                            0,
                            0,
                            "N/A",
                            "AT_STOP_DEAD_RUN",
                        )
                        logger.info(
                            f"Bus {bus.bus_id} dead ran from {current_stop.stop_id} to {target_stop_id}, arriving at {format_time(bus.current_time)} for next trip."
                        )
                        # If arrived early, wait for scheduled departure
                        if bus.current_time < scheduled_departure_time:
                            if not bus.schedule or bus.schedule[-1][8] != "WAITING":
                                bus.add_event_to_schedule(
                                    self.current_time,
                                    f"Waiting for scheduled departure ({format_time(scheduled_departure_time)})",
                                    bus.current_stop_id,
                                    len(bus.onboard_passengers),
                                    current_stop.get_waiting_passengers_count(),
                                    0,
                                    0,
                                    "N/A",
                                    "WAITING",
                                )
                            continue  # Wait for the time to pass

                    # If at the starting stop of the next scheduled trip and it's time to depart
                    if (
                        bus.current_stop_id == next_route.stops_ids_in_order[0]
                        and self.current_time >= scheduled_departure_time
                    ):
                        bus.current_time = (
                            self.current_time
                        )  # Align bus time with current simulation time
                        bus.start_route(next_route, self.current_time)
                        logger.info(
                            f"Time {format_time(self.current_time)}: Bus {bus.bus_id} started scheduled trip on Route {next_route_id}."
                        )
                        self.bus_schedules_planned[bus.bus_id].pop(0)

                        boarded_count = bus.board_passengers(
                            self.current_time, current_stop
                        )
                        self.cumulative_stop_data[current_stop.stop_id]["boarded"] += (
                            boarded_count
                        )

                        if bus.schedule:
                            last_event = list(bus.schedule[-1])
                            last_event[3] = len(bus.onboard_passengers)
                            last_event[4] = current_stop.get_waiting_passengers_count()
                            last_event[5] = boarded_count
                            bus.schedule[-1] = tuple(last_event)

                        bus.move_to_next_stop(
                            self.current_time
                        )  # Immediately move to the next stop after starting route
                        continue  # Processed this bus for this minute, move to next bus or next minute

                    elif (
                        bus.current_stop_id == next_route.stops_ids_in_order[0]
                        and self.current_time < scheduled_departure_time
                    ):
                        # Bus is at the start of the next route but it's not time to depart yet
                        if not bus.schedule or bus.schedule[-1][8] != "WAITING":
                            bus.add_event_to_schedule(
                                self.current_time,
                                f"Waiting for scheduled departure ({format_time(scheduled_departure_time)})",
                                bus.current_stop_id,
                                len(bus.onboard_passengers),
                                current_stop.get_waiting_passengers_count(),
                                0,
                                0,
                                "N/A",
                                "WAITING",
                            )
                        continue  # Wait for the time to pass

                # If no more scheduled trips and not en route, return to depot or idle
                elif (
                    not self.bus_schedules_planned.get(bus.bus_id)
                    and not bus.is_en_route
                ):
                    if bus.current_stop_id != bus.initial_start_point:
                        self._return_bus_to_depot(bus, self.current_time)
                    elif bus.schedule and bus.schedule[-1][8] != "IDLE":
                        bus.add_event_to_schedule(
                            self.current_time,
                            "Idle",
                            bus.current_stop_id,
                            len(bus.onboard_passengers),
                            current_stop.get_waiting_passengers_count(),
                            0,
                            0,
                            "N/A",
                            "IDLE",
                        )
                    continue

            self.current_time += 1

            all_buses_idle = True
            for bus_id, bus in self.buses.items():
                if (
                    bus.is_en_route
                    or self.bus_schedules_planned.get(bus_id)
                    or len(bus.onboard_passengers) > 0
                ):
                    all_buses_idle = False
                    break

            any_passengers_waiting = any(
                stop.get_waiting_passengers_count() > 0 for stop in self.stops.values()
            )
            any_passengers_onboard = any(
                len(bus.onboard_passengers) > 0 for bus in self.buses.values()
            )
            any_pending_passengers = len(self.pending_passengers) > 0

            if (
                all_buses_idle
                and not any_passengers_waiting
                and not any_passengers_onboard
                and not any_pending_passengers
                and self.current_time > self.start_time_minutes
            ):
                logger.info(
                    "All buses have completed their schedules, no passengers waiting or onboard, and no pending demands. Ending simulation early."
                )
                break

        logger.info("===== Simulation Complete =====")

        for bus_id, bus in self.buses.items():
            if bus.current_stop_id != bus.initial_start_point:
                self._return_bus_to_depot(bus, self.current_time)

        self.export_all_bus_schedules_to_separate_csvs()
        self._calculate_passenger_metrics()  # This will now include the cumulative stop data
        self._report_cumulative_stop_data()  # New method call

        if not self.use_optimized_schedule:
            self._save_emulator_schedule_to_db()

        return self.check_bus_return_to_start()
