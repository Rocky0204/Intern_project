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

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

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

logging.basicConfig(level=logging.DEBUG, format="%(levelname)s:%(name)s:%(message)s")
logger = logging.getLogger(__name__)

def haversine_distance(lat1: float, lon1: float, lat2: float, lon2: float) -> float:

    R = 6371 

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
    sim_datetime = datetime(2025, 6, 10, 0, 0, 0) + timedelta(
        minutes=total_minutes_from_midnight
    )
    return sim_datetime.strftime("%H:%M")


class Passenger:
    _id_counter = 0

    def __init__(
        self, origin_stop_id: int, destination_stop_id: int, arrival_time_at_stop: int
    ):
        Passenger._id_counter += 1
        self.id = Passenger._id_counter
        self.origin_stop_id = origin_stop_id
        self.destination_stop_id = destination_stop_id
        self.arrival_time_at_stop = (
            arrival_time_at_stop  
        )
        self.board_time: Optional[int] = None  
        self.alight_time: Optional[int] = None  

    @property
    def wait_time(self) -> Optional[int]:
        if self.board_time is not None and self.arrival_time_at_stop is not None:
            return self.board_time - self.arrival_time_at_stop
        return None

    @property
    def travel_time(self) -> Optional[int]:
        if self.alight_time is not None and self.board_time is not None:
            return self.alight_time - self.board_time
        return None

    @property
    def total_trip_time(self) -> Optional[int]:
        if self.alight_time is not None and self.arrival_time_at_stop is not None:
            return self.alight_time - self.arrival_time_at_stop
        return None

    def __repr__(self) -> str:
        return f"Passenger(ID: {self.id}, From: {self.origin_stop_id}, To: {self.destination_stop_id}, Arrived: {format_time(self.arrival_time_at_stop)})"


class Stop:

    def __init__(self, stop_id: int, name: str):
        self.stop_id = stop_id
        self.name = name
        self.waiting_passengers: collections.deque[Passenger] = collections.deque()
        self.last_bus_arrival_time = -float("inf")

    def add_passenger(self, passenger: Passenger):
        self.waiting_passengers.append(passenger)

    def get_waiting_passengers_count(self) -> int:
        return len(self.waiting_passengers)

    def __repr__(self) -> str:
        return f"Stop(ID: {self.stop_id}, Name: {self.name})"


class Bus:
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
            None  
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

        self.current_route = route
        self.current_direction = "Outbound"
        self.route_index = 0
        self.is_en_route = False  
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
            "AT_STOP",  
        )

    def move_to_next_stop(self, global_simulation_time: int):
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
            ]  
            self.destination_stop_id_on_segment = next_stop_id  

            total_segments = len(self.current_route.stops_ids_in_order) - 1
            if total_segments > 0:
                self.time_to_next_stop = (
                    self.current_route.total_outbound_route_time_minutes
                    / total_segments
                )
            else:
                self.time_to_next_stop = 0

            self.is_en_route = True
            logger.info(
                f"Time {format_time(global_simulation_time)}: Bus {self.bus_id} en route from {self.current_stop_id} to {next_stop_id}. Expected arrival in {self.time_to_next_stop:.1f} minutes."
            )

            self.add_event_to_schedule(
                global_simulation_time,
                f"Departed {self.current_stop_id} towards {next_stop_id}",
                self.current_stop_id,  
                len(self.onboard_passengers),
                0,
                0,
                0,  
                self.current_direction,
                "EN_ROUTE_DEPARTURE",
            )

        else:
            logger.info(
                f"Time {format_time(global_simulation_time)}: Bus {self.bus_id} is already at the end of route {self.current_route.route_id} at {self.current_stop_id} and cannot move further on this route."
            )
            self.is_en_route = False 
            self.time_to_next_stop = 0

    def alight_passengers(self, current_time: int, stop: Stop) -> list[Passenger]:
        
        alighted_this_stop = []
        remaining_onboard = []
        self.passenger_alighted_count = 0

        for passenger in self.onboard_passengers:
            if passenger.destination_stop_id == stop.stop_id:
                passenger.alight_time = current_time
                alighted_this_stop.append(passenger)
                self.passenger_alighted_count += 1
                # logger.info(
                #     f"Time {format_time(current_time)}: Passenger {passenger.id} alighted from Bus {self.bus_id} at {stop.stop_id}."
                # )
            else:
                remaining_onboard.append(passenger)

        self.onboard_passengers = remaining_onboard
        return alighted_this_stop

    def board_passengers(self, current_time: int, stop: Stop) -> int:

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
                # logger.info(
                #     f"Time {format_time(current_time)}: Passenger {passenger.id} boarded Bus {self.bus_id} at {stop.stop_id} (Dest: {passenger.destination_stop_id})."
                # )
            else:
                passengers_to_requeue.append(passenger)

        stop.waiting_passengers.extendleft(reversed(passengers_to_requeue))
        self.passenger_boarded_count = boarded_count
        return boarded_count


class SimRoute:

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
    ):  
        self.stops = {
            k: copy.deepcopy(v) for k, v in stops.items()
        }  
        self.routes = routes
        self.buses = buses
        self.all_raw_demands = all_raw_demands
        self.start_time_minutes = start_time_minutes
        self.end_time_minutes = end_time_minutes
        self.config = config
        self.stop_points_data = stop_points_data  
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
            # logger.debug(
            #     f"Optimizer: Rush hour detected at {format_time(current_time_minutes)}. Applying traffic factor {traffic_factor}."
            # )

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
                            )  
                    else:
                        temp_passengers_not_boarded_at_this_stop.append(
                            p_sim
                        )  

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
        temp_stops_with_passengers = {
            stop_id: collections.deque() for stop_id in self.stops.keys()
        }

        sorted_initial_demands = sorted(
            list(self.all_raw_demands), key=lambda p: p["arrival_time"]
        )

        optimizer_served_passenger_ids = set()

        while True:
            earliest_available_time = min(self.bus_availability.values())

            newly_arrived_demands = []
            while (
                sorted_initial_demands
                and sorted_initial_demands[0]["arrival_time"] <= earliest_available_time
            ):
                demand = sorted_initial_demands.pop(0)  
                for _ in range(int(demand["count"])):
                    p = Passenger(
                        demand["origin"], demand["destination"], demand["arrival_time"]
                    )
                    if p.id not in optimizer_served_passenger_ids:
                        temp_stops_with_passengers[demand["origin"]].append(p)
                newly_arrived_demands.append(demand)

            next_bus_id_to_schedule = None
            earliest_bus_available_time = (
                self.end_time_minutes + 1
            )  

            for bus_id, availability_time in self.bus_availability.items():
                if availability_time <= earliest_bus_available_time:
                    earliest_bus_available_time = availability_time
                    next_bus_id_to_schedule = bus_id

            if (
                next_bus_id_to_schedule is None
                or earliest_bus_available_time > self.end_time_minutes
            ):
                break

            bus_obj = self.buses[next_bus_id_to_schedule]
            current_bus_location = self.bus_current_locations[next_bus_id_to_schedule]

            best_trip_score = -1  
            best_trip_details = None
            actual_served_passengers_for_best_trip = []  

            for route_id, route_obj in self.routes.items():
                if not route_obj.stops_ids_in_order:
                    continue  

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
                    self.bus_availability[next_bus_id_to_schedule] = (
                        self.end_time_minutes + 1
                    )
            else:
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
        self.stop_points_data = {}  

        self.bus_schedules_planned = collections.defaultdict(list)
        self.initial_bus_schedules_for_db_save = collections.defaultdict(
            list
        ) 

        self.pending_passengers: collections.deque[Passenger] = collections.deque()
        self.completed_passengers: list[Passenger] = []

        self.cumulative_stop_data = collections.defaultdict(
            lambda: {
                "arrived": 0,
                "boarded": 0,
                "alighted": 0,
                "name": "", 
            }
        )

        self._load_data_from_db()
        self._prepare_initial_passengers()
        self._initialize_buses()
        self._plan_schedules()

        self._perform_initial_bus_positioning()

    def _load_default_config(self) -> dict:
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
            "scheduling_interval_minutes": 5,  
        }

    def _load_data_from_db(self):
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
        }  
        for sp in db_stop_points:
            self.cumulative_stop_data[sp.atco_code]["name"] = sp.name

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
                ) 
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
        stop_point = (
            self.db.query(StopPoint)
            .filter(StopPoint.stop_area_code == stop_area_code)
            .first()
        )
        return stop_point.atco_code if stop_point else None

    def _initialize_buses(self):
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
        
        estimated_demand = collections.defaultdict(lambda: collections.defaultdict(int))
        for demand_record in self.all_raw_demands:
            origin_id = demand_record["origin"]
            arrival_time = demand_record["arrival_time"]
            count = demand_record["count"]
            if self.start_time_minutes <= arrival_time <= self.end_time_minutes:
                estimated_demand[origin_id][arrival_time] += count
        return estimated_demand

    def _generate_random_schedules(self):
        logger.info("Generating demand-aware random schedules for buses...")

        _ = self.config["min_trips_per_bus"]
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
        while (
            self.pending_passengers
            and self.pending_passengers[0].arrival_time_at_stop <= current_time
        ):
            passenger = self.pending_passengers.popleft()
            if passenger.origin_stop_id in self.stops:
                self.stops[passenger.origin_stop_id].add_passenger(passenger)
                self.cumulative_stop_data[passenger.origin_stop_id]["arrived"] += 1
                # logger.debug(
                #     f"Time {format_time(current_time)}: Passenger {passenger.id} dynamically arrived at stop {passenger.origin_stop_id}."
                # )
            else:
                logger.warning(
                    f"Passenger {passenger.id} requested arrival at unknown stop {passenger.origin_stop_id}. Skipping."
                )

    def _get_stop_by_id(self, stop_id: int) -> Optional[Stop]:
        return self.stops.get(stop_id)

    def _get_route_by_id(self, route_id: int) -> Optional["SimRoute"]:
        return self.routes.get(route_id)

    def _return_bus_to_depot(self, bus: "Bus", global_simulation_time: int):
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
        logger.warning(
            "Performing aggressive cleanup of old VehicleJourneys, JourneyPatterns, and Blocks for debugging."
        )
        self.db.query(VehicleJourney).delete()
        self.db.query(JourneyPattern).delete()
        self.db.query(Block).delete()
        self.db.commit()
        logger.warning("Aggressive cleanup complete.")

        default_operator = (
            self.db.query(Operator).filter_by(operator_code="DEFAULT").first()
        )
        if not default_operator:
            default_operator = Operator(
                operator_code="DEFAULT", name="Default Operator"
            )
            self.db.add(default_operator)
            self.db.flush()  
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
                self.db.flush()  
                logger.debug(f"Generated jp_code: {jp_code}")
                logger.debug(f"Created JourneyPattern: {assigned_jp.jp_id}")

                block_name = f"EMU_BLOCK_{sim_bus_id}_R{route_id}_T{departure_time_minutes}_{simulation_run_timestamp}"
                assigned_block = Block(
                    name=block_name,
                    operator_id=default_operator.operator_id,
                    bus_type_id=assigned_db_bus.bus_type_id,  
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
                )
                new_vj.assigned_bus_obj = assigned_db_bus

                self.db.add(new_vj)
                self.db.flush()
                logger.info(
                    f"Saved generated VehicleJourney for Bus {sim_bus_id} (DB Reg: {assigned_db_bus.bus_id}) on Route {route_id} at {format_time(departure_time_minutes)}. VJ ID: {new_vj.vj_id}"
                )
                total_vjs_saved += 1
                logger.debug(
                    f"total_vjs_saved incremented to: {total_vjs_saved}"
                )  

        logger.debug(
            f"Final total_vjs_saved before commit/rollback: {total_vjs_saved}"
        )  
        if total_vjs_saved > 0:
            self.db.commit()
            logger.info(
                f"Emulator-generated schedule saved to database successfully. Total {total_vjs_saved} new VJs added."
            )
        else:
            self.db.rollback()  
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

                        if (
                            bus.current_route
                            and bus.destination_stop_id_on_segment is not None
                        ):
                            bus.current_stop_id = bus.destination_stop_id_on_segment
                            bus.destination_stop_id_on_segment = None  
                        else:
                            logger.error(
                                f"Bus {bus.bus_id} arrived but its destination_stop_id_on_segment was not set or current_route is missing. Halting its movement."
                            )
                            bus.is_en_route = False  
                            continue  

                        bus.is_en_route = False  

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


                    else:  
                        continue  

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
                    if bus.current_stop_id == bus.current_route.stops_ids_in_order[-1]:
                        logger.info(
                            f"Time {format_time(self.current_time)}: Bus {bus.bus_id} completed route {bus.current_route.route_id} at {bus.current_stop_id}."
                        )
                        bus.current_route = None  
                    else:
                        bus.move_to_next_stop(self.current_time)
                        continue  
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
                            continue  

                    # If at the starting stop of the next scheduled trip and it's time to depart
                    if (
                        bus.current_stop_id == next_route.stops_ids_in_order[0]
                        and self.current_time >= scheduled_departure_time
                    ):
                        bus.current_time = (
                            self.current_time
                        )  
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
                        )  
                        continue  

                    elif (
                        bus.current_stop_id == next_route.stops_ids_in_order[0]
                        and self.current_time < scheduled_departure_time
                    ):
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
                        continue  

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
        self._calculate_passenger_metrics()  
        self._report_cumulative_stop_data()  

        if not self.use_optimized_schedule:
            self._save_emulator_schedule_to_db()

        return self.check_bus_return_to_start()
