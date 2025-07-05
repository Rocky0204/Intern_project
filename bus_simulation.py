import collections
import logging
import random
from datetime import datetime, timedelta
import pandas as pd
from typing import Optional # Import Optional for type hinting
import copy # Import copy module for deepcopy
import math # Import math for haversine distance calculation

# --- Database Imports ---
from sqlalchemy.orm import Session, joinedload
from api.database import get_db # Assuming get_db provides a session context manager
from api.models import (
    StopPoint,
    StopArea,
    BusType,
    Bus as DBBus, # Renamed to avoid conflict with simulation's Bus class
    Demand,
    Route, # This is the SQLAlchemy ORM model for Route
    RouteDefinition,
    JourneyPattern,
    VehicleJourney,
    Block,
    Operator,
    Line,
    Service,
    Garage # Needed for bus depot info
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

    a = math.sin(dlat / 2)**2 + math.cos(lat1_rad) * math.cos(lat2_rad) * math.sin(dlon / 2)**2
    c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))

    distance = R * c
    return distance

def is_rush_hour(current_minutes_from_midnight: int) -> bool:
    """
    Determines if the current time falls within defined rush hour periods.
    Morning rush: 07:00 - 09:00 (420-540 minutes)
    Evening rush: 16:00 - 18:00 (960-1080 minutes)
    """
    # Convert minutes from midnight to a readable time for logging if needed
    # current_time_str = format_time(current_minutes_from_midnight)
    
    # Morning rush hour: 7:00 AM (420 min) to 9:00 AM (540 min)
    morning_rush_start = 7 * 60
    morning_rush_end = 9 * 60

    # Evening rush hour: 4:00 PM (960 min) to 6:00 PM (1080 min)
    evening_rush_start = 16 * 60
    evening_rush_end = 18 * 60

    if (morning_rush_start <= current_minutes_from_midnight <= morning_rush_end) or \
       (evening_rush_start <= current_minutes_from_midnight <= evening_rush_end):
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

    def __init__(self, origin_stop_id: int, destination_stop_id: int, arrival_time_at_stop: int):
        Passenger._id_counter += 1
        self.id = Passenger._id_counter
        self.origin_stop_id = origin_stop_id
        self.destination_stop_id = destination_stop_id
        self.arrival_time_at_stop = arrival_time_at_stop # Time they arrive at their origin stop
        self.board_time: Optional[int] = None # Time they board a bus
        self.alight_time: Optional[int] = None # Time they alight from a bus

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
        # A deque is used for waiting passengers to efficiently add/remove from ends
        self.waiting_passengers: collections.deque[Passenger] = collections.deque()
        self.last_bus_arrival_time = -float("inf") # Track last bus arrival for metrics (not currently used extensively)

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
        bus_id: str, # Simulator's internal bus ID (e.g., 'S1', 'L2')
        capacity: int,
        depot_stop_id: int,
        initial_internal_time: int,
        overcrowding_factor: float = 1.0,
        db_registration: Optional[str] = None # Original DB registration for mapping
    ):
        self.bus_id = bus_id
        self.capacity = capacity
        self.overcrowding_factor = overcrowding_factor
        self.onboard_passengers: list[Passenger] = [] # Stores Passenger objects
        self.current_stop_id = depot_stop_id
        self.initial_start_point = depot_stop_id
        self.current_route = None # The SimRoute object the bus is currently on
        self.route_index = 0 # Current index within the current_route's stops_ids_in_order
        self.schedule = [] # List of tuples representing bus events for logging/analysis
        self.current_time = initial_internal_time # Internal clock of the bus
        self.db_registration = db_registration # Original DB registration for mapping

        # Initial event: Bus is at depot
        self.add_event_to_schedule(
            self.current_time,
            f"Bus {self.bus_id} initialized at depot",
            depot_stop_id,
            0, 0, 0, 0, "N/A", "INITIALIZED",
        )
        self.miles_traveled = 0 # Not fully implemented for calculation, but can be extended
        self.fuel_consumed = 0  # Not fully implemented for calculation, but can be extended
        self.is_en_route = False # True if bus is moving between stops
        self.time_to_next_stop = 0 # Remaining time (minutes) until next stop
        self.total_route_duration_minutes = 0 # Duration of the current full route
        self.passenger_boarded_count = 0 # Passengers boarded in current stop interaction
        self.passenger_alighted_count = 0 # Passengers alighted in current stop interaction
        self.last_stop_arrival_time = self.current_time # Time of last arrival at a stop

    def add_event_to_schedule(
        self,
        time: int,
        event_description: str,
        stop_id: Optional[int], # Stop ID can be None if en-route
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

    def start_route(self, route: 'SimRoute', global_simulation_time: int): # Changed type hint to SimRoute
        """
        Initiates a bus on a new route.
        """
        self.current_route = route
        self.current_direction = "Outbound" # Assuming single direction for simplicity
        self.route_index = 0 # Reset to the start of the route
        self.is_en_route = False # Bus is at the start stop, not yet moving
        self.time_to_next_stop = 0 # Will be calculated by move_to_next_stop
        self.total_route_duration_minutes = route.total_outbound_route_time_minutes
        self.last_stop_arrival_time = global_simulation_time
        # Record event of starting the route
        self.add_event_to_schedule(
            global_simulation_time,
            f"Start Route {route.route_id}",
            self.current_stop_id,
            len(self.onboard_passengers),
            0, # Waiting passengers at start of route is handled by emulator
            0, # Boarded/Alighted count at start of route is handled by emulator
            0,
            "N/A",
            "AT_STOP", # Status is "AT_STOP" as it's at the departure stop
        )

    def move_to_next_stop(self, global_simulation_time: int):
        """
        Calculates the time to the next stop and updates bus state to 'en route'.
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
            logger.error(f"Bus {self.bus_id} is at {self.current_stop_id} which is not on its current route {self.current_route.route_id}. Cannot move to next stop.")
            self.is_en_route = False
            return

        if current_stop_idx < len(self.current_route.stops_ids_in_order) - 1:
            next_stop_idx = current_stop_idx + 1
            next_stop_id = self.current_route.stops_ids_in_order[next_stop_idx]

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

            for i in range(1, int(self.time_to_next_stop) + 1):
                self.add_event_to_schedule(
                    global_simulation_time + i,
                    f"En Route (Remaining: {self.time_to_next_stop - i:.1f} min)",
                    None, # No specific stop ID when en route
                    len(self.onboard_passengers),
                    0, 0, 0,
                    self.current_direction,
                    "EN_ROUTE",
                )

        else:
            logger.info(
                f"Time {format_time(global_simulation_time)}: Bus {self.bus_id} reached end of route {self.current_route.route_id} at {self.current_stop_id}."
            )
            self.is_en_route = False
            self.time_to_next_stop = 0
            self.current_route = None


    def alight_passengers(self, current_time: int, stop: Stop) -> list[Passenger]:
        """
        Simulates passengers alighting from the bus at the current stop.
        Returns a list of alighted Passenger objects.
        """
        alighted_this_stop = []
        remaining_onboard = []
        self.passenger_alighted_count = 0 # Reset for current stop interaction

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
            # Determine which stops are still ahead on the current route
            try:
                current_stop_idx = self.current_route.stops_ids_in_order.index(
                    self.current_stop_id
                )
                remaining_stops_on_current_route = self.current_route.stops_ids_in_order[
                    current_stop_idx + 1 :
                ]
            except ValueError:
                logger.warning(f"Bus {self.bus_id} current stop {self.current_stop_id} not found in its current route {self.current_route.route_id}. Cannot determine remaining stops for boarding.")
                remaining_stops_on_current_route = []


        passengers_to_requeue = collections.deque() # Passengers who cannot board or are not going on this route
        while (
            stop.waiting_passengers # Are there passengers waiting?
            and len(self.onboard_passengers) < max_onboard_with_overcrowding # Is there space on the bus?
        ):
            passenger = stop.waiting_passengers.popleft()

            # Check if the bus's current route goes to the passenger's destination
            if passenger.destination_stop_id in remaining_stops_on_current_route:
                passenger.board_time = current_time
                self.onboard_passengers.append(passenger)
                boarded_count += 1
                logger.info(
                    f"Time {format_time(current_time)}: Passenger {passenger.id} boarded Bus {self.bus_id} at {stop.stop_id} (Dest: {passenger.destination_stop_id})."
                )
            else:
                # Passenger cannot board this bus (wrong direction/destination)
                passengers_to_requeue.append(passenger)

        # Return passengers who couldn't board to the front of the queue
        stop.waiting_passengers.extendleft(reversed(passengers_to_requeue))
        self.passenger_boarded_count = boarded_count # Update for event logging
        return boarded_count


class SimRoute: # Renamed from Route to SimRoute
    """
    Represents a bus route with its sequence of stops and total travel time.
    This is the simulation's internal representation of a route.
    """
    def __init__(self, route_id: int, stops_ids_in_order: list[int], total_outbound_route_time_minutes: int):
        self.route_id = route_id
        self.stops_ids_in_order = stops_ids_in_order
        self.total_outbound_route_time_minutes = total_outbound_route_time_minutes
        
        # Calculate average time per segment
        if len(stops_ids_in_order) > 1:
            self.segment_time = total_outbound_route_time_minutes / (
                len(stops_ids_in_order) - 1
            )
        else:
            self.segment_time = 0 # No segments if only one stop

    def __repr__(self) -> str:
        return f"SimRoute(ID: {self.route_id}, Stops: {self.stops_ids_in_order})"


class BusEmulator:
    """
    The main simulation engine for bus operations.
    Manages buses, stops, passengers, and time progression.
    Can run based on an optimized schedule or generate its own.
    Directly connects to the database for data loading and saving.
    """
    def __init__(
        self,
        db: Session, # Direct database session
        use_optimized_schedule: bool = False, # Flag to use optimized schedule
        start_time_minutes: int = 0,
        end_time_minutes: int = 1440,
        config: Optional[dict] = None # New: Allow external configuration
    ):
        self.db = db # Store the database session
        self.use_optimized_schedule = use_optimized_schedule
        self.current_time = start_time_minutes
        self.total_passengers_completed_trip = 0
        self.total_passengers_waiting = 0

        self.start_time_minutes = start_time_minutes
        self.end_time_minutes = end_time_minutes

        self.config = config or self._load_default_config() # Load configuration

        # Data loaded from DB
        self.stops = {} # Dict of Stop objects
        self.routes = {} # Dict of SimRoute objects
        self.all_raw_demands = [] # List of raw demand data from DB
        self.buses = {} # Dict of Bus objects (simulation instances)
        self.bus_types_map = {} # Map type_id to BusType object (DB model)
        self.db_buses_map = {} # Map db_registration to DBBus object (DB model)

        self.bus_schedules_planned = collections.defaultdict(list) # Stores planned trips for each sim Bus
        # New attribute to store the initial schedules for database saving
        self.initial_bus_schedules_for_db_save = collections.defaultdict(list)

        # Passenger tracking for dynamic demand and metrics
        self.pending_passengers: collections.deque[Passenger] = collections.deque() # Passengers not yet arrived at their origin stop
        self.completed_passengers: list[Passenger] = [] # Passengers who have completed their trip

        self._load_data_from_db() # Load all initial data from database
        self._prepare_initial_passengers() # Distribute initial passengers and populate pending_passengers
        self._initialize_buses() # Create Bus objects based on loaded data
        self._plan_schedules() # Generate or load schedules

        self._perform_initial_bus_positioning() # Position buses for their first scheduled trip

    def _load_default_config(self) -> dict:
        """
        Loads default configuration values. These can be overridden by a 'config' DB table
        or by parameters passed to the constructor.
        """
        # Attempt to load from DB first if a config table existed, otherwise use hardcoded defaults
        # For now, we'll use hardcoded defaults as a placeholder.
        # In a real scenario, you'd query a 'Configuration' table here.
        logger.info("Loading default simulation configuration.")
        return {
            "avg_stop_time_per_passenger": 3,
            "passenger_wait_threshold": 1,
            "dead_run_travel_rate_km_per_hour": 30, # Average speed for dead runs
            "overcrowding_factor": 1.2, # Added to config
            "min_trips_per_bus": 2, # Added to config
            "max_trips_per_bus": 5, # Added to config
            "min_layover_minutes": 5, # Added to config
            "max_layover_minutes": 15, # Added to config
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
            logger.error("No stop points found in DB. Simulation cannot proceed without stops.")
            raise ValueError("No stop points in database.") # Critical error, halt simulation
        self.stops = {sp.atco_code: Stop(sp.atco_code, sp.name) for sp in db_stop_points}
        
        # Ensure default depot exists and is a valid stop point
        self.default_depot_id = db_stop_points[0].atco_code if db_stop_points else None
        if self.default_depot_id is None or self.default_depot_id not in self.stops:
            logger.error("No valid default depot stop point found. Simulation cannot proceed.")
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
        db_buses = self.db.query(DBBus).options(joinedload(DBBus.bus_type), joinedload(DBBus.garage)).all()
        if not db_buses:
            logger.error("No individual buses found in DB. Cannot run simulation.")
            raise ValueError("No individual buses in database.")
        self.db_buses_map = {db_bus.bus_id: db_bus for db_bus in db_buses} # Map registration to DBBus object
        logger.info(f"Loaded {len(self.db_buses_map)} individual buses.")

        # 4. Load Routes and Route Definitions with joinedload for efficiency
        db_routes = self.db.query(Route).options(
            joinedload(Route.route_definitions).joinedload(RouteDefinition.stop_point)
        ).all()
        if not db_routes:
            logger.warning("No routes found in DB. Simulation will not schedule trips.")
        
        # Create SimRoute objects from DB Route data
        for route_db in db_routes:
            stops_ids_in_order = []
            total_outbound_route_time_minutes = 0
            
            # Ensure route_definitions exist and are sorted
            sorted_route_defs = sorted(route_db.route_definitions, key=lambda rd: rd.sequence)
            if not sorted_route_defs:
                logger.warning(f"Route {route_db.route_id} has no defined stop points. Skipping this route.")
                continue

            for i, rd in enumerate(sorted_route_defs):
                if rd.stop_point: # Ensure stop_point object is not None
                    stops_ids_in_order.append(rd.stop_point.atco_code)
                else:
                    logger.warning(f"Route Definition {rd.route_def_id} for Route {route_db.route_id} has a missing StopPoint. Skipping this route definition.")
                    continue

                # Simple segment time calculation; could be enhanced with actual distances
                if i < len(sorted_route_defs) - 1:
                    segment_travel_time = 5 # Default 5 minutes per segment
                    total_outbound_route_time_minutes += segment_travel_time
            
            # Only add route if it has at least two stops to form a segment
            if len(stops_ids_in_order) > 1:
                self.routes[route_db.route_id] = SimRoute( # Use SimRoute here
                    route_db.route_id,
                    stops_ids_in_order,
                    total_outbound_route_time_minutes
                )
            else:
                logger.warning(f"Route {route_db.route_id} only has {len(stops_ids_in_order)} stop(s). Skipping as it cannot form a valid trip.")

        logger.info(f"Loaded {len(self.routes)} routes and their definitions.")

        # 5. Load Demand Records
        db_demands = self.db.query(Demand).all()
        if not db_demands:
            logger.warning("No demand records found in DB. Simulation will have no passengers.")
        
        self.all_raw_demands = [] # Store raw demands to process dynamically
        for d in db_demands:
            # Need to map StopArea codes to StopPoint ATCO codes for simulation
            origin_sp_id = self._get_stop_area_representative_stop_point(d.origin)
            destination_sp_id = self._get_stop_area_representative_stop_point(d.destination)

            if origin_sp_id is None or destination_sp_id is None:
                logger.warning(f"Demand origin/destination StopArea ({d.origin}, {d.destination}) could not be mapped to StopPoints. Skipping this demand record.")
                continue

            # Basic validation: ensure origin and destination stops exist in our loaded stops
            if origin_sp_id not in self.stops or destination_sp_id not in self.stops:
                logger.warning(f"Demand for {origin_sp_id} to {destination_sp_id} references unknown stops. Skipping.")
                continue

            self.all_raw_demands.append({
                "origin": origin_sp_id,
                "destination": destination_sp_id,
                "count": d.count,
                "arrival_time": d.start_time.hour * 60 + d.start_time.minute
            })
        logger.info(f"Loaded {len(self.all_raw_demands)} demand records.")
        logger.info("Simulation data loading complete.")

    def _get_stop_area_representative_stop_point(self, stop_area_code: int) -> Optional[int]:
        """Helper to find a representative stop point for a given stop area code."""
        # This function needs to query the DB directly since it's now in BusEmulator
        # Consider caching StopArea to StopPoint mappings if this is called frequently
        stop_point = self.db.query(StopPoint).filter(StopPoint.stop_area_code == stop_area_code).first()
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
                logger.error(f"Bus {db_reg} has unknown bus type ID {db_bus_obj.bus_type_id}. Skipping.")
                continue

            sim_bus_id = f"{bus_type.name[0].upper()}{sim_bus_id_counter_by_type[bus_type.name]}"
            sim_bus_id_counter_by_type[bus_type.name] += 1
            
            # Use the default_depot_id for all buses, ensuring it's a valid stop
            depot_id = self.default_depot_id
            if depot_id is None or depot_id not in self.stops:
                logger.error(f"Bus {db_reg} has no suitable depot stop point. Skipping.")
                continue

            self.buses[sim_bus_id] = Bus(
                bus_id=sim_bus_id,
                capacity=bus_type.capacity,
                depot_stop_id=depot_id, # Use the valid default_depot_id
                initial_internal_time=self.start_time_minutes,
                overcrowding_factor=self.config["overcrowding_factor"], # From config
                db_registration=db_reg # Store the original DB registration
            )
        logger.info(f"Initialized {len(self.buses)} simulation Bus objects.")


    def _plan_schedules(self):
        """
        Determines the schedule for each bus: either from optimized VehicleJourneys
        or by generating a random schedule.
        """
        self.bus_schedules_planned = collections.defaultdict(list)

        if self.use_optimized_schedule:
            logger.info("Attempting to load optimized schedules from database...")
            optimized_vjs = self.db.query(VehicleJourney).options(
                joinedload(VehicleJourney.journey_pattern).joinedload(JourneyPattern.route),
                joinedload(VehicleJourney.assigned_bus_obj)
            ).all()

            for vj in optimized_vjs:
                assigned_bus_reg = vj.assigned_bus_obj.bus_id if vj.assigned_bus_obj else None
                
                sim_bus_id = None
                for s_bus_id, s_bus_obj in self.buses.items():
                    if s_bus_obj.db_registration == assigned_bus_reg:
                        sim_bus_id = s_bus_id
                        break
                
                if not sim_bus_id:
                    logger.warning(f"Optimized VJ {vj.vj_id} assigned to DB bus {assigned_bus_reg} could not be mapped to a simulator bus. Skipping this trip.")
                    continue

                route_obj = self.routes.get(vj.journey_pattern.route.route_id)
                if not route_obj:
                    logger.warning(f"Route {vj.journey_pattern.route.route_id} for VJ {vj.vj_id} not found in loaded routes. Skipping.")
                    continue

                self.bus_schedules_planned[sim_bus_id].append(
                    {
                        "route_id": route_obj.route_id,
                        "departure_time_minutes": (vj.departure_time.hour * 60) + vj.departure_time.minute,
                        "layover_duration": 0, # Assuming optimizer includes layover in departure time
                    }
                )
            logger.info(f"Loaded {len(optimized_vjs)} optimized trips into bus schedules.")
            if not optimized_vjs:
                logger.warning("No optimized VehicleJourneys found in DB. Falling back to generated schedules.")
                self._generate_random_schedules() # Fallback if DB is empty for optimized schedules
        else:
            logger.info("Generating random schedules for buses...")
            self._generate_random_schedules() # Generate if not using optimized

        # Store a deep copy of the planned schedules for database saving
        self.initial_bus_schedules_for_db_save = copy.deepcopy(self.bus_schedules_planned)

    def _estimate_future_demand(self) -> collections.defaultdict:
        """
        Estimates future passenger demand at each stop based on raw demand data.
        Returns: defaultdict(lambda: defaultdict(int)) where
                 estimated_demand[stop_id][time_minute] = passenger_count
        """
        estimated_demand = collections.defaultdict(lambda: collections.defaultdict(int))
        for demand in self.all_raw_demands:
            origin_stop_id = demand["origin"]
            arrival_time = demand["arrival_time"]
            count = demand["count"]
            # Distribute demand over a small window around arrival_time
            for t in range(arrival_time - 15, arrival_time + 15): # +/- 15 minutes
                if self.start_time_minutes <= t <= self.end_time_minutes:
                    estimated_demand[origin_stop_id][t] += count
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

        estimated_demand = self._estimate_future_demand() # Pre-calculate demand

        for bus_id, bus in self.buses.items():
            possible_routes = []
            for route_id, route in self.routes.items():
                if route.stops_ids_in_order and route.stops_ids_in_order[0] == bus.initial_start_point:
                    possible_routes.append(route_id)
            
            if not possible_routes:
                logger.error(
                    f"CRITICAL: No suitable route found starting at depot {bus.initial_start_point} for Bus {bus_id}. Cannot schedule this bus."
                )
                continue

            last_trip_end_time = self.start_time_minutes

            for i in range(MAX_TRIPS_PER_BUS): # Try to schedule up to MAX_TRIPS_PER_BUS
                layover_duration = random.randint(MIN_LAYOVER_MINUTES, MAX_LAYOVER_MINUTES)
                min_departure_for_current_trip = last_trip_end_time + layover_duration

                # Calculate the latest possible departure time for this trip
                max_departure_for_current_trip = self.end_time_minutes - (5 * 60) # Leave 5 hours buffer for return to depot

                if min_departure_for_current_trip > max_departure_for_current_trip:
                    logger.debug(f"Bus {bus_id}: No more time slots for additional trips. Breaking after {i} trips.")
                    break

                # --- Demand-aware route selection ---
                candidate_routes_with_demand = []
                for route_id in possible_routes:
                    route_obj = self.routes.get(route_id)
                    if not route_obj:
                        continue
                    start_stop_id = route_obj.stops_ids_in_order[0]

                    # Sum estimated demand around the potential departure time
                    demand_at_start_stop = 0
                    for t_offset in range(-30, 31): # Check demand +/- 30 minutes around min_departure
                        check_time = min_departure_for_current_trip + t_offset
                        if self.start_time_minutes <= check_time <= self.end_time_minutes:
                            demand_at_start_stop += estimated_demand[start_stop_id][check_time]
                    
                    candidate_routes_with_demand.append((demand_at_start_stop, route_id))
                
                # Sort by demand (descending) and pick from top N or use weighted random choice
                candidate_routes_with_demand.sort(key=lambda x: x[0], reverse=True)

                if not candidate_routes_with_demand:
                    logger.debug(f"Bus {bus_id}: No routes with estimated demand found. Breaking.")
                    break

                # Simple selection: pick from the top 3 routes by demand, or if less than 3, pick from available
                top_routes_for_selection = [r[1] for r in candidate_routes_with_demand[:min(3, len(candidate_routes_with_demand))]]
                
                # If all top routes have 0 demand, fall back to random choice from all possible routes
                if all(r[0] == 0 for r in candidate_routes_with_demand[:min(3, len(candidate_routes_with_demand))]):
                    route_id = random.choice(possible_routes)
                else:
                    route_id = random.choice(top_routes_for_selection) # Pick one of the top demand routes

                route_obj = self.routes.get(route_id)
                if not route_obj: # Should not happen if selected from self.routes.keys()
                    continue

                # Generate a random departure time within the valid window
                departure_time_minutes = random.randint(min_departure_for_current_trip, max_departure_for_current_trip)
                
                self.bus_schedules_planned[bus_id].append(
                    {
                        "route_id": route_id,
                        "layover_duration": layover_duration,
                        "departure_time_minutes": departure_time_minutes
                    }
                )
                logger.info(
                    f"Bus {bus_id} scheduled trip {i+1}: Route {route_id}, Departure {format_time(departure_time_minutes)} (Demand-aware)."
                )
                
                last_trip_end_time = departure_time_minutes + route_obj.total_outbound_route_time_minutes

            # Sort the planned schedules by departure time for each bus
            self.bus_schedules_planned[bus_id].sort(key=lambda x: x['departure_time_minutes'])

        logger.info("Demand-aware random bus schedules generated.")


    def _calculate_dead_run_time(self, from_stop_id: int, to_stop_id: int, current_time_minutes: int) -> int:
        """
        Calculates the travel time for a dead run between two stops.
        Considers distance and applies a traffic factor if it's rush hour.
        Returns time in minutes (rounded up).
        """
        from_stop_db = self.db.query(StopPoint).filter_by(atco_code=from_stop_id).first()
        to_stop_db = self.db.query(StopPoint).filter_by(atco_code=to_stop_id).first()

        if not from_stop_db or not to_stop_db:
            logger.error(f"Cannot calculate dead run time: One or both stops ({from_stop_id}, {to_stop_id}) not found in DB.")
            return 5 # Fallback to a default small time

        # Ensure lat/lon are not None before passing to haversine
        if from_stop_db.latitude is None or from_stop_db.longitude is None or \
           to_stop_db.latitude is None or to_stop_db.longitude is None:
            logger.warning(f"Missing lat/lon for stops {from_stop_id} or {to_stop_id}. Using default dead run time.")
            return 5 # Fallback if coordinates are missing

        distance_km = haversine_distance(
            from_stop_db.latitude, from_stop_db.longitude,
            to_stop_db.latitude, to_stop_db.longitude
        )

        base_speed_kmph = self.config["dead_run_travel_rate_km_per_hour"]
        
        # Avoid division by zero
        if base_speed_kmph <= 0:
            logger.error("Dead run speed configured as zero or negative. Using default time.")
            return 5

        # Time = Distance / Speed (in hours)
        time_hours = distance_km / base_speed_kmph
        time_minutes = time_hours * 60

        traffic_factor = 1.0
        if is_rush_hour(current_time_minutes):
            traffic_factor = 1.5 # 50% increase during rush hour
            logger.debug(f"Rush hour detected at {format_time(current_time_minutes)}. Applying traffic factor {traffic_factor}.")

        return max(1, int(math.ceil(time_minutes * traffic_factor))) # Ensure at least 1 minute, round up


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
            route = self.routes.get(route_id) # This is SimRoute

            if not route:
                logger.error(
                    f"Route {route_id} not found for Bus {bus_id}'s first scheduled trip. Cannot position bus."
                )
                continue

            first_route_stop_id = route.stops_ids_in_order[0]

            scheduled_departure_time = first_schedule_entry.get("departure_time_minutes", self.start_time_minutes)

            if bus.current_stop_id != first_route_stop_id:
                # Calculate dead run time dynamically
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
                        0, 0, 0,
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
                    0, 0,
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
                        0, 0,
                        "N/A",
                        "READY_AT_START",
                    )
                logger.info(
                    f"Bus {bus_id} is already at its first route stop {first_route_stop_id} and ready for departure at {format_time(bus.current_time)}."
                )

            self.bus_schedules_planned[bus_id][0]["departure_time_minutes"] = bus.current_time
            
    def _prepare_initial_passengers(self):
        """
        Initializes passenger objects from raw demand data and populates
        either `self.stops[].waiting_passengers` for immediate arrivals
        or `self.pending_passengers` for future arrivals.
        """
        # Reset Passenger ID counter for consistent runs
        Passenger._id_counter = 0 
        
        all_arrival_times = []
        
        # Create Passenger objects from all raw demands
        for demand in self.all_raw_demands:
            origin_stop_id = demand["origin"]
            destination_stop_id = demand["destination"]
            count = int(demand["count"]) # Ensure count is an integer
            passenger_arrival_time = demand.get("arrival_time")

            if passenger_arrival_time is None:
                logger.warning(f"Demand missing 'arrival_time': {demand}. Skipping.")
                continue

            if origin_stop_id not in self.stops:
                logger.warning(f"Origin stop {origin_stop_id} for demand not found in loaded stops. Skipping demand.")
                continue
            if destination_stop_id not in self.stops:
                logger.warning(f"Destination stop {destination_stop_id} for demand not found in loaded stops. Skipping demand.")
                continue

            for _ in range(count):
                new_passenger = Passenger(origin_stop_id, destination_stop_id, passenger_arrival_time)
                # If passenger arrives at or before simulation start, add to stop directly
                if new_passenger.arrival_time_at_stop <= self.start_time_minutes:
                    self.stops[origin_stop_id].add_passenger(new_passenger)
                else:
                    # Otherwise, add to pending passengers for dynamic generation
                    self.pending_passengers.append(new_passenger)
                all_arrival_times.append(new_passenger.arrival_time_at_stop)

        # Sort pending passengers by arrival time for efficient processing
        self.pending_passengers = collections.deque(sorted(list(self.pending_passengers), key=lambda p: p.arrival_time_at_stop))

        logger.info(f"Prepared {Passenger._id_counter} initial passenger demands.")

        if (
            self.start_time_minutes == 0
            and self.end_time_minutes == 1440
            and all_arrival_times
        ):
            self.start_time_minutes = min(all_arrival_times)
            self.end_time_minutes = max(all_arrival_times) + 120 # Give 2 hours buffer
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
        while self.pending_passengers and self.pending_passengers[0].arrival_time_at_stop <= current_time:
            passenger = self.pending_passengers.popleft()
            if passenger.origin_stop_id in self.stops:
                self.stops[passenger.origin_stop_id].add_passenger(passenger)
                logger.debug(f"Time {format_time(current_time)}: Passenger {passenger.id} dynamically arrived at stop {passenger.origin_stop_id}.")
            else:
                logger.warning(f"Passenger {passenger.id} requested arrival at unknown stop {passenger.origin_stop_id}. Skipping.")


    def _get_stop_by_id(self, stop_id: int) -> Optional[Stop]:
        """Helper to retrieve a Stop object by its ID."""
        return self.stops.get(stop_id)

    def _get_route_by_id(self, route_id: int) -> Optional['SimRoute']: # Changed type hint to SimRoute
        """Helper to retrieve a SimRoute object by its ID."""
        return self.routes.get(route_id)

    def _return_bus_to_depot(self, bus: 'Bus', global_simulation_time: int):
        """
        Simulates a bus instantly returning to its initial depot after all trips are done.
        This is a simplified "teleport" for reporting purposes.
        """
        if bus.current_stop_id == bus.initial_start_point:
            logger.info(f"Bus {bus.bus_id} is already at its depot {bus.initial_start_point}.")
            return

        logger.info(f"Time {format_time(global_simulation_time)}: Bus {bus.bus_id} returning to depot {bus.initial_start_point}.")

        # Update bus's final position and time
        bus.current_time = global_simulation_time
        bus.current_stop_id = bus.initial_start_point
        bus.is_en_route = False # Bus is now at depot

        bus.add_event_to_schedule(
            bus.current_time,
            f"Returned to Depot {bus.initial_start_point}",
            bus.initial_start_point,
            len(bus.onboard_passengers),
            0, 0, 0, # No passengers waiting/boarding/alighting at depot
            "N/A",
            "AT_DEPOT",
        )
        logger.info(f"Time {format_time(bus.current_time)}: Bus {bus.bus_id} successfully returned to depot {bus.initial_start_point}.")


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

            # --- 2. Process buses ---
            for bus_id, bus in self.buses.items():
                if bus.current_time > self.current_time:
                    continue

                # If bus is at a stop (not en-route)
                if not bus.is_en_route:
                    current_stop = self._get_stop_by_id(bus.current_stop_id)
                    if not current_stop:
                        logger.error(f"Bus {bus.bus_id} is at an unknown stop ID: {bus.current_stop_id}. Halting its simulation.")
                        continue

                    # Alight passengers first, as soon as bus is at a stop
                    alighted_passengers = bus.alight_passengers(self.current_time, current_stop)
                    self.completed_passengers.extend(alighted_passengers)
                    
                    # Check for next scheduled trip for this bus
                    if self.bus_schedules_planned.get(bus_id):
                        next_scheduled_trip = self.bus_schedules_planned[bus_id][0]
                        scheduled_departure_time = next_scheduled_trip["departure_time_minutes"]
                        next_route_id = next_scheduled_trip["route_id"]
                        next_route = self._get_route_by_id(next_route_id)

                        if not next_route:
                            logger.error(f"Scheduled route {next_route_id} not found for Bus {bus_id}. Skipping its future trips.")
                            self.bus_schedules_planned[bus_id].pop(0)
                            continue

                        # Handle dead run to next trip's start point if necessary
                        if bus.current_stop_id != next_route.stops_ids_in_order[0]:
                            target_stop_id = next_route.stops_ids_in_order[0]
                            
                            # Calculate dead run time dynamically
                            dead_run_duration = self._calculate_dead_run_time(
                                bus.current_stop_id, target_stop_id, self.current_time
                            )

                            dead_run_arrival_time = self.current_time + dead_run_duration
                            
                            # Ensure bus is ready at the next departure point by its scheduled time
                            final_arrival_at_next_start_point = max(dead_run_arrival_time, scheduled_departure_time)

                            # Simulate dead run progression and log events
                            for t in range(self.current_time + 1, final_arrival_at_next_start_point + 1):
                                bus.add_event_to_schedule(
                                    t,
                                    f"Dead Run En Route from {bus.current_stop_id} to {target_stop_id}",
                                    None, # No specific stop ID when en route
                                    len(bus.onboard_passengers),
                                    0, 0, 0,
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
                                0, 0,
                                "N/A",
                                "AT_STOP_DEAD_RUN",
                            )
                            logger.info(
                                f"Bus {bus_id} dead ran from {current_stop.stop_id} to {target_stop_id}, arriving at {format_time(bus.current_time)} for next trip."
                            )
                            # After dead run, if it's still before scheduled departure, bus waits.
                            # The boarding will happen when the departure time is met.
                            if bus.current_time < scheduled_departure_time:
                                if not bus.schedule or bus.schedule[-1][8] != "WAITING":
                                    bus.add_event_to_schedule(self.current_time, f"Waiting for scheduled departure ({format_time(scheduled_departure_time)})", bus.current_stop_id, len(bus.onboard_passengers), current_stop.get_waiting_passengers_count(), 0, 0, "N/A", "WAITING")
                                continue # Skip to next bus, as this one is waiting or just arrived from dead run

                        # If bus is at the correct stop for the next scheduled trip and it's time to depart
                        if bus.current_stop_id == next_route.stops_ids_in_order[0] and self.current_time >= scheduled_departure_time:
                            bus.current_time = self.current_time
                            bus.start_route(next_route, self.current_time) # Set current_route HERE
                            logger.info(f"Time {format_time(self.current_time)}: Bus {bus_id} started scheduled trip on Route {next_route_id}.")
                            self.bus_schedules_planned[bus_id].pop(0)

                            # NOW that current_route is set, passengers can board for this trip
                            boarded_count = bus.board_passengers(self.current_time, current_stop)
                            if bus.schedule: # Update the 'start route' event with boarding info
                                last_event = list(bus.schedule[-1])
                                last_event[3] = len(bus.onboard_passengers)
                                last_event[4] = current_stop.get_waiting_passengers_count()
                                last_event[5] = boarded_count
                                bus.schedule[-1] = tuple(last_event)

                            bus.move_to_next_stop(self.current_time)
                            continue # Bus has processed its action for this minute, move to next bus

                        elif bus.current_stop_id == next_route.stops_ids_in_order[0] and self.current_time < scheduled_departure_time:
                            # Bus is at the correct start stop but waiting for scheduled departure
                            if not bus.schedule or bus.schedule[-1][8] != "WAITING":
                                bus.add_event_to_schedule(self.current_time, f"Waiting for scheduled departure ({format_time(scheduled_departure_time)})", bus.current_stop_id, len(bus.onboard_passengers), current_stop.get_waiting_passengers_count(), 0, 0, "N/A", "WAITING")
                            continue # Bus is waiting, move to next bus
                    
                    # If no more scheduled trips for this bus AND it's not en-route, mark as idle or return to depot
                    elif not self.bus_schedules_planned.get(bus_id) and not bus.is_en_route:
                        if bus.current_stop_id != bus.initial_start_point:
                            self._return_bus_to_depot(bus, self.current_time)
                        elif bus.schedule and bus.schedule[-1][8] != "IDLE":
                            bus.add_event_to_schedule(
                                self.current_time, "Idle", bus.current_stop_id,
                                len(bus.onboard_passengers), current_stop.get_waiting_passengers_count(),
                                0, 0, "N/A", "IDLE"
                            )
                        continue # Bus is idle, move to next bus

                # If bus is en-route, advance its position
                elif bus.is_en_route:
                    bus.time_to_next_stop -= 1
                    if bus.time_to_next_stop <= 0:
                        # Bus arrived at next stop (intermediate or end of route)
                        bus.current_time = self.current_time # Sync bus time
                        
                        # Determine the stop it just arrived at (which is the next stop in the sequence)
                        # bus.current_stop_id still holds the *previous* stop at this point.
                        # We need to find the actual next stop in the route sequence.
                        try:
                            prev_stop_idx = bus.current_route.stops_ids_in_order.index(bus.current_stop_id)
                            next_stop_id_actual = bus.current_route.stops_ids_in_order[prev_stop_idx + 1]
                        except (ValueError, IndexError):
                            logger.error(f"Bus {bus.bus_id} route logic error: current_stop_id {bus.current_stop_id} not found or no next stop in route {bus.current_route.route_id}. Halting movement.")
                            bus.is_en_route = False
                            bus.time_to_next_stop = 0
                            continue # Skip to next bus

                        bus.current_stop_id = next_stop_id_actual # Update bus's location to the newly arrived stop
                        bus.is_en_route = False
                        bus.time_to_next_stop = 0

                        current_stop_obj = self._get_stop_by_id(bus.current_stop_id) # Get the actual stop object for metrics
                        current_stop_passengers_waiting = current_stop_obj.get_waiting_passengers_count() if current_stop_obj else 0

                        # Log arrival event
                        bus.add_event_to_schedule(
                            self.current_time,
                            f"Arrived At Stop {bus.current_stop_id}", # More specific description
                            bus.current_stop_id,
                            len(bus.onboard_passengers),
                            current_stop_passengers_waiting,
                            0, 0, # Boarded/Alighted will be handled in the next iteration when not en-route
                            bus.current_direction,
                            "AT_STOP",
                        )
                        logger.info(f"Time {format_time(self.current_time)}: Bus {bus.bus_id} arrived at {bus.current_stop_id}.")

                        # If this was the end of the route
                        if bus.current_route and bus.current_stop_id == bus.current_route.stops_ids_in_order[-1]: # Ensure current_route is not None
                            logger.info(f"Time {format_time(self.current_time)}: Bus {bus.bus_id} completed route {bus.current_route.route_id} at {bus.current_stop_id}.")
                            bus.current_route = None # Clear current route after completion
                            # After completing a route, if no more trips, return to depot
                            if not self.bus_schedules_planned.get(bus_id):
                                if bus.current_stop_id != bus.initial_start_point:
                                    self._return_bus_to_depot(bus, self.current_time)
                                else:
                                    if bus.schedule and bus.schedule[-1][8] != "AT_DEPOT_FINAL":
                                        bus.add_event_to_schedule(self.current_time, "Final stop at depot", bus.current_stop_id, len(bus.onboard_passengers), 0, 0, 0, "N/A", "AT_DEPOT_FINAL")
                                        logger.info(f"Time {format_time(self.current_time)}: Bus {bus.bus_id} finished all trips and is at depot.")
                        
                        # Now that the bus is at a stop, it will be processed in the next minute's loop
                        # for alighting/boarding and starting the next trip.
                        continue # Process next bus, this bus is done for this minute's movement.

                    else:
                        # Bus is still en-route, just continue decrementing time_to_next_stop
                        pass # No action needed, event already logged by move_to_next_stop

            self.current_time += 1

            # Check for simulation termination conditions
            all_buses_idle = True
            for bus_id, bus in self.buses.items():
                # A bus is considered "not idle" if it's en-route OR has planned trips remaining
                # OR if it has passengers onboard (they still need to alight)
                if bus.is_en_route or self.bus_schedules_planned.get(bus_id) or len(bus.onboard_passengers) > 0:
                    all_buses_idle = False
                    break

            any_passengers_waiting = any(
                stop.get_waiting_passengers_count() > 0 for stop in self.stops.values()
            )
            any_passengers_onboard = any(
                len(bus.onboard_passengers) > 0 for bus in self.buses.values()
            )
            # Check if there are any pending passengers yet to arrive
            any_pending_passengers = len(self.pending_passengers) > 0

            if (
                all_buses_idle
                and not any_passengers_waiting
                and not any_passengers_onboard # This condition needs to be here, if passengers are onboard, simulation should continue
                and not any_pending_passengers # Added check for pending passengers
                and self.current_time > self.start_time_minutes # Ensure it runs for at least one minute
            ):
                logger.info(
                    "All buses have completed their schedules, no passengers waiting or onboard, and no pending demands. Ending simulation early."
                )
                break

        logger.info("===== Simulation Complete =====")
        
        # Ensure all buses are returned to their depot before the final check
        for bus_id, bus in self.buses.items():
            if bus.current_stop_id != bus.initial_start_point:
                self._return_bus_to_depot(bus, self.current_time)

        self.export_all_bus_schedules_to_separate_csvs()
        self._calculate_passenger_metrics() # Calculate and log passenger metrics
        
        # Save generated schedule to DB if it wasn't an optimized one
        if not self.use_optimized_schedule:
            self._save_emulator_schedule_to_db()

        return self.check_bus_return_to_start()

    def export_all_bus_schedules_to_separate_csvs(self):
        """
        Exports the detailed schedule for each bus to a separate CSV file.
        """
        for bus_id, bus in self.buses.items():
            if bus.schedule:
                df = pd.DataFrame(
                    bus.schedule,
                    columns=[
                        "Time (minutes)",
                        "Event Description",
                        "Stop ID",
                        "Passengers Onboard",
                        "Passengers Waiting at Stop",
                        "Passengers Boarded",
                        "Passengers Alighted",
                        "Direction",
                        "Status",
                    ],
                )
                df["Time (HH:MM)"] = df["Time (minutes)"].apply(format_time)
                # Reorder columns for better readability
                df = df[[
                    "Time (HH:MM)",
                    "Time (minutes)",
                    "Stop ID",
                    "Event Description",
                    "Passengers Onboard",
                    "Passengers Waiting at Stop",
                    "Passengers Boarded",
                    "Passengers Alighted",
                    "Direction",
                    "Status",
                ]]
                filename = f"bus_schedule_{bus_id}.csv"
                df.to_csv(filename, index=False)
                logger.info(f"Schedule for Bus {bus_id} exported to {filename}")
            else:
                logger.info(f"No schedule records for Bus {bus_id} to export.")

    def check_bus_return_to_start(self):
        """
        Checks if each bus returned to its initial start point (depot) at the end of the simulation.
        Logs the status for each bus.
        """
        return_status = {}
        logger.info("\n===== Checking Bus Return to Start Points =====")
        for bus_id, bus in self.buses.items():
            final_stop_id = None
            # Iterate through schedule in reverse to find the last recorded stop
            for i in reversed(range(len(bus.schedule))):
                record = bus.schedule[i]
                event_status = record[8]
                event_stop_id = record[2]
                # Consider AT_DEPOT as the definitive return point
                if event_stop_id is not None and event_status in [
                    "AT_STOP",
                    "IDLE",
                    "COMPLETED_ROUTE",
                    "AT_DEPOT", # Added AT_DEPOT to explicitly check for return
                    "AT_DEPOT_FINAL" # Added AT_DEPOT_FINAL
                ]:
                    final_stop_id = event_stop_id
                    break

            if final_stop_id == bus.initial_start_point:
                logger.info(
                    f"Bus {bus_id}: Returned to its initial start point ({bus.initial_start_point})."
                )
                return_status[bus_id] = True
            else:
                logger.warning(
                    f"Bus {bus_id}: Did NOT return to its initial start point. Started at {bus.initial_start_point}, ended at {final_stop_id}. Initial depot: {bus.initial_start_point}"
                )
                return_status[bus_id] = False
        return return_status

    def _calculate_passenger_metrics(self):
        """
        Calculates and logs key passenger-related metrics at the end of the simulation.
        """
        logger.info("\n===== Passenger Metrics =====")
        if not self.completed_passengers:
            logger.info("No passengers completed their trips during the simulation.")
            return

        total_wait_time = 0
        total_travel_time = 0
        total_trip_time = 0
        missed_connections = 0 # Placeholder for future transfer logic

        for passenger in self.completed_passengers:
            if passenger.wait_time is not None:
                total_wait_time += passenger.wait_time
            if passenger.travel_time is not None:
                total_travel_time += passenger.travel_time
            if passenger.total_trip_time is not None:
                total_trip_time += passenger.total_trip_time
            
            # Future: Add logic to detect missed connections if transfers are implemented

        num_completed_passengers = len(self.completed_passengers)
        
        avg_wait_time = total_wait_time / num_completed_passengers if num_completed_passengers > 0 else 0
        avg_travel_time = total_travel_time / num_completed_passengers if num_completed_passengers > 0 else 0
        avg_total_trip_time = total_trip_time / num_completed_passengers if num_completed_passengers > 0 else 0

        logger.info(f"Total passengers who completed trips: {num_completed_passengers}")
        logger.info(f"Average passenger wait time: {avg_wait_time:.2f} minutes")
        logger.info(f"Average passenger travel time: {avg_travel_time:.2f} minutes")
        logger.info(f"Average total trip time (arrival at origin to alighting at destination): {avg_total_trip_time:.2f} minutes")
        # logger.info(f"Missed connections: {missed_connections}") # Uncomment if implemented

    def _save_emulator_schedule_to_db(self):
        """
        Saves the schedule generated by the BusEmulator into the database.
        This will create new VehicleJourney, JourneyPattern, and Block entries.
        """
        logger.info("Saving emulator-generated schedule to database...")
        # Use the initial_bus_schedules_for_db_save for saving
        logger.debug(f"Bus schedules planned for saving: {self.initial_bus_schedules_for_db_save}")

        # Generate a unique timestamp for this simulation run
        run_timestamp = datetime.now().strftime("%Y%m%d%H%M%S%f")
        logger.debug(f"Simulation run timestamp: {run_timestamp}")

        # --- Aggressive Cleanup for Debugging if not using optimized schedule ---
        if not self.use_optimized_schedule:
            logger.warning("Performing aggressive cleanup of old VehicleJourneys, JourneyPatterns, and Blocks for debugging.")
            try:
                self.db.query(VehicleJourney).delete()
                self.db.query(JourneyPattern).delete()
                self.db.query(Block).delete()
                self.db.commit() # Commit deletions immediately
                logger.warning("Aggressive cleanup complete.")
            except Exception as e:
                self.db.rollback()
                logger.error(f"Error during aggressive cleanup: {e}")
        # --- End Aggressive Cleanup ---

        default_operator = self.db.query(Operator).filter_by(operator_code="EMU_OP").first()
        if not default_operator:
            default_operator = Operator(operator_code="EMU_OP", name="Emulator Operator")
            self.db.add(default_operator)
            self.db.flush()
            logger.debug(f"Created default operator: {default_operator.operator_id}")
        else:
            logger.debug(f"Default operator already exists: {default_operator.operator_id}")


        default_line = self.db.query(Line).filter_by(line_name="Emulator Line").first()
        if not default_line:
            default_line = Line(line_name="Emulator Line", operator_id=default_operator.operator_id)
            self.db.add(default_line)
            self.db.flush()
            logger.debug(f"Created default line: {default_line.line_id}")
        else:
            logger.debug(f"Default line already exists: {default_line.line_id}")


        default_service = self.db.query(Service).filter_by(service_code="EMU_SVC").first()
        if not default_service:
            default_service = Service(
                service_code="EMU_SVC",
                name="Emulator Service",
                operator_id=default_operator.operator_id,
                line_id=default_line.line_id,
            )
            self.db.add(default_service)
            self.db.flush()
            logger.debug(f"Created default service: {default_service.service_id}")
        else:
            logger.debug(f"Default service already exists: {default_service.service_id}")

        total_vjs_saved = 0
        logger.debug(f"total_vjs_saved initialized to: {total_vjs_saved}") # Debug log

        # Iterate over the *initial* schedules for saving
        for sim_bus_id, trips in self.initial_bus_schedules_for_db_save.items():
            logger.debug(f"Processing trips for simulator bus ID: {sim_bus_id}, Trips: {trips}")
            
            sim_bus_obj = self.buses.get(sim_bus_id)
            if not sim_bus_obj or not sim_bus_obj.db_registration:
                logger.warning(f"Simulator bus {sim_bus_id} not found or missing DB registration. Skipping saving its trips.")
                continue

            assigned_db_bus = self.db_buses_map.get(sim_bus_obj.db_registration)
            if not assigned_db_bus:
                logger.warning(f"DB Bus with registration {sim_bus_obj.db_registration} not found. Skipping saving trips for {sim_bus_id}.")
                continue

            for i, trip_info in enumerate(trips):
                route_id = trip_info["route_id"]
                departure_time_minutes = trip_info["departure_time_minutes"]
                
                route_obj = self.routes.get(route_id) # This is SimRoute
                route_name = route_obj.route_id if route_obj else f"Unknown Route {route_id}"

                # Make jp_code and block_name unique per run using the timestamp
                jp_code = f"EMU_JP_{sim_bus_id}_R{route_id}_T{departure_time_minutes}_{run_timestamp}"
                logger.debug(f"Generated jp_code: {jp_code}")
                # Removed the existing_vj check, as we intend to add new VJs after cleanup
                # The aggressive cleanup should ensure we don't have duplicates from previous runs
                assigned_jp = JourneyPattern( # Always create new JP after cleanup
                    jp_code=jp_code,
                    name=f"Emulator JP for {sim_bus_id} on {route_name} at {format_time(departure_time_minutes)} (Run {run_timestamp})",
                    route_id=route_id,
                    service_id=default_service.service_id,
                    line_id=default_line.line_id,
                    operator_id=default_operator.operator_id,
                )
                self.db.add(assigned_jp)
                self.db.flush() # Flush to get the jp_id for the newly added object
                logger.debug(f"Created JourneyPattern: {assigned_jp.jp_id} for trip {i+1} of bus {sim_bus_id}")


                block_name = f"EMU_BLOCK_{sim_bus_id}_R{route_id}_T{departure_time_minutes}_{run_timestamp}"
                logger.debug(f"Generated block_name: {block_name}")
                # Removed the existing_block check, always create new block after cleanup
                assigned_block = Block( # Always create new Block after cleanup
                    name=block_name,
                    operator_id=default_operator.operator_id,
                    bus_type_id=assigned_db_bus.bus_type_id,
                )
                self.db.add(assigned_block)
                self.db.flush() # Flush to get the block_id
                logger.debug(f"Created Block: {assigned_block.block_id} for trip {i+1} of bus {sim_bus_id}")


                departure_time_obj = (datetime.min + timedelta(minutes=departure_time_minutes)).time()
                
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
                logger.info(f"Saved generated VehicleJourney for Bus {sim_bus_id} (DB Reg: {assigned_db_bus.bus_id}) on Route {route_id} at {format_time(departure_time_minutes)}. VJ ID: {new_vj.vj_id}")
                total_vjs_saved += 1
                logger.debug(f"total_vjs_saved incremented to: {total_vjs_saved}") # Debug log

        logger.debug(f"Final total_vjs_saved before commit/rollback: {total_vjs_saved}") # Debug log
        if total_vjs_saved > 0:
            self.db.commit()
            logger.info(f"Emulator-generated schedule saved to database successfully. Total {total_vjs_saved} new VJs added.")
        else:
            self.db.rollback() # Rollback if no new VJs were added
            logger.info("No new VehicleJourneys to save to database. Rolling back transaction.")
            # This comment is purely for ensuring file update detection.
