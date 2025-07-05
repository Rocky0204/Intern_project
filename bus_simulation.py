import collections
import logging
import random
from datetime import datetime, timedelta
import pandas as pd
from typing import Optional # Import Optional for type hinting
import copy # Import copy module for deepcopy

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
# Changed logging level to DEBUG to see the new debug messages
logging.basicConfig(level=logging.DEBUG, format="%(levelname)s:%(name)s:%(message)s")
logger = logging.getLogger(__name__)

# --- Constants ---
AVG_STOP_TIME_PER_PASSENGER = 3 # Average time (in minutes) a passenger takes to board/alight
PASSENGER_WAIT_THRESHOLD = 1   # Threshold for passenger arrival time vs. current simulation time
DEAD_RUN_TRAVEL_RATE_MIN_PER_SEGMENT = 2 # Minutes per segment for deadheading (empty bus travel)

# --- Helper Function ---
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

class Stop:
    """
    Represents a bus stop in the simulation.
    Manages waiting passengers at this stop.
    """
    def __init__(self, stop_id: int, name: str):
        self.stop_id = stop_id
        self.name = name
        # A deque is used for waiting passengers to efficiently add/remove from ends
        self.waiting_passengers = collections.deque()
        self.last_bus_arrival_time = -float("inf") # Track last bus arrival for metrics (not currently used extensively)

    def add_passenger(self, passenger_id: int, arrival_time: int, destination_stop_id: int):
        """
        Adds a passenger to the waiting queue at this stop.
        """
        self.waiting_passengers.append(
            (passenger_id, arrival_time, destination_stop_id)
        )

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
        # FIX: Assign overcrowding_factor to self
        self.overcrowding_factor = overcrowding_factor
        self.onboard_passengers = []
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
            logger.error(f"Bus {self.bus_id} is at {self.current_stop_id} which is not on its current route {self.current_route.route_id}.")
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


    def alight_passengers(self, current_time: int, stop: Stop) -> int:
        """
        Simulates passengers alighting from the bus at the current stop.
        """
        alighted_this_stop = []
        remaining_onboard = []
        self.passenger_alighted_count = 0 # Reset for current stop interaction

        for passenger_id, destination_stop_id in self.onboard_passengers:
            if destination_stop_id == stop.stop_id:
                alighted_this_stop.append(passenger_id)
                self.passenger_alighted_count += 1
                logger.info(
                    f"Time {format_time(current_time)}: Passenger {passenger_id} alighted from Bus {self.bus_id} at {stop.stop_id}."
                )
            else:
                remaining_onboard.append((passenger_id, destination_stop_id))

        self.onboard_passengers = remaining_onboard
        return self.passenger_alighted_count

    def board_passengers(self, current_time: int, stop: Stop) -> int:
        """
        Simulates passengers boarding the bus from the current stop's waiting queue.
        Considers bus capacity and overcrowding factor.
        """
        boarded_this_stop = []
        self.passenger_boarded_count = 0

        max_onboard_with_overcrowding = int(self.capacity * self.overcrowding_factor)

        remaining_stops_on_current_route = []
        if self.current_route:
            # Determine which stops are still ahead on the current route
            current_stop_idx = self.current_route.stops_ids_in_order.index(
                self.current_stop_id
            )
            remaining_stops_on_current_route = self.current_route.stops_ids_in_order[
                current_stop_idx + 1 :
            ]

        passengers_to_requeue = collections.deque() # Passengers who cannot board or are not going on this route
        while (
            stop.waiting_passengers # Are there passengers waiting?
            and len(self.onboard_passengers) < max_onboard_with_overcrowding # Is there space on the bus?
        ):
            passenger_id, arrival_time, destination_stop_id = (
                stop.waiting_passengers.popleft()
            )

            # Check if the bus's current route goes to the passenger's destination
            if destination_stop_id in remaining_stops_on_current_route:
                self.onboard_passengers.append((passenger_id, destination_stop_id))
                boarded_this_stop.append(passenger_id)
                logger.info(
                    f"Time {format_time(current_time)}: Passenger {passenger_id} boarded Bus {self.bus_id} at {stop.stop_id} (Dest: {destination_stop_id})."
                )
            else:
                # Passenger cannot board this bus (wrong direction/destination)
                passengers_to_requeue.append(
                    (passenger_id, arrival_time, destination_stop_id)
                )

        # Return passengers who couldn't board to the front of the queue
        # FIX: Corrected extendLeft to extendleft for collections.deque
        stop.waiting_passengers.extendleft(reversed(passengers_to_requeue))

        return self.passenger_boarded_count


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
    ):
        self.db = db # Store the database session
        self.use_optimized_schedule = use_optimized_schedule
        self.current_time = start_time_minutes
        self.total_passengers_completed_trip = 0
        self.total_passengers_waiting = 0

        self.start_time_minutes = start_time_minutes
        self.end_time_minutes = end_time_minutes

        # Data loaded from DB
        self.stops = {} # Dict of Stop objects
        self.routes = {} # Dict of SimRoute objects
        self.passenger_demands = [] # List of raw demand data
        self.buses = {} # Dict of Bus objects (simulation instances)
        self.bus_types_map = {} # Map type_id to BusType object (DB model)
        self.db_buses_map = {} # Map db_registration to DBBus object (DB model)

        self.bus_schedules_planned = collections.defaultdict(list) # Stores planned trips for each sim Bus
        # New attribute to store the initial schedules for database saving
        self.initial_bus_schedules_for_db_save = collections.defaultdict(list)

        self._load_data_from_db() # Load all initial data from database
        self._initialize_stops_with_passengers() # Distribute initial passengers
        self._initialize_buses() # Create Bus objects based on loaded data
        self._plan_schedules() # Generate or load schedules

        self._perform_initial_bus_positioning() # Position buses for their first scheduled trip

    def _load_data_from_db(self):
        """
        Loads all necessary initial data for the simulation directly from the database.
        """
        logger.info("Loading simulation data from database...")

        # 1. Load Stop Points
        db_stop_points = self.db.query(StopPoint).all()
        if not db_stop_points:
            logger.warning("No stop points found in DB. Simulation may not be meaningful.")
        self.stops = {sp.atco_code: Stop(sp.atco_code, sp.name) for sp in db_stop_points}
        self.default_depot_id = db_stop_points[0].atco_code if db_stop_points else None
        if self.default_depot_id is None:
            logger.error("No stop points found, cannot determine default depot.")
            # This should ideally raise an error or handle gracefully if essential data is missing.
        logger.info(f"Loaded {len(self.stops)} stop points.")

        # 2. Load Bus Types
        db_bus_types = self.db.query(BusType).all()
        if not db_bus_types:
            logger.warning("No bus types found in DB. Cannot initialize buses.")
            # This should ideally raise an error or handle gracefully
        self.bus_types_map = {bt.type_id: bt for bt in db_bus_types}
        logger.info(f"Loaded {len(self.bus_types_map)} bus types.")

        # 3. Load individual Bus instances (DBBus)
        db_buses = self.db.query(DBBus).options(joinedload(DBBus.bus_type), joinedload(DBBus.garage)).all()
        if not db_buses:
            logger.warning("No individual buses found in DB. Cannot run simulation.")
            # This should ideally raise an error or handle gracefully
        self.db_buses_map = {db_bus.bus_id: db_bus for db_bus in db_buses} # Map registration to DBBus object
        logger.info(f"Loaded {len(self.db_buses_map)} individual buses.")

        # 4. Load Routes and Route Definitions
        # Ensure we are querying the ORM model 'Route' from api.models
        db_routes = self.db.query(Route).options(joinedload(Route.route_definitions).joinedload(RouteDefinition.stop_point)).all()
        if not db_routes:
            logger.warning("No routes found in DB. Simulation will not schedule trips.")
        
        # Create SimRoute objects from DB Route data
        for route_db in db_routes:
            stops_ids_in_order = []
            total_outbound_route_time_minutes = 0
            sorted_route_defs = sorted(route_db.route_definitions, key=lambda rd: rd.sequence)
            for i, rd in enumerate(sorted_route_defs):
                stops_ids_in_order.append(rd.stop_point.atco_code)
                if i < len(sorted_route_defs) - 1:
                    segment_travel_time = 5 # Default 5 minutes per segment
                    total_outbound_route_time_minutes += segment_travel_time
            
            self.routes[route_db.route_id] = SimRoute( # Use SimRoute here
                route_db.route_id,
                stops_ids_in_order,
                total_outbound_route_time_minutes
            )
        logger.info(f"Loaded {len(self.routes)} routes and their definitions.")

        # 5. Load Demand Records
        db_demands = self.db.query(Demand).all()
        if not db_demands:
            logger.warning("No demand records found in DB. Simulation will have no passengers.")
        
        self.passenger_demands = []
        for d in db_demands:
            # Need to map StopArea codes to StopPoint ATCO codes for simulation
            origin_sp_id = self._get_stop_area_representative_stop_point(d.origin)
            destination_sp_id = self._get_stop_area_representative_stop_point(d.destination)

            if origin_sp_id is None or destination_sp_id is None:
                logger.warning(f"Demand origin/destination StopArea ({d.origin}, {d.destination}) could not be mapped to StopPoints. Skipping this demand record.")
                continue

            self.passenger_demands.append({
                "origin": origin_sp_id,
                "destination": destination_sp_id,
                "count": d.count,
                "arrival_time": d.start_time.hour * 60 + d.start_time.minute
            })
        # FIX: passenger_id_counter is not defined in this scope. It should be defined before the loop or removed if not needed.
        # For now, let's just log the count of demands loaded.
        logger.info(f"Loaded {len(self.passenger_demands)} demand records.")
        logger.info("Simulation data loading complete.")

    def _get_stop_area_representative_stop_point(self, stop_area_code: int) -> Optional[int]:
        """Helper to find a representative stop point for a given stop area code."""
        # This function needs to query the DB directly since it's now in BusEmulator
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
            
            # FIX: Ensure depot_id is a valid StopPoint ATCO code.
            # The garage_id (e.g., 1) is not an ATCO code (e.g., 100101).
            # We will use the default_depot_id (which is a valid ATCO code)
            # for all buses, or implement a mapping if garages are meant to be at specific stops.
            # For now, simplify and use the default_depot_id for all buses.
            depot_id = self.default_depot_id
            if depot_id is None:
                logger.error(f"Bus {db_reg} has no suitable depot stop point. Skipping.")
                continue

            self.buses[sim_bus_id] = Bus(
                bus_id=sim_bus_id,
                capacity=bus_type.capacity,
                depot_stop_id=depot_id, # Use the valid default_depot_id
                initial_internal_time=self.start_time_minutes,
                # overcrowding_factor is now defaulted in the Bus class __init__
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


    def _generate_random_schedules(self):
        """
        Generates a simple random schedule for each bus, used when no optimized schedule is provided.
        Each bus is assigned multiple random trips throughout the simulation window.
        """
        logger.info("Generating random schedules for buses...")
        
        # Define parameters for random schedule generation
        MIN_TRIPS_PER_BUS = 2
        MAX_TRIPS_PER_BUS = 5
        MIN_LAYOVER_MINUTES = 5
        MAX_LAYOVER_MINUTES = 15
        
        for bus_id, bus in self.buses.items():
            possible_routes = []
            for route_id, route in self.routes.items():
                # Only consider routes that start at the bus's depot
                if route.stops_ids_in_order and route.stops_ids_in_order[0] == bus.initial_start_point:
                    possible_routes.append(route_id)
            
            if not possible_routes:
                logger.error(
                    f"CRITICAL: No suitable route found starting at depot {bus.initial_start_point} for Bus {bus_id}. Cannot schedule this bus."
                )
                continue

            num_trips = random.randint(MIN_TRIPS_PER_BUS, MAX_TRIPS_PER_BUS)
            last_trip_end_time = self.start_time_minutes # Start scheduling from the beginning of the simulation window

            for i in range(num_trips):
                route_id = random.choice(possible_routes)
                route_obj = self.routes.get(route_id)
                if not route_obj:
                    logger.warning(f"Randomly selected route {route_id} not found. Skipping trip for bus {bus_id}.")
                    continue

                # Ensure next trip starts after the previous one ends, plus a layover
                layover_duration = random.randint(MIN_LAYOVER_MINUTES, MAX_LAYOVER_MINUTES)
                
                # Minimum departure time for the current trip
                min_departure_for_current_trip = last_trip_end_time + layover_duration

                # Calculate the latest possible departure time for this trip
                # This is the end of the simulation window minus the route duration
                max_departure_for_current_trip = self.end_time_minutes - route_obj.total_outbound_route_time_minutes

                # Ensure there's a valid time window to schedule the trip
                if min_departure_for_current_trip > max_departure_for_current_trip:
                    logger.debug(f"Bus {bus_id}: No more time slots for additional trips. Breaking after {i} trips.")
                    break # No more time slots available for this bus

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
                    f"Bus {bus_id} scheduled trip {i+1}: Route {route_id}, Departure {format_time(departure_time_minutes)}."
                )
                
                # Update last_trip_end_time for the next iteration
                last_trip_end_time = departure_time_minutes + route_obj.total_outbound_route_time_minutes

            # Sort the planned schedules by departure time for each bus
            self.bus_schedules_planned[bus_id].sort(key=lambda x: x['departure_time_minutes'])

        logger.info("Random bus schedules generated.")

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
                travel_time_needed = DEAD_RUN_TRAVEL_RATE_MIN_PER_SEGMENT * 5

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
            
    def _initialize_stops_with_passengers(self):
        """
        Distributes initial passenger demands to their respective origin stops.
        Also dynamically adjusts simulation start/end times based on demand.
        """
        passenger_id_counter = 0
        all_arrival_times = []

        for demand in self.passenger_demands:
            origin_stop_id = demand["origin"]
            destination_stop_id = demand["destination"]
            count = demand["count"] # This is the float value
            passenger_arrival_time = demand.get("arrival_time")

            if passenger_arrival_time is None:
                logger.warning(f"Demand missing 'arrival_time': {demand}. Skipping.")
                continue

            if origin_stop_id not in self.stops:
                logger.warning(
                    f"Origin stop {origin_stop_id} for demand not found. Skipping."
                )
                continue
            if destination_stop_id not in self.stops:
                logger.warning(
                    f"Destination stop {destination_stop_id} for demand not found. Skipping."
                )
                continue

            for _ in range(int(count)): # Cast count to int here
                passenger_id_counter += 1
                self.stops[origin_stop_id].add_passenger(
                    passenger_id_counter, passenger_arrival_time, destination_stop_id
                )
                all_arrival_times.append(passenger_arrival_time)

        logger.info(
            f"Loaded {passenger_id_counter} initial passenger demands with specified arrival times."
        )

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
            # --- 1. Process passengers arriving at stops ---
            for _, stop in self.stops.items():
                passengers_not_yet_arrived = collections.deque()
                ready_to_board_passengers = collections.deque()

                while stop.waiting_passengers:
                    p_id, p_arrival_time, p_dest_id = stop.waiting_passengers.popleft()
                    if p_arrival_time <= self.current_time:
                        ready_to_board_passengers.append(
                            (p_id, p_arrival_time, p_dest_id)
                        )
                    else:
                        passengers_not_yet_arrived.append(
                            (p_id, p_arrival_time, p_dest_id)
                        )

                stop.waiting_passengers.extend(passengers_not_yet_arrived)
                stop.waiting_passengers.extendleft(reversed(ready_to_board_passengers))


            # --- 2. Process buses ---
            for bus_id, bus in self.buses.items():
                if bus.current_time > self.current_time:
                    continue

                if not bus.is_en_route: # Bus is currently at a stop
                    current_stop = self._get_stop_by_id(bus.current_stop_id)

                    if not current_stop:
                        logger.error(
                            f"Bus {bus.bus_id} is at an unknown stop ID: {bus.current_stop_id}. Halting its simulation."
                        )
                        continue

                    # If bus just arrived at a stop from being en-route
                    if (
                        bus.schedule
                        and bus.schedule[-1][8] in ["EN_ROUTE", "EN_ROUTE_DEAD_RUN"]
                        and bus.current_time <= self.current_time # Ensure event is for current minute
                    ):
                        bus.current_time = self.current_time
                        bus.add_event_to_schedule(
                            bus.current_time,
                            "Arrived At Stop",
                            current_stop.stop_id,
                            len(bus.onboard_passengers),
                            current_stop.get_waiting_passengers_count(),
                            bus.passenger_boarded_count,
                            bus.passenger_alighted_count,
                            bus.current_direction,
                            "AT_STOP",
                        )
                        logger.info(f"Time {format_time(self.current_time)}: Bus {bus.bus_id} arrived at {current_stop.stop_id}.")


                    alighted_count = bus.alight_passengers(self.current_time, current_stop)
                    boarded_count = bus.board_passengers(self.current_time, current_stop)

                    if bus.schedule:
                        last_event = list(bus.schedule[-1])
                        last_event[3] = len(bus.onboard_passengers)
                        last_event[4] = current_stop.get_waiting_passengers_count()
                        last_event[5] = boarded_count
                        last_event[6] = alighted_count
                        bus.schedule[-1] = tuple(last_event)


                    if self.bus_schedules_planned.get(bus_id):
                        next_scheduled_trip = self.bus_schedules_planned[bus_id][0]
                        scheduled_departure_time = next_scheduled_trip["departure_time_minutes"]
                        next_route_id = next_scheduled_trip["route_id"]
                        next_route = self._get_route_by_id(next_route_id) # This is SimRoute

                        if not next_route:
                            logger.error(f"Scheduled route {next_route_id} not found for Bus {bus_id}. Skipping its future trips.")
                            self.bus_schedules_planned[bus_id].pop(0)
                            continue

                        # --- NEW LOGIC: Handle dead run to next trip's start point ---
                        if bus.current_stop_id != next_route.stops_ids_in_order[0]:
                            target_stop_id = next_route.stops_ids_in_order[0]
                            # For simplicity, assume a fixed dead run time or calculate based on distance
                            # For now, let's just instantly move it and adjust time if needed.
                            
                            # Calculate dead run travel time (simplified)
                            # This needs to be more sophisticated, e.g., by finding a path and summing segment times.
                            # For now, a fixed time or a simple calculation based on "segments"
                            # Let's assume 5 minutes per "segment" for dead run
                            # A segment here is just a conceptual unit for dead run time, not actual route segments
                            dead_run_duration = DEAD_RUN_TRAVEL_RATE_MIN_PER_SEGMENT * 5 # Example: 10 minutes

                            dead_run_arrival_time = self.current_time + dead_run_duration
                            
                            # Ensure bus is ready at the next departure point by its scheduled time
                            final_arrival_at_next_start_point = max(dead_run_arrival_time, scheduled_departure_time)

                            # Record dead run events
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
                        # --- END NEW LOGIC ---

                        if self.current_time >= scheduled_departure_time:
                            bus.current_time = self.current_time
                            bus.start_route(next_route, self.current_time)
                            logger.info(
                                f"Time {format_time(self.current_time)}: Bus {bus_id} started scheduled trip on Route {next_route_id}."
                            )
                            self.bus_schedules_planned[bus_id].pop(0)
                            bus.move_to_next_stop(self.current_time)
                        else:
                            if not bus.schedule or bus.schedule[-1][8] != "WAITING":
                                bus.add_event_to_schedule(
                                    self.current_time,
                                    f"Waiting for scheduled departure ({format_time(scheduled_departure_time)})",
                                    bus.current_stop_id,
                                    len(bus.onboard_passengers),
                                    current_stop.get_waiting_passengers_count(),
                                    0, 0, "N/A", "WAITING"
                                )

                    elif bus.current_route is None: # Bus has no current route and no more scheduled trips
                        if not self.bus_schedules_planned.get(bus_id): # Check if there are truly no more trips
                            if bus.current_stop_id != bus.initial_start_point:
                                # If bus is not at depot and no more trips, return it to depot
                                self._return_bus_to_depot(bus, self.current_time)
                            elif bus.schedule and bus.schedule[-1][8] != "IDLE":
                                # If bus is at depot and has no more scheduled trips, mark as idle
                                bus.add_event_to_schedule(
                                    self.current_time,
                                    "Idle",
                                    bus.current_stop_id,
                                    len(bus.onboard_passengers),
                                    current_stop.get_waiting_passengers_count(),
                                    0, 0, "N/A", "IDLE"
                                )
                        # This 'else' block is now largely redundant due to the new dead-run logic above
                        # but kept for robustness if schedules are empty for some reason.
                        else: # Bus has no current route but has future scheduled trips (e.g., waiting for next trip)
                            if bus.schedule and bus.schedule[-1][8] != "WAITING":
                                bus.add_event_to_schedule(
                                    self.current_time,
                                    "Waiting for next scheduled trip",
                                    bus.current_stop_id,
                                    len(bus.onboard_passengers),
                                    current_stop.get_waiting_passengers_count(),
                                    0, 0, "N/A", "WAITING"
                                )


                elif bus.is_en_route:
                    bus.time_to_next_stop -= 1
                    if bus.time_to_next_stop <= 0:
                        current_stop_idx = bus.current_route.stops_ids_in_order.index(
                            bus.current_stop_id
                        )
                        next_stop_idx = current_stop_idx + 1
                        
                        if next_stop_idx >= len(bus.current_route.stops_ids_in_order):
                            # Bus reached the end of its current route
                            bus.is_en_route = False
                            bus.time_to_next_stop = 0
                            bus.current_time = self.current_time # Ensure time is current simulation time

                            # Get the current stop object for logging passenger counts
                            current_stop_obj = self._get_stop_by_id(bus.current_stop_id)
                            current_stop_passengers_waiting = current_stop_obj.get_waiting_passengers_count() if current_stop_obj else 0

                            bus.add_event_to_schedule(
                                self.current_time,
                                f"End Route {bus.current_route.route_id}",
                                bus.current_stop_id,
                                len(bus.onboard_passengers),
                                current_stop_passengers_waiting,
                                bus.passenger_boarded_count,
                                bus.passenger_alighted_count,
                                bus.current_direction,
                                "ROUTE_END",
                            )
                            logger.info(
                                f"Time {format_time(self.current_time)}: Bus {bus.bus_id} completed route {bus.current_route.route_id} at {bus.current_stop_id}."
                            )
                            bus.current_route = None # Clear current route after completion

                            # After completing a route, check if there are more scheduled trips
                            if not self.bus_schedules_planned.get(bus_id):
                                # If no more scheduled trips, return to depot if not already there
                                if bus.current_stop_id != bus.initial_start_point:
                                    self._return_bus_to_depot(bus, self.current_time)
                                else:
                                    # If already at depot and no more trips, mark as final idle
                                    if bus.schedule and bus.schedule[-1][8] != "AT_DEPOT_FINAL":
                                        bus.add_event_to_schedule(
                                            self.current_time,
                                            "Final stop at depot",
                                            bus.current_stop_id,
                                            len(bus.onboard_passengers),
                                            0, 0, 0, "N/A", "AT_DEPOT_FINAL"
                                        )
                                        logger.info(f"Time {format_time(self.current_time)}: Bus {bus.bus_id} finished all trips and is at depot.")

                        else:
                            # Bus arrived at an intermediate stop on its current route
                            bus.current_stop_id = bus.current_route.stops_ids_in_order[
                                next_stop_idx
                            ]
                            bus.is_en_route = False
                            bus.time_to_next_stop = 0
                            bus.current_time = self.current_time # Ensure time is current simulation time

                            logger.info(
                                f"Time {format_time(self.current_time)}: Bus {bus.bus_id} arrived at {bus.current_stop_id} (Intermediate)."
                            )
                    else:
                        # Bus is still en-route to the next stop
                        pass

            self.current_time += 1

            # Check for simulation termination conditions
            all_buses_idle = True
            for bus_id, bus in self.buses.items():
                # A bus is considered "not idle" if it's en-route OR has planned trips remaining
                if bus.is_en_route or self.bus_schedules_planned.get(bus_id):
                    all_buses_idle = False
                    break

            any_passengers_waiting = any(
                stop.get_waiting_passengers_count() > 0 for stop in self.stops.values()
            )
            any_passengers_onboard = any(
                len(bus.onboard_passengers) > 0 for bus in self.buses.values()
            )

            if (
                all_buses_idle
                and not any_passengers_waiting
                and not any_passengers_onboard
                and self.current_time > self.start_time_minutes # Ensure it runs for at least one minute
            ):
                logger.info(
                    "All buses have completed their schedules, no passengers waiting or onboard. Ending simulation early."
                )
                break

        logger.info("===== Simulation Complete =====")
        
        # FIX: Ensure all buses are returned to their depot before the final check
        for bus_id, bus in self.buses.items():
            if bus.current_stop_id != bus.initial_start_point:
                self._return_bus_to_depot(bus, self.current_time)

        self.export_all_bus_schedules_to_separate_csvs()
        
        # Save generated schedule to DB if it wasn't an optimized one
        if not self.use_optimized_schedule:
            self._save_emulator_schedule_to_db()

        return self.check_bus_return_to_start()

    def export_all_bus_schedules_to_separate_csvs(self):
        """
        Exports the detailed schedule for each bus to a separate CSV file.
        """
        # This method is now correctly placed within the BusEmulator class.
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
