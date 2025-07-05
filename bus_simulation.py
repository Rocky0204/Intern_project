import collections
import logging
import random
from datetime import datetime, timedelta

import pandas as pd

AVG_STOP_TIME_PER_PASSENGER = 3
PASSENGER_WAIT_THRESHOLD = 1

logger = logging.getLogger(__name__)


def format_time(total_minutes_from_midnight):
    sim_datetime = datetime(2025, 6, 10, 0, 0, 0) + timedelta(
        minutes=total_minutes_from_midnight
    )
    return sim_datetime.strftime("%H:%M")


class Stop:
    def __init__(self, stop_id, name):
        self.stop_id = stop_id
        self.name = name
        self.waiting_passengers = collections.deque()
        self.last_bus_arrival_time = -float("inf")

    def add_passenger(self, passenger_id, arrival_time, destination_stop_id):
        self.waiting_passengers.append(
            (passenger_id, arrival_time, destination_stop_id)
        )

    def get_waiting_passengers_count(self):
        return len(self.waiting_passengers)

    def __repr__(self):
        return f"Stop(ID: {self.stop_id}, Name: {self.name})"


class Bus:
    def __init__(
        self,
        bus_id,
        capacity,
        depot_stop_id,
        initial_internal_time,
        overcrowding_factor=1.0,
    ):
        self.bus_id = bus_id
        self.capacity = capacity
        self.overcrowding_factor = overcrowding_factor
        self.onboard_passengers = []
        self.current_stop_id = depot_stop_id
        self.initial_start_point = depot_stop_id
        self.current_route = None
        self.route_index = 0
        self.schedule = []
        self.current_time = initial_internal_time

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

    def add_event_to_schedule(
        self,
        time,
        event_description,
        stop_id,
        passengers_onboard,
        passengers_waiting,
        boarded_count,
        alighted_count,
        direction,
        status,
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

    def start_route(self, route, global_simulation_time):
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

    def move_to_next_stop(self, global_simulation_time):
        if not self.current_route:
            logger.warning(f"Bus {self.bus_id} has no route assigned.")
            self.is_en_route = False
            return

        current_stop_idx = self.current_route.stops_ids_in_order.index(
            self.current_stop_id
        )

        if current_stop_idx < len(self.current_route.stops_ids_in_order) - 1:
            next_stop_idx = current_stop_idx + 1
            next_stop_id = self.current_route.stops_ids_in_order[next_stop_idx]

            total_stops_in_route = len(self.current_route.stops_ids_in_order)
            if total_stops_in_route > 1:
                self.time_to_next_stop = (
                    self.current_route.total_outbound_route_time_minutes
                    / (total_stops_in_route - 1)
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
                    None,
                    len(self.onboard_passengers),
                    0,
                    0,
                    0,
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

    def alighting_time(self, alighted_count):
        return (alighted_count * AVG_STOP_TIME_PER_PASSENGER) / 60

    def boarding_time(self, boarded_count):
        return (boarded_count * AVG_STOP_TIME_PER_PASSENGER) / 60

    def alight_passengers(self, current_time, stop):
        alighted_this_stop = []
        remaining_onboard = []
        self.passenger_alighted_count = 0

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

    def board_passengers(self, current_time, stop):
        boarded_this_stop = []
        self.passenger_boarded_count = 0

        max_onboard_with_overcrowding = int(self.capacity * self.overcrowding_factor)

        remaining_stops_on_current_route = []
        if self.current_route:
            current_stop_idx = self.current_route.stops_ids_in_order.index(
                self.current_stop_id
            )
            remaining_stops_on_current_route = self.current_route.stops_ids_in_order[
                current_stop_idx + 1 :
            ]

        passengers_to_requeue = collections.deque()
        while (
            stop.waiting_passengers
            and len(self.onboard_passengers) < max_onboard_with_overcrowding
        ):
            passenger_id, arrival_time, destination_stop_id = (
                stop.waiting_passengers.popleft()
            )

            if destination_stop_id in remaining_stops_on_current_route:
                self.onboard_passengers.append((passenger_id, destination_stop_id))
                boarded_this_stop.append(passenger_id)
                self.passenger_boarded_count += 1
                logger.info(
                    f"Time {format_time(current_time)}: Passenger {passenger_id} boarded Bus {self.bus_id} at {stop.stop_id} (Dest: {destination_stop_id})."
                )
            else:
                passengers_to_requeue.append(
                    (passenger_id, arrival_time, destination_stop_id)
                )

        stop.waiting_passengers.extendleft(reversed(passengers_to_requeue))

        return self.passenger_boarded_count


class Route:
    def __init__(self, route_id, stops_ids_in_order, total_outbound_route_time_minutes):
        self.route_id = route_id
        self.stops_ids_in_order = stops_ids_in_order
        self.total_outbound_route_time_minutes = total_outbound_route_time_minutes
        if len(stops_ids_in_order) > 1:
            self.segment_time = total_outbound_route_time_minutes / (
                len(stops_ids_in_order) - 1
            )
        else:
            self.segment_time = 0

    def __repr__(self):
        return f"Route(ID: {self.route_id}, Stops: {self.stops_ids_in_order})"


class BusEmulator:
    def __init__(
        self,
        bus_types_and_counts: list[dict],
        bus_config_df: pd.DataFrame,
        routes_df: pd.DataFrame,
        stops_df: pd.DataFrame,
        demands_df: pd.DataFrame,
        start_time_minutes: int = 0,
        end_time_minutes: int = 1440,
        default_depot_id: str = None,
    ):
        self.current_time = start_time_minutes
        self.total_passengers_completed_trip = 0
        self.total_passengers_waiting = 0

        self.start_time_minutes = start_time_minutes
        self.end_time_minutes = end_time_minutes

        self.stops = {
            row["stop_id"]: Stop(row["stop_id"], row["name"])
            for index, row in stops_df.iterrows()
        }

        self.routes = {
            row["route_id"]: Route(**row) for index, row in routes_df.iterrows()
        }

        self.passenger_demands = demands_df.to_dict(orient="records")
        self._initialize_stops_with_passengers()

        if default_depot_id is None and not stops_df.empty:
            default_depot_id = stops_df.iloc[0]["stop_id"]
        elif default_depot_id is None:
            logger.error(
                "No default depot ID provided and stops_df is empty. Cannot initialize buses."
            )
            raise ValueError(
                "No default depot ID or stops data for bus initialization."
            )
        all_buses_data = []
        bus_id_counter = 1
        for bus_type_entry in bus_types_and_counts:
            bus_type = bus_type_entry["type"]
            num_buses = bus_type_entry["count"]

            type_config_rows = bus_config_df[bus_config_df["bus_type"] == bus_type]
            if type_config_rows.empty:
                logger.error(
                    f"Bus type '{bus_type}' not found in bus_config_df. Skipping these buses."
                )
                continue
            type_config = type_config_rows.iloc[0]

            for _ in range(num_buses):
                bus_id = f"{bus_type[0].upper()}{bus_id_counter}"
                all_buses_data.append(
                    {
                        "bus_id": bus_id,
                        "capacity": type_config["capacity"],
                        "overcrowding_factor": type_config["overcrowding_factor"],
                        "depot_stop_id": type_config.get(
                            "depot_stop_id", default_depot_id
                        ),
                    }
                )
                bus_id_counter += 1
        self.buses_df = pd.DataFrame(all_buses_data)

        self.buses = {}
        for _, row in self.buses_df.iterrows():
            bus_id = row["bus_id"]
            capacity = row["capacity"]
            overcrowding_factor = row.get("overcrowding_factor", 1.0)
            bus_depot = row.get("depot_stop_id", default_depot_id)

            self.buses[bus_id] = Bus(
                bus_id,
                capacity,
                bus_depot,
                0,
                overcrowding_factor,
            )
        schedules_df = self.generate_schedules()

        self.bus_schedules_planned = collections.defaultdict(list)
        for _, row in schedules_df.iterrows():
            self.bus_schedules_planned[row["bus_id"]].append(
                {
                    "route_id": row["route_id"],
                    "layover_duration": row["layover_duration"],
                }
            )

        self._perform_initial_bus_positioning()

    def generate_schedules(self):
        schedules = []
        for bus_id, bus in self.buses.items():
            possible_routes = []
            for route_id, route in self.routes.items():
                if route.stops_ids_in_order[0] == bus.initial_start_point:
                    possible_routes.append(route_id)
            if not possible_routes:
                logger.error(
                    f"CRITICAL: No suitable route found starting at depot {bus.initial_start_point} for Bus {bus_id}. This bus cannot be scheduled to return to start."
                )
                route_id = random.choice(list(self.routes.keys()))
            else:
                route_id = random.choice(possible_routes)

            schedules.append(
                {
                    "bus_id": bus_id,
                    "route_id": route_id,
                    "layover_duration": random.randint(0, 5),
                }
            )
            logger.info(
                f"Bus {bus_id} assigned route {route_id} starting from its depot {bus.initial_start_point}."
            )
        return pd.DataFrame(schedules)

    def _perform_initial_bus_positioning(self):
        DEAD_RUN_TRAVEL_RATE_MIN_PER_SEGMENT = 2

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
                    f"Route {route_id} not found for Bus {bus_id}. Cannot position bus."
                )
                continue

            first_route_stop_id = route.stops_ids_in_order[0]

            if bus.current_stop_id != first_route_stop_id:
                travel_time_needed = DEAD_RUN_TRAVEL_RATE_MIN_PER_SEGMENT * 5

                dead_run_arrival_time = bus.current_time + travel_time_needed

                final_dead_run_arrival_time = max(
                    dead_run_arrival_time, self.start_time_minutes
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
                if bus.current_time < self.start_time_minutes:
                    bus.current_time = self.start_time_minutes
                    bus.add_event_to_schedule(
                        bus.current_time,
                        f"Ready at {bus.current_stop_id}",
                        bus.current_stop_id,
                        len(bus.onboard_passengers),
                        self.stops[bus.current_stop_id].get_waiting_passengers_count(),
                        0,
                        0,
                        "N/A",
                        "READY_AT_START",
                    )
                logger.info(
                    f"Bus {bus_id} is already at its first route stop {first_route_stop_id}."
                )

            bus.current_route = route
            bus.current_direction = "Outbound"
            bus.route_index = 0
            bus.is_en_route = False
            bus.time_to_next_stop = 0
            bus.total_route_duration_minutes = route.total_outbound_route_time_minutes
            bus.last_stop_arrival_time = bus.current_time

            self.bus_schedules_planned[bus_id].pop(0)

    def _initialize_stops_with_passengers(self):
        passenger_id_counter = 0
        all_arrival_times = []

        for demand in self.passenger_demands:
            origin_stop_id = demand["origin"]
            destination_stop_id = demand["destination"]
            count = demand["count"]
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

            for _ in range(count):
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
            self.end_time_minutes = max(all_arrival_times) + PASSENGER_WAIT_THRESHOLD
            logger.info(
                f"Dynamic simulation window set: {format_time(self.start_time_minutes)} to {format_time(self.end_time_minutes)}"
            )
        elif not all_arrival_times:
            logger.warning(
                "No passenger demands with arrival times found. Simulation window will default to 24 hours if not explicitly set."
            )

    def _get_stop_by_id(self, stop_id):
        return self.stops.get(stop_id)

    def _get_route_by_id(self, route_id):
        return self.routes.get(route_id)

    def run_simulation(self):
        logger.info(
            f"===== Starting Bus Simulation ({format_time(self.start_time_minutes)} to {format_time(self.end_time_minutes)}) ====="
        )

        self.current_time = self.start_time_minutes

        while self.current_time <= self.end_time_minutes:
            _ = True

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

            for bus_id, bus in self.buses.items():
                if bus.current_time > self.current_time:
                    _ = False
                    continue

                if bus.current_route or self.bus_schedules_planned.get(bus_id):
                    _ = False

                if not bus.is_en_route:
                    current_stop = self._get_stop_by_id(bus.current_stop_id)

                    if current_stop:
                        if (
                            bus.schedule
                            and bus.schedule[-1][8] in ["EN_ROUTE", "EN_ROUTE_DEAD_RUN"]
                        ) and bus.current_time <= self.current_time:
                            bus.current_time = self.current_time
                            bus.add_event_to_schedule(
                                bus.current_time,
                                "At Stop",
                                current_stop.stop_id,
                                len(bus.onboard_passengers),
                                current_stop.get_waiting_passengers_count(),
                                bus.passenger_boarded_count,
                                bus.passenger_alighted_count,
                                bus.current_direction,
                                "AT_STOP",
                            )
                        elif bus.schedule and bus.schedule[-1][8] not in [
                            "AT_STOP",
                            "AT_STOP_DEAD_RUN",
                            "READY_AT_START",
                        ]:
                            bus.add_event_to_schedule(
                                bus.current_time,
                                "At Stop",
                                current_stop.stop_id,
                                len(bus.onboard_passengers),
                                current_stop.get_waiting_passengers_count(),
                                bus.passenger_boarded_count,
                                bus.passenger_alighted_count,
                                bus.current_direction,
                                "AT_STOP",
                            )

                        alighted_count = bus.alight_passengers(
                            self.current_time, current_stop
                        )
                        boarded_count = bus.board_passengers(
                            self.current_time, current_stop
                        )

                        if bus.schedule:
                            last_event = list(bus.schedule[-1])
                            last_event[3] = len(bus.onboard_passengers)
                            last_event[4] = current_stop.get_waiting_passengers_count()
                            last_event[5] = boarded_count
                            last_event[6] = alighted_count
                            bus.schedule[-1] = tuple(last_event)

                        if bus.current_route:
                            current_stop_idx = (
                                bus.current_route.stops_ids_in_order.index(
                                    bus.current_stop_id
                                )
                            )
                            if (
                                current_stop_idx
                                < len(bus.current_route.stops_ids_in_order) - 1
                            ):
                                bus.move_to_next_stop(self.current_time)
                            else:
                                bus.add_event_to_schedule(
                                    self.current_time,
                                    f"End Route {bus.current_route.route_id}",
                                    bus.current_stop_id,
                                    len(bus.onboard_passengers),
                                    current_stop.get_waiting_passengers_count(),
                                    bus.passenger_boarded_count,
                                    bus.passenger_alighted_count,
                                    bus.current_direction,
                                    "ROUTE_END",
                                )

                                next_schedule_entry = None
                                if self.bus_schedules_planned.get(bus_id):
                                    next_schedule_entry = self.bus_schedules_planned[
                                        bus_id
                                    ][0]

                                if next_schedule_entry:
                                    layover_duration = next_schedule_entry.get(
                                        "layover_duration", 0
                                    )
                                    if layover_duration > 0:
                                        bus.add_event_to_schedule(
                                            self.current_time,
                                            f"Layover ({layover_duration} min)",
                                            bus.current_stop_id,
                                            len(bus.onboard_passengers),
                                            current_stop.get_waiting_passengers_count(),
                                            0,
                                            0,
                                            "N/A",
                                            "LAYOVER_START",
                                        )
                                        bus.current_time += layover_duration
                                        self.current_time = max(
                                            self.current_time, bus.current_time
                                        )

                                        bus.add_event_to_schedule(
                                            self.current_time,
                                            "Layover End",
                                            bus.current_stop_id,
                                            len(bus.onboard_passengers),
                                            current_stop.get_waiting_passengers_count(),
                                            0,
                                            0,
                                            "N/A",
                                            "LAYOVER_END",
                                        )

                                    next_route_id = next_schedule_entry["route_id"]
                                    next_route = self._get_route_by_id(next_route_id)
                                    if next_route:
                                        bus.current_route = next_route
                                        bus.current_stop_id = (
                                            next_route.stops_ids_in_order[0]
                                        )
                                        bus.route_index = 0
                                        bus.move_to_next_stop(self.current_time)
                                        self.bus_schedules_planned[bus_id].pop(0)
                                    else:
                                        logger.error(
                                            f"Next route {next_route_id} not found for Bus {bus_id}."
                                        )
                                        bus.current_route = None
                                else:
                                    bus.current_route = None
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
                        else:
                            if self.bus_schedules_planned.get(bus_id):
                                next_schedule_entry = self.bus_schedules_planned[
                                    bus_id
                                ][0]
                                next_route_id = next_schedule_entry["route_id"]
                                next_route = self._get_route_by_id(next_route_id)

                                if next_route:
                                    layover_duration = next_schedule_entry.get(
                                        "layover_duration", 0
                                    )
                                    if layover_duration > 0:
                                        bus.add_event_to_schedule(
                                            self.current_time,
                                            f"Layover ({layover_duration} min)",
                                            bus.current_stop_id,
                                            len(bus.onboard_passengers),
                                            current_stop.get_waiting_passengers_count(),
                                            0,
                                            0,
                                            "N/A",
                                            "LAYOVER_START",
                                        )
                                        bus.current_time += layover_duration
                                        self.current_time = max(
                                            self.current_time, bus.current_time
                                        )

                                        bus.add_event_to_schedule(
                                            self.current_time,
                                            "Layover End",
                                            bus.current_stop_id,
                                            len(bus.onboard_passengers),
                                            current_stop.get_waiting_passengers_count(),
                                            0,
                                            0,
                                            "N/A",
                                            "LAYOVER_END",
                                        )

                                    bus.start_route(next_route, self.current_time)
                                    self.bus_schedules_planned[bus_id].pop(0)
                                else:
                                    logger.error(
                                        f"Route {next_route_id} not found for Bus {bus_id}."
                                    )
                                    bus.current_route = None
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
                            else:
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
                    else:
                        logger.error(
                            f"Bus {bus.bus_id} is at an unknown stop ID: {bus.current_stop_id}. Halting its simulation."
                        )
                        bus.is_en_route = False

                elif bus.is_en_route:
                    bus.time_to_next_stop -= 1
                    if bus.time_to_next_stop <= 0:
                        current_stop_idx = bus.current_route.stops_ids_in_order.index(
                            bus.current_stop_id
                        )
                        next_stop_idx = current_stop_idx + 1
                        bus.current_stop_id = bus.current_route.stops_ids_in_order[
                            next_stop_idx
                        ]
                        bus.is_en_route = False
                        bus.time_to_next_stop = 0
                        bus.current_time = self.current_time
                        logger.info(
                            f"Time {format_time(self.current_time)}: Bus {bus.bus_id} arrived at {bus.current_stop_id}."
                        )

            if self.current_time < self.end_time_minutes:
                self.current_time += 1
            else:
                break

            all_buses_truly_idle = True
            for bus_id, bus in self.buses.items():
                if (
                    bus.current_route
                    or self.bus_schedules_planned.get(bus_id)
                    or bus.current_time > self.current_time
                ):
                    all_buses_truly_idle = False
                    break

            if all_buses_truly_idle and self.current_time > self.start_time_minutes:
                logger.info(
                    "All buses have completed their schedules and are truly idle. Ending simulation early."
                )
                break

        logger.info("===== Simulation Complete =====")
        self.export_all_bus_schedules_to_separate_csvs()
        return self.check_bus_return_to_start()

    def export_schedule_to_csv(self, filename):
        try:
            records = []
            for bus in self.buses.values():
                bus_schedule_filtered = [
                    event for event in bus.schedule if event[0] <= self.current_time
                ]
                for event in bus_schedule_filtered:
                    records.append(
                        {
                            "Bus ID": bus.bus_id,
                            "Time": format_time(event[0]),
                            "Event": event[1],
                            "Stop ID": event[2],
                            "Passengers Onboard": event[3],
                            "Passengers Waiting": event[4],
                            "Boarded": event[5],
                            "Alighted": event[6],
                            "Direction": event[7],
                            "Status": event[8],
                        }
                    )
            out_df = pd.DataFrame(records)
            out_df.to_csv(filename, index=False)
            logger.info(f"Schedule exported to {filename}")
            return out_df
        except Exception as e:
            logger.error(f"Error exporting CSV {filename}: {e}")
            return None

    def export_all_bus_schedules_to_separate_csvs(self):
        for bus_id, bus in self.buses.items():
            filename = f"bus_schedule_{bus_id}.csv"
            records = []
            for event in bus.schedule:
                records.append(
                    {
                        "Bus ID": bus.bus_id,
                        "Time": format_time(event[0]),
                        "Event": event[1],
                        "Stop ID": event[2],
                        "Passengers Onboard": event[3],
                        "Passengers Waiting": event[4],
                        "Boarded": event[5],
                        "Alighted": event[6],
                        "Direction": event[7],
                        "Status": event[8],
                    }
                )
            if records:
                out_df = pd.DataFrame(records)
                out_df.to_csv(filename, index=False)
                logger.info(f"Schedule for Bus {bus_id} exported to {filename}")
            else:
                logger.info(f"No schedule records for Bus {bus_id} to export.")

    def check_bus_return_to_start(self):
        return_status = {}
        logger.info("\n===== Checking Bus Return to Start Points =====")
        for bus_id, bus in self.buses.items():
            final_stop_id = None
            for i in reversed(range(len(bus.schedule))):
                record = bus.schedule[i]
                event_status = record[8]
                event_stop_id = record[2]
                if event_stop_id is not None and event_status in [
                    "AT_STOP",
                    "IDLE",
                    "COMPLETED_ROUTE",
                    "AT_DEPOT",
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
