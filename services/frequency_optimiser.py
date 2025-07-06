import logging
from ortools.linear_solver import pywraplp
from sqlalchemy.orm import Session
from datetime import time, timedelta, datetime
import math
import os # Added for path manipulation
import sys # Added for path manipulation

# Add the project root to sys.path to enable absolute imports
# This assumes the script is located at your_project_root/services/frequency_optimiser.py
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

# Import your database models (changed to absolute imports)
from api.models import (
    Bus,
    BusType,
    Demand,
    Route,
    StopArea,
    StopPoint,
    RouteDefinition,
    VehicleJourney,
    Block,
    Operator,
    Line,
    JourneyPattern,
    Service,
)
from api.database import get_db # Changed to absolute import

logger = logging.getLogger(__name__)


class FrequencyOptimiser:
    def __init__(self, num_slots, slot_length, layover, solver_name="SCIP",
                 min_demand_threshold=1.0, min_frequency_trips_per_period=1, min_frequency_period_minutes=60):
        self.num_slots = num_slots
        self.slot_length = slot_length
        self.layover_delay = layover
        self.min_demand_threshold = min_demand_threshold
        self.min_frequency_trips_per_period = min_frequency_trips_per_period
        self.min_frequency_period_minutes = min_frequency_period_minutes
        self.demand = {}
        self.stops = []
        self.lookup_stops = {}
        self.routes = []
        self.route_ids = {}
        self.trip_length_on_route = []
        self.trip_duration_in_slots = []
        self.route_coverage = []
        self.bus_types = []
        self.max_capacity = {}
        self.num_avl_buses = {}
        self.bus_ids_by_type = {}
        self.solver = pywraplp.Solver.CreateSolver(solver_name)
        if not self.solver:
            raise RuntimeError(f"Could not create solver {solver_name}")
        self.stop_point_to_area_map = {}

    def fit_data(self, db: Session, start_time_minutes: int = 0):
        logger.info("Loading data from database for optimization...")

        # Clear existing data structures for multiple runs
        self.demand = {}
        self.stops = []
        self.lookup_stops = {}
        self.routes = []
        self.route_ids = {}
        self.trip_length_on_route = []
        self.trip_duration_in_slots = []
        self.route_coverage = []
        self.bus_types = []
        self.max_capacity = {}
        self.num_avl_buses = {}
        self.bus_ids_by_type = {}
        self.stop_point_to_area_map = {}


        # 1. Load Stop Points
        db_stop_points = db.query(StopPoint).all()
        if not db_stop_points:
            logger.warning("No stop points found. Optimization may not be meaningful.")
        self.stops = [sp.atco_code for sp in db_stop_points]
        self.lookup_stops = {sp.atco_code: sp for sp in db_stop_points}
        for sp in db_stop_points:
            self.stop_point_to_area_map[sp.atco_code] = sp.stop_area_code

        logger.info(f"Loaded {len(self.stops)} stop points.")

        # 2. Load Stop Areas and map to representative stop points
        db_stop_areas = db.query(StopArea).all()
        self.lookup_stop_areas = {sa.stop_area_code: sa for sa in db_stop_areas}

        self.stop_area_to_stop_point = {}
        for sa in db_stop_areas:
            representative_sp = db.query(StopPoint).filter(StopPoint.stop_area_code == sa.stop_area_code).first()
            if representative_sp:
                self.stop_area_to_stop_point[sa.stop_area_code] = representative_sp.atco_code
            else:
                logger.warning(f"No stop points found for stop area {sa.stop_area_code}. Demand involving this area might be partially or fully ignored.")

        logger.info(f"Loaded {len(db_stop_areas)} stop areas and mapped to representative stop points.")

        # 3. Load Bus Types
        db_bus_types = db.query(BusType).all()
        if not db_bus_types:
            logger.warning("No bus types found. Cannot optimize frequencies.")
            return
        self.bus_types = [bt.type_id for bt in db_bus_types]
        self.max_capacity = {bt.type_id: bt.capacity for bt in db_bus_types}
        self.bus_type_names = {bt.type_id: bt.name for bt in db_bus_types}
        logger.info(f"Loaded {len(self.bus_types)} bus types.")

        # 4. Load Buses and count available buses by type
        db_buses = db.query(Bus).all()
        for bt_id in self.bus_types:
            self.num_avl_buses[bt_id] = 0
            self.bus_ids_by_type[bt_id] = []

        for bus in db_buses:
            if bus.bus_type_id in self.num_avl_buses:
                self.num_avl_buses[bus.bus_type_id] += 1
                self.bus_ids_by_type[bus.bus_type_id].append(bus.reg_num)
            else:
                logger.warning(f"Bus {bus.reg_num} has unknown bus_type_id {bus.bus_type_id}. Skipping.")

        logger.info(f"Loaded {len(db_buses)} buses. Available buses by type: {self.num_avl_buses}")

        # 5. Load Demand
        db_demand = db.query(Demand).all()
        if not db_demand:
            logger.warning("No demand records found. Optimization will not serve passengers.")
        logger.info(f"Loaded {len(db_demand)} demand records.")

        for d in db_demand:
            # Pre-filtering: Ignore demand below threshold
            if d.count < self.min_demand_threshold:
                # logger.debug(f"Skipping demand {d.origin}-{d.destination} count {d.count} as it's below threshold {self.min_demand_threshold}")
                continue

            origin_sp_id = self.stop_area_to_stop_point.get(d.origin)
            destination_sp_id = self.stop_area_to_stop_point.get(d.destination)

            if origin_sp_id is None or destination_sp_id is None:
                logger.warning(f"Demand origin/destination StopArea ({d.origin}, {d.destination}) could not be mapped to StopPoints. Skipping this demand record.")
                continue

            demand_start_minutes = d.start_time.hour * 60 + d.start_time.minute
            adjusted_demand_start_minutes = demand_start_minutes - start_time_minutes

            if adjusted_demand_start_minutes < 0:
                continue

            demand_slot_idx = adjusted_demand_start_minutes // self.slot_length

            if demand_slot_idx >= self.num_slots:
                continue

            self.demand.setdefault(origin_sp_id, {}).setdefault(destination_sp_id, {})[demand_slot_idx] = \
                self.demand.get(origin_sp_id, {}).get(destination_sp_id, {}).get(demand_slot_idx, 0) + d.count


        # 6. Load Routes and Route Definitions to calculate trip lengths and coverage
        db_routes = db.query(Route).all()
        if not db_routes:
            logger.warning("No routes found. Cannot optimize frequencies.")
            return
        self.routes = [route.route_id for route in db_routes]
        self.route_objects = {route.route_id: route for route in db_routes}
        self.routes_definitions = {route.route_id: [] for route in db_routes}

        for r_def in db.query(RouteDefinition).order_by(RouteDefinition.route_id, RouteDefinition.sequence).all():
            if r_def.route_id in self.routes_definitions:
                self.routes_definitions[r_def.route_id].append(r_def)

        # Initialize these lists to hold values for each route
        self.trip_length_on_route = [0] * len(self.routes)
        self.trip_duration_in_slots = [0] * len(self.routes)

        self.travel_times = {}
        # Populate travel_times for segments (can be default if not in DB)
        for r_id in self.routes:
            route_def_list = self.routes_definitions.get(r_id, [])
            for i in range(len(route_def_list) - 1):
                from_sp = route_def_list[i].stop_point_id
                to_sp = route_def_list[i+1].stop_point_id
                if (from_sp, to_sp) not in self.travel_times:
                    self.travel_times[(from_sp, to_sp)] = 5 # Default travel time of 5 minutes

        # Calculate trip lengths and durations per route
        for r_idx, route_id in enumerate(self.routes):
            route_def_list = self.routes_definitions.get(route_id, [])
            current_route_trip_length_minutes = 0
            if route_def_list: # Only calculate if route has definitions
                for i in range(len(route_def_list) - 1):
                    from_sp = route_def_list[i].stop_point_id
                    to_sp = route_def_list[i+1].stop_point_id
                    # Sum up travel times for each segment of the route
                    current_route_trip_length_minutes += self.travel_times.get((from_sp, to_sp), 0)

            self.trip_length_on_route[r_idx] = current_route_trip_length_minutes
            total_trip_time_minutes = current_route_trip_length_minutes + self.layover_delay
            self.trip_duration_in_slots[r_idx] = max(1, math.ceil(total_trip_time_minutes / self.slot_length))

        self.route_coverage = [[0 for _ in self.stops] for _ in self.routes]
        for r_idx, route_id in enumerate(self.routes):
            route_def_list = self.routes_definitions.get(route_id, [])
            for r_def in route_def_list:
                try:
                    sp_idx = self.stops.index(r_def.stop_point_id)
                    self.route_coverage[r_idx][sp_idx] = 1
                except ValueError:
                    logger.warning(f"StopPoint {r_def.stop_point_id} from route definition not found in loaded stop points.")

        logger.info(f"Loaded {len(self.routes)} routes and their definitions.")


    def optimise_frequencies(self, db: Session, start_time_minutes: int = 0):
        logger.info("Starting frequency optimization...")

        if not self.routes or not self.bus_types or not self.stops:
            logger.error("Missing critical data (routes, bus types, or stops). Please ensure fit_data was successful and data exists.")
            return {
                "status": "ERROR",
                "message": "Missing critical data for optimization.",
                "total_passengers_served": 0,
                "schedule": []
            }

        rts = range(len(self.routes))
        bts = range(len(self.bus_types))
        ts = range(self.num_slots)
        ss = range(len(self.stops))

        x = {} # x[r, b, t] = number of buses of type b assigned to route r starting at time slot t
        for r_idx in rts:
            for b_idx in bts:
                for t_idx in ts:
                    x[r_idx, b_idx, t_idx] = self.solver.IntVar(0, self.num_avl_buses.get(self.bus_types[b_idx], 0), f"x_{r_idx}_{b_idx}_{t_idx}")

        # y[r_idx, i_idx, j_idx, t_start_idx, d_slot_idx] = passengers for demand from origin i_idx to dest j_idx, starting at d_slot_idx, served by route r_idx departing at t_start_idx
        y = {}
        for r_idx in rts:
            route_def_list = self.routes_definitions.get(self.routes[r_idx], [])
            if not route_def_list:
                continue

            for origin_sp_id, dest_demands in self.demand.items():
                for destination_sp_id, slot_demands in dest_demands.items():
                    try:
                        i_idx = self.stops.index(origin_sp_id)
                        j_idx = self.stops.index(destination_sp_id)
                    except ValueError:
                        continue # Should not happen if fit_data is correct

                    if i_idx == j_idx or self.route_coverage[r_idx][i_idx] == 0 or self.route_coverage[r_idx][j_idx] == 0:
                        continue

                    # Ensure origin appears before destination on the route
                    origin_seq = -1
                    dest_seq = -1
                    for rd in route_def_list:
                        if rd.stop_point_id == origin_sp_id:
                            origin_seq = rd.sequence
                        if rd.stop_point_id == destination_sp_id:
                            dest_seq = rd.sequence

                    if not (0 <= origin_seq < dest_seq):
                        continue # Route does not directly go from origin to destination in forward sequence


                    for d_slot_idx in slot_demands.keys(): # Loop over actual DEMAND START SLOTS
                        for t_start_idx in ts: # Loop over ALL possible TRIP DEPARTURE SLOTS
                            # Condition: A trip departing at t_start_idx can serve demand starting at d_slot_idx.
                            # Assume a trip serves demand in its departure slot.
                            if t_start_idx == d_slot_idx: # NEW CONDITION ADDED HERE
                                y[r_idx, i_idx, j_idx, t_start_idx, d_slot_idx] = self.solver.NumVar(0, self.solver.infinity(), f"y_{r_idx}_{i_idx}_{j_idx}_{t_start_idx}_{d_slot_idx}")

        z = {} # z[r, s] = total passengers on route r at time slot s (current passengers on board)
        for r_idx in rts:
            for current_slot_idx in ts:
                z[r_idx, current_slot_idx] = self.solver.NumVar(0, self.solver.infinity(), f"z_{r_idx}_{current_slot_idx}")

        obj = self.solver.Objective()
        for key, var in y.items():
            obj.SetCoefficient(var, 1)

        # Add a small penalty for each trip to encourage sparsity
        TRIP_COST_PENALTY = 0.001 # A small value less than 1 passenger
        for r_idx in rts:
            for b_idx in bts:
                for t_idx in ts:
                    obj.SetCoefficient(x[r_idx, b_idx, t_idx], -TRIP_COST_PENALTY)

        obj.SetMaximization()

        # Constraint 1: Capacity Constraint
        for r_idx in rts:
            for current_slot_idx in ts: # This is the slot when passengers are ON THE BUS
                total_capacity_at_current_slot = self.solver.Sum(
                    x[r_idx, b_idx, t_start_idx] * self.max_capacity[self.bus_types[b_idx]]
                    for b_idx in bts
                    for t_start_idx in ts # Trip's departure slot
                    # A trip departing at t_start_idx is active at current_slot_idx
                    if t_start_idx <= current_slot_idx < t_start_idx + self.trip_duration_in_slots[r_idx]
                )
                self.solver.Add(z[r_idx, current_slot_idx] <= total_capacity_at_current_slot, f"Capacity_R{r_idx}_CSlot{current_slot_idx}")

        # Constraint 2: Demand Satisfaction Constraint
        # Passengers served cannot exceed actual demand for any O-D pair at any demand start time slot
        for origin_sp_id, dest_demands in self.demand.items():
            for destination_sp_id, slot_demands in dest_demands.items():
                for d_slot_idx, actual_demand in slot_demands.items(): # This d_slot_idx is the demand's start slot
                    try:
                        i_idx = self.stops.index(origin_sp_id)
                        j_idx = self.stops.index(destination_sp_id)
                    except ValueError:
                        continue

                    # Sum of passengers served for this O-D pair and demand slot across all relevant routes and trip starts
                    relevant_y_vars = []
                    for r_idx in rts:
                        # Check if route covers O-D (already handled in y creation, but safety check)
                        if self.route_coverage[r_idx][i_idx] == 0 or self.route_coverage[r_idx][j_idx] == 0:
                            continue

                        # Ensure origin appears before destination on the route
                        route_def_list = self.routes_definitions.get(self.routes[r_idx], [])
                        origin_seq = -1
                        dest_seq = -1
                        for rd in route_def_list:
                            if rd.stop_point_id == origin_sp_id:
                                origin_seq = rd.sequence
                            if rd.stop_point_id == destination_sp_id:
                                dest_seq = rd.sequence

                        if not (0 <= origin_seq < dest_seq):
                            continue # Route does not directly go from origin to destination in forward sequence

                        for t_start_idx in ts:
                            if (r_idx, i_idx, j_idx, t_start_idx, d_slot_idx) in y: # Check if this specific y variable exists
                                relevant_y_vars.append(y[r_idx, i_idx, j_idx, t_start_idx, d_slot_idx])

                    if relevant_y_vars: # Only add constraint if there are variables to sum
                        self.solver.Add(
                            self.solver.Sum(relevant_y_vars) <= actual_demand,
                            f"DemandSat_SP{origin_sp_id}toSP{destination_sp_id}_DSlot{d_slot_idx}"
                        )

        # Constraint 3: Definition of Z (Total passengers on board)
        for r_idx in rts:
            for current_slot_idx in ts: # This `current_slot_idx` is the time slot for which we are calculating total passengers ON BOARD
                relevant_y_vars_for_z = []
                for i_idx in ss:
                    for j_idx in ss:
                        if i_idx == j_idx:
                            continue
                        for t_start_idx in ts: # Trip departure slot
                            # If a trip departs at t_start_idx and is active (carrying passengers) at current_slot_idx
                            if t_start_idx <= current_slot_idx < t_start_idx + self.trip_duration_in_slots[r_idx]:
                                # Then, any passengers served by this trip from any demand slot (d_slot_idx)
                                # will be on board at current_slot_idx.
                                # IMPORTANT: Now d_slot_idx MUST be equal to t_start_idx for y to exist based on the new y creation logic.
                                # So, we only need to consider y where d_slot_idx == t_start_idx
                                d_slot_idx = t_start_idx # This implicitly enforces the new y creation rule here as well
                                if (r_idx, i_idx, j_idx, t_start_idx, d_slot_idx) in y:
                                    relevant_y_vars_for_z.append(y[r_idx, i_idx, j_idx, t_start_idx, d_slot_idx])

                if relevant_y_vars_for_z:
                    self.solver.Add(
                        z[r_idx, current_slot_idx] == self.solver.Sum(relevant_y_vars_for_z),
                        f"Z_Def_R{r_idx}_CSlot{current_slot_idx}"
                    )
                else:
                    self.solver.Add(z[r_idx, current_slot_idx] == 0, f"Z_Def_R{r_idx}_CSlot{current_slot_idx}_NoY")

        # Constraint 4: Minimum Frequency Constraint
        min_freq_period_slots = math.ceil(self.min_frequency_period_minutes / self.slot_length)
        if min_freq_period_slots <= 0:
            logger.warning("Minimum frequency period is too small or invalid. Skipping min frequency constraint.")
        else:
            for r_idx in rts:
                for period_start_slot in range(0, self.num_slots, min_freq_period_slots):
                    period_end_slot = min(period_start_slot + min_freq_period_slots -1, self.num_slots - 1)

                    trips_in_period = self.solver.Sum(
                        x[r_idx, b_idx, t_idx]
                        for b_idx in bts
                        for t_idx in range(period_start_slot, period_end_slot + 1)
                    )
                    self.solver.Add(
                        trips_in_period >= self.min_frequency_trips_per_period,
                        f"MinFreq_R{r_idx}_Period{period_start_slot}-{period_end_slot}"
                    )

        logger.info("Solving optimization problem...")
        status = self.solver.Solve()

        solver_status_map = {
            pywraplp.Solver.OPTIMAL: "OPTIMAL",
            pywraplp.Solver.FEASIBLE: "FEASIBLE",
            pywraplp.Solver.INFEASIBLE: "INFEASIBLE",
            pywraplp.Solver.UNBOUNDED: "UNBOUNDED",
            pywraplp.Solver.ABNORMAL: "ABNORMAL",
            pywraplp.Solver.MODEL_INVALID: "MODEL_INVALID",
            pywraplp.Solver.NOT_SOLVED: "NOT_SOLVED",
        }
        status_name_str = solver_status_map.get(status, f"UNKNOWN STATUS ({status})")
        logger.info(f"Optimization run completed with status: {status_name_str}")

        result = {
            "status": status_name_str,
            "total_passengers_served": 0,
            "schedule": [],
            "message": "Optimization completed.",
            "solver_runtime_ms": self.solver.wall_time(),
            "solver_iterations": self.solver.iterations()
        }

        if status == pywraplp.Solver.OPTIMAL or status == pywraplp.Solver.FEASIBLE:
            logger.info("Optimization successful!.")
            total_served = int(self.solver.Objective().Value())
            # The objective value will now be (Passengers - Penalty * Trips).
            # To get actual passengers, we need to manually sum y values.
            actual_passengers_served = 0
            for key, var in y.items():
                if var.solution_value() > 0.5:
                    actual_passengers_served += int(var.solution_value())

            logger.info(f"Objective (Total passengers served): {actual_passengers_served}")
            result["total_passengers_served"] = actual_passengers_served

            logger.info("Creating Vehicle journeys and building structured schedule.")

            default_operator = db.query(Operator).filter_by(operator_code="OP1").first()
            if not default_operator:
                default_operator = Operator(operator_code="OPT1", name="Optimised Ops")
                db.add(default_operator)
                db.flush()

            default_line = db.query(Line).filter_by(line_name="Optimized Line").first()
            if not default_line:
                default_line = Line(line_name="Optimized Line", operator_id=default_operator.operator_id)
                db.add(default_line)
                db.flush()

            default_service = db.query(Service).filter_by(service_code="SVC1").first()
            if not default_service:
                default_service = Service(
                    service_code="SVC1",
                    name="Optimized Service",
                    operator_id=default_operator.operator_id,
                    line_id=default_line.line_id,
                )
                db.add(default_service)
                db.flush()

            if not default_operator or not default_line or not default_service:
                logger.error("Default Operator, Line, or Service could not be created/found. Cannot create VehicleJourneys.")
                result["message"] = "Error: Default entities not found/created for VJ generation."
                db.rollback()
                return result

            bus_counter_by_type = {bt_id: 0 for bt_id in self.bus_types}

            for r_idx, route_id in enumerate(self.routes):
                route_obj = self.route_objects[route_id]
                r_name = route_obj.name

                for b_idx, bus_type_id in enumerate(self.bus_types):
                    b_name = self.bus_type_names[bus_type_id]
                    for t_idx in range(self.num_slots):
                        num_trips_to_assign = int(x[r_idx, b_idx, t_idx].solution_value())

                        for _ in range(num_trips_to_assign):
                            bus_reg_num_assigned = None
                            if bus_counter_by_type[bus_type_id] < self.num_avl_buses.get(bus_type_id, 0):
                                bus_reg_num_assigned = self.bus_ids_by_type[bus_type_id][bus_counter_by_type[bus_type_id]]
                                bus_counter_by_type[bus_type_id] += 1
                            else:
                                logger.warning(f"Not enough physical buses of type {b_name} available for all assigned trips on Route {r_name} at Slot {t_idx}. Max available: {self.num_avl_buses.get(bus_type_id, 0)}. Creating VJ without specific bus registration.")

                            jp_code = f"JP_CODE_{r_name}_T{t_idx}_B{b_name}_Trip{_}"
                            assigned_jp = db.query(JourneyPattern).filter_by(jp_code=jp_code).first()
                            if not assigned_jp:
                                assigned_jp = JourneyPattern(
                                    jp_code=jp_code,
                                    name=f"JP_{r_name}_T{t_idx}_Trip{_}",
                                    route_id=route_obj.route_id,
                                    service_id=default_service.service_id,
                                    line_id=default_line.line_id,
                                    operator_id=default_operator.operator_id,
                                )
                                db.add(assigned_jp)
                                db.flush()

                            block_name = f"BLOCK_{r_name}_T{t_idx}_BType{bus_type_id}_Trip{_}"
                            assigned_block = db.query(Block).filter_by(name=block_name).first()
                            if not assigned_block:
                                assigned_block = Block(
                                    name=block_name,
                                    operator_id=default_operator.operator_id,
                                    bus_type_id=bus_type_id,
                                )
                                db.add(assigned_block)
                                db.flush()

                            departure_minutes = start_time_minutes + t_idx * self.slot_length
                            departure_time_obj = (datetime.min + timedelta(minutes=departure_minutes)).time()
                            departure_time_str = departure_time_obj.strftime("%H:%M")

                            new_vj = VehicleJourney(
                                departure_time=departure_time_obj,
                                dayshift=1, # Default to day shift
                                jp_id=assigned_jp.jp_id,
                                block_id=assigned_block.block_id,
                                operator_id=default_operator.operator_id,
                                line_id=default_line.line_id,
                                service_id=default_service.service_id,
                            )
                            db.add(new_vj)

                            # Add to structured output
                            result["schedule"].append({
                                "route_id": route_id,
                                "route_name": r_name,
                                "bus_type_id": bus_type_id,
                                "bus_type_name": b_name,
                                "departure_time_slot": t_idx,
                                "departure_time_minutes": departure_minutes,
                                "departure_time_str": departure_time_str,
                                "assigned_bus_reg_num": bus_reg_num_assigned,
                                "trip_duration_minutes": self.trip_length_on_route[r_idx],
                                "trip_duration_slots": self.trip_duration_in_slots[r_idx]
                            })

            db.commit()
            logger.info("Vehicle journeys created and structured schedule built.")

            # Log detailed passenger service per segment
            for key, var in y.items():
                r_idx, i_idx, j_idx, t_start_idx, d_slot_idx = key
                served_passengers = var.solution_value()
                if served_passengers > 0.5: # Use 0.5 threshold for floating point solutions
                    r_name = self.route_objects[self.routes[r_idx]].name
                    i_stop_name = self.lookup_stops[self.stops[i_idx]].name
                    j_stop_name = self.lookup_stops[self.stops[j_idx]].name
                    logger.info(
                        f"  Route {r_name}, {i_stop_name} -> {j_stop_name}, Demand Slot {d_slot_idx} "
                        f"(served by trip starting at {t_start_idx}): {int(served_passengers)} passengers"
                    )
        else:
            logger.warning("No optimal or feasible solution found for frequency optimization.")
            result["message"] = "Optimization failed to find an optimal or feasible solution."
            db.rollback() # Rollback any partial changes if not optimal/feasible

        return result

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)

    try:
        # Configuration for the optimizer
        optimiser = FrequencyOptimiser(
            num_slots=24,
            slot_length=60,
            layover=10,
            min_demand_threshold=1.0,
            min_frequency_trips_per_period=1,     # Relaxed: At least 1 trip
            min_frequency_period_minutes=24 * 60  # Relaxed: Over the entire day (1440 minutes)
        )
        with next(get_db()) as db:
            optimiser.fit_data(db, start_time_minutes=0)
            optimisation_results = optimiser.optimise_frequencies(db, start_time_minutes=0)

            # Print the structured results
            print("\n--- Optimization Results Summary ---")
            print(f"Status: {optimisation_results['status']}")
            print(f"Total Passengers Served: {optimisation_results['total_passengers_served']}")
            print(f"Message: {optimisation_results['message']}")
            print(f"Solver Runtime (ms): {optimisation_results['solver_runtime_ms']}")
            print(f"Solver Iterations: {optimisation_results['solver_iterations']}")
            print("\n--- Scheduled Trips ---")
            if optimisation_results['schedule']:
                for i, trip in enumerate(optimisation_results['schedule']):
                    print(f"  Trip {i+1}: Route {trip['route_name']} ({trip['route_id']}), "
                          f"Bus Type: {trip['bus_type_name']} ({trip['bus_type_id']}), "
                          f"Departure: {trip['departure_time_str']} (Slot {trip['departure_time_slot']}), "
                          f"Assigned Bus: {trip['assigned_bus_reg_num'] if trip['assigned_bus_reg_num'] else 'N/A'}")
            else:
                print("  No trips scheduled.")
            print("------------------------------------\n")


    except Exception as e:
        logger.error(f"An error occurred during optimization: {e}")
        import traceback
        traceback.print_exc()
