import logging
from ortools.linear_solver import pywraplp
from sqlalchemy.orm import Session
from datetime import time, timedelta, datetime
import math

# Import your database models
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
from api.database import get_db

logger = logging.getLogger(__name__)


class FrequencyOptimiser:
    def __init__(self, num_slots, slot_length, layover, solver_name="SCIP"):
        self.num_slots = num_slots
        self.slot_length = slot_length
        self.layover_delay = layover
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

        self.travel_times = {}
        for r_id in self.routes:
            route_def_list = self.routes_definitions.get(r_id, [])
            for i in range(len(route_def_list) - 1):
                from_sp = route_def_list[i].stop_point_id
                to_sp = route_def_list[i+1].stop_point_id
                if (from_sp, to_sp) not in self.travel_times:
                    self.travel_times[(from_sp, to_sp)] = 5

        self.trip_length_on_route = []
        self.trip_duration_in_slots = []

        for r_idx, route_id in enumerate(self.routes):
            trip_length_minutes = 0
            route_def_list = self.routes_definitions.get(route_id, [])
            for i in range(len(route_def_list) - 1):
                from_stop_id = route_def_list[i].stop_point_id
                to_stop_id = route_def_list[i + 1].stop_point_id
                travel_time_minutes = self.travel_times.get((from_stop_id, to_stop_id), 0)
                trip_length_minutes += travel_time_minutes
            self.trip_length_on_route.append(trip_length_minutes)

            total_trip_time_minutes = trip_length_minutes + self.layover_delay
            self.trip_duration_in_slots.append(max(1, math.ceil(total_trip_time_minutes / self.slot_length)))

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
        logger.info("Data loading complete.")


    def optimise_frequencies(self, db: Session, start_time_minutes: int = 0):
        logger.info("Starting frequency optimization...")

        if not self.routes or not self.bus_types or not self.stops or not self.demand:
            logger.error("Missing data. Please ensure fit_data was successful and data exists.")
            return

        rts = range(len(self.routes))
        bts = range(len(self.bus_types))
        ts = range(self.num_slots)
        ss = range(len(self.stops))

        x = {}
        for r_idx in rts:
            for b_idx in bts:
                for t_idx in ts:
                    x[r_idx, b_idx, t_idx] = self.solver.IntVar(0, self.num_avl_buses.get(self.bus_types[b_idx], 0), f"x_{r_idx}_{b_idx}_{t_idx}")

        y = {}
        for r_idx in rts:
            for i_idx in ss:
                for j_idx in ss:
                    if i_idx == j_idx or self.route_coverage[r_idx][i_idx] == 0 or self.route_coverage[r_idx][j_idx] == 0:
                        continue
                    for t_start_idx in ts:
                        for s_idx in ts:
                            if s_idx >= t_start_idx and s_idx < t_start_idx + self.trip_duration_in_slots[r_idx]:
                                y[r_idx, i_idx, j_idx, t_start_idx, s_idx] = self.solver.NumVar(0, self.solver.infinity(), f"y_{r_idx}_{i_idx}_{j_idx}_{t_start_idx}_{s_idx}")

        z = {}
        for r_idx in rts:
            for s_idx in ts:
                z[r_idx, s_idx] = self.solver.NumVar(0, self.solver.infinity(), f"z_{r_idx}_{s_idx}")

        obj = self.solver.Objective()
        for key, var in y.items():
            r_idx, i_idx, j_idx, t_start_idx, s_idx = key
            origin_sp_id = self.stops[i_idx]
            destination_sp_id = self.stops[j_idx]
            origin_area_code = self.stop_point_to_area_map.get(origin_sp_id)
            destination_area_code = self.stop_point_to_area_map.get(destination_sp_id)

            if origin_area_code is not None and destination_area_code is not None:
                actual_demand_for_segment_slot = self.demand.get(origin_area_code, {}).get(destination_area_code, {}).get(s_idx, 0)
                obj.SetCoefficient(var, actual_demand_for_segment_slot)
        obj.SetMaximization()

        for r_idx in rts:
            for t_idx in ts:
                total_capacity_at_t = self.solver.Sum(
                    x[r_idx, b_idx, start_slot_idx] * self.max_capacity[self.bus_types[b_idx]]
                    for b_idx in bts
                    for start_slot_idx in ts
                    if start_slot_idx <= t_idx < start_slot_idx + self.trip_duration_in_slots[r_idx]
                )
                self.solver.Add(z[r_idx, t_idx] <= total_capacity_at_t, f"Capacity_R{r_idx}_T{t_idx}")

        for origin_sp_id, dest_demands in self.demand.items():
            for destination_sp_id, slot_demands in dest_demands.items():
                for s_slot_idx, actual_demand in slot_demands.items():
                    try:
                        i_idx = self.stops.index(origin_sp_id)
                        j_idx = self.stops.index(destination_sp_id)
                    except ValueError:
                        continue

                    for r_idx in rts:
                        if self.route_coverage[r_idx][i_idx] == 0 or self.route_coverage[r_idx][j_idx] == 0:
                            continue

                        relevant_y_vars = []
                        for t_start_idx in ts:
                            if (r_idx, i_idx, j_idx, t_start_idx, s_slot_idx) in y:
                                relevant_y_vars.append(y[r_idx, i_idx, j_idx, t_start_idx, s_slot_idx])

                        if relevant_y_vars:
                            self.solver.Add(
                                self.solver.Sum(relevant_y_vars) <= actual_demand,
                                f"DemandSat_R{r_idx}_S{origin_sp_id}to{destination_sp_id}_Slot{s_slot_idx}"
                            )

        for r_idx in rts:
            for s_idx in ts:
                relevant_y_vars_for_z = []
                for i_idx in ss:
                    for j_idx in ss:
                        if i_idx == j_idx:
                            continue
                        for t_start_idx in ts:
                            if (r_idx, i_idx, j_idx, t_start_idx, s_idx) in y:
                                relevant_y_vars_for_z.append(y[r_idx, i_idx, j_idx, t_start_idx, s_idx])
                if relevant_y_vars_for_z:
                    self.solver.Add(
                        z[r_idx, s_idx] == self.solver.Sum(relevant_y_vars_for_z),
                        f"Z_Def_R{r_idx}_S{s_idx}"
                    )
                else:
                    self.solver.Add(z[r_idx, s_idx] == 0, f"Z_Def_R{r_idx}_S{s_idx}_NoY")

        for b_idx in bts:
            bus_type_id = self.bus_types[b_idx]
            self.solver.Add(
                self.solver.Sum([x[r_idx, b_idx, t_idx] for r_idx in rts for t_idx in ts]) <= self.num_avl_buses.get(bus_type_id, 0),
                f"GlobalBusAvailability_BType{bus_type_id}"
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

        if status == pywraplp.Solver.OPTIMAL or status == pywraplp.Solver.FEASIBLE:
            logger.info("Optimization successful!.")
            logger.info(f"Objective (Total passengers served): {int(self.solver.Objective().Value())}")

            logger.info("Creating Vehicle journeys based on optimized frequencies.")

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
                    line_id=default_line.line_id, # Added line_id here
                )
                db.add(default_service)
                db.flush()

            if not default_operator or not default_line or not default_service:
                logger.error("Default Operator, Line, or Service could not be created/found. Cannot create VehicleJourneys.")
                db.rollback()
                return

            bus_counter_by_type = {bt_id: 0 for bt_id in self.bus_types}

            for r_idx, route_id in enumerate(self.routes):
                route_obj = self.route_objects[route_id]
                r_name = route_obj.name

                for b_idx, bus_type_id in enumerate(self.bus_types):
                    b_name = self.bus_type_names[bus_type_id]
                    for t_idx in range(self.num_slots):
                        num_trips_to_assign = int(x[r_idx, b_idx, t_idx].solution_value())

                        for _ in range(num_trips_to_assign):
                            if bus_counter_by_type[bus_type_id] < self.num_avl_buses.get(bus_type_id, 0):
                                bus_reg_num_assigned = self.bus_ids_by_type[bus_type_id][bus_counter_by_type[bus_type_id]]
                                bus_counter_by_type[bus_type_id] += 1
                            else:
                                logger.warning(f"Not enough physical buses of type {b_name} available for all assigned trips on Route {r_name} at Slot {t_idx}. Max available: {self.num_avl_buses.get(bus_type_id, 0)}")
                                break

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
                                    direction="outbound"
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

                            new_vj = VehicleJourney(
                                departure_time=departure_time_obj,
                                dayshift=1,
                                jp_id=assigned_jp.jp_id,
                                block_id=assigned_block.block_id,
                                operator_id=default_operator.operator_id,
                                line_id=default_line.line_id,
                                service_id=default_service.service_id
                            )
                            db.add(new_vj)

            db.commit()
            logger.info("Vehicle journeys created based on optimized frequencies.")

            for r_idx in rts:
                r_name = self.route_objects[self.routes[r_idx]].name
                for i_idx in ss:
                    i_sp_id = self.stops[i_idx]
                    i_stop_name = self.lookup_stops[i_sp_id].name
                    for j_idx in ss:
                        j_sp_id = self.stops[j_idx]
                        j_stop_name = self.lookup_stops[j_sp_id].name
                        if i_sp_id == j_sp_id:
                            continue
                        for t_start_idx in ts:
                            for s_idx in ts:
                                if (r_idx, i_idx, j_idx, t_start_idx, s_idx) in y:
                                    if y[r_idx, i_idx, j_idx, t_start_idx, s_idx].solution_value() > 0.5:
                                        logger.info(
                                            f"  Route {r_name}, {i_stop_name} -> {j_stop_name}, slot {s_idx} "
                                            f"from bus starting at {t_start_idx}: {int(y[r_idx, i_idx, j_idx, t_start_idx, s_idx].solution_value())} passengers"
                                        )
        else:
            logger.warning("No optimal or feasible solution found for frequency optimization.")

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)

    try:
        optimiser = FrequencyOptimiser(num_slots=24, slot_length=60, layover=10)
        with next(get_db()) as db:
            optimiser.fit_data(db, start_time_minutes=0)
            optimiser.optimise_frequencies(db, start_time_minutes=0)

    except Exception as e:
        logger.error(f"An error occurred during optimization: {e}")
        import traceback
        traceback.print_exc()