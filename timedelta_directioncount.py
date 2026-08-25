# timedelta_directioncount.py
# Consumes messages from Kafka topic 'direction', aggregates vehicle/pedestrian/bike
# movements per time window, and produces aggregated results to topic 'directioncount'.
#
# Counting model: each 'direction' message is one completed crossing. We accumulate
# counts for a fixed wall-clock window, emit at the boundary, then reset and start the
# next window.
#
# NOTE ON THE OVERCOUNT FIX (2026-08): the previous version wrapped `for message in
# consumer` inside `while now < future` and re-entered the iterator every window while
# resetting the dedup set. A rebalance / uncommitted-offset replay at the window boundary
# then re-delivered the same messages, and they were recounted fresh -- inflating counts
# (~2x at 15-min windows, worse at shorter windows, since the recount scales with the
# number of window boundaries). This version consumes with a single poll() loop, commits
# offsets manually at each emit, and dedups on (partition, offset) so no message is ever
# counted twice.

from kafka import KafkaConsumer, KafkaProducer
import json
import pytz
from datetime import datetime, timedelta

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
BOOTSTRAP_SERVERS = 'localhost:9092'
IN_TOPIC = 'direction'
OUT_TOPIC = 'directioncount'
# Aggregation window. 15*60 = 15 minutes (production). Set to 1*60 for 1-minute testing.
WINDOW_SECONDS = 15 * 60
# How long each poll() waits for records before returning control so the window clock
# can be re-checked during quiet periods.
POLL_TIMEOUT_MS = 1000

est = pytz.timezone('America/New_York')

directions = ["nn", "ns", "ne", "nw", "ss", "sn", "se", "sw",
              "ee", "en", "es", "ew", "ww", "wn", "ws", "we"]

################ change this to add more bike/ped only cameras ################
bikepedonly = ["3157B", "3157A", "3156", "3156A"]
###############################################################################

# Polygon record template ---------------------------------------------------
# Built once by a factory instead of repeating the same ~36-key literal in three places,
# which kept the ped/bike copies from drifting apart.
POLYGON_NAMES = [
    "n-crosswalk", "s-crosswalk", "e-crosswalk", "w-crosswalk",
    "w-sidewalk-n", "w-sidewalk-s", "s-sidewalk-w", "s-sidewalk-e",
    "e-sidewalk-s", "e-sidewalk-n", "n-sidewalk-e", "n-sidewalk-w",
    "w-bikelane-wb", "w-bikelane-eb", "s-bikelane-sb", "s-bikelane-nb",
    "e-bikelane-eb", "e-bikelane-wb", "n-bikelane-nb", "n-bikelane-sb",
    "n-lane-1", "n-lane-2", "n-lane-3", "n-lane-4",
    "s-lane-1", "s-lane-2", "s-lane-3", "s-lane-4",
    "e-lane-1", "e-lane-2", "e-lane-3", "e-lane-4",
    "w-lane-1", "w-lane-2", "w-lane-3", "w-lane-4",
]
PED_FIELDS = (
    "ped-count", "bike-count", "ped-wait-time", "ped-cross-time",
    "ped-violation-count", "bike-violation-count", "ped-wait-time-max", "ped-cross-time-max",
)


def new_polygon_record():
    """One camera's per-polygon counters, all zeroed."""
    return {name: {field: 0 for field in PED_FIELDS} for name in POLYGON_NAMES}


def now_est():
    """Current time formatted in America/New_York, as used in every emitted record."""
    return datetime.now().astimezone(est).strftime("%Y-%m-%d %H:%M:%S")


class WindowState:
    """All the per-window accumulators. Recreated at every window boundary so a new
    window starts from zero and nothing carries over."""

    def __init__(self):
        self.camera_dictionary = {}        # cam -> {direction: count}
        self.road_dictionary = {}          # "cam-direction" -> {start_road_name, end_road_name}
        self.polygon_dictionary_ped = {}   # cam -> polygon record (ped)
        self.polygon_dictionary_bike = {}  # cam -> polygon record (bike)
        self.seen_offsets = set()          # (partition, offset) already counted this window
        self.message_count = 0


# ---------------------------------------------------------------------------
# Counting
# ---------------------------------------------------------------------------
def process_message(data, state):
    """Fold one 'direction' message into the current window's accumulators."""
    camSensorId = data['sensor_id']
    classType = data['class']

    if classType == "Person":
        rec = state.polygon_dictionary_ped.setdefault(camSensorId, new_polygon_record())
        _count_polygons(data, rec, count_field="ped-count", violation_field="ped-violation-count",
                        is_bike=False)

    elif classType == "bicycle":
        rec = state.polygon_dictionary_bike.setdefault(camSensorId, new_polygon_record())
        _count_polygons(data, rec, count_field="bike-count", violation_field="bike-violation-count",
                        is_bike=True)

    elif classType == "car":
        cam_dirs = state.camera_dictionary.setdefault(camSensorId, {d: 0 for d in directions})
        direction = str(data['start_direction']) + str(data['end_direction'])
        # Only count recognized directions; "unknownunknown" etc. fall through, as before.
        if direction in cam_dirs:
            key = str(camSensorId) + "-" + direction
            if key not in state.road_dictionary:
                state.road_dictionary[key] = {"start_road_name": data['start_direction'],
                                              "end_road_name": data['end_direction']}
            cam_dirs[direction] += 1


def _count_polygons(data, rec, count_field, violation_field, is_bike):
    """Shared ped/bike per-polygon accumulation (they differ only in which count/violation
    field is incremented and how the violation is looked up)."""
    waiting = data.get('waiting_time')
    crossing = data.get('crossing_time')
    vd = data.get('violation_details')
    is_violation = data.get('violation') is True

    for poly in data.get('polygons', []):
        if poly not in rec:
            continue
        cell = rec[poly]
        cell[count_field] += 1

        if isinstance(waiting, dict) and poly in waiting:
            cell["ped-wait-time"] += waiting[poly]
            if waiting[poly] > cell["ped-wait-time-max"]:
                cell["ped-wait-time-max"] = waiting[poly]
        if isinstance(crossing, dict) and poly in crossing:
            cell["ped-cross-time"] += crossing[poly]
            if crossing[poly] > cell["ped-cross-time-max"]:
                cell["ped-cross-time-max"] = crossing[poly]

        if is_violation:
            if is_bike:
                # bike violations are keyed under violation_details['pedestrian_lane']
                if isinstance(vd, dict) and poly in vd.get('pedestrian_lane', ''):
                    cell[violation_field] += 1
            else:
                if isinstance(vd, (str, dict, list)) and poly in vd:
                    cell[violation_field] += 1


# ---------------------------------------------------------------------------
# Emit
# ---------------------------------------------------------------------------
def _send(producer, val):
    producer.send(OUT_TOPIC, value=json.dumps(val).encode('utf-8'))
    print("sending to topic", OUT_TOPIC, "value", val)


def emit_window(state, producer):
    """Push one aggregated record per non-zero (camera, direction) and per non-zero
    (camera, polygon) to the output topic."""
    stamp = now_est()

    # Vehicles: per camera (excluding bike/ped-only cameras), per direction with count > 0.
    for cam, dir_counts in state.camera_dictionary.items():
        if str(cam) in bikepedonly:
            continue
        print(str(cam), ":", dir_counts)
        for direc, count in dir_counts.items():
            if count <= 0:
                continue
            roads = state.road_dictionary.get(str(cam) + "-" + str(direc))
            if roads is None:
                continue
            _send(producer, {
                "id": cam, "class": "vehicle", "polygon": "",
                "ped_count": "", "bike_count": "", "ped_wait_time": "", "ped_cross_time": "",
                "ped_violation_count": "", "bike_violation_count": "",
                "ped_wait_time_max": "", "ped_cross_time_max": "",
                "time": stamp, "rddir": direc,
                "start_direction": str(direc)[0], "end_direction": str(direc)[1],
                "count": count,
                "start_road_name": roads["start_road_name"], "end_road_name": roads["end_road_name"],
                "bikepedonly": "false",
            })

    # Pedestrians: per camera, per polygon with ped-count > 0.
    for cam, polys in state.polygon_dictionary_ped.items():
        flag = "true" if str(cam) in bikepedonly else "false"
        for poly, pv in polys.items():
            if pv["ped-count"] <= 0:
                continue
            _send(producer, {
                "id": cam, "class": "ped", "polygon": poly,
                "ped_count": pv["ped-count"], "bike_count": "",
                "ped_wait_time": pv["ped-wait-time"], "ped_cross_time": pv["ped-cross-time"],
                "ped_violation_count": pv["ped-violation-count"],
                "bike_violation_count": pv["bike-violation-count"],
                "ped_wait_time_max": pv["ped-wait-time-max"],
                "ped_cross_time_max": pv["ped-cross-time-max"],
                "time": stamp, "rddir": "",
                "start_direction": "", "end_direction": "", "count": "",
                "start_road_name": "", "end_road_name": "",
                "bikepedonly": flag,
            })

    # Bikes: per camera, per polygon with bike-count > 0.
    for cam, polys in state.polygon_dictionary_bike.items():
        flag = "true" if str(cam) in bikepedonly else "false"
        for poly, pv in polys.items():
            if pv["bike-count"] <= 0:
                continue
            _send(producer, {
                "id": cam, "class": "bike", "polygon": poly,
                "ped_count": "", "bike_count": pv["bike-count"],
                "ped_wait_time": pv["ped-wait-time"], "ped_cross_time": pv["ped-cross-time"],
                "ped_violation_count": pv["ped-violation-count"],
                "bike_violation_count": pv["bike-violation-count"],
                "ped_wait_time_max": pv["ped-wait-time-max"],
                "ped_cross_time_max": pv["ped-cross-time-max"],
                "time": stamp, "rddir": "",
                "start_direction": "", "end_direction": "", "count": "",
                "start_road_name": "", "end_road_name": "",
                "bikepedonly": flag,
            })

    print("number of messages:", state.message_count)


# ---------------------------------------------------------------------------
# Main consume loop
# ---------------------------------------------------------------------------
def main():
    producer = KafkaProducer(bootstrap_servers=BOOTSTRAP_SERVERS)
    # Manual commit: offsets advance only after a window's messages are counted and the
    # results are flushed, so a rebalance/restart cannot replay an already-counted window.
    consumer = KafkaConsumer(IN_TOPIC,
                             bootstrap_servers=BOOTSTRAP_SERVERS,
                             group_id='directioncount-consumer',
                             enable_auto_commit=False,
                             auto_offset_reset='latest')

    state = WindowState()
    window_end = datetime.now() + timedelta(seconds=WINDOW_SECONDS)
    print("window start", datetime.now(), "-> end", window_end)

    try:
        while True:
            now = datetime.now()
            if now >= window_end:
                emit_window(state, producer)
                producer.flush()
                consumer.commit()          # durably advance past this window's messages
                state = WindowState()
                # Advance the boundary, catching up if a slow window overran several boundaries.
                while window_end <= now:
                    window_end += timedelta(seconds=WINDOW_SECONDS)
                print("window start", now, "-> end", window_end)
                continue

            records = consumer.poll(timeout_ms=POLL_TIMEOUT_MS)
            for _tp, messages in records.items():
                for message in messages:
                    key = (message.partition, message.offset)
                    if key in state.seen_offsets:
                        continue          # guard against redelivery within this window
                    state.seen_offsets.add(key)
                    try:
                        data = json.loads(message.value.decode('utf-8'))
                    except (ValueError, UnicodeDecodeError) as e:
                        print("skipping undecodable message:", e)
                        continue
                    try:
                        process_message(data, state)
                        state.message_count += 1
                    except (KeyError, TypeError) as e:
                        print("skipping malformed message:", e)
                        continue
    finally:
        # Do not emit the in-progress (partial) window on shutdown -- it would be an
        # incomplete count. Just release resources cleanly.
        consumer.close()
        producer.flush()
        producer.close()
        print("end script")


if __name__ == "__main__":
    main()
