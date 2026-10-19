# timedelta_directioncount.py
# Consumes messages from Kafka topic 'direction', aggregates vehicle/pedestrian/bike
# movements per time window, and produces aggregated results to topic 'directioncount'.
#
# Counting model: each 'direction' message is one completed crossing. A crossing is
# counted in the window containing its `start_timestamp` (event time, UTC in the feed),
# not the time the message arrives -- 'direction' messages arrive ~2-5 min after the
# object first appears. Windows are aligned to multiples of WINDOW_SECONDS (e.g. :00,
# :15, :30, :45) and each is held open for ALLOWED_LATENESS_SECONDS after it ends so
# late-arriving messages still land in the right window; then it is emitted.
#
# NOTE ON THE OVERCOUNT FIX (2026-08): the previous version wrapped `for message in
# consumer` inside `while now < future` and re-entered the iterator every window while
# resetting the dedup set. A rebalance / uncommitted-offset replay at the window boundary
# then re-delivered the same messages, and they were recounted fresh -- inflating counts
# (~2x at 15-min windows, worse at shorter windows, since the recount scales with the
# number of window boundaries). This version consumes with a single poll() loop, commits
# offsets manually after each emit, and dedups on (partition, offset) so no message is ever
# counted twice.

from kafka import KafkaConsumer, KafkaProducer, TopicPartition
from kafka.structs import OffsetAndMetadata
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
# How long a window stays open after it ends, waiting for late messages. Observed feed lag
# (arrival minus start_timestamp) is ~2.5 min typical, ~5 min p99, ~13 min worst case.
# Messages arriving after their window has been emitted are dropped and logged.
ALLOWED_LATENESS_SECONDS = 15 * 60
# start_timestamp / end_timestamp in the feed are UTC (no zone suffix).
FEED_TIMESTAMP_FORMAT = "%Y-%m-%d %H:%M:%S"

# TEMPORARY (validation): add an "object_ids" list to each vehicle record so counts can be
# checked against individual cars. Only on for 1-minute windows. To remove: delete this
# flag and the blocks marked "OBJECT_IDS" below.
INCLUDE_OBJECT_IDS = WINDOW_SECONDS <= 60

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


def format_est(dt):
    """Aware datetime formatted in America/New_York, as used in every emitted record."""
    return dt.astimezone(est).strftime("%Y-%m-%d %H:%M:%S")


def parse_start_timestamp(data):
    """The message's start_timestamp as an aware UTC datetime."""
    return pytz.utc.localize(datetime.strptime(data['start_timestamp'], FEED_TIMESTAMP_FORMAT))


def window_start_for(ts):
    """Start of the WINDOW_SECONDS-aligned window containing aware datetime ts."""
    epoch = int(ts.timestamp())
    return datetime.fromtimestamp(epoch - epoch % WINDOW_SECONDS, pytz.utc)


class WindowState:
    """All the accumulators for one event-time window. One exists per open window, so
    nothing carries over between windows."""

    def __init__(self, window_start):
        self.window_start = window_start
        self.window_end = window_start + timedelta(seconds=WINDOW_SECONDS)
        self.camera_dictionary = {}        # cam -> {direction: count}
        self.road_dictionary = {}          # "cam-direction" -> {start_road_name, end_road_name}
        self.polygon_dictionary_ped = {}   # cam -> polygon record (ped)
        self.polygon_dictionary_bike = {}  # cam -> polygon record (bike)
        self.seen_offsets = set()          # (partition, offset) already counted this window
        self.min_offsets = {}              # TopicPartition -> lowest offset counted here
        self.object_ids = {}               # OBJECT_IDS: "cam-direction" -> [object_id, ...]
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
            if INCLUDE_OBJECT_IDS:  # OBJECT_IDS
                state.object_ids.setdefault(key, []).append(data.get('object_id'))


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
    # Stamp = window end, so the time on each record is when its counting period closed.
    stamp = format_est(state.window_end)
    print("emitting window", format_est(state.window_start), "->", stamp)

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
            record = {
                "id": cam, "class": "vehicle", "polygon": "",
                "ped_count": "", "bike_count": "", "ped_wait_time": "", "ped_cross_time": "",
                "ped_violation_count": "", "bike_violation_count": "",
                "ped_wait_time_max": "", "ped_cross_time_max": "",
                "time": stamp, "rddir": direc,
                "start_direction": str(direc)[0], "end_direction": str(direc)[1],
                "count": count,
                "start_road_name": roads["start_road_name"], "end_road_name": roads["end_road_name"],
                "bikepedonly": "false",
            }
            if INCLUDE_OBJECT_IDS:  # OBJECT_IDS
                ids = state.object_ids.get(str(cam) + "-" + str(direc), [])
                record["object_ids"] = sorted(ids, key=lambda i: (i is None, i or 0))
            _send(producer, record)

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
def _offset_and_metadata(offset):
    """OffsetAndMetadata gained a leader_epoch field in kafka-python 2.1."""
    try:
        return OffsetAndMetadata(offset, None, -1)
    except TypeError:
        return OffsetAndMetadata(offset, None)


def commit_safe_offsets(consumer, windows):
    """Commit, per partition, up to the oldest message still held in an open window (or
    everything consumed if none is held). A restart then replays exactly the messages of
    windows that were never emitted; replays belonging to emitted windows are dropped as
    late, so nothing is counted twice."""
    offsets = {}
    for tp in consumer.assignment():
        held = [w.min_offsets[tp] for w in windows.values() if tp in w.min_offsets]
        offsets[tp] = _offset_and_metadata(min(held) if held else consumer.position(tp))
    if offsets:
        consumer.commit(offsets)


def main():
    producer = KafkaProducer(bootstrap_servers=BOOTSTRAP_SERVERS)
    # Manual commit: offsets advance only past messages whose window has been counted and
    # flushed, so a rebalance/restart cannot replay an already-counted window.
    consumer = KafkaConsumer(IN_TOPIC,
                             bootstrap_servers=BOOTSTRAP_SERVERS,
                             group_id='directioncount-consumer',
                             enable_auto_commit=False,
                             auto_offset_reset='latest')

    windows = {}  # window_start -> WindowState, for every window not yet emitted
    lateness = timedelta(seconds=ALLOWED_LATENESS_SECONDS)
    print("window", WINDOW_SECONDS, "s, allowed lateness", ALLOWED_LATENESS_SECONDS, "s")

    try:
        while True:
            now = datetime.now(pytz.utc)
            due = sorted(ws for ws, w in windows.items() if now >= w.window_end + lateness)
            if due:
                for ws in due:
                    emit_window(windows[ws], producer)
                producer.flush()
                for ws in due:
                    del windows[ws]
                commit_safe_offsets(consumer, windows)
                continue

            records = consumer.poll(timeout_ms=POLL_TIMEOUT_MS)
            for _tp, messages in records.items():
                for message in messages:
                    try:
                        data = json.loads(message.value.decode('utf-8'))
                        start_ts = parse_start_timestamp(data)
                    except (ValueError, UnicodeDecodeError, KeyError, TypeError) as e:
                        print("skipping undecodable message:", e)
                        continue
                    ws = window_start_for(start_ts)
                    if now >= ws + timedelta(seconds=WINDOW_SECONDS) + lateness:
                        print("dropping late message: start_timestamp", data['start_timestamp'],
                              "UTC, its window was already emitted")
                        continue
                    state = windows.get(ws)
                    if state is None:
                        state = windows[ws] = WindowState(ws)
                    key = (message.partition, message.offset)
                    if key in state.seen_offsets:
                        continue          # guard against redelivery within this window
                    state.seen_offsets.add(key)
                    try:
                        process_message(data, state)
                        state.message_count += 1
                    except (KeyError, TypeError) as e:
                        print("skipping malformed message:", e)
                        continue
                    tp = TopicPartition(message.topic, message.partition)
                    if message.offset < state.min_offsets.get(tp, message.offset + 1):
                        state.min_offsets[tp] = message.offset
    finally:
        # Do not emit open (possibly incomplete) windows on shutdown. Their offsets were not
        # committed, so they are replayed and counted on the next start.
        consumer.close()
        producer.flush()
        producer.close()
        print("end script")


if __name__ == "__main__":
    main()
