# `timedelta_directioncount.py` — Technical Reference

**Component:** Traffic movement aggregation service
**Source file:** `timedelta_directioncount.py`
**Runtime:** Python 3, long-running process managed by `systemd` (host `metroprdlp1`, path `/u01/dev_workspace/cor_deepstream/src/utils/`)
**Document status:** Reference — reflects the post-2026-08 overcount fix (branch `fix/window-replay-overcount`)
**Audience:** Backend / data engineers, reviewers, on-call operators

---

## 1. Purpose and scope

### 1.1 Purpose
This service is a **Kafka stream aggregator**. It consumes individual traffic-crossing
events emitted by a DeepStream computer-vision pipeline on the topic `direction`, tallies
them into fixed wall-clock windows (15 minutes in production), and publishes one aggregated
record per non-empty `(camera, movement)` and `(camera, polygon)` bucket to the topic
`directioncount`.

### 1.2 Scope of this document
An exhaustive, line-referenced explanation of the source file, plus the data contracts,
delivery semantics, failure handling, and operational notes required to run and maintain
it. Line numbers refer to the current revision of the file.

### 1.3 Out of scope
Upstream DeepStream detection/tracking configuration, the downstream consumer of
`directioncount`, and Kafka broker administration.

---

## 2. System context and data flow

```
 DeepStream CV pipeline
        │  (one message per completed crossing)
        ▼
 Kafka topic: "direction"          ← INPUT
        │
        ▼
 timedelta_directioncount.py       ← THIS SERVICE
   • consume with a single poll loop
   • accumulate per 15-min window
   • emit aggregates at each boundary
        │
        ▼
 Kafka topic: "directioncount"     ← OUTPUT
        │
        ▼
 Downstream storage / dashboards
```

Each inbound message represents **one completed movement** of one tracked object (car,
pedestrian, or bicycle) through one camera's field of view. The service converts that
high-volume per-object stream into low-volume periodic counts.

---

## 3. Data contracts

### 3.1 Input message (`direction` topic)
JSON object. Fields consumed by this service are marked **used**; others are ignored.

| Field | Type | Usage |
|---|---|---|
| `sensor_id` | string | **used** — camera identifier (grouping key) |
| `class` | string | **used** — `"car"`, `"Person"`, or `"bicycle"` (others ignored) |
| `start_direction` | string | **used** — first char of movement code / road name |
| `end_direction` | string | **used** — second char of movement code / road name |
| `polygons` | list[string] | **used** — zones the object passed through (ped/bike) |
| `waiting_time` | dict{poly:number} or `""` | **used** — per-polygon wait seconds |
| `crossing_time` | dict{poly:number} or `""` | **used** — per-polygon crossing seconds |
| `violation` | bool | **used** — whether a violation occurred |
| `violation_details` | dict or `""` | **used** — violation zones |
| `object_id` | int | ignored (deliberately — see §7.2) |
| `start_timestamp` / `end_timestamp` | string | ignored |
| `movement`, `near_miss_detection`, `time`, `message_type` | mixed | ignored |

### 3.2 Output message (`directioncount` topic)
JSON object, UTF-8 encoded. One record per non-zero bucket. The schema is uniform across
the three record classes; unused fields are emitted as empty strings so every record has
the same shape.

| Field | Vehicle record | Ped record | Bike record |
|---|---|---|---|
| `id` | camera id | camera id | camera id |
| `class` | `"vehicle"` | `"ped"` | `"bike"` |
| `polygon` | `""` | polygon name | polygon name |
| `ped_count` | `""` | count | `""` |
| `bike_count` | `""` | `""` | count |
| `ped_wait_time` / `ped_cross_time` | `""` | sum | sum |
| `ped_violation_count` / `bike_violation_count` | `""` | value | value |
| `ped_wait_time_max` / `ped_cross_time_max` | `""` | max | max |
| `time` | Eastern timestamp | Eastern timestamp | Eastern timestamp |
| `rddir` | movement code (e.g. `we`) | `""` | `""` |
| `start_direction` / `end_direction` | 1st / 2nd char of `rddir` | `""` | `""` |
| `count` | vehicle count | `""` | `""` |
| `start_road_name` / `end_road_name` | road names | `""` | `""` |
| `bikepedonly` | `"false"` | `"true"`/`"false"` | `"true"`/`"false"` |

---

## 4. Line-by-line walkthrough

### 4.1 Module header and imports (lines 1–21)

- **Lines 1–16 — module docstring.** Describes the service and, importantly, records the
  root cause of the historical overcount bug and how this version prevents it (single
  `poll()` loop, manual commit, `(partition, offset)` dedup). Retained in-source so future
  maintainers do not reintroduce the nested-iterator pattern.
- **Line 18 — `from kafka import KafkaConsumer, KafkaProducer`.** The `kafka-python`
  client classes for reading and writing topics.
- **Line 19 — `import json`.** Message payloads are JSON; used to decode input and encode
  output.
- **Line 20 — `import pytz`.** Provides the `America/New_York` timezone object for
  localized output timestamps.
- **Line 21 — `from datetime import datetime, timedelta`.** `datetime.now()` drives the
  window clock; `timedelta` expresses the window length.

### 4.2 Configuration constants (lines 23–33)

- **Line 26 — `BOOTSTRAP_SERVERS = 'localhost:9092'`.** Kafka broker address. The service
  runs on the same host as the broker.
- **Line 27 — `IN_TOPIC = 'direction'`.** Source topic.
- **Line 28 — `OUT_TOPIC = 'directioncount'`.** Destination topic.
- **Line 30 — `WINDOW_SECONDS = 15 * 60`.** Aggregation window length. **This is the
  single knob that controls window size.** Set to `1 * 60` for 1-minute validation runs.
- **Line 33 — `POLL_TIMEOUT_MS = 1000`.** Maximum time a single `poll()` call blocks
  waiting for records. Bounds how long the loop can go without re-checking the window
  clock — i.e. window boundaries are honored to within ~1 second even in dead air.

### 4.3 Runtime constants (lines 35–61)

- **Line 35 — `est = pytz.timezone('America/New_York')`.** Timezone used to stamp every
  output record. See §7.1 for the naive-datetime nuance.
- **Lines 37–38 — `directions`.** The 16 valid two-character movement codes
  (`start`+`end`, e.g. `we` = west→east). Any movement not in this list (notably
  `unknownunknown`) is silently not counted.
- **Line 41 — `bikepedonly`.** Cameras that observe only bike/ped activity. They are
  **excluded from vehicle output** but still produce ped/bike output (flagged
  `bikepedonly:"true"`). Extend this list to add such cameras.
- **Lines 47–57 — `POLYGON_NAMES`.** The 36 named zones a camera can report: 4 crosswalks,
  8 sidewalks, 8 bike lanes, 16 vehicle lanes. Order is irrelevant (used as dict keys).
- **Lines 58–61 — `PED_FIELDS`.** The 8 per-polygon counters tracked for both peds and
  bikes.

> **Design note.** `POLYGON_NAMES` × `PED_FIELDS` replaced three copies of a hand-written
> 36-key literal. This eliminates the risk of the ped and bike templates silently drifting
> apart, and was verified to reproduce the original structure key-for-key.

### 4.4 Helpers (lines 64–71)

- **Lines 64–66 — `new_polygon_record()`.** Factory returning a fresh, fully-zeroed
  per-polygon record: `{polygon_name: {field: 0, ...}, ...}`. A factory (not a shared
  constant) is required because each camera needs its **own mutable** copy.
- **Lines 69–71 — `now_est()`.** Returns the current time formatted as
  `YYYY-MM-DD HH:MM:SS` in Eastern time. Called once per emit so all records in a window
  share one stamp.

### 4.5 `WindowState` (lines 74–84)
Holds all per-window accumulators in one object so a window can be reset by simply
constructing a new instance (line 253).

- **Line 79 — `camera_dictionary`.** `cam → {direction: count}`; vehicle tallies.
- **Line 80 — `road_dictionary`.** `"cam-direction" → {start_road_name, end_road_name}`;
  captures human-readable road names the first time a movement is seen.
- **Lines 81–82 — `polygon_dictionary_ped` / `polygon_dictionary_bike`.**
  `cam → polygon record`; per-zone ped and bike tallies.
- **Line 83 — `seen_offsets`.** Set of `(partition, offset)` pairs already counted this
  window — the in-window redelivery guard.
- **Line 84 — `message_count`.** Diagnostic counter of messages folded in this window.

### 4.6 `process_message(data, state)` (lines 90–114)
Folds one decoded message into the current window.

- **Lines 92–93.** Extract `sensor_id` and `class`. A missing key raises `KeyError`, caught
  by the caller (line 275) and logged as a skipped malformed message.
- **Lines 95–98 — Person branch.** Lazily create this camera's ped polygon record via
  `setdefault`, then delegate to `_count_polygons` with the ped count/violation fields.
- **Lines 100–103 — bicycle branch.** Same pattern into the bike record, with
  `is_bike=True` (changes how violations are looked up — see §4.7).
- **Lines 105–114 — car branch.**
  - **106.** Lazily seed this camera's direction dict with all 16 directions at 0.
  - **107.** Build the movement code by concatenating start+end direction. `str(...)`
    guards against non-string/`None` values.
  - **109.** Count **only** recognized movement codes; anything else (e.g.
    `unknownunknown`) falls through uncounted, matching the original behavior.
  - **110–113.** On first sighting of a `(camera, direction)`, record its road names.
  - **114.** Increment the vehicle count for this movement.

> Note: `object_id`, `start_timestamp`, and `end_timestamp` are intentionally **not** used
> as identity keys here (see §7.2).

### 4.7 `_count_polygons(...)` (lines 117–147)
Shared ped/bike per-polygon accumulation; the two classes differ only in which count field
is incremented and how violations are keyed.

- **Lines 120–123.** Read optional structures once. `violation_details` and the time maps
  can legitimately be an empty string `""` in the feed, so they are read defensively.
  `is_violation = data.get('violation') is True` requires an actual boolean `True`.
- **Line 125 — `for poly in data.get('polygons', [])`.** Iterate reported zones; missing
  `polygons` yields an empty list (no crash).
- **Lines 126–127.** Ignore any polygon not in the known template.
- **Line 129.** Increment the class-appropriate count (`ped-count` or `bike-count`).
- **Lines 131–134 — waiting time.** Only if `waiting_time` is a dict containing this
  polygon: add to the running sum and update the running max. The `isinstance(..., dict)`
  test skips the `""` sentinel safely.
- **Lines 135–138 — crossing time.** Same pattern for `crossing_time`.
- **Lines 140–147 — violations.**
  - **Bike (141–144):** increment if `violation_details` is a dict and this polygon is in
    its `pedestrian_lane` entry.
  - **Ped (145–147):** increment if this polygon is present in `violation_details`
    (works whether that field is a dict, list, or string).

### 4.8 Emit path (lines 153–226)

- **Lines 153–155 — `_send(producer, val)`.** Serialize a dict to UTF-8 JSON, produce it to
  `OUT_TOPIC`, and log it. `producer.send()` is asynchronous — durability is guaranteed by
  the explicit `flush()` at the window boundary (line 251), not by `send()` itself.
- **Lines 158–161 — `emit_window` opening.** Compute a single Eastern timestamp shared by
  all records in this window.
- **Lines 164–184 — vehicle records.**
  - **165–166.** Skip bike/ped-only cameras.
  - **167.** Log the camera's raw direction tallies (operational visibility).
  - **168–170.** Emit only movements with a positive count.
  - **171–173.** Look up road names; skip if absent (defensive — normally always present
    because they are recorded alongside the first increment).
  - **174–184.** Build and send the vehicle record. `start_direction`/`end_direction` are
    the two characters of the movement code.
- **Lines 187–204 — pedestrian records.** One per polygon with `ped-count > 0`. `flag`
  reflects `bikepedonly` membership.
- **Lines 207–224 — bike records.** Symmetric, keyed on `bike-count > 0`.
- **Line 226.** Log the window's total message count.

### 4.9 `main()` — the consume loop (lines 232–288)

- **Line 233 — producer.** Created once for the process lifetime.
- **Lines 236–240 — consumer.** Key configuration:
  - `group_id='directioncount-consumer'` — stable group so committed offsets persist across
    restarts.
  - `enable_auto_commit=False` — offsets are committed **manually** (line 252) only after a
    window is fully counted and flushed. This is the core of the overcount fix.
  - `auto_offset_reset='latest'` — on a **first-ever** join (no committed offset), start at
    the tail; a backlog is skipped rather than replayed. On subsequent restarts the
    committed offset is used.
- **Lines 242–244 — initialize** the first `WindowState` and compute the first
  `window_end`.
- **Line 247 — `while True`.** Runs until the process is stopped (by `systemd`).
- **Lines 248–258 — boundary handling.** When wall-clock reaches `window_end`:
  emit → `producer.flush()` (make output durable) → `consumer.commit()` (durably advance
  past this window's inputs) → new `WindowState()` → advance `window_end`, using a `while`
  loop (255–256) to skip forward multiple windows if a slow window overran several
  boundaries. `continue` re-checks the clock before the next poll.
- **Lines 260–277 — consume and count.**
  - **260.** `poll()` returns a `{TopicPartition: [messages]}` batch, blocking at most
    `POLL_TIMEOUT_MS`.
  - **261–262.** Iterate every message in every assigned partition.
  - **263–266.** Compute `(partition, offset)`; skip if already counted this window
    (in-window redelivery guard); otherwise record it.
  - **267–271.** Decode JSON; on bad bytes/JSON, log and skip (one poison message cannot
    kill the process).
  - **272–277.** Fold the message in and increment `message_count`; a malformed shape
    (`KeyError`/`TypeError`) is logged and skipped.
- **Lines 278–284 — `finally` (shutdown).** On any exit, close the consumer, flush and
  close the producer. The in-progress partial window is **deliberately not emitted** — it
  would be an incomplete count.

---

## 5. Delivery and correctness semantics

- **No double counting within a run.** Each message is counted once because the consumer
  advances through offsets linearly (no iterator re-entry) and `seen_offsets` guards against
  redelivery inside a window. This is the defect that was fixed.
- **Commit ordering.** `emit → flush → commit`. Inputs are only acknowledged after their
  aggregates are durably produced.
- **Windowing is wall-clock, not event-time.** A message is attributed to the window that
  is open when it is *consumed*, not when the crossing occurred. Under normal low-latency
  delivery these coincide; under consumer lag they can differ.

---

## 6. Failure modes and handling

| Condition | Behavior |
|---|---|
| Malformed / non-JSON message | Logged, skipped; processing continues (lines 269–271) |
| Message missing required keys | Logged as malformed, skipped (lines 275–277) |
| `violation_details` / time maps are `""` | Handled by `isinstance` guards; no crash |
| Slow window overruns a boundary | Catch-up loop advances `window_end` past all missed boundaries (255–256) |
| Process restart (clean) | Resumes from committed offset; no replay of counted windows |
| Broker unavailable at start | `KafkaProducer`/`KafkaConsumer` construction will raise; process exits (systemd restarts) |

---

## 7. Notes, nuances, and known limitations

### 7.1 Timezone dependency (important)
`now_est()` calls `datetime.now()` (a **naive**, system-local time) and then
`.astimezone(est)`. Python treats the naive value as system-local and converts it to
Eastern. **This is only correct if the host clock/timezone is what you expect.** On a
UTC-configured host the conversion yields correct Eastern time; if the host timezone is
misconfigured, output stamps will be wrong. Output timestamps are therefore Eastern, while
the raw feed's `start_timestamp` is UTC — a 4/5-hour offset to keep in mind when
correlating input and output.

### 7.2 Object identity is intentionally not used
`object_id` and the crossing timestamps are ignored for counting. Analysis showed the feed
assigns **unique** object IDs per record with no exact republishing, so an ID-based dedup
was inert. The correct dedup dimension is the Kafka `(partition, offset)`, which is what
this service uses.

### 7.3 Crash between flush and commit (residual at-least-once edge)
If the process dies **after** `producer.flush()` but **before** `consumer.commit()`
(lines 251–252), the last window's output was produced but its inputs were not
acknowledged. On restart those inputs are re-consumed and that one window is re-emitted —
a duplicate for the final window only. This is the classic dual-write gap; it is narrow and
affects at most one window. Downstream idempotency (keying on `id`+`rddir`+`time`) is the
recommended mitigation if this ever matters.

### 7.4 Partial windows are dropped on shutdown
By design (§4.9). A restart loses at most the in-progress window's partial counts.

### 7.5 Single-process assumption
Exactly one instance must run per `group_id`. Two instances would split partitions (not
double-count), but running a second instance under a *different* group id would double all
counts. Operationally: one `systemd` unit, one process.

---

## 8. Operational runbook

- **Change window size:** edit `WINDOW_SECONDS` (line 30). No other change required.
- **Add a bike/ped-only camera:** append its id to `bikepedonly` (line 41).
- **Add a new zone:** add the name to `POLYGON_NAMES` (lines 47–57).
- **Verify a code change is count-neutral:** replay a raw capture through
  `process_message` / `emit_window` offline and assert emitted totals equal a direct tally
  of the file (this is how the fix was validated across four captures).
- **Health signals in logs:** `window start ... -> end ...` per boundary,
  `number of messages:` per window, and `skipping ...` lines. A *flood* of skip lines
  indicates a feed schema change.
- **Restart:** `systemd` restart is safe; it resumes from the committed offset.

---

## 9. Glossary

- **Window** — a fixed wall-clock interval (`WINDOW_SECONDS`) over which counts accumulate.
- **Movement code / `rddir`** — two-character start+end direction (e.g. `we` = west→east).
- **Polygon / zone** — a named region in a camera's view (lane, crosswalk, sidewalk, bike lane).
- **Offset** — a Kafka message's position within its partition; unique and monotonic.
- **Commit** — durably recording the consumer's read position so a restart resumes correctly.
- **`bikepedonly` camera** — a camera excluded from vehicle output but included for ped/bike.
