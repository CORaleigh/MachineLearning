# `timedelta_directioncount.py` — Technical Reference

**Component:** Traffic movement aggregation service
**Source file:** `timedelta_directioncount.py`
**Runtime:** Python 3, long-running process managed by `systemd` (host `metroprdlp1`, path `/u01/dev_workspace/cor_deepstream/src/utils/`)
**Document status:** Reference — reflects the post-2026-08 overcount fix and the 2026-10 change to event-time (`start_timestamp`) windowing (branch `fix/window-replay-overcount`)
**Audience:** Backend / data engineers, reviewers, on-call operators

---

## 1. Purpose and scope

### 1.1 Purpose
This service is a **Kafka stream aggregator**. It consumes individual traffic-crossing
events emitted by a DeepStream computer-vision pipeline on the topic `direction`, tallies
them into fixed **event-time** windows keyed on each crossing's `start_timestamp`
(15 minutes in production), and publishes one aggregated record per non-empty
`(camera, movement)` and `(camera, polygon)` bucket to the topic `directioncount`.

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
        │  (one message per completed crossing, ~2–5 min after the object first appears)
        ▼
 Kafka topic: "direction"          ← INPUT
        │
        ▼
 timedelta_directioncount.py       ← THIS SERVICE
   • consume with a single poll loop
   • bucket each message by its start_timestamp into an aligned window
   • hold each window open for ALLOWED_LATENESS_SECONDS after it ends
   • emit aggregates, then commit offsets
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
| `start_timestamp` | string `YYYY-MM-DD HH:MM:SS`, **UTC** | **used** — selects the window the crossing is counted in (all classes) |
| `start_direction` | string | **used** — first char of movement code / road name |
| `end_direction` | string | **used** — second char of movement code / road name |
| `polygons` | list[string] | **used** — zones the object passed through (ped/bike) |
| `waiting_time` | dict{poly:number} or `""` | **used** — per-polygon wait seconds |
| `crossing_time` | dict{poly:number} or `""` | **used** — per-polygon crossing seconds |
| `violation` | bool | **used** — whether a violation occurred |
| `violation_details` | dict or `""` | **used** — violation zones |
| `object_id` | int | reported only (temporary `object_ids` list, cars, 1-min windows); never used for counting or dedup — see §7.2 |
| `end_timestamp` | string | ignored |
| `movement`, `near_miss_detection`, `time`, `message_type` | mixed | ignored |

> `start_timestamp` carries no zone suffix but is UTC. This was confirmed from the
> 2026-08-19 capture: the file was saved at 15:02 EDT and contains `start_timestamp`
> values up to 18:57 (UTC = EDT + 4h).

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
| `time` | window **end**, Eastern | window **end**, Eastern | window **end**, Eastern |
| `rddir` | movement code (e.g. `we`) | `""` | `""` |
| `start_direction` / `end_direction` | 1st / 2nd char of `rddir` | `""` | `""` |
| `count` | vehicle count | `""` | `""` |
| `start_road_name` / `end_road_name` | road names | `""` | `""` |
| `bikepedonly` | `"false"` | `"true"`/`"false"` | `"true"`/`"false"` |
| `object_ids` *(temporary)* | sorted list of counted `object_id`s; present only when `WINDOW_SECONDS <= 60` | absent | absent |

`time` is the end of the event-time window the record covers (e.g. a 15-minute window
covering 14:00:00–14:14:59 Eastern is stamped `14:15:00`). It is **not** the time the
record was sent; records are sent `ALLOWED_LATENESS_SECONDS` after that (§7.6).

When present, `object_ids` has exactly `count` entries — one per counted car — so a
duplicate `object_id` from upstream would appear twice.

---

## 4. Line-by-line walkthrough

### 4.1 Module header and imports (lines 1–25)

- **Lines 1–10 — module docstring / counting model.** States that each crossing is
  counted in the window containing its `start_timestamp`, that windows are aligned to
  multiples of `WINDOW_SECONDS`, and that each window is held open for
  `ALLOWED_LATENESS_SECONDS` to absorb feed lag.
- **Lines 12–19 — overcount fix note.** Records the root cause of the historical overcount
  bug and how this version prevents it (single `poll()` loop, manual commit,
  `(partition, offset)` dedup). Retained in-source so future maintainers do not
  reintroduce the nested-iterator pattern.
- **Line 21 — `from kafka import KafkaConsumer, KafkaProducer, TopicPartition`.** The
  `kafka-python` client classes for reading and writing topics; `TopicPartition` keys the
  per-partition offsets held by open windows.
- **Line 22 — `from kafka.structs import OffsetAndMetadata`.** Used to build explicit
  per-partition commits (§4.9).
- **Line 23 — `import json`.** Message payloads are JSON; used to decode input and encode
  output.
- **Line 24 — `import pytz`.** Provides `pytz.utc` (feed timestamps, the wall clock) and the
  `America/New_York` timezone (output stamps).
- **Line 25 — `from datetime import datetime, timedelta`.** Timestamp parsing, the wall
  clock, and window arithmetic.

### 4.2 Configuration constants (lines 27–48)

- **Line 30 — `BOOTSTRAP_SERVERS = 'localhost:9092'`.** Kafka broker address. The service
  runs on the same host as the broker.
- **Line 31 — `IN_TOPIC = 'direction'`.** Source topic.
- **Line 32 — `OUT_TOPIC = 'directioncount'`.** Destination topic.
- **Line 34 — `WINDOW_SECONDS = 15 * 60`.** Aggregation window length. **This is the knob
  that controls window size.** Set to `1 * 60` for 1-minute validation runs (which also
  turns on `object_ids`, line 48).
- **Line 37 — `POLL_TIMEOUT_MS = 1000`.** Maximum time a single `poll()` call blocks
  waiting for records. Bounds how long the loop can go without re-checking whether a
  window is due, so windows are emitted within ~1 second of becoming due even in dead air.
- **Line 41 — `ALLOWED_LATENESS_SECONDS = 15 * 60`.** How long a window stays open after
  its end, waiting for late messages. Sized from observed feed lag (arrival minus
  `start_timestamp`) across the 2026-08 captures: ~2.5 min typical, ~5 min p99, ~13 min
  worst case. Larger values catch more stragglers but delay output; messages arriving
  after their window is emitted are dropped (§7.5).
- **Line 43 — `FEED_TIMESTAMP_FORMAT`.** `strptime` format of `start_timestamp`.
- **Line 48 — `INCLUDE_OBJECT_IDS = WINDOW_SECONDS <= 60`.** **Temporary validation
  feature.** When true, vehicle records carry an `object_ids` list. It switches on
  automatically for 1-minute windows. To remove the feature, delete this flag and the
  three blocks marked `# OBJECT_IDS` (lines 113, 145–146, 219–221).

### 4.3 Runtime constants (lines 50–76)

- **Line 50 — `est = pytz.timezone('America/New_York')`.** Timezone used to stamp every
  output record.
- **Lines 52–53 — `directions`.** The 16 valid two-character movement codes
  (`start`+`end`, e.g. `we` = west→east). Any movement not in this list (notably
  `unknownunknown`) is silently not counted.
- **Line 56 — `bikepedonly`.** Cameras that observe only bike/ped activity. They are
  **excluded from vehicle output** but still produce ped/bike output (flagged
  `bikepedonly:"true"`). Extend this list to add such cameras.
- **Lines 62–72 — `POLYGON_NAMES`.** The 36 named zones a camera can report: 4 crosswalks,
  8 sidewalks, 8 bike lanes, 16 vehicle lanes. Order is irrelevant (used as dict keys).
- **Lines 73–76 — `PED_FIELDS`.** The 8 per-polygon counters tracked for both peds and
  bikes.

> **Design note.** `POLYGON_NAMES` × `PED_FIELDS` replaced three copies of a hand-written
> 36-key literal. This eliminates the risk of the ped and bike templates silently drifting
> apart, and was verified to reproduce the original structure key-for-key.

### 4.4 Helpers (lines 79–97)

- **Lines 79–81 — `new_polygon_record()`.** Factory returning a fresh, fully-zeroed
  per-polygon record: `{polygon_name: {field: 0, ...}, ...}`. A factory (not a shared
  constant) is required because each camera needs its **own mutable** copy.
- **Lines 84–86 — `format_est(dt)`.** Formats an aware datetime as `YYYY-MM-DD HH:MM:SS`
  in Eastern time. Used for the output `time` stamp and log lines.
- **Lines 89–91 — `parse_start_timestamp(data)`.** Parses `start_timestamp` and attaches
  UTC, returning an aware datetime. A missing or malformed value raises
  `KeyError`/`ValueError`/`TypeError`, which the caller treats as an undecodable message.
- **Lines 94–97 — `window_start_for(ts)`.** Floors a timestamp to the start of its
  `WINDOW_SECONDS`-aligned window (epoch-aligned, so 15-minute windows start at :00, :15,
  :30, :45). Returns an aware UTC datetime, which is also the window's dictionary key.

### 4.5 `WindowState` (lines 100–114)
Holds all accumulators for **one** event-time window. One instance exists per window that
has received messages and not yet been emitted; several are typically open at once
(roughly `(WINDOW_SECONDS + ALLOWED_LATENESS_SECONDS) / WINDOW_SECONDS` of them).

- **Lines 105–106 — `window_start` / `window_end`.** The window's half-open interval
  `[start, end)`, aware UTC.
- **Line 107 — `camera_dictionary`.** `cam → {direction: count}`; vehicle tallies.
- **Line 108 — `road_dictionary`.** `"cam-direction" → {start_road_name, end_road_name}`;
  captures human-readable road names the first time a movement is seen.
- **Lines 109–110 — `polygon_dictionary_ped` / `polygon_dictionary_bike`.**
  `cam → polygon record`; per-zone ped and bike tallies.
- **Line 111 — `seen_offsets`.** Set of `(partition, offset)` pairs already counted in this
  window — the redelivery guard. A redelivered message always maps to the same window
  (same `start_timestamp`), so a per-window set is sufficient.
- **Line 112 — `min_offsets`.** `TopicPartition → lowest offset counted in this window`.
  Determines how far offsets may be safely committed while the window is still open
  (§4.9).
- **Line 113 — `object_ids`** *(temporary)*. `"cam-direction" → [object_id, ...]`; only
  populated when `INCLUDE_OBJECT_IDS` is true.
- **Line 114 — `message_count`.** Diagnostic counter of messages folded into this window.

### 4.6 `process_message(data, state)` (lines 120–146)
Folds one decoded message into the window chosen by the caller.

- **Lines 122–123.** Extract `sensor_id` and `class`. A missing key raises `KeyError`,
  caught by the caller (line 342) and logged as a skipped malformed message.
- **Lines 125–128 — Person branch.** Lazily create this camera's ped polygon record via
  `setdefault`, then delegate to `_count_polygons` with the ped count/violation fields.
- **Lines 130–133 — bicycle branch.** Same pattern into the bike record, with
  `is_bike=True` (changes how violations are looked up — see §4.7).
- **Lines 135–146 — car branch.**
  - **136.** Lazily seed this camera's direction dict with all 16 directions at 0.
  - **137.** Build the movement code by concatenating start+end direction. `str(...)`
    guards against non-string/`None` values.
  - **139.** Count **only** recognized movement codes; anything else (e.g.
    `unknownunknown`) falls through uncounted, matching the original behavior.
  - **140–143.** On first sighting of a `(camera, direction)`, record its road names.
  - **144.** Increment the vehicle count for this movement.
  - **145–146** *(temporary)*. When `INCLUDE_OBJECT_IDS`, append the car's `object_id`
    to the list for this `(camera, direction)`. Appended once per increment, so the list
    length always equals the count.

### 4.7 `_count_polygons(...)` (lines 149–179)
Shared ped/bike per-polygon accumulation; the two classes differ only in which count field
is incremented and how violations are keyed.

- **Lines 152–155.** Read optional structures once. `violation_details` and the time maps
  can legitimately be an empty string `""` in the feed, so they are read defensively.
  `is_violation = data.get('violation') is True` requires an actual boolean `True`.
- **Line 157 — `for poly in data.get('polygons', [])`.** Iterate reported zones; missing
  `polygons` yields an empty list (no crash).
- **Lines 158–159.** Ignore any polygon not in the known template.
- **Line 161.** Increment the class-appropriate count (`ped-count` or `bike-count`).
- **Lines 163–166 — waiting time.** Only if `waiting_time` is a dict containing this
  polygon: add to the running sum and update the running max. The `isinstance(..., dict)`
  test skips the `""` sentinel safely.
- **Lines 167–170 — crossing time.** Same pattern for `crossing_time`.
- **Lines 172–179 — violations.**
  - **Bike (173–176):** increment if `violation_details` is a dict and this polygon is in
    its `pedestrian_lane` entry.
  - **Ped (177–179):** increment if this polygon is present in `violation_details`
    (works whether that field is a dict, list, or string).

### 4.8 Emit path (lines 185–264)

- **Lines 185–187 — `_send(producer, val)`.** Serialize a dict to UTF-8 JSON, produce it to
  `OUT_TOPIC`, and log it. `producer.send()` is asynchronous — durability is guaranteed by
  the explicit `flush()` after emitting (line 312), not by `send()` itself.
- **Lines 190–195 — `emit_window` opening.** Stamp all records in the window with the
  window **end** in Eastern time, and log the window's start → end.
- **Lines 198–222 — vehicle records.**
  - **199–200.** Skip bike/ped-only cameras.
  - **201.** Log the camera's raw direction tallies (operational visibility).
  - **202–204.** Emit only movements with a positive count.
  - **205–207.** Look up road names; skip if absent (defensive — normally always present
    because they are recorded alongside the first increment).
  - **208–218.** Build the vehicle record. `start_direction`/`end_direction` are the two
    characters of the movement code.
  - **219–221** *(temporary)*. When `INCLUDE_OBJECT_IDS`, attach the sorted `object_ids`
    list. The sort key tolerates a `None` id (placed last) rather than raising.
  - **222.** Send the record.
- **Lines 225–242 — pedestrian records.** One per polygon with `ped-count > 0`. `flag`
  reflects `bikepedonly` membership.
- **Lines 245–262 — bike records.** Symmetric, keyed on `bike-count > 0`.
- **Line 264.** Log the window's total message count.

### 4.9 Offset commit and `main()` — the consume loop (lines 270–358)

- **Lines 270–275 — `_offset_and_metadata(offset)`.** Builds an `OffsetAndMetadata`.
  `kafka-python` 2.1 added a third field (`leader_epoch`); this tries the 3-field form and
  falls back to the 2-field form so the script runs on either version.
- **Lines 278–288 — `commit_safe_offsets(consumer, windows)`.** For each assigned
  partition, commits the **lowest offset still held by any open window**, or the
  consumer's current position if no open window holds a message from that partition.
  Because several windows are open at once, committing the consumer's full position would
  acknowledge messages whose windows have not been emitted yet; this function avoids
  that. Messages *after* the committed offset that belong to already-emitted windows are
  re-read on restart and dropped as late (§5), so nothing is double-counted.
- **Line 292 — producer.** Created once for the process lifetime.
- **Lines 295–299 — consumer.** Key configuration:
  - `group_id='directioncount-consumer'` — stable group so committed offsets persist across
    restarts.
  - `enable_auto_commit=False` — offsets are committed **manually** (line 315) only after
    windows are emitted and flushed. This is the core of the overcount fix.
  - `auto_offset_reset='latest'` — on a **first-ever** join (no committed offset), start at
    the tail; a backlog is skipped rather than replayed. On subsequent restarts the
    committed offset is used.
- **Lines 301–303 — initialize.** `windows` maps `window_start → WindowState` for every
  window not yet emitted. Logs the window size and allowed lateness.
- **Line 306 — `while True`.** Runs until the process is stopped (by `systemd`).
- **Lines 307–316 — emit due windows.** A window is **due** once the wall clock (UTC)
  reaches `window_end + ALLOWED_LATENESS_SECONDS`. All due windows are emitted oldest
  first → `producer.flush()` (make output durable) → removed from `windows` →
  `commit_safe_offsets()`. `continue` re-checks before the next poll.
- **Lines 318–347 — consume and count.**
  - **318.** `poll()` returns a `{TopicPartition: [messages]}` batch, blocking at most
    `POLL_TIMEOUT_MS`.
  - **319–320.** Iterate every message in every assigned partition.
  - **321–326.** Decode JSON and parse `start_timestamp`; on bad bytes/JSON or a
    missing/malformed timestamp, log and skip (one poison message cannot kill the
    process).
  - **327.** Pick the window from `start_timestamp`.
  - **328–331 — late check.** If that window would already be due (and therefore has
    already been emitted, or would be emitted immediately), the message is dropped and
    logged. Uses the same rule as line 308, so a message is never added to a window after
    it was sent.
  - **332–334.** Get or create the window's `WindowState`.
  - **335–338.** Skip if this `(partition, offset)` was already counted in this window;
    otherwise record it.
  - **339–344.** Fold the message in and increment `message_count`; a malformed shape
    (`KeyError`/`TypeError`) is logged and skipped.
  - **345–347.** Record the lowest offset this window holds per partition (input to
    `commit_safe_offsets`).
- **Lines 348–354 — `finally` (shutdown).** On any exit, close the consumer, flush and
  close the producer. Open windows are **deliberately not emitted** — they may be
  incomplete. Their offsets were never committed, so they are rebuilt on the next start
  (§7.4).

---

## 5. Delivery and correctness semantics

- **Windowing is event-time.** A message is counted in the window containing its
  `start_timestamp`, regardless of when it is consumed. Consumer lag or feed delay does not
  move a crossing into a different window, provided it arrives within
  `ALLOWED_LATENESS_SECONDS` of the window's end.
- **No double counting within a run.** The consumer advances through offsets linearly (no
  iterator re-entry), and `seen_offsets` guards against redelivery into a window.
- **No double counting across restarts.** Offsets are committed only up to the oldest
  message held by an open window. On restart, replayed messages belonging to emitted
  windows fail the late check and are dropped; replayed messages belonging to un-emitted
  windows are counted into fresh windows.
- **Commit ordering.** `emit → flush → commit`. Inputs are only acknowledged after their
  aggregates are durably produced.

---

## 6. Failure modes and handling

| Condition | Behavior |
|---|---|
| Malformed / non-JSON message | Logged (`skipping undecodable message`), skipped; processing continues (lines 324–326) |
| Missing or unparseable `start_timestamp` | Logged as undecodable, skipped (lines 324–326) |
| Message missing other required keys | Logged as malformed, skipped (lines 342–344) |
| `violation_details` / time maps are `""` | Handled by `isinstance` guards; no crash |
| Message arrives after its window was emitted | Logged (`dropping late message`), not counted (lines 328–331) |
| Process restart (clean) | Resumes from the committed offset; open windows are rebuilt from replay; emitted windows are not recounted |
| Broker unavailable at start | `KafkaProducer`/`KafkaConsumer` construction will raise; process exits (systemd restarts) |

---

## 7. Notes, nuances, and known limitations

### 7.1 Time zones and clocks
All internal times are **aware UTC** datetimes: `start_timestamp` is parsed as UTC, and
the wall clock is `datetime.now(pytz.utc)`. The host's configured timezone therefore no
longer affects counting or stamps. Output `time` is converted to Eastern only at emit.

What does matter is **clock accuracy**: the wall clock decides when a window is due and
whether a message is late, while `start_timestamp` comes from the DeepStream host. Both
hosts should be NTP-synchronized. A skew of a few minutes is absorbed by the lateness
allowance; a large skew would either delay output or cause messages to be dropped as late.

When correlating input and output, remember that `start_timestamp` is UTC and output
`time` is Eastern (a 4- or 5-hour offset).

### 7.2 Object identity is not used for counting
`object_id` is not used for counting or dedup. Analysis showed the feed assigns **unique**
object IDs per record with no exact republishing, so an ID-based dedup was inert. The
correct dedup dimension is the Kafka `(partition, offset)`, which is what this service
uses. The temporary `object_ids` output list (1-minute windows only) reports the IDs that
were counted, for validation against other sources; it does not change any count.

### 7.3 Crash between flush and commit (residual at-least-once edge)
If the process dies **after** `producer.flush()` (line 312) but **before**
`commit_safe_offsets()` (line 315), the emitted windows' output was produced but their
inputs were not acknowledged. On restart those inputs are re-consumed, but because a
window is emitted only once the clock is past `window_end + ALLOWED_LATENESS_SECONDS`,
every replayed message for it fails the late check and is dropped — the window is not
re-emitted. The residual risk is limited to clock anomalies (e.g. the wall clock stepping
backwards across the restart). Downstream
idempotency (keying on `id`+`rddir`+`polygon`+`time`) remains the recommended
mitigation if this ever matters.

### 7.4 Open windows on shutdown and restart
Open windows are not emitted on shutdown (§4.9). Their messages are replayed and counted
on restart, **provided the service is back up before those windows become due**. If the
service is down longer than `ALLOWED_LATENESS_SECONDS` past a window's end, that window's
messages are dropped as late on replay and the window is never emitted (a gap, not a
partial or duplicate count). A first-ever start (no committed offset) begins at the tail
of the topic.

### 7.5 Late messages are dropped
A message is counted only if it arrives before `window_end + ALLOWED_LATENESS_SECONDS`.
With the current 15-minute allowance, every message in the 2026-08 captures would have been
counted (worst observed lag ~13 min). Watch the `dropping late message` log line; if it
appears regularly outside restarts, raise `ALLOWED_LATENESS_SECONDS`.

### 7.6 Output latency
A window's records are sent `ALLOWED_LATENESS_SECONDS` after the window ends — with the
defaults, a 15-minute window is published 15 minutes after it closes (30 minutes after it
opens). This also applies to 1-minute validation runs: the first output appears about
16 minutes after start-up.

### 7.7 Window alignment
Windows are aligned to the epoch (multiples of `WINDOW_SECONDS`), not to process start.
For 15-minute windows this means :00, :15, :30 and :45. Window sizes that do not divide an
hour evenly still work but will not align to clock-face boundaries.

### 7.8 Single-process assumption
Exactly one instance must run per `group_id`. Two instances would split partitions (not
double-count), but running a second instance under a *different* group id would double all
counts. Operationally: one `systemd` unit, one process.

### 7.9 Temporary `object_ids` feature
The `object_ids` list is a validation aid tied to 1-minute windows (`INCLUDE_OBJECT_IDS`,
line 48). It is not intended to remain long-term. Remove it by deleting the flag and the
three `# OBJECT_IDS` blocks; no other code depends on it.

---

## 8. Operational runbook

- **Change window size:** edit `WINDOW_SECONDS` (line 34). Setting it to `60` also enables
  `object_ids` on vehicle records.
- **Change late-message tolerance:** edit `ALLOWED_LATENESS_SECONDS` (line 41). This
  trades output latency against how many delayed messages are counted.
- **Add a bike/ped-only camera:** append its id to `bikepedonly` (line 56).
- **Add a new zone:** add the name to `POLYGON_NAMES` (lines 62–72).
- **Verify a code change is count-neutral:** replay a raw capture through the service
  offline (stubbed Kafka consumer/producer and a simulated clock driven by each message's
  arrival time) and assert that each emitted window's totals equal a direct tally of the
  file grouped by `start_timestamp`. The event-time change was validated this way against
  the 2026-08-21 capture (640 cars, 1-minute and 15-minute windows, including every
  `object_ids` list).
- **Health signals in logs:** `window ... s, allowed lateness ... s` at start-up,
  `emitting window <start> -> <end>` per emitted window, `number of messages:` per window,
  `dropping late message` and `skipping ...` lines. A *flood* of skip lines indicates a
  feed schema change; regular late drops indicate the lateness allowance is too small or a
  clock is skewed.
- **Restart:** `systemd` restart is safe; it resumes from the committed offset and rebuilds
  open windows. Keep downtime shorter than `ALLOWED_LATENESS_SECONDS` to avoid gaps
  (§7.4).

---

## 9. Glossary

- **Window** — a fixed event-time interval `[start, start + WINDOW_SECONDS)`, aligned to
  multiples of `WINDOW_SECONDS`, over which counts accumulate.
- **Event time** — when the crossing happened (`start_timestamp`), as opposed to when its
  message was consumed.
- **Allowed lateness** — how long after a window ends it stays open for delayed messages
  before being emitted.
- **Movement code / `rddir`** — two-character start+end direction (e.g. `we` = west→east).
- **Polygon / zone** — a named region in a camera's view (lane, crosswalk, sidewalk, bike lane).
- **Offset** — a Kafka message's position within its partition; unique and monotonic.
- **Commit** — durably recording the consumer's read position so a restart resumes correctly.
- **`bikepedonly` camera** — a camera excluded from vehicle output but included for ped/bike.
