## Initial Setup — IMPORTANT

The first time you interact with this repository, review and follow the
initial setup instructions in AGENTS.md.

---

## What this project is

A **Kubernetes controller daemon** — not a web application.  It has no
database and no user-facing frontend.  It authenticates *outbound* to the GPU
Reservation API using a long-lived service key, and *inbound* to Kubernetes
using either a kubeconfig file or an in-cluster service account.  It also
exposes a small optional **inbound API** that the reservation app calls: a push
endpoint (`POST /api/reservations/push`) to propagate reservation updates faster
than the poll interval, and a take-back endpoint
(`POST /api/reservations/take-back`) to reclaim idle reclaim blocks before
re-booking them; both are guarded by a single static bearer token and are
disabled unless that token is configured.

---

## Technology choices

| Choice | Rationale |
|--------|-----------|
| **FastAPI** | Provides the `GET /health` liveness endpoint, the `POST /api/reservations/push` inbound API, and clean lifespan management for background tasks; no routers or static files needed |
| **asyncio** | Single event loop drives all three background loops concurrently without threads for the application logic |
| **httpx** | Async HTTP client for the reservation management API; supports connection pooling and clean timeout handling |
| **kubernetes** (official Python client) | LIST + WATCH pod streams; strategic-merge-patch for toleration injection; supports both in-cluster and kubeconfig auth |
| **Pydantic v2** | Validates and deserialises reservation API responses into typed models |

> **Do not add** SQLAlchemy, SQLite, Argon2id, python-jose, or a frontend.
> This daemon has no persistent state and no human login flow.

---

## Architecture

```
app/
├── __init__.py
├── main.py               Entry point — FastAPI app, lifespan, three background tasks
├── config.py             Config dataclass populated from environment variables
├── schemas.py            Pydantic models mirroring RESERVATION-API.md §6
├── reservation_client.py httpx async client — fetches reservations + GPU class details
├── k8s_client.py         Kubernetes wrapper — PodWatcher, apply_toleration, set_active_deadline, emit_runtime_capped_event, snapshot_tolerated_pods (occupancy rebuild)
└── controller.py         ControllerState, QueueEntry, matching, window arithmetic
```

### Background tasks (started in `lifespan`, cancelled on shutdown)

| Task | Cadence | Responsibility |
|------|---------|----------------|
| `reservation_fetch_loop` | every `RESERVATION_FETCH_INTERVAL` s (default 300) | Re-fetches active reservations; resolves `gpu_class_id → label_value`; reconciles stale queue entries |
| `pod_watch_loop` | continuous (LIST + WATCH) | Enqueues pods with `gpu-class` label that lack the toleration; dequeues deleted pods; **fast-path**: applies toleration immediately when a new pod arrives inside an open window |
| `queue_processor_loop` | every `POD_LIST_TICK_INTERVAL` s (default 300) | Handles pods queued before their window opened; retries pods that were over-budget; schedules retries with 2–5 min jitter |

---

## Key design decisions

### Matching pods to reservations

A pod matches a reservation when **both** of the following hold:

```
pod.metadata.namespace  ==  reservation.user.username
pod.labels["gpu-class"] ==  gpu_class.label_value   # from GET /api/gpu-classes/{id}
```

`label_value` is cached in `ControllerState.gpu_class_labels`.  Each refresh
cycle re-resolves only GPU classes **not already cached** (classes that drop
out of the active reservation list also drop out of the cache); a cached value
is reused as long as the class stays in the list, so changing a class's
`label_value` in the reservation app takes effect only after the class has no
active reservations for a cycle or the controller restarts.  If a GPU class
has no `label_value`, its reservations are skipped with a warning.

### Toleration applied

```
key:      gpu-class-reservation
operator: Equal
value:    <pod's gpu-class label value>   # mirrors the pod's own label
effect:   NoSchedule
```

When patching, all existing tolerations are preserved (Kubernetes rejects
patches that remove tolerations from running pods).  The pod is re-fetched
immediately before patching to avoid working with a stale toleration list.

The patch also stamps the pod with the `horae/booking-reference` annotation:
`res-<id>` (reserved path), `ondemand-<id>`, or `noshow-<id>`.  This is the
controller's single record of which reservation a pod was admitted under.  The
GPU **budget check** (`ControllerState.available`) counts every pod recorded
against a reservation id in the **unified occupancy map** (one map for reserved,
on-demand, and no-show alike — keyed by reservation id), so each reservation has
an independent budget; the id parsed from this annotation
(`parse_booking_reference`) is also how occupancy is rebuilt from the cluster
after a restart.  The prefix records the admission path and is otherwise
cosmetic.

`set_active_deadline` additionally writes `horae/pod-runtime-limit-seconds`
(mirroring the `activeDeadlineSeconds` spec patch; consumed by in-pod
notification widgets).

### Runtime capping

When a pod is admitted (toleration successfully applied), the controller
immediately enforces a maximum lifetime via `spec.activeDeadlineSeconds`.

**Calculation** — the maximum is:

1. Remaining seconds in the current reservation window (from *now* to
   `slot_end(current)`), **plus**
2. The full duration of any directly **back-to-back** future reservations
   sharing the same `user.username`, GPU class, and `gpu_count`, where
   `slot_start(next) == slot_end(previous)` with no gap.

The logic lives in `ControllerState.compute_max_deadline_seconds` in
`controller.py`.  Users may schedule consecutive identical reservations to
extend their session; the controller chains those windows into one deadline.

**Enforcement** — after calling `apply_toleration`:

- If the pod's current `activeDeadlineSeconds` is unset or exceeds the
  computed maximum, `set_active_deadline` patches the pod spec.
- `emit_runtime_capped_event` creates a `Normal` Kubernetes Event on the pod
  with `reason: RuntimeCapped`, `action: CapRuntime`, and a human-readable
  message explaining the cap.
- Both steps are best-effort inside `_enforce_deadline` in `main.py`.  A
  failure logs a warning and does **not** revoke the toleration.

**RBAC**: the controller's ServiceAccount must have `create` on `events` in
addition to the existing pod permissions.

### Timezone

All reservation window arithmetic uses **UTC-aware `datetime` objects** (`timezone.utc`).
`slot_start` and `slot_end` return `r.start_utc` / `r.end_utc` directly from the API
response; no local-time conversion is performed in the controller.  Every
`datetime.now()` call in the codebase uses `datetime.now(timezone.utc)`.

The `TZ` environment variable is no longer needed for correctness (it only affects
log timestamp display).

### Fast path for mid-window pods

When a pod ADDED event arrives while its reservation window is already open
(the common case for JupyterHub notebook servers launched during a session),
`pod_watch_loop` calls `_try_apply_toleration` immediately rather than waiting
up to a full `POD_LIST_TICK_INTERVAL` (default 300 s) for the next
queue-processor tick.

Only ADDED events trigger the fast path.  MODIFIED events — which can arrive in
rapid bursts as Kubernetes reconciles pod state — go through the normal queue so
the Kubernetes API is not hammered.  If the immediate attempt fails (budget full
or transient error), the entry remains in the queue and the processor retries it
on its normal schedule.

`_try_apply_toleration` is the single shared coroutine that performs the budget
check and patch; both the fast path and the queue processor call it.

### Reclaim-block merging (on-demand runtime extension)

On-demand jobs are capped to the end of the single block they land on
(`slot_end(block)`, no chaining — unlike reserved holders).  To let a job that
begins near the end of a block run longer, the controller merges a **subject
block** — any currently-open on-demand window (a `kind="reclaim"` hold, a declared
no-show, or a cancelled-in-window reservation) — with a directly **abutting future
reclaim block** when that future block is **committed**.

A future block is committed when its `start_utc` was within
`reclaim_preempt_guard_minutes` (fetched from `GET /api/settings`) **at the last
reservation fetch** (`last_reservation_fetch_at`) — not merely by the between-fetch
tick clock drifting it into the guard.  Anchoring to fetch time is essential:
judging against the advancing tick clock would let a block that was still
preemptible when we last fetched drift into the guard and get merged, racing a
last-minute front-end booking the controller has not yet seen.  Because the guard
is sized to exceed the poll interval, a block legitimately entering the guard is
always re-seen by a fresh fetch (still present, or gone if preempted) before it is
merged.  Inside the guard the reservation app will not preempt the hold with a new
booking, so it is safe to schedule onto.  The future block must be `kind="reclaim"`, share the
subject's GPU class label, and have an **equal `gpu_count`** (capacity is uniform
across the merged span, mirroring the reserved back-to-back chaining rule
`slot_start(next) == slot_end(prev)`).  If several abut, the longest (latest
`slot_end`) is chosen; chaining is applied iteratively as further blocks enter the
guard on later ticks.

The merge (`ControllerState.reconcile_reclaim_merges`, run after each reservation
refresh and each queue-processor tick) extends the subject's `end_utc` to the
absorbed block's end and records the absorbed reclaim id in `merged_stub_ids` so it
is **excluded** from independent on-demand placement.  `find_ondemand_block` then
naturally returns the longer block (it already prefers the latest `slot_end`), and
the on-demand deadline cap extends with it — no separate deadline logic.

Merges are **persistent**: `reclaim_merges` (keyed by subject id) is re-applied to
the freshly loaded reservation objects on every reload — they are otherwise
replaced wholesale — and is pruned only once `now >= extended_end` (the **whole**
merged span has ended), not when the subject's original window closes.  This keeps
the absorbed block stubbed for the full lifetime of any deadline-extended job, so a
reload never re-exposes it for double-booking.  This is the on-demand analogue of
how `claimed_reservation_ids` protects chained reserved windows.  Merging is
skipped entirely when on-demand placement is disabled or the guard is unknown
(settings fetch failed / recovery disabled).

### Inbound push API

`POST /api/reservations/push` lets the reservation app push **one or more
updated reservation entries** so changes (today: cancellations; later: standby
assignments) propagate within seconds instead of waiting up to a full
`RESERVATION_FETCH_INTERVAL`.  Bulk synchronisation stays a controller-initiated
**pull** — the push is a partial delta, and the next full fetch remains the
source of truth.

- **Auth**: a single static bearer token in `INBOUND_API_TOKEN` (mount from a
  Secret).  Unset ⇒ endpoint disabled (503); wrong/missing bearer ⇒ 401
  (constant-time compare).  It rides the existing FastAPI app / `HEALTH_PORT`,
  so no extra container port or Service is needed.
- **Body**: `{"reservations": [ReservationResponse, …]}` — the same entry shape
  the pull returns (`schemas.ReservationPushRequest`).
- **Semantics**: entries are **upserted by id** (`apply_push_to_active` in
  `controller.py`); an entry whose `status` is not `"active"` drops that id from
  the active set, and an in-window cancellation evicts its admitted pod and
  reclaims capacity — the *same* path a mid-window cancellation takes on a fetch.
  An entry that keeps its id but changes owner (**adoption** — a reservation
  reassigned to a teammate) evicts the prior owner's admitted pod from its
  namespace and releases the capacity, so the new owner can claim the
  still-active window (`detect_owner_changed_in_window` + `_handle_owner_changes`).
- **Shared reconciliation**: both the fetch loop and the push run
  `_reconcile_after_reservation_change` in `main.py` (label resolution, queue
  reconcile, cancellation eviction, owner-change eviction, reclaim-merge re-apply).
  The push passes
  `update_fetch_stamp=False` so it does **not** advance
  `last_reservation_fetch_at` (that stamp anchors the reclaim-merge commitment
  guard; advancing it on partial data could race an unseen booking).
- **Concurrency**: the endpoint and the fetch loop both mutate `reservations`
  across `await` points, so each reconcile is serialised by
  `ControllerState.reservation_lock` (the one place the "no locking" rule is
  relaxed).
- **RBAC**: unchanged — eviction reuses the existing `pods: delete` /
  `events: create` permissions.

### Reclaim-block take-back API

`POST /api/reservations/take-back` (`{"reclaim_ids": [id, …]}`) lets the
reservation app **reclaim specific `kind="reclaim"` blocks from the controller**
so it can re-book capacity that is already inside the preempt guard (the guard's
one-way "committed, will not preempt" promise becomes a handshake: the app asks
first, and only commits its tentative booking once the controller has ceded the
blocks).  Guarded by the same `INBOUND_API_TOKEN` bearer as the push API.

- **All-or-nothing**: every requested block must be idle or the whole request is
  rejected with 409 and nothing is mutated.  "In use" means: a live pod is
  admitted under the block's id (unified occupancy — the in-memory map is merged
  with a fresh `snapshot_tolerated_pods` LIST so both in-flight optimistic
  placements and pods from a prior controller lifetime count), the id is claimed
  by a chained holder (defensive; holders never chain into reclaim blocks), or —
  for a block absorbed into a reclaim merge — some pod on the merge's *subject*
  **projects** past the block's start (`status.startTime +
  spec.activeDeadlineSeconds`, captured in the snapshot; an unknowable deadline
  counts as unbounded, failing safe).  A job admitted *before* the merge
  (deadline ≤ the subject's own end) therefore never blocks a take-back.
- **Merge detachment** (`ControllerState.take_back_blocks`): taking an absorbed
  stub truncates its `ReclaimMerge` at the earliest taken position — earlier
  stubs (which a running job's extended deadline may still reach) stay stubbed,
  later untaken stubs detach back to standalone blocks (a taken block leaves a
  hole, so they no longer abut).  Taking a provably idle merge *subject*
  dissolves the record and detaches all untaken stubs.  Merge rediscovery is
  deliberately not re-run inside the handler (wholesale dissolve-and-rediscover
  would un-stub blocks under a *claimed* subject — the B4 case); the next
  tick/refresh reconciles normally.
- **Tombstones** (`ControllerState.taken_back`): granted ids are remembered
  until their window's `end_utc` and **filtered from fetch results**
  (`filter_taken_back`) — a fetch snapshot taken before the grant, or the app's
  own DB until its replacement booking commits, must not resurrect ceded
  capacity.  An explicit **push** of the id clears the tombstone
  (`clear_taken_back`) — the sanctioned restore path when the app's booking
  falls through.  An id the controller has never seen is granted (it can never
  have placed pods on it), reported as `unknown`, and tombstoned with a
  `2 × RESERVATION_FETCH_INTERVAL` fallback expiry that is pinned to the real
  window the first time a fetch observes the id.
- **Atomicity**: the handler holds `reservation_lock` and performs its single
  `await` (the pod snapshot) *before* the check; check and mutation then run in
  one synchronous section.  Both placement coroutines record occupancy
  synchronously before their first `await`, so a placement either shows up in
  the occupancy the check reads, or runs after the block is already gone.
- **Fail closed**: a failed pod snapshot returns 503 and grants nothing.  A
  non-reclaim id in the request returns 400 (`not-a-reclaim-block`).  Retries
  are idempotent (already-granted ids report `already_taken_back`).
- **Follow-up compatibility**: the planned "push a single reservation built from
  taken-back blocks" needs no new surface — the existing push upserts new ids,
  and tombstones never filter pushes.  The request/response envelopes leave room
  for an atomic replacement field later.

### Startup behaviour

On startup, the controller performs an initial reservation fetch and then issues
a LIST of all current pods before entering the WATCH stream.  This ensures that
pods created before the controller was running (e.g. during a restart) are not
missed.

### In-memory state only

No database.  If the controller restarts, it rebuilds all state from the
Kubernetes API and the reservation API within one fetch cycle.  Queue entries
that were waiting for a window that has already opened will be re-evaluated
immediately on the next processor tick.

Occupancy is reconstructed from the `horae/booking-reference` annotation: the
reservation id parsed from each admitted pod's booking-reference is summed into
the unified occupancy map.  The startup pod LIST seeds this, and every
queue-processor tick rebuilds the map wholesale from a live cluster snapshot
(`snapshot_tolerated_pods` → `reconcile_occupancy`), so a missed watch event
self-heals within one tick.  An optimistic placement recorded between ticks whose
patch is not yet visible in the snapshot may be briefly dropped and re-captured
on the next tick — a window bounded by the `POD_LIST_TICK_INTERVAL` tick
(default 300 s).

**No-show state does not survive restarts.**  `noshow_reservation_ids` is
in-memory and never written back to the reservation API (the key is
read-only).  After a restart, every mid-window user reservation — including
ones previously declared no-show — receives a fresh
`NOSHOWN_GRACE_MINUTES` deadline, so a late-arriving holder can reclaim
their window across a restart.  Within a single controller lifetime the
declaration is permanent.  Related: when a holder's pod is deleted
mid-window (e.g. idle-culled), the reservation's deadline was already
cleared, so the next refresh's `update_noshow_tracking` re-arms it with the
grace timeout — this is what eventually converts vacated windows to
on-demand capacity.

**Chained holders are protected from no-show conversion.**  A pod admitted
under `res-X` whose runtime cap is chained across back-to-back windows (`X`,
`X+1`, …) physically occupies those later windows even though no pod is booked
directly under them.  Each queue-processor tick scans live reserved-path holder
pods and marks every window they occupy (`reservations_claimed_by`) as
**claimed**; claimed reservations have their no-show deadline cleared and are
skipped by `check_noshow_deadlines`, `update_noshow_tracking`, and
`find_ondemand_block`.  This stops a reservation a holder is still using from
being declared a no-show and double-booked as on-demand capacity.  When the
holder vacates, the reservation leaves the claimed set and the grace re-arm path
above applies.

---

## Environment variables

| Variable | Default | Purpose |
|----------|---------|---------|
| `RESERVATION_API_URL` | *(required)* | Base URL of the reservation management app |
| `RESERVATION_API_KEY` | *(required)* | `gpures_…` service key; mount from a Kubernetes Secret |
| `RESERVATION_FETCH_INTERVAL` | `300` | Seconds between reservation refresh cycles |
| `RESERVATION_LOOKAHEAD_DAYS` | `7` | Calendar days ahead to fetch reservations |
| `KUBECONFIG` | *(absent = in-cluster)* | Path to kubeconfig file for out-of-cluster use |
| `HEALTH_PORT` | `8000` | Port for `GET /health` (also serves `POST /api/reservations/push`) |
| `INBOUND_API_TOKEN` | *(absent = inbound API disabled)* | Bearer token guarding `POST /api/reservations/push` and `POST /api/reservations/take-back`; mount from a Kubernetes Secret. Unset ⇒ endpoints return 503 |
| `TZ` | system default | Affects log timestamp display only; no longer required for window arithmetic |
| `ONDEMAND_PLACEMENT_ENABLED` | `true` | Set to `false` to disable on-demand placement entirely |
| `NOSHOW_TIMEOUT_MINUTES` | `15` | Minutes after window opens before a reservation is declared a no-show (legacy alias `NOSHOWN_TIMEOUT_MINUTES` still honored) |
| `NOSHOW_GRACE_MINUTES` | `30` | Grace period after controller startup before mid-window no-shows are declared (legacy alias `NOSHOWN_GRACE_MINUTES` still honored) |
| `POD_LIST_TICK_INTERVAL` | `300` | Seconds between queue-processor ticks (pod LIST frequency) |
| `POD_SCHEDULING_GATE_NAME` | *(absent)* | Name of the SchedulingGate to remove after admitting a pod; unset = disabled |
| `LOG_LEVEL` | `INFO` | Root Python logging level (parsed by `config.py`) |

---

## Configuration

Runtime configuration is through environment variables only — no config files,
no database, no secrets embedded in the image.

---

## Deployment

The Dockerfile builds a minimal image:

- Base: `python:3.13-slim`
- Non-root user `appuser` (UID 1000)
- Health check: `GET http://localhost:8000/health`
- Entrypoint: `uvicorn app.main:app --host 0.0.0.0 --port 8000`

A Helm chart at `helm/gpu-reservation-controller/` renders the
ServiceAccount, ClusterRole/Binding, Deployment, and `/health` Service; keep
its `values.yaml`/`deployment.yaml` env wiring in sync when adding settings
to `config.py`.  See README.md for RBAC requirements and a sample manual
Deployment manifest.
