## Initial Setup — IMPORTANT

The first time you interact with this repository, review and follow the
initial setup instructions in AGENTS.md.

---

## What this project is

A **Kubernetes controller daemon** — not a web application.  It has no
database, no user-facing frontend, and no authentication layer of its own.
It authenticates *outbound* to the GPU Reservation API using a long-lived
service key, and it authenticates *inbound* to Kubernetes using either a
kubeconfig file or an in-cluster service account.

---

## Technology choices

| Choice | Rationale |
|--------|-----------|
| **FastAPI** | Provides the `GET /health` liveness endpoint and clean lifespan management for background tasks; no routers or static files needed |
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
├── k8s_client.py         Kubernetes wrapper — PodWatcher, apply_toleration, set_active_deadline, emit_runtime_capped_event, count usage
└── controller.py         ControllerState, QueueEntry, matching, window arithmetic
```

### Background tasks (started in `lifespan`, cancelled on shutdown)

| Task | Cadence | Responsibility |
|------|---------|----------------|
| `reservation_fetch_loop` | every `RESERVATION_FETCH_INTERVAL` s (default 300) | Re-fetches active reservations; resolves `gpu_class_id → label_value`; reconciles stale queue entries |
| `pod_watch_loop` | continuous (LIST + WATCH) | Enqueues pods with `gpu-class` label that lack the toleration; dequeues deleted pods; **fast-path**: applies toleration immediately when a new pod arrives inside an open window |
| `queue_processor_loop` | every 30 s | Handles pods queued before their window opened; retries pods that were over-budget; schedules retries with 2–5 min jitter |

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

The patch also stamps the pod with the `dsmlp/booking-reference` annotation:
`res-<id>` (reserved path), `ondemand-<id>`, or `noshow-<id>`.  This is the
controller's single record of which reservation a pod was admitted under.  The
GPU **budget check** (`ControllerState.available`) counts every pod recorded
against a reservation id in the **unified occupancy map** (one map for reserved,
on-demand, and no-show alike — keyed by reservation id), so each reservation has
an independent budget; the id parsed from this annotation
(`parse_booking_reference`) is also how occupancy is rebuilt from the cluster
after a restart.  The prefix records the admission path and is otherwise
cosmetic.

`set_active_deadline` additionally writes `dsmlp/pod-runtime-limit-seconds`
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

All reservation window arithmetic uses **local system time** (naive `datetime`).
Set the `TZ` environment variable on the controller pod to match the timezone
the reservation server uses (coordinate with the cluster operator).

### Fast path for mid-window pods

When a pod ADDED event arrives while its reservation window is already open
(the common case for JupyterHub notebook servers launched during a session),
`pod_watch_loop` calls `_try_apply_toleration` immediately rather than waiting
up to 30 s for the next queue-processor tick.

Only ADDED events trigger the fast path.  MODIFIED events — which can arrive in
rapid bursts as Kubernetes reconciles pod state — go through the normal queue so
the Kubernetes API is not hammered.  If the immediate attempt fails (budget full
or transient error), the entry remains in the queue and the processor retries it
on its normal schedule.

`_try_apply_toleration` is the single shared coroutine that performs the budget
check and patch; both the fast path and the queue processor call it.

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

Occupancy is reconstructed from the `dsmlp/booking-reference` annotation: the
reservation id parsed from each admitted pod's booking-reference is summed into
the unified occupancy map.  The startup pod LIST seeds this, and every
queue-processor tick rebuilds the map wholesale from a live cluster snapshot
(`snapshot_tolerated_pods` → `reconcile_occupancy`), so a missed watch event
self-heals within one tick.  An optimistic placement recorded between ticks whose
patch is not yet visible in the snapshot may be briefly dropped and re-captured
on the next tick — a window bounded by the 30 s tick interval.

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
| `HEALTH_PORT` | `8000` | Port for `GET /health` |
| `TZ` | system default | Timezone for reservation window arithmetic |
| `ONDEMAND_PLACEMENT_ENABLED` | `true` | Set to `false` to disable on-demand placement entirely |
| `NOSHOWN_TIMEOUT_MINUTES` | `15` | Minutes after window opens before a reservation is declared a no-show |
| `NOSHOWN_GRACE_MINUTES` | `30` | Grace period after controller startup before mid-window no-shows are declared |

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
