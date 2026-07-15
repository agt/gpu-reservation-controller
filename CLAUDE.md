## Initial Setup — IMPORTANT

The first time you interact with this repository, review and follow the
initial setup instructions in AGENTS.md.

---

## What this project is

A **Kubernetes controller daemon** — not a web application.  It has no
database and no user-facing frontend.  It authenticates *outbound* to the GPU
Reservation API using a long-lived service key, and *inbound* to Kubernetes
using either a kubeconfig file or an in-cluster service account.  It also
exposes a small optional **inbound push API**
(`POST /api/reservations/push`) that the reservation app calls to propagate
reservation updates faster than the poll interval; it is guarded by a single
static bearer token and disabled unless that token is configured.

The app schedules only real reservations (`kind="booking"`) — there is no
ad-hoc "reclaim" capacity type.  A pod with no reservation open now or soon
gets one **just-in-time (JIT)**: the controller requests a short on-demand
booking on the pod's behalf (`POST /api/reservations`), so on-demand jobs are
ordinary reservations — charged SU, protected by runtime guarantees, and able
to displace overstayers via the same boundary preemption as any other booking
(see **Just-in-time (JIT) on-demand leases**).

---

## Technology choices

| Choice | Rationale |
|--------|-----------|
| **FastAPI** | Provides the `GET /health` liveness endpoint, the `POST /api/reservations/push` inbound API, and clean lifespan management for background tasks; no routers or static files needed |
| **asyncio** | Single event loop drives all four background loops concurrently without threads for the application logic |
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
├── main.py               Entry point — FastAPI app, lifespan, four background tasks
├── config.py             Config dataclass populated from environment variables
├── schemas.py            Pydantic models mirroring RESERVATION-API.md §6
├── reservation_client.py httpx async client — fetches reservations + GPU classes; creates/cancels JIT on-demand reservations
├── k8s_client.py         Kubernetes wrapper — PodWatcher, apply_toleration, annotate_runtime_guarantee, emit_preempted_event, snapshot_tolerated_pods / snapshot_node_gpu_capacity (occupancy + capacity)
└── controller.py         ControllerState, QueueEntry, matching, window arithmetic, preemption planning
```

### Background tasks (started in `lifespan`, cancelled on shutdown)

| Task | Cadence | Responsibility |
|------|---------|----------------|
| `reservation_fetch_loop` | every `RESERVATION_FETCH_INTERVAL` s (default 300) | Re-fetches active reservations; refreshes `gpu_class_id ↔ label_value` maps; reconciles stale queue entries |
| `pod_watch_loop` | continuous (LIST + WATCH) | Routes a pod with the `gpu-class` label and no toleration to the reserved queue (a match is open or opens soon) or to a JIT on-demand lease request; dequeues deleted pods; **fast-path**: applies toleration immediately when a new pod arrives inside an open window |
| `queue_processor_loop` | every `POD_LIST_TICK_INTERVAL` s (default 300) | Handles pods queued before their window opened; retries pods that were over-budget; requests/retries JIT leases; cancels declared no-shows; schedules retries with 2–5 min jitter |
| `preemption_loop` | every `PREEMPTION_CHECK_INTERVAL` s (default 60) | Recovers capacity from pods running past their runtime guarantee, only when an upcoming reservation boundary needs it (see **Runtime guarantees and demand-driven preemption**) |

---

## Key design decisions

### Matching pods to reservations

A pod matches a reservation when **both** of the following hold:

```
pod.metadata.namespace  ==  reservation.user.username
pod.labels["gpu-class"] ==  gpu_class.label_value   # from GET /api/gpu-classes/{id}
```

`label_value` is cached in `ControllerState.gpu_class_labels`, refreshed every
reconcile (fetch or push) from `GET /api/gpu-classes` (the full list), with a
per-id `GET /api/gpu-classes/{id}` fallback for a class referenced by a
reservation but missing from that list (e.g. one created since the last
successful bulk fetch).  A failed bulk fetch keeps the previous cycle's map
rather than losing all label resolution.  If a GPU class has no `label_value`,
its reservations are skipped with a warning.  The reverse map,
`ControllerState.gpu_class_ids` (label → id), is refreshed the same way and is
what the JIT lease path resolves a pod's `gpu-class` label to the numeric
`gpu_class_id` a booking request needs (see **Just-in-time (JIT) on-demand
leases**).

**Optional usage-group constraint** (`REQUIRED_GROUP_LABEL`, default off).  When
set to a pod-label name (e.g. `dsmlp/course`), a **third** equality is required:

```
pod.labels[REQUIRED_GROUP_LABEL]  ==  reservation.group.name
```

It behaves like `gpu-class` — an additional match axis threaded alongside
`gpu_class_label` (`ControllerState._group_ok`, gated on
`ControllerState.required_group_label`, set once from config at startup).  The
gate applies to the **reserved path**: `find_best_reservation`,
`find_admittable_reservation` (the JIT routing gate), `find_open_booking_for`
(adoption), `mark_pod_seen_for_noshow`, and reserved back-to-back chaining
(`_chain_for` additionally requires equal `group_id`, so a guarantee never
chains across a different group's window).  Matching is against the group
**name** (the reservation carries only `GroupBrief{id, name}`; there is no
per-group `label_value`).  A pod with no such label (feature on) matches **no**
grouped reservation on the reserved path; it is also never JIT-eligible (a
JIT request carries the group name from the pod's label, so a labelless pod
has none to carry) and is left Pending.

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
`res-<id>`, where `<id>` is the reservation the pod was admitted under —
whether it pre-existed or was requested just-in-time on the pod's behalf, every
admitted pod is a reserved-path holder under a real reservation, so there is
only the one prefix.  This is the controller's single record of which
reservation a pod was admitted under.  The GPU **budget check**
(`ControllerState.available`) counts every pod recorded against a reservation
id in the **unified occupancy map** (keyed by reservation id), so each
reservation has an independent budget; the id parsed from this annotation
(`parse_booking_reference`) is also how occupancy is rebuilt from the cluster
after a restart.

`annotate_runtime_guarantee` additionally writes `horae/pod-runtime-limit-seconds`
and `horae/guaranteed-until` — see **Runtime guarantees and demand-driven
preemption** below.  Neither is enforced by Kubernetes; both are
informational only.

### Runtime guarantees and demand-driven preemption

When a pod is admitted (toleration successfully applied), the controller
records how long its GPU access is **guaranteed** — but does **not** enforce
that guarantee with `spec.activeDeadlineSeconds`.  Users kept overestimating
their runtime "just in case" when the controller hard-killed pods at a fixed
estimate, so a pod may now keep running past its guarantee freely; the
controller reclaims capacity from an overstaying pod only when a new
reservation actually needs it.

**Guarantee calculation** — the guaranteed instant is:

1. `slot_end` of the current reservation window, **plus**
2. The full duration of any directly **back-to-back** future reservations
   sharing the same `user.username`, GPU class, and `gpu_count`, where
   `slot_start(next) == slot_end(previous)` with no gap.

This is the same back-to-back chaining rule the old hard cap used
(`ControllerState.compute_guaranteed_until`).  Unlike the old cap, the result
is an **absolute UTC instant recomputed live on every call**, not a duration
frozen at admission — so a pod's guarantee can *grow* after admission (the
user books an abutting follow-on reservation), something
`spec.activeDeadlineSeconds` could never do (Kubernetes forbids raising an
existing deadline).  `ControllerState.guarantee_end` is the general-purpose
entry point: given a booking-reference id, it returns the live guarantee
instant, or `None` if the reservation is no longer active (its window is
unconditionally over) — every admitted pod resolves through this one path
now that on-demand jobs are ordinary reservations too.

**Recording the guarantee** — after calling `apply_toleration`,
`_record_guarantee` in `main.py`:

- Annotates the pod (`annotate_runtime_guarantee`) with informational-only
  `horae/pod-runtime-limit-seconds` (guaranteed duration in seconds — the
  legacy key name; it no longer backs a hard cap) and `horae/guaranteed-until`
  (the same instant as an absolute UTC ISO-8601 timestamp).  A guarantee can
  technically shrink after the annotation is written (a window shortened
  server-side, or a merge component vanishing) — nothing re-reads these
  annotations to make a decision, so a widget should treat them as
  best-effort, not authoritative.
- Emits a `Normal` Kubernetes Event with `reason: RuntimeGuaranteed`,
  `action: GuaranteeRuntime`, stating when the guarantee ends and that the
  pod may be preempted afterward if capacity is needed.
- Both steps are best-effort.  A failure logs a warning and does **not**
  revoke the toleration.

**Recovering capacity** is the job of `preemption_loop` (`_run_preemption_sweep`
in `main.py`), which on every tick:

1. Finds every distinct booking `slot_start` ("boundary") within
   `PREEMPTION_LEAD_MINUTES` (default 15) of now.
2. Snapshots **physical capacity** per GPU class (`snapshot_node_gpu_capacity`
   — one node LIST summing allocatable `nvidia.com/gpu` grouped by the
   `gpu-class-reservation` taint value on each node).  This is the
   controller's only notion of how many GPUs physically exist; nothing else
   in the codebase tracks it.
3. For each boundary/phase not yet evaluated, computes **demand**
   (`ControllerState.boundary_demand`): per class, the sum over bookings
   starting exactly there of their remaining budget (`available`) minus GPUs
   already supplied by a live reserved-path holder whose back-to-back chain
   covers that reservation — subtracted per-reservation rather than excluding
   the whole claimed reservation from demand, so a partially-chained
   multi-GPU booking is not under-counted.
4. Computes **free** capacity (`free_capacity_by_class`): physical capacity
   minus GPUs used by node-resident, non-terminating pods.
5. If `demand > free`, the controller identifies the **eligible candidate
   pool** per class (`ControllerState.plan_boundary_candidates`) — pods of the
   same GPU class, admitted by this controller, live, not already terminating,
   and **past their runtime guarantee** (`guarantee_end` is `None` or `<= now`)
   — and the per-class GPU shortfall (`kills_needed`).  A pod within its
   guarantee is **never** a candidate, however severe the shortfall.  **Which**
   candidates become victims is then **delegated to the reservation app**
   (`POST /api/reservations/preemption-victims`, write-scope key; see
   `ReservationClient.select_preemption_victims`), so prioritisation policy
   (by reservation owner/group/kind) can live in the app rather than the
   controller.  The controller kills only pods it offered — an unknown uid in
   the response is ignored, and an **empty** response ("spare everyone") is
   respected.  When delegation is disabled (`PREEMPTION_DELEGATE_SELECTION=false`)
   or the app call **fails** (network/non-2xx/absent endpoint), the sweep falls
   back to local **uniform-random** selection (`select_victims_locally`) so
   preemption still works — this is the same greedy random pick the controller
   did before delegation existed.  A shortfall the returned victims do not cover
   is logged as an "unmet" warning (priority ranking within the app's random
   policy is deferred future design; the controller's own fallback is uniform).
6. Selected victims are deleted (`_preempt_pod`: emit a `Normal` Event with
   `reason: Preempted`, `action: PreemptPod`, then delete and release
   occupancy — all best-effort, mirroring the cancellation-eviction shape in
   `_handle_cancelled_reservations`).

**Two-phase, boundary-anchored trigger**: phase **A** (lead-time,
`boundary > now`) runs at `T − PREEMPTION_LEAD_MINUTES` and proactively frees
capacity from overstayers regardless of whether the incoming holder ever
shows up (this can preempt jobs on behalf of a reservation that turns out to
be a no-show — an accepted trade-off, not a bug).  Phase **B** (at-boundary,
`boundary <= now`) runs at the boundary itself and additionally makes
eligible any pod whose own guarantee ends exactly there.  Each boundary/phase
combination is evaluated at most once (`ControllerState.preemption_fired`,
pruned once the boundary leaves scope) — a restart loses the marks, but
re-evaluation is safe because a killed pod's capacity already shows as freed
(`deletionTimestamp` set, or gone) by the time anything re-checks.  Either
the pod snapshot or the node-capacity snapshot failing skips the whole sweep
with a warning — the controller never kills a pod based on unknown physical
state.

**RBAC**: the controller's ServiceAccount must have `create` on `events` and
`get`/`list` on `nodes`, in addition to the existing pod permissions.

### Adopting overstay pods into a re-booked reservation

Because pods may overrun, a user can book a **fresh** reservation (a new,
distinct id) while their pod from the previous window is still running.  The
running pod should continue under the new reservation rather than be preempted.

When the new window **abuts** the old one (`slot_start(new) == slot_end(old)`),
same user, GPU class, and `gpu_count`, the existing back-to-back **chaining**
(`_chain_for` / `compute_guaranteed_until`) already covers this: the old
reservation stays in the fetched set (status is only `active`/`cancelled`, and
`fetch_reservations` widens `date_start` to `today − 1`), so `guarantee_end`
recomputes live and grows to the new window's end, and `boundary_demand`
credits the pod via `reservations_claimed_by` — no re-link needed.

**Adoption** (`POD_ADOPTION_ENABLED`, default on) covers what chaining cannot,
via `ControllerState.plan_pod_adoptions` (pure), which pairs a pod already
**past its runtime guarantee** (`_past_guarantee`, shared with the preemption
planner) — a **non-abutting** follow-on window (a gap, or one that starts at
"now") or a **different `gpu_count`** that chaining cannot reach — with a
currently-open booking the same user holds that has spare budget
(`find_open_booking_for`).  There is no proactive-upgrade trigger: every
admitted pod is already tied to a real reservation (JIT or otherwise), so
there is nothing to "upgrade" — only overstay pods are ever rescued.

`_adopt_pods` in `main.py` then re-annotates the pod's `horae/booking-reference`
to `res-<new id>` (the toleration is already present, so this patch only
rewrites the annotation) and, **only on patch success**, re-homes occupancy
(`relink_occupancy`) and refreshes the in-memory `PodRuntimeView`.  It emits an
`OverstayRelinked` event and re-records the runtime guarantee.

Adoption runs in two places, both under `reservation_lock`: inside
`_run_preemption_sweep` **before** victims are planned — so a pod the user has
just re-booked is re-homed (zeroing that boundary's demand) and can never be
selected as a victim — and once per **queue-processor tick** as a lazy tidy-up
that re-links pods even when no boundary is near.  A pod with no open booking to
adopt is left untouched (legitimately overstay).

Separately, a still-*pending* JIT candidate's own routing is re-checked on
every attempt (`main._try_request_lease`, step 2): if a reservation has since
become admittable for it, it is routed to the reserved queue instead of
requesting a lease — the pre-admission analogue of adoption, before there is
even a pod to preempt.

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

### Just-in-time (JIT) on-demand leases

A pod with the `gpu-class` label but no reservation open now or opening soon
does not wait indefinitely and is not opportunistically placed onto ad-hoc
spare capacity — instead the controller requests a **real reservation** on
its behalf, just-in-time.  On-demand jobs are therefore ordinary
reservations: charged SU, protected by the same runtime guarantee as any
booking, and able to displace overstayers via the existing boundary
preemption — none of that machinery needed a separate on-demand code path.

**Routing** (`pod_watch_loop`, re-evaluated on every attempt): for a pod
without the toleration,

1. `ControllerState.find_admittable_reservation` — the budget/horizon-aware
   sibling of `find_best_reservation` — looks for a match whose window is
   open now or opens within `ONDEMAND_HORIZON_MINUTES` (default 30) **and**
   has spare budget (`available(r) >= gpu_requested`).  If found, the pod is
   queued for it (`enqueue_pod`), with the same fast-path immediate-apply
   when the window is already open.
2. Otherwise, if the pod is **JIT-eligible** — `ONDEMAND_PLACEMENT_ENABLED`,
   `Pending`, carries `horae/minimum-runtime-seconds`, and carries the group
   label when `REQUIRED_GROUP_LABEL` is set — it becomes an
   `OnDemandCandidate` and `main._try_request_lease` is attempted immediately
   (and again on later `MODIFIED` events, respecting its retry cooldown, so a
   guard-1 "not yet scheduled" short retry resolves quickly).
3. Otherwise, if `find_best_reservation` finds *any* future match (beyond the
   horizon, or over budget), the pod is queued for it anyway — this preserves
   the plain wait-for-window behaviour for a pod that isn't JIT-eligible.
4. Otherwise the pod is left **Pending** (a pod missing the group label or the
   minimum-runtime annotation is deliberately not guessed at; that is left
   for a future "born overstay" design).

**Requesting a lease** (`main._try_request_lease`): re-reads the pod (drops it
if gone/terminal/Unknown), re-runs step 1 above (a matching reservation may
have appeared since the candidate was queued), applies guard 1
(`is_gpu_only_pending`) and guard 3 (`stuck_holder_gpu_classes`), resolves the
pod's `gpu-class` label to a numeric id via `ControllerState.gpu_class_ids`,
then calls `POST /api/reservations` with
`duration_seconds = minimum-runtime + ONDEMAND_LEASE_BUFFER_MINUTES * 60` (default
buffer 10 min) and `on_demand=True` (the app relaxes policy limits — SU,
caps, minimum duration — never physical calendar capacity).  The request is
**idempotent by the pod's UID** (`idempotency_key`): a retry after a prior
grant returns the same reservation rather than creating a duplicate.

- **Denied** (409 / error → `None`): the candidate cools down 2–5 min and
  retries — no different from a reserved-path pod retrying a budget-full window.
- **Granted**: the lease is upserted into `state.reservations`
  (`apply_push_to_active`) and the pod is admitted under it immediately
  (`_try_apply_toleration`) — the existing admission path, so it stamps
  `res-<id>`, records the guarantee, and emits `RuntimeGuaranteed` exactly like
  any other reservation.  **If admission does not succeed** (budget race, a
  transient patch error, or the pod having gone terminal in the interim), the
  controller issues a compensating cancel
  (`POST /api/reservations/{id}/cancel`, `reason="controller-revoked"`) so the
  grant is never left dangling, and removes the lease from `state.reservations`.

The queue-processor tick retries any still-pending candidate in FIFO order
the same way; there is no separate on-demand admission function — both call
sites share `_try_request_lease`.

**RBAC / config**: no new Kubernetes permissions (admission reuses the
existing pod-patch path); `ONDEMAND_HORIZON_MINUTES` and
`ONDEMAND_LEASE_BUFFER_MINUTES` tune the routing horizon and lease sizing.

### Inbound push API

`POST /api/reservations/push` lets the reservation app push **one or more
updated reservation entries** (today: cancellations and owner changes) so they
propagate within seconds instead of waiting up to a full
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
  releases its capacity — the *same* path a mid-window cancellation takes on a
  fetch.  An entry that keeps its id but changes owner (**adoption** — a
  reservation reassigned to a teammate) evicts the prior owner's admitted pod
  from its namespace and releases the capacity, so the new owner can claim the
  still-active window (`detect_owner_changed_in_window` + `_handle_owner_changes`).
- **Shared reconciliation**: both the fetch loop and the push run
  `_reconcile_after_reservation_change` in `main.py` (GPU class map refresh,
  queue reconcile, cancellation eviction, owner-change eviction).
- **Concurrency**: the endpoint and the fetch loop both mutate `reservations`
  across `await` points, so each reconcile is serialised by
  `ControllerState.reservation_lock` (the one place the "no locking" rule is
  relaxed).
- **RBAC**: unchanged — eviction reuses the existing `pods: delete` /
  `events: create` permissions.

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

**No-show declaration is in-memory, but the resulting cancel is durable.**
`noshow_reservation_ids` (and `pending_noshow_cancels`, the queue of ids still
awaiting their cancel) are never written back and don't survive a restart.
After a restart, every mid-window user reservation — including ones the prior
controller lifetime already declared no-show — receives a fresh
`NOSHOWN_GRACE_MINUTES` deadline, so a late-arriving holder can reclaim their
window across a restart.  But once a no-show's cancel actually **lands**
(`POST /api/reservations/{id}/cancel`, `reason="no-show"`), that id is
durably gone from the app's active set — the very next fetch simply no longer
reports it, so there is nothing to re-arm and no window for a restart to race.
Related: when a holder's pod is deleted mid-window (e.g. idle-culled), the
reservation's deadline was already cleared, so the next refresh's
`update_noshow_tracking` re-arms it with the grace timeout — this is what
eventually converts a vacated window into a no-show cancel.

**Chained holders are protected from no-show conversion.**  A pod admitted
under `res-X` whose runtime guarantee is chained across back-to-back windows
(`X`, `X+1`, …) physically occupies those later windows even though no pod is
booked directly under them.  Each queue-processor tick scans live reserved-path holder
pods and marks every window they occupy (`reservations_claimed_by`) as
**claimed**; claimed reservations have their no-show deadline cleared and are
skipped by `check_noshow_deadlines` and `update_noshow_tracking`.  This stops a
reservation a holder is still using from being declared a no-show and
cancelled out from under it.  When the holder vacates, the reservation leaves
the claimed set and the grace re-arm path above applies.

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
| `INBOUND_API_TOKEN` | *(absent = inbound API disabled)* | Bearer token guarding `POST /api/reservations/push`; mount from a Kubernetes Secret. Unset ⇒ endpoint returns 503 |
| `TZ` | system default | Affects log timestamp display only; no longer required for window arithmetic |
| `ONDEMAND_PLACEMENT_ENABLED` | `true` | Set to `false` to disable the JIT on-demand lease path entirely (a non-JIT-eligible pod still waits for a matching reservation; an ineligible one is left Pending) |
| `ONDEMAND_HORIZON_MINUTES` | `30` | JIT routing horizon: a pod is queued for a reservation that opens within this many minutes (with budget) instead of requesting a lease |
| `ONDEMAND_LEASE_BUFFER_MINUTES` | `10` | Minutes added to a pod's `horae/minimum-runtime-seconds` when sizing a requested JIT lease's duration |
| `NOSHOW_TIMEOUT_MINUTES` | `15` | Minutes after window opens before a reservation is declared a no-show (legacy alias `NOSHOWN_TIMEOUT_MINUTES` still honored) |
| `NOSHOW_GRACE_MINUTES` | `30` | Grace period after controller startup before mid-window no-shows are declared (legacy alias `NOSHOWN_GRACE_MINUTES` still honored) |
| `POD_LIST_TICK_INTERVAL` | `300` | Seconds between queue-processor ticks (pod LIST frequency) |
| `POD_SCHEDULING_GATE_NAME` | *(absent)* | Name of the SchedulingGate to remove after admitting a pod; unset = disabled |
| `REQUIRED_GROUP_LABEL` | *(absent)* | Pod label naming the usage group (e.g. `dsmlp/course`); when set, the pod's value must equal the reservation's `group.name` — an extra match axis alongside `gpu-class` (see **Matching pods to reservations**), and a pod without the label is never JIT-eligible either. Unset = disabled |
| `PREEMPTION_LEAD_MINUTES` | `15` | Minutes before a reservation slot boundary that phase-A preemption runs |
| `PREEMPTION_CHECK_INTERVAL` | `60` | Seconds between preemption sweeps |
| `PREEMPTION_DELEGATE_SELECTION` | `true` | Ask the app to choose preemption victims from the eligible pool (`POST /api/reservations/preemption-victims`); `false` (or any app-call failure) falls back to local uniform-random selection |
| `POD_ADOPTION_ENABLED` | `true` | Re-link an overstay pod to a reservation its user has since booked (see **Adopting overstay pods into a re-booked reservation**); `false` disables |
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
