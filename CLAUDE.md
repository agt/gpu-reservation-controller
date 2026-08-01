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
ordinary reservations — charged SU and protected by runtime guarantees.  (A
granted lease does **not** itself trigger boundary preemption of overstayers —
see the caveat in **Just-in-time (JIT) on-demand leases**.)

---

## Technology choices

| Choice | Rationale |
|--------|-----------|
| **FastAPI** | Provides the `GET /health` liveness endpoint, the `POST /api/reservations/push` inbound API, the `GET /api/forecast/preemption-risk` forecast, and clean lifespan management for background tasks; no routers or static files needed |
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
├── reservation_client.py httpx async client — fetches reservations (each page with a bounded transient retry) + GPU classes; creates/cancels JIT on-demand reservations
├── log_fields.py         kv() — renders log message bodies as key=value fields (see docs/LOG-FIELDS.md)
├── trace.py              Per-unit-of-work trace ids + X-Client-Trace propagation (see **Trace ids**)
├── k8s_client.py         Kubernetes wrapper — PodWatcher, apply_toleration, annotate_runtime_guarantee, emit_preempted_event, snapshot_tolerated_pods / snapshot_node_gpu_inventory (per-node) / snapshot_node_gpu_capacity (per-class collapse of it)
└── controller.py         ControllerState, QueueEntry, matching, window arithmetic, preemption planning, preemption-risk forecast
```

### Background tasks (started in `lifespan`, cancelled on shutdown)

| Task | Cadence | Responsibility |
|------|---------|----------------|
| `reservation_fetch_loop` | every `RESERVATION_FETCH_INTERVAL` s (default 300) | Re-fetches active reservations; refreshes `gpu_class_id ↔ label_value` maps; reconciles stale queue entries |
| `pod_watch_loop` | continuous (LIST + WATCH) | Routes a pod with the `gpu-class` label and no toleration to the reserved queue (a match is open or opens soon) or to a JIT on-demand lease request; dequeues deleted pods and, when a deleted/terminated pod was admitted under a JIT lease, cancels that lease; **fast-path**: applies toleration immediately when a new pod arrives inside an open window |
| `queue_processor_loop` | every `QUEUE_PROCESSOR_INTERVAL` s (default 300) | Handles pods queued before their window opened; retries pods that were over-budget; requests/retries JIT leases; cancels declared no-shows; schedules retries with 2–5 min jitter |
| `preemption_loop` | every `PREEMPTION_CHECK_INTERVAL` s (default 60) | Recovers capacity from pods running past their runtime guarantee, only when an upcoming reservation boundary needs it (see **Runtime guarantees and demand-driven preemption**) |
| `capacity_audit_loop` | every `CAPACITY_CHECK_INTERVAL` s (default 3600) | Compares app-side per-class GPU capacity (`total_gpus`) against physical cluster capacity; logs any difference as a WARNING and pauses on-demand admission for over-committed classes (see **App-side vs physical capacity reconciliation**) |

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
that guarantee with `spec.activeDeadlineSeconds`.  A pod may keep running past
its guarantee freely; the controller does not hard-kill it at a fixed runtime
estimate and reclaims capacity from an overstaying pod only when a new
reservation actually needs it.

**Guarantee calculation** — the guaranteed instant is:

1. `slot_end` of the current reservation window, **plus**
2. The full duration of any directly **back-to-back** future reservations
   sharing the same `user.username`, GPU class, and `gpu_count`, where
   `slot_start(next) == slot_end(previous)` with no gap.

The back-to-back chaining rule is `ControllerState.compute_guaranteed_until`.
The result is an **absolute UTC instant recomputed live on every call**, not a
duration frozen at admission — so a pod's guarantee can *grow* after admission
(the user books an abutting follow-on reservation), something
`spec.activeDeadlineSeconds` cannot express (Kubernetes forbids raising an
existing deadline).  `ControllerState.guarantee_end` is the general-purpose
entry point: given a booking-reference id, it returns the live guarantee
instant, or `None` if the reservation is no longer active (its window is
unconditionally over) — every admitted pod resolves through this one path
now that on-demand jobs are ordinary reservations too.

**Recording the guarantee** — after calling `apply_toleration`,
`_record_guarantee` in `main.py`:

- Annotates the pod (`annotate_runtime_guarantee`) with informational-only
  `horae/pod-runtime-limit-seconds` (guaranteed duration in seconds),
  `horae/guaranteed-until` (the same instant as an absolute UTC ISO-8601
  timestamp), and `horae/guarantee-status` (`guaranteed` at admission — see
  **Live guarantee-status annotations** below).  A guarantee can technically
  shrink after the annotation is written (a window shortened server-side, or a
  merge component vanishing) — nothing re-reads these
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

### Termination-warning annotations

After each preemption sweep executes its kills, it stamps the **survivors that
are still at risk** of being preempted at an upcoming boundary with a set of
informational `horae/termination-warning-*` annotations (`TERMINATION_WARNING_ENABLED`,
default on).  Like the runtime-guarantee annotations these enforce nothing and
are never read back to make a decision — a widget should treat them as a
best-effort heads-up so a job can checkpoint, extend, or re-book.

- **Annotations** (written by `k8s_client.annotate_termination_warning`):
  `horae/termination-warning-at` (the projected **kill instant** —
  `max(boundary − PREEMPTION_LEAD_MINUTES, guarantee_end)`, the start of the
  sweep's kill window and the earliest the pod could actually be deleted,
  absolute UTC ISO-8601), `horae/termination-warning-risk`
  (`min(1, shortfall/pool_gpus)` at that boundary, rounded to 2 decimals), and
  `horae/termination-warning-message` (human-readable, rendered deterministically
  from the two).  A pod killed proactively at a boundary (a **phase-A** victim,
  already past its guarantee) therefore reports the earlier `boundary − lead`
  kill time, not the boundary; a **phase-B** victim whose guarantee ends at the
  boundary degrades to the boundary itself.
- **Identification** (`ControllerState.plan_termination_warnings`, pure): reusing
  the forecast primitive `forecast_boundary_need`, for each in-scope boundary it
  flags the **full eligible pool** of any class with a residual shortfall.  The
  warning look-ahead is **decoupled from the kill lead** (`TERMINATION_WARNING_LEAD_MINUTES`,
  default 30, wider than `PREEMPTION_LEAD_MINUTES`): the boundary set is the
  **union** of the sweep's own kill window (`upcoming_boundaries(now, lead)`) with
  a wider forward horizon (`forecast_boundaries(now, now + warning_lead)` —
  forward-only, so it never widens the already-open side, and it drops no-shows).
  This gives a **phase-A** victim advance notice: an overstayer killed
  proactively at `boundary − lead` is flagged *before* its boundary enters the
  kill window, rather than on the very tick the sweep kills it (which is all the
  old kill-window-only horizon could manage — phase-B victims already got notice,
  phase-A victims got none).  Eligibility is **boundary-relative**
  (`guarantee_end <= boundary`, chains intact — computed once at the real now), so
  a pod whose guarantee expires *between* now and the boundary is flagged even
  though the sweep's own now-relative candidate planning would not yet consider
  it.  The set is computed from the **post-kill** pod set (pods preempted this
  tick are excluded), so a warning never targets a pod already being terminated.
  Iterating boundaries ascending gives each pod its **soonest** at-risk boundary
  (both the kill-instant timestamp and the risk come from it).  Because the wider
  horizon can put an at-risk boundary beyond the kill window, the sweep now runs
  (and takes its snapshots) whenever *either* a kill-window or a warning boundary
  is in scope, not only when there is a boundary to kill at.
- **Reconciliation** (`main._apply_termination_warnings`, best-effort, outside
  `reservation_lock`): diffs the desired warning against what each pod in the
  snapshot already carries (surfaced on `ToleratedPodInfo`) — writes a new/changed
  warning, **clears** (`clear_termination_warning`, annotation values set to
  `None`) a stale one when a pod leaves the at-risk pool (its user re-booked,
  demand evaporated, or it was adopted), and skips an unchanged one to avoid
  per-tick API churn.  This is restart-safe: the pod's own annotations are the
  state, so nothing leaks across a restart.
- **RBAC / config**: none new — the write/clear reuses the existing `pods: patch`
  permission.  `TERMINATION_WARNING_ENABLED=false` disables the feature;
  `TERMINATION_WARNING_LEAD_MINUTES` (default 30) sets how far ahead warnings
  look, independent of the kill lead — larger gives more advance notice at the
  cost of more speculative warnings on future demand that may change (bounded by
  the best-effort framing, the diff-and-skip reconcile, and live self-clearing
  when a user re-books or the booking no-shows).

### Live guarantee-status annotations

Distinct from the at-risk termination warning, every controller-admitted pod
carries a **general, always-present status** so a widget can show a job's
guarantee standing regardless of whether preemption is imminent.  Two keys,
both informational-only and never read back to make a decision:

- `horae/guarantee-status` — `guaranteed` while the pod is inside its live
  (chain-aware) runtime guarantee, `overstay` once it is running past it.
- `horae/guaranteed-until` — the guarantee-end instant (the *same* key
  `annotate_runtime_guarantee` writes at admission), kept **live**: future while
  in-guarantee, and left at its now-past value once overstay.

**Stamping and lifecycle.**  The status is written at the moments the guarantee
itself changes, plus a periodic reconcile for the one transition no event
covers:

- **Admission / adoption / merge** — `annotate_runtime_guarantee` (called by
  `_record_guarantee`) now also stamps `horae/guarantee-status: guaranteed`
  alongside the runtime-limit and `guaranteed-until` keys, so a freshly admitted
  or re-linked pod is immediately `guaranteed` with a forward end.
- **Expiration** — `main._apply_guarantee_status` runs once per
  **queue-processor tick** (reusing that tick's `snapshot_tolerated_pods`, no
  extra API call): `ControllerState.plan_guarantee_status` (pure) computes each
  admitted pod's live status from `guarantee_end`, and the reconcile flips a
  lapsed pod to `overstay` (leaving its now-past `guaranteed-until` frozen).  A
  pod whose guarantee **grew** (an abutting follow-on booking) gets its
  `guaranteed-until` refreshed forward on the same path.

**Diff-and-skip / restart-safe.**  The reconcile compares against the status the
pod already carries (surfaced on `ToleratedPodInfo.guarantee_status /
guaranteed_until`) and skips an unchanged pod, so it patches only on a real
transition.  There is **no clear path** — unlike the termination warning, the
status persists for the pod's admitted life and vanishes with the pod.  The
pod's own annotations are the state, so nothing leaks across a restart.  Each
pod is best-effort (a failure logs and is retried next tick).

**RBAC / config**: none new — the writes reuse the existing `pods: patch`
permission, and the feature is always on (like the runtime-guarantee
annotations it extends).

### Overstay reporting

For **offline analysis** of how far jobs run past their guarantee, the controller
reports each ended overstay to the app (`OVERSTAY_REPORT_ENABLED`, default off —
ships dark).  The report is filed when the overstay *ends* (the moment the full
duration is known), never during the run.

`main._report_overstay_if_any` is the single shared helper.  Given a pod at
teardown it: resolves the reservation id from `horae/booking-reference`; takes the
overstay **start** as the live chain-aware `guarantee_end` when the reservation is
still resolvable, else the pod's frozen `horae/guaranteed-until` annotation (the
reservation has often already left `state.reservations` by teardown); takes the
**end** as now; and **skips** the pod unless that is a genuine overstay
(`start` resolved and `now > start`) — a pod that finished within its guarantee
files nothing.  It then calls `ReservationClient.report_overstay`
(`POST /api/reservations/{id}/overstay`, write-scope key, modelled on
`cancel_reservation`: best-effort, swallows every error, never raises).  The app
copies GPU class / owner / group from the parent reservation, so the request
carries only the pod's `gpu_count`, the UTC window, and an `end_reason`.

Wired at the three sites a pod's life ends, adjacent to `release_pod`:
`pod_watch_loop`'s **DELETED** branch (`end_reason="deleted"`, before
`_teardown_ondemand_lease` removes the lease so `guarantee_end` can still resolve)
and its **terminal-phase** branch (`"pod-terminated"`), and `_preempt_pod`
(`"preempted"`, filed **before** the delete so it wins the app-side dedup over the
`"deleted"` report the pod's own DELETED event files moments later).  The app is
idempotent on `pod_uid`, so the preempt + DELETED double-fire records one row.

**RBAC / config**: none new (reuses the existing outbound service key); the
feature is off unless `OVERSTAY_REPORT_ENABLED` is set.

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

### Merging a JIT lease into a matching booking

A user who starts a pod **before** their booked window opens is admitted under a
just-in-time on-demand **lease** (`kind="on_demand"`; see **Just-in-time (JIT)
on-demand leases**).  Once that user's matching **booking** window opens, the
lease and the booking cover the same job — double-holding capacity and
double-charging SU on any overlap — until the pod happens to fall past its lease
guarantee and adoption re-links it.  **Merge** (`ONDEMAND_MERGE_ENABLED`, default
on) closes that gap proactively: as soon as the booking is open, the pod is
re-linked onto it and the lease is retired — **without** waiting for the lease
guarantee to lapse (the one intended difference from adoption).

`ControllerState.plan_ondemand_merges` (pure) is the planner: it pairs a pod
whose **current** reservation is a live `kind="on_demand"` lease with a
currently-open booking the same user holds that has spare budget
(`find_open_booking_for` — budget-based, **not** equal-`gpu_count`, so a pod using
**fewer** GPUs than the booking reserved still merges, which chaining never
could).  It is the proactive sibling of `plan_pod_adoptions`, sharing the same
occupancy-budget tally, but drops the `_past_guarantee` gate.  A pod whose
current reservation is already a booking, or that has no open booking, is left
untouched (a pod still in its **pre-booking** window stays on its lease —
correctly — until the booking opens).

`_merge_ondemand_into_bookings` in `main.py` executes it, modelled on
`_adopt_pods`: re-annotate the pod's `horae/booking-reference` to the booking
(annotation-only patch), and **only on patch success** re-home occupancy
(`relink_occupancy`), refresh the in-memory view, re-record the guarantee
(now the booking's — up to its chain end), and emit an `OverstayRelinked` event.
**Then** it retires the now-superfluous lease: cancels it **penalty-exempt**
(`POST /api/reservations/{id}/cancel`, `reason="superseded"` — the app charges
only already-consumed time, never a penalty on the unused tail, since the booking
re-covers the lease's remaining time) and drops it from `state.reservations`.
Ordering is deliberate — re-link **first** (the pod is never stranded without a
reservation), cancel **second**.  A cancel that does not land parks the lease id
in `ControllerState.pending_ondemand_merge_cancels`, retried every
queue-processor tick by `_drain_pending_merge_cancels` (mirroring
`pending_noshow_cancels`); the lease's short natural expiry is the backstop.

Merge runs in the same two places adoption does — inside `_run_preemption_sweep`
**before** victims are planned (a merged pod is on its booking and can never be a
victim) and once per **queue-processor tick** — always **before** `_adopt_pods`,
threading one shared view list so a just-merged pod is not re-processed by
adoption.  **RBAC / config**: no new Kubernetes permissions (merge reuses the
existing pod-patch + cancel paths).

### Log field grammar

Log lines are moving to a single `key=value` grammar — an `event=` noun followed
by flat fields, one concept per field, with no parentheses, slashes (`pod
ns/name` becomes `ns=… pod=…`), arrows or `a..b` ranges inside a value.
`docs/LOG-FIELDS.md` is the canonical field dictionary and `app/log_fields.py`
(`kv()`) is the only thing that renders it.

Both are **duplicated verbatim in the reservation app**, the same arrangement
`docs/RESERVATION-API.md` / the app's `API.md` already use — update the copies
together.  Sharing the dictionary is the point: a controller line and an app line
join on the same key (`rid`, `poduid`, `cid`), which the old vocabularies could
not do (`uid=` meant the pod UID here and nothing there; a reservation was `#42`
here and `id=42` there).

`kv()` is also the **sanitisation chokepoint** — pod names, namespaces and
annotation values come from whoever created the pod, and it scrubs every value of
non-printable characters so no call site can forget.  Never pre-format such a
value into the message string.

**Every call site in this repo now emits it** — no prose log lines remain, and the
`actor=controller` envelope field is constant here (unlike the app, this daemon has
one principal).  `tests/test_log_grammar.py` fails the build if a new call site
skips `kv()`, emits a field `docs/LOG-FIELDS.md` does not document, or adds an
`event=` that `OBSERVABILITY.md` does not list — which is what stops that file
drifting the way it did before.  `OBSERVABILITY.md` is the per-event inventory.

Two renames the grammar forced, worth knowing when reading older lines:
`phase=` now means the **pod lifecycle phase** (`Pending`/`Running`/`Succeeded`/…)
and the preemption sweep's A/B is `sweep=`; and the per-class `demand=`/`free=`
maps the sweep used to print as dicts are **fanned out to one line per GPU class**,
so each class's shortfall is independently greppable.

### Trace ids

Every log line carries two **envelope** fields the formatter writes, not the call
site: `actor=controller` (constant — this daemon has one principal) and `trace=`,
a correlation id for the **unit of work** that produced the line.  `app/trace.py`
owns it; it is deliberately *not* part of `log_fields.py`, which must stay
byte-identical to the app's copy.

`rid` and `poduid` already join an app line to a controller line *about the same
object*.  A trace joins the lines of one **operation**, including the ones with no
object in common — "what did that JIT admission batch do, on both sides" becomes
one grep rather than a reconstruction from timestamps across concurrent loops.

- **Minted per unit of work**, via `with trace.scope(prefix)`: one fetch cycle
  (`fetch-`), one queue-processor tick (`queue-`), one preemption sweep (`sweep-`),
  one capacity audit (`audit-`), one JIT admission batch (`jit-`), one pod watch
  event (`pod-`), and lifespan startup (`startup-`).  The id is
  `<prefix>-<10 hex chars>`; `-` when nothing is in scope.  Each background loop is
  its own asyncio task and a task gets a *copy* of the context, so one loop's trace
  is invisible to the others; work fanned out *within* a scope inherits it.
- **Propagated outbound** by an httpx `event_hooks={"request": ...}` hook on the
  `ReservationClient` — the one chokepoint, so no call site passes the header and an
  endpoint added later cannot forget it.  The header is `X-Client-Trace`, exactly the
  one the app's `_RequestLoggingMiddleware` already reads, so a controller-initiated
  request lands in the app's log under the controller's trace **with no app-side
  change**.  Nothing is sent when no scope is in force (the literal `-` must never be
  echoed as if it were an id).
- **Adopted inbound** on both inbound endpoints via the `_bind_trace(prefix)` yield
  dependency (`push-` / `forecast-`), so a pushed reservation update is logged under
  the *app's* trace and the two sides of that call join.
- **The inbound value is untrusted** and is whitelist-matched against `_TRACE_RE`
  (`[A-Za-z0-9_-]{1,36}`, kept identical to the app's), never escaped — the formatter
  interpolates it and cannot decide to quote it, so a crafted newline would otherwise
  forge whole log lines.  A value that fails is dropped for a locally-minted one.
  Minted ids are truncated to satisfy the same regex, because the app **silently
  discards** a trace it cannot match — propagation would look fine from here while the
  far end logged `trace=-`.

### Timezone

All reservation window arithmetic uses **UTC-aware `datetime` objects** (`timezone.utc`).
`slot_start` and `slot_end` return `r.start_utc` / `r.end_utc` directly from the API
response; no local-time conversion is performed in the controller.  Every
`datetime.now()` call in the codebase uses `datetime.now(timezone.utc)`.

The `TZ` environment variable affects log timestamp display only; window
arithmetic is UTC-based and does not depend on it.

### Fast path for mid-window pods

When a pod ADDED event arrives while its reservation window is already open
(the common case for JupyterHub notebook servers launched during a session),
`pod_watch_loop` calls `_try_apply_toleration` immediately rather than waiting
up to a full `QUEUE_PROCESSOR_INTERVAL` (default 300 s) for the next
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
reservations: charged SU and protected by the same runtime guarantee as any
booking — most of that machinery needed no separate on-demand code path.
One asymmetry remains: the preemption sweep enumerates boundaries and demand
only for `kind == "booking"` reservations (`upcoming_boundaries`,
`boundary_demand`), so a granted lease (`kind="on_demand"`) never *triggers*
boundary preemption of overstayers on its own behalf — a JIT pod that lands on
squatted GPUs waits (guard 3) rather than displacing them.  Extending the
sweep to on-demand boundaries is possible future work; until then the
preemption-risk forecast reports pending JIT pressure as informational only
for the same reason (see **Preemption-risk forecast API**).

**Routing** (`pod_watch_loop`, re-evaluated on every attempt): for a pod
without the toleration,

1. `ControllerState.find_admittable_reservation` — the budget/horizon-aware
   sibling of `find_best_reservation` — looks for a match whose window is
   open now or opens within `ONDEMAND_HORIZON_MINUTES` (default 30) **and**
   has spare budget (`available(r) >= gpu_requested`).  If found, the pod is
   queued for it (`enqueue_pod`), with the same fast-path immediate-apply
   when the window is already open.
2. Otherwise, if the pod is **JIT-eligible** — `ONDEMAND_LEASE_ENABLED`,
   `Pending`, carries `horae/minimum-runtime-seconds`, and names its usage
   group (the group label when `REQUIRED_GROUP_LABEL` is set, else the
   `horae/usage-group` annotation — the lease request's `group_name` is a
   **required** natural key app-side) — it becomes an
   `OnDemandCandidate` and, on the **ADDED** event, kicks an immediate
   admission batch (`main._run_ondemand_admission`) covering it plus every
   other due waiter.  Most `MODIFIED` events do **not** re-trigger a batch —
   denial and guard short-retries ride the queue-processor tick — so a burst of
   reconcile `MODIFIED`s cannot hammer the app.  The **one** exception is the
   `MODIFIED` that carries the scheduler's verdict: when a fresh pod's
   `PodScheduled` condition is not yet set at ADDED time, guard 1 is
   indeterminate and the candidate is parked with `awaiting_schedule_signal`;
   the `MODIFIED` that finally sets the condition (`is_gpu_only_pending` now
   returns non-`None`) clears the flag, resets the candidate's cooldown, and
   re-attempts immediately — resolving in ~1 s + a lease round-trip instead of
   waiting up to a full periodic scan (~270–300 s).  The flag is set **only** by
   the indeterminate guard-1 branch, so this fast-path reset can never
   short-circuit a 409-denial or guard-3/4 backoff, and it fires at most once
   per park (the flag is cleared before the batch runs), preserving the
   anti-hammer property.
3. Otherwise, if `find_best_reservation` finds *any* future match (beyond the
   horizon, or over budget), the pod is queued for it anyway — this preserves
   the plain wait-for-window behaviour for a pod that isn't JIT-eligible.
4. Otherwise the pod is left **Pending** (a pod missing the group label or the
   minimum-runtime annotation is deliberately not guessed at; that is left
   for a future "born overstay" design).

**Batch admission** (`main._run_ondemand_admission`, the single entry point for
both the ADDED trigger and the queue-processor tick): gathers every **due**
candidate (`now >= next_attempt_at`) in FIFO order and runs each through a
two-step **preflight → delegate → grant** pipeline.

- **Preflight** (`_preflight_ondemand_candidate`): re-reads the pod (drops it
  if gone/terminal/Unknown), re-runs step 1 above (a matching reservation may
  have appeared since the candidate was queued), applies guard 1
  (`is_gpu_only_pending`), guard 3 (`stuck_holder_gpu_classes`), guard 4
  (`overcommitted_gpu_classes`), and guard 5 (per-node feasibility — see
  **Per-node capacity accounting** below), and resolves the pod's `gpu-class`
  label to a numeric id via `ControllerState.gpu_class_ids`.  Survivors become an
  `OnDemandAdmissionCandidate` — the exact "ask" (username, group, class id, gpu
  count, and `duration_seconds = minimum-runtime + ONDEMAND_LEASE_BUFFER_MINUTES
  * 60`, default buffer 10 min).
- **Delegate** (only when `ONDEMAND_DELEGATE_ADMISSION` is on): the whole
  survivor set is offered to the app in one call
  (`POST /api/reservations/ondemand-admission`,
  `ReservationClient.select_ondemand_admissions`), which returns the subset of
  `pod_uid`s to admit this round — the delegation point for **future LAS
  prioritization**.  Mirrors the preemption-victims pattern: only offered uids
  are honoured (`_map_granted_uids` drops unknowns), an **empty** answer is
  respected (grant none), and a **call failure or the flag being off** falls
  back to granting *every* survivor — the prior greedy per-pod behaviour, so the
  change ships safely dark.
- **Grant** (`_grant_and_admit`, per granted pod): calls `POST /api/reservations`
  with `on_demand=True` (the app relaxes policy limits — SU, caps, minimum
  duration — never physical calendar capacity), **idempotent by the pod's UID**
  (`idempotency_key`).  The call returns a `LeaseResult` whose `outcome`
  distinguishes a **denial** (4xx — the app answered "no") and a **malformed**
  2xx body, which cool the candidate down 2–5 min, from an **unavailable** app
  (network error or 5xx), which takes the 30 s short retry instead — the app may
  answer differently the moment it is reachable, and re-asking is safe because
  the request is idempotent on the pod UID.  On **grant** the lease is upserted into `state.reservations`
  (`apply_push_to_active`) and the pod is admitted immediately
  (`_try_apply_toleration`) — the existing admission path, so it stamps
  `res-<id>`, records the guarantee, and emits `RuntimeGuaranteed`.  **If
  admission does not succeed** (budget race, transient patch error, or the pod
  having gone terminal), the controller issues a compensating cancel
  (`POST /api/reservations/{id}/cancel`, `reason="controller-revoked"`) so the
  grant is never left dangling.  A **non-granted** survivor cools down like a
  denial and is re-offered on a later tick.

The batch is coalesced by `ControllerState.ondemand_admission_lock`: only one
runs at a time, and a trigger arriving mid-batch sets `ondemand_rerun_requested`
so exactly one trailing pass follows — an ADDED burst collapses into at most one
in-flight + one trailing batch.  The single-pod `_try_request_lease` remains as a
thin `preflight → grant` wrapper (the non-delegated path and the unit-test seam).

**Surviving the fetch that hasn't seen the grant yet.**  The grant upserts the
lease locally, but the periodic `reservation_fetch_loop` takes its
`GET /api/reservations` snapshot **before** acquiring `reservation_lock` and then
**wholesale-replaces** `state.reservations`.  A lease granted in the gap between
that snapshot and the replace is absent from the snapshot, and a naive replace
would drop it even though its pod is live inside its guarantee — leaving
`guarantee_end` unresolvable (`None`), which the preemption planner treats as
*past guarantee* and hence a valid victim.  To close that race, the fetch loop
re-adds any locally-granted on-demand lease the snapshot omits
(`ControllerState.preserve_local_ondemand_leases`): a `kind="on_demand"`,
locally-`active` lease whose window is still open (`slot_end > now`) and whose id
does **not** appear anywhere in the full `status=all` response.  Checking the full
response (not just the active subset) means a lease the app reports **cancelled**
carries its id and is therefore **not** preserved — a genuine server-side cancel,
including the controller's own compensating cancel, still wins and its pod is
released.  Only replication lag (grant not yet surfaced) is bridged; the push path
is unaffected because it merges deltas (`apply_push_to_active`) instead of
replacing.

**Lease teardown when the pod goes away** (`main._teardown_ondemand_lease`): a
JIT lease exists solely to cover one pod (its `idempotency_key` is the pod's
UID), so once that pod terminates on its own (Succeeded/Failed), is deleted, or
is preempted, the lease is no longer needed and the controller cancels it
(`POST /api/reservations/{id}/cancel`, `reason="pod-terminated"`), releasing the
capacity and stopping SU accrual instead of letting the lease linger until it
expires.  `pod_watch_loop` calls this from both its DELETED and terminal-phase
branches, right after `release_pod`.  **No on-demand-vs-booking state is tracked
in memory**: the pod's `horae/booking-reference` annotation resolves to the
reservation id, and the reservation's own `kind` field — the app returns leases
as `kind="on_demand"` and the pull keeps them in `state.reservations` — is read
live to decide.  Only `kind == "on_demand"` rows are touched (a booking's pod
ending never cancels anything); the cancel is idempotent and best-effort (a
failure just logs — the next poll reconciles), and occupancy is released
independently.

**RBAC / config**: no new Kubernetes permissions (admission reuses the
existing pod-patch path); `ONDEMAND_HORIZON_MINUTES` and
`ONDEMAND_LEASE_BUFFER_MINUTES` tune the routing horizon and lease sizing.

### Transient-failure retry policy

The controller is a **level-triggered reconciler**, so the general answer to a
failure is "re-evaluate on the next tick from live state", not "retry the call".
That is why the eleven `next_attempt_at` sites in `main.py` are requeue delays
(mostly after a *guard denial*, where nothing failed and there is no call to
re-drive) rather than call-retry loops, and why they intentionally have **no
attempt cap** — a pod waiting for capacity must be re-evaluated indefinitely, and
giving up would strand it.  Adding a retry library here would not fit: `tenacity`
et al. retry a callable that raised, which is not what these sites do.

Two places are genuine exceptions, where the next tick is too late or too blunt:

- **`fetch_reservations` pagination** (`_get_reservations_page`) retries a
  network error or 5xx up to 3 times with a 1 s / 2 s backoff.  Without it a blip
  on page 3 of 5 aborts the whole refresh cycle, leaving the controller on stale
  reservation state for a full `RESERVATION_FETCH_INTERVAL` (300 s default).  A
  **4xx is not retried** (a bad key or filter will not fix itself), and a failure
  outliving the retries still **raises** — that fail-safe is load-bearing, since
  acting on a partial page set would wholesale-replace `state.reservations` with
  an under-count.
- **`create_ondemand_reservation`** returns a `LeaseResult` rather than a bare
  `None`, so the caller can tell an app that *decided* against the lease from one
  it could not reach, and pick the matching cooldown (see **Just-in-time (JIT)
  on-demand leases** → Grant).

Note the cooldown split is bounded by the queue-processor tick: `next_attempt_at`
only gates *eligibility*, so at the default `QUEUE_PROCESSOR_INTERVAL` (300 s) a
30 s and a 2–5 min cooldown often both resolve to "next tick".  The distinction
bites when the interval is tuned down, and on the ADDED-triggered admission
batches that run between ticks.

### App-side vs physical capacity reconciliation

The controller derives physical GPU capacity solely from Kubernetes node taints
(`snapshot_node_gpu_capacity` — total allocatable `nvidia.com/gpu` per
`gpu-class-reservation` taint value).  The reservation app has its **own**
per-class GPU count, `total_gpus` (RESERVATION-API.md §4), now modelled on
`schemas.GpuClassDetail` and cached per label in
`ControllerState.gpu_class_capacity` — refreshed on every reconcile from the
same `GET /api/gpu-classes` fetch that builds the label maps (only classes whose
`total_gpus` is known are recorded; a payload omitting it degrades to
"unknown").

`capacity_audit_loop` (`_run_capacity_audit` in `main.py`) runs every
`CAPACITY_CHECK_INTERVAL` s (default hourly, plus once synchronously at
startup):

1. Snapshots physical capacity (`snapshot_node_gpu_capacity`).  **Fail-safe**:
   if the node LIST fails, the audit is skipped and the current pause set is
   left unchanged — a transient failure must never silently lift a pause (the
   same "never act on unknown physical state" rule the preemption sweep
   follows).
2. Calls the pure `controller.reconcile_capacity(app_side, physical)`, which
   walks the union of class labels and returns (a) a `CapacityDiff` for every
   class whose two counts disagree in either direction — a class present in
   only one map treats the other side as `0` — and (b) the set of
   **over-committed** labels (`app_side > physical`).  A class whose app-side
   count is unknown is never flagged over-committed (overcommit cannot be
   concluded from missing data).
3. Logs every diff at **WARNING**; sets `ControllerState.overcommitted_gpu_classes`
   to the over-committed set (logging INFO as classes enter/leave it).

**Per-class on-demand pause.**  `_preflight_ondemand_candidate` gates JIT
admission on `overcommitted_gpu_classes` as **guard 4** (mirroring the guard-3
`stuck_holder_gpu_classes` interlock): a candidate whose `gpu-class` is
over-committed is short-retried rather than granted, so it stays queued and
resumes automatically once the class leaves the set on a later audit tick.  Only
the JIT/on-demand path is gated; reserved-path admission under a real user
booking is untouched (a booking already implies the app granted real calendar
capacity).  **RBAC**: none new — the audit reuses the existing `nodes: list`
permission.

### Per-node capacity accounting

Extended resources (`nvidia.com/gpu`) are **node-scoped and atomic**: the
Kubernetes scheduler only places an N-GPU pod on a *single* node with N free, or
leaves it Pending — it never splits a job across nodes.  The controller therefore
cannot (and need not) enforce single-node placement.  But its own budget/capacity
accounting is otherwise **per-class and count-based** (`available`,
`free_capacity_by_class`, `snapshot_node_gpu_capacity`), which is exactly right
for 1-GPU jobs (any free GPU is interchangeable) yet **blind to fragmentation**
for multi-GPU jobs: a class can show enough free GPUs in aggregate while they are
scattered one-per-node so no single node can host a 2+ GPU pod.

Per-node accounting closes that blind spot for the **JIT on-demand path**, using
data already fetched (no new API calls, no new RBAC):

- `k8s_client.snapshot_node_gpu_inventory` returns allocatable GPUs **per class,
  per node** (`{gpu_class: {node_name: allocatable}}`); `snapshot_node_gpu_capacity`
  is now a per-class collapse of it.  `ToleratedPodInfo` / `PodRuntimeView` carry
  `node_name` (`spec.nodeName`), captured through `_pod_view`.
- Two pure helpers in `controller.py`: `free_gpus_by_node_class` (per-node free =
  allocatable minus the GPUs of bound, node-resident, non-terminating pods on that
  node — an unscheduled pod belongs to no node and counts against none) and
  `largest_node_free_by_class` (the largest single-node opening per class).  Because
  GPU nodes are tainted `gpu-class-reservation=<class>:NoSchedule`, controller-tolerated
  pods are the only GPU consumers on them, so a tolerated-pods-only occupancy count is
  accurate.
- `ControllerState.node_free_by_class` (largest single-node free GPUs per class) is
  refreshed every `queue_processor_loop` tick from a node-inventory + tolerated-pod
  snapshot.  **Fail-safe**: if either snapshot fails, the prior map is left unchanged
  (never open admission on unknown physical state).

**Guard 5** (`_preflight_ondemand_candidate`): a JIT candidate requesting **≥2
GPUs** whose `gpu-class` has a *known* largest-single-node-free below the ask is
short-retried rather than granted — the controller does not mint an SU-charged
lease the pod could never schedule under.  **Fail-open on unknown**: a class absent
from `node_free_by_class` (no data yet, or a snapshot gap) never blocks, so a stale
map cannot wedge admission; the reactive guard-3 interlock and the compensating
cancel in `_grant_and_admit` remain the backstop.  The 1-GPU path is unaffected.

Two node-aware consumers are **deliberately deferred** to a follow-up: preemption
victim-targeting (concentrating kills on one node so a reserved multi-GPU booking
can land — today the sweep still frees GPUs per class, count-based) and the
preemption-risk forecast's shortfall (still per-class).  Guard 5 is a per-candidate
feasibility check against a snapshot, not batch-level bin-packing: two ≥2-GPU
candidates can both pass against the same single-node opening in one batch, with
guard 3 backstopping the loser.

### Inbound push API

`POST /api/reservations/push` lets the reservation app push **one or more
updated reservation entries** (today: cancellations and owner changes) so they
propagate within seconds instead of waiting up to a full
`RESERVATION_FETCH_INTERVAL`.  Bulk synchronisation stays a controller-initiated
**pull** — the push is a partial delta, and the next full fetch remains the
source of truth.

- **Auth**: a single static bearer token in `INBOUND_API_TOKEN` (mount from a
  Secret).  Unset ⇒ endpoint disabled (503); wrong/missing bearer ⇒ 401
  (constant-time compare).  It rides the existing FastAPI app / `HTTP_PORT`,
  so no extra container port or Service is needed.
- **Body**: `{"reservations": [ReservationResponse, …]}` — the same entry shape
  the pull returns (`schemas.ReservationPushRequest`).
- **Semantics**: entries are **upserted by id** (`apply_push_to_active` in
  `controller.py`); an entry whose `status` is not `"active"` drops that id from
  the active set, and an in-window cancellation evicts its admitted pod and
  releases its capacity — after first attempting an adoption re-link onto
  another currently-open booking the same user holds (`POD_ADOPTION_ENABLED`;
  this is what carries a pod forward when the app supersedes its reservation
  via `POST /api/reservations/{id}/continue` and pushes the `superseded`
  source together with its replacement) — the *same* path a mid-window
  cancellation takes on a fetch.  An entry that keeps its id but changes owner (**adoption** — a
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

### Preemption-risk forecast API

`GET /api/forecast/preemption-risk` (optional `?namespace=`) answers, per
controller-admitted pod, "how likely is this job to be preempted during the
remainder of the current hour and the next two full hours?" — computed
entirely from in-memory state (reservation map, occupancy, pending JIT
candidates) plus the same two cluster snapshots the preemption sweep takes
(pods, node capacity; either failing ⇒ 503 — the sweep's fail-safe rule,
never report risk from unknown physical state).

- **Auth / wiring**: guarded by the same `INBOUND_API_TOKEN` bearer check as
  the push API (`_require_inbound_auth`; unset ⇒ 503, bad bearer ⇒ 401) and
  rides the same FastAPI app / `HTTP_PORT` — no new port, Service, or RBAC.
- **Model** (`ControllerState.forecast_preemption_risk`, pure and
  synchronous, computed under `reservation_lock` with snapshots awaited
  outside it): three calendar-aligned buckets (`[now, top of next hour)` plus
  two full hours).  For every future booking boundary in `(now, horizon_end +
  lead]` — the tail extension mirrors the front, a boundary just past the
  last bucket still lands its phase-A kill inside it — demand reuses
  `boundary_demand` and free capacity `free_capacity_by_class`; an eligible
  pod's single-boundary risk is `min(1, shortfall / eligible-pool GPUs)`,
  attributed to the buckets overlapping the kill window
  `[max(boundary − PREEMPTION_LEAD_MINUTES, guarantee_end), boundary]` and
  combined per bucket as `1 − Π(1 − r)`.  A pod inside its runtime guarantee
  has exactly zero risk (`state: "guaranteed"`); past it, `"overstay"`
  (`"mixed"` when the guarantee ends mid-bucket).
- **Chain-safe eligibility**: guarantees are computed once at the real "now"
  (`guarantee_end`) and compared against each boundary — never by simulating
  `now = boundary`, which would sever back-to-back chains exactly at the
  boundary instants (`_chain_for` keeps members only while `slot_end > now`)
  and fabricate phantom overstay and phantom demand.
- **Documented approximations** (the response carries `selection_delegated`
  so consumers can label the number): with delegation enabled the app picks
  victims by its own policy — the numeric risk models the local
  uniform-random fallback, while pool *membership* (at-risk vs safe) is exact
  either way; boundaries combine as independent chances (conservative — an
  earlier kill actually frees capacity for later boundaries); running pods
  are assumed to keep running (free capacity constant across buckets);
  pending JIT candidates surface per class as `pending_jit_gpus` on the
  current bucket, informational only, never folded into shortfall (see the
  JIT caveat above).
- **`?namespace=`** filters the `pods` list only; bucket/class summaries stay
  cluster-global (every displayed risk's denominator and demand driver is
  global).  Unknown namespace ⇒ empty `pods`, 200.

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
on the next tick — a window bounded by the `QUEUE_PROCESSOR_INTERVAL` tick
(default 300 s).

**No-show declaration is in-memory, but the resulting cancel is durable.**
`noshow_reservation_ids` (and `pending_noshow_cancels`, the queue of ids still
awaiting their cancel) are never written back and don't survive a restart.
After a restart, every mid-window user reservation — including ones the prior
controller lifetime already declared no-show — receives a fresh
`NOSHOW_GRACE_MINUTES` deadline, so a late-arriving holder can reclaim their
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
| `HTTP_PORT` | `8000` | Bind port for the **whole** HTTP listener — `GET /health` plus `POST /api/reservations/push` and `GET /api/forecast/preemption-risk` |
| `INBOUND_API_TOKEN` | *(absent = inbound API disabled)* | Bearer token guarding the inbound APIs (`POST /api/reservations/push` and `GET /api/forecast/preemption-risk`); mount from a Kubernetes Secret. Unset ⇒ both endpoints return 503 |
| `TZ` | system default | Affects log timestamp display only; window arithmetic is UTC-based and does not depend on it |
| `ONDEMAND_LEASE_ENABLED` | `true` | Set to `false` to disable the JIT on-demand lease path entirely (a non-JIT-eligible pod still waits for a matching reservation; an ineligible one is left Pending) |
| `ONDEMAND_HORIZON_MINUTES` | `30` | JIT routing horizon: a pod is queued for a reservation that opens within this many minutes (with budget) instead of requesting a lease |
| `ONDEMAND_LEASE_BUFFER_MINUTES` | `10` | Minutes added to a pod's `horae/minimum-runtime-seconds` when sizing a requested JIT lease's duration |
| `ONDEMAND_DELEGATE_ADMISSION` | `false` | Ask the app which pending pods to admit on-demand from the eligible batch (`POST /api/reservations/ondemand-admission`) for LAS prioritization; `false` (or any app-call failure) grants every eligible candidate — the prior greedy per-pod behaviour. Opt-in: enable once the app implements the endpoint |
| `NOSHOW_TIMEOUT_MINUTES` | `15` | Minutes after window opens before a reservation is declared a no-show |
| `NOSHOW_GRACE_MINUTES` | `30` | Grace period after controller startup before mid-window no-shows are declared |
| `QUEUE_PROCESSOR_INTERVAL` | `300` | Seconds between queue-processor ticks — the whole work-queue loop (pod LIST, JIT lease retries, no-show cancels, overstay adoption), not just a pod LIST |
| `POD_SCHEDULING_GATE_NAME` | *(absent)* | Name of the SchedulingGate to remove after admitting a pod; unset = disabled |
| `REQUIRED_GROUP_LABEL` | *(absent)* | Pod label naming the usage group (e.g. `dsmlp/course`); when set, the pod's value must equal the reservation's `group.name` — an extra match axis alongside `gpu-class` (see **Matching pods to reservations**), and a pod without the label is never JIT-eligible either. Unset = disabled |
| `PREEMPTION_LEAD_MINUTES` | `15` | Minutes before a reservation slot boundary that phase-A preemption runs |
| `PREEMPTION_CHECK_INTERVAL` | `60` | Seconds between preemption sweeps |
| `CAPACITY_CHECK_INTERVAL` | `3600` | Seconds between app-side vs physical GPU capacity audits; each audit logs per-class differences as WARNING and pauses on-demand admission for classes the app over-counts (see **App-side vs physical capacity reconciliation**) |
| `PREEMPTION_DELEGATE_SELECTION` | `true` | Ask the app to choose preemption victims from the eligible pool (`POST /api/reservations/preemption-victims`); `false` (or any app-call failure) falls back to local uniform-random selection |
| `POD_ADOPTION_ENABLED` | `true` | Re-link an overstay pod to a reservation its user has since booked (see **Adopting overstay pods into a re-booked reservation**); `false` disables |
| `ONDEMAND_MERGE_ENABLED` | `true` | Merge a JIT on-demand lease's pod into the user's matching booking the moment that booking's window opens — re-link the pod and retire the lease penalty-exempt, without waiting for the lease guarantee to lapse (see **Merging a JIT lease into a matching booking**); `false` disables (the pod then converges lazily via adoption once past its lease guarantee) |
| `TERMINATION_WARNING_ENABLED` | `true` | After each preemption sweep, stamp pods still at risk of preemption at an upcoming boundary with informational `horae/termination-warning-*` annotations — projected termination time, risk score, and a message (see **Termination-warning annotations**); `false` disables |
| `TERMINATION_WARNING_LEAD_MINUTES` | `30` | How far ahead (minutes) the termination-warning look-ahead scans, decoupled from `PREEMPTION_LEAD_MINUTES` so a pod killed proactively at `boundary − lead` (a phase-A victim) is warned before its boundary enters the kill window; larger = more advance notice but more speculative warnings |
| `OVERSTAY_REPORT_ENABLED` | `false` | When on, report each ended overstay's duration to the app for offline analysis (`POST /api/reservations/{id}/overstay`) — see **Overstay reporting**. Best-effort and analysis-only; ships dark (default off) |
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
- Entrypoint: `python -m app.main` (starts uvicorn programmatically so `HTTP_PORT` controls the bind port)

A Helm chart at `helm/gpu-reservation-controller/` renders the
ServiceAccount, ClusterRole/Binding, Deployment, and `/health` Service; keep
its `values.yaml`/`deployment.yaml` env wiring in sync when adding settings
to `config.py`.  See README.md for RBAC requirements and a sample manual
Deployment manifest.
