# GPU Reservation System — External Daemon API

This document covers every endpoint needed to build two external daemons:

- **Kubernetes controller** — reads active reservations and manages pod
  scheduling by injecting GPU-reservation tolerations and enforcing runtime
  caps (`activeDeadlineSeconds`) on admitted pods.
- **Roster sync daemon** — provisions user accounts and manages usage-group
  membership from an institutional directory.

Both daemons authenticate with a long-lived service key (see §1).
An interactive API explorer is available at `GET /api/docs` on any running instance.

---

## Contents

1. [Authentication](#1-authentication)
2. [Conventions](#2-conventions)
3. [Key management](#3-key-management)
4. [Kubernetes controller endpoints](#4-kubernetes-controller-endpoints)
5. [Roster sync endpoints](#5-roster-sync-endpoints)
6. [Type reference](#6-type-reference)

---

## 1. Authentication

All daemon-accessible endpoints require a service key sent in the
`X-API-Key` request header.

```
X-API-Key: gpures_<64 hex characters>
```

Service keys are **scoped** (never admin-equivalent). Each key has a `scope` of
`read_only` or `read_write`:

- **`read_only`** keys may call the read (GET) endpoints listed in §4 and §5.
- **`read_write`** keys may additionally call the write endpoints (create user,
  manage group membership).

Neither scope grants administrator privileges. Admin-only **write** surfaces
(creating or modifying GPU classes, site settings, email
settings, key management) are unreachable by any service key. Read access to
GPU classes (`GET /api/gpu-classes`, `GET /api/gpu-classes/{class_id}`) is
available to both scopes — the Kubernetes controller uses it to resolve
node-label values (§4).

Keys are separate from user accounts, do not expire, but can be revoked
instantly (§3).

**Generating a key** — run the CLI on the server (requires database access):

```bash
python manage_service_keys.py create --name k8s-controller-prod
```

The raw key is printed exactly once. Store it immediately as a Kubernetes Secret
or equivalent:

```bash
kubectl create secret generic gpu-reservation-api-key \
  --from-literal=api-key='gpures_...'
```

See §3 for the full key lifecycle API.

---

## 2. Conventions

### Base URL

All paths below are relative to the server root, e.g. `https://gpures.example.edu`.

### Content type

Request bodies are JSON (`Content-Type: application/json`).
Responses are always JSON.

### Date and time formats

| Type | Format | Example | Notes |
|------|--------|---------|-------|
| Date | `YYYY-MM-DD` | `2026-09-01` | Calendar date; no timezone |
| Datetime (audit) | ISO 8601, `Z` suffix | `2026-09-01T08:00:00Z` | `created_at`, `updated_at`, `cancelled_at` — always UTC |
| Datetime (UTC bounds) | ISO 8601, `Z` suffix | `2026-09-12T03:00:00Z` | `start_utc`, `end_utc` — reservation boundaries converted to UTC by the server |
| Datetime (local) | ISO 8601, no suffix | `2026-09-01T08:00:00` | `start_dt`, `end_dt` — site-local wall-clock, no timezone |
| Time-of-day | `HH:MM:SS` | `08:00:00` | discount-schedule `start_time`/`end_time` — local wall-clock, no timezone |

### Pagination

Endpoints that return lists accept:

| Parameter | Type | Default | Max | Description |
|-----------|------|---------|-----|-------------|
| `limit` | integer | 200 | 1000 | Maximum records to return |
| `offset` | integer | 0 | — | Records to skip |

### Errors

All errors return a JSON body with a single `detail` field:

```json
{ "detail": "User not found" }
```

Common status codes:

| Code | Meaning |
|------|---------|
| 400 | Invalid input or business-rule violation |
| 401 | Missing or invalid credential |
| 403 | Valid credential but insufficient permission |
| 404 | Resource not found |
| 409 | Conflict (duplicate unique field) |

---

## 3. Key management

These endpoints require an **admin Bearer JWT** (human login token), not a
service key. A service key cannot mint new service keys.

### `GET /api/service-keys`

List all service keys. Never returns the raw key value.

**Response** `200` — array of [ServiceKeyResponse](#servicekeyresponse)

---

### `POST /api/service-keys`

Create a new service key. The `raw_key` field is present **only in this
response** and is never retrievable again.

**Request body**

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `name` | string (1–128 chars) | yes | Human label, e.g. `k8s-controller-prod` |
| `scope` | `"read_only"` \| `"read_write"` | no | Default `"read_only"`. `"read_write"` enables user-creation and group-membership endpoints. |

**Response** `201` — [ServiceKeyCreateResponse](#servicekeycreateresponse)

```json
{
  "id": 3,
  "name": "k8s-controller-prod",
  "key_prefix": "gpures_a3f9c012",
  "scope": "read_only",
  "is_active": true,
  "created_at": "2026-06-07T14:22:00",
  "last_used_at": null,
  "raw_key": "gpures_a3f9c012..."
}
```

**Errors**

| Code | Condition |
|------|-----------|
| 400 | Name already exists |

---

### `DELETE /api/service-keys/{key_id}`

Revoke a key immediately. In-flight requests using the key are unaffected,
but all subsequent requests with that key return 401.

**Response** `204` No Content

**Errors**

| Code | Condition |
|------|-----------|
| 404 | Key not found |

---

### CLI equivalent

The `manage_service_keys.py` script at the project root provides the same
operations without an HTTP server:

```bash
python manage_service_keys.py create --name k8s-controller-prod
python manage_service_keys.py list
python manage_service_keys.py revoke --name k8s-controller-prod
python manage_service_keys.py revoke --id 3
```

---

## 4. Kubernetes controller endpoints

The controller polls for active reservations, computes the time window for
each slot, and creates or deletes Kubernetes resource objects accordingly.
When pending **on-demand** pods need capacity, the controller also *requests*
a reservation from the app (an on-demand lease — see
§"Creating on-demand reservations") using a `read_write` service key; the app
judges feasibility with the same capacity/borrowing/budget analysis it applies
to a user's web booking.

### `GET /api/reservations`

Retrieve reservations with optional filters and pagination. Service keys see
all reservations across all users and groups.

**Query parameters**

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `status` | `active` \| `cancelled` \| `all` | `active` | Filter by reservation status |
| `date_start` | date | — | Include reservations on or after this date |
| `date_end` | date | — | Include reservations on or before this date |
| `gpu_class_id` | integer | — | Filter by GPU class ID |
| `gpu_class_name` | string | — | Filter by GPU class name (exact match) |
| `user_id` | integer | — | Filter by user ID |
| `username` | string | — | Filter by username (exact match) |
| `group_id` | integer | — | Filter by usage group ID |
| `include_teammates` | boolean | `false` | Non-privileged callers only: switch to the personal *"me + my team"* scope — own reservations **plus** teammates' reservations in team-enabled groups (Team Mode). Replaces the default own+managed scope. Ignored for admins/auditors/service keys (they already see all). |
| `created_after` | datetime | — | Include reservations created at or after this time |
| `created_before` | datetime | — | Include reservations created at or before this time |
| `limit` | integer | 200 | Max records (1–1000) |
| `offset` | integer | 0 | Records to skip |

**Response** `200` — array of [ReservationResponse](#reservationresponse), ordered by `(date, id)`

**Example — fetch all active reservations starting today:**

```
GET /api/reservations?status=active&date_start=2026-09-01
X-API-Key: gpures_...
```

```json
[
  {
    "id": 42,
    "user_id": 7,
    "user": { "id": 7, "username": "jsmith" },
    "group_id": 2,
    "group": { "id": 2, "name": "CS151B-FA26" },
    "gpu_class_id": 1,
    "gpu_class": { "id": 1, "name": "H100", "label_value": "h100" },
    "start_dt": "2026-09-01T08:00:00",
    "end_dt": "2026-09-01T16:00:00",
    "date": "2026-09-01",
    "start_utc": "2026-09-01T15:00:00Z",
    "end_utc": "2026-09-01T23:00:00Z",
    "gpu_count": 4,
    "su_cost_user": 32,
    "su_cost_group": 32,
    "su_cost_original": 32,
    "kind": "booking",
    "status": "active",
    "notes": null,
    "submitted_by_id": 7,
    "submitted_by": { "id": 7, "username": "jsmith" },
    "created_at": "2026-08-20T10:15:00Z",
    "updated_at": "2026-08-20T10:15:00Z",
    "cancelled_at": null,
    "cancelled_by_id": null,
    "cancel_reason": null
  }
]
```

`start_dt` / `end_dt` are the booking's site-local wall-clock interval (naive, no
timezone). A user-scheduled reservation (`kind: "booking"`) is a whole-hour range
that **may cross midnight** (`end_dt` on the next calendar day); non-privileged
members are limited to 48 hours, while admins and group managers have no
server-side cap. An on-demand lease (`kind: "on_demand"`) starts and ends on
**arbitrary second-granularity timestamps** — clients must not assume the hourly
grid. `date` mirrors `start_dt`'s date
and is provided for convenience filtering. `su_cost_user` is the Service Units
charged to the individual user for this booking; `su_cost_group` is the SU charged
against the group's shared pool budget.  Both are computed from the GPU class base
rate (`su_rate_per_hour`) and the active discount schedules and stored at creation
time.  They differ only when a group manager (but not an admin) waives a
cancellation penalty: in that case `su_cost_user` is zeroed while `su_cost_group`
retains the penalty amount.  `su_cost_original` records the SU charged at creation
time and is never altered by later adjustments (cancellation-penalty rewrites or
waives), so it always reflects the booking's original full cost.

### Reservation kinds

Every reservation has a `kind` field:

| `kind` | Created by | Time grid | Attribution |
|--------|-----------|-----------|-------------|
| `"booking"` | A user through the web UI (or an admin/manager on a user's behalf) | Whole hours — **except** a booking minted by `POST /api/reservations/{id}/continue`, which is anchored at "now" with an arbitrary second-granularity window (`continued_from_id` set) | `user_id` + `group_id` always set |
| `"on_demand"` | The Kubernetes controller via §"Creating on-demand reservations" | Arbitrary timestamps, anchored at creation time | `user_id` + `group_id` always set |

Both kinds are returned by `GET /api/reservations` under every `status` filter —
there is no kind-based filtering. (Historical note: a third kind, `"reclaim"`,
existed before the lease model; those rows carried no user/group and are deleted
by migration, but `user_id`/`group_id` remain nullable in the schema for
pre-migration data.)

### Creating on-demand reservations

```
POST /api/reservations
X-API-Key: gpures_...            (read_write scope required)
```

When the controller detects pending on-demand pods, it requests a lease. The
body is distinguished from a user booking by `on_demand: true`:

```json
{
  "on_demand": true,
  "username": "jsmith",
  "group_name": "CS151B-FA26",
  "gpu_class_id": 1,
  "gpu_count": 2,
  "duration_seconds": 4200,
  "idempotency_key": "8f14e45f-ceea-4e07-8c2f-pod-uid",
  "notes": "on-demand lease for pod train-7c9"
}
```

Semantics:

- **The app anchors `start_dt` at its own "now"** (avoiding controller/app clock
  skew) and sets `end_dt = start + duration_seconds`. The controller typically
  sends `duration_seconds = min_runtime_seconds + buffer`. Bounds: 60 s ≤
  `duration_seconds` ≤ 604 800 (7 days).
- `username` / `group_name` are natural keys. The user must be an **active
  member** of the named active group, and the GPU class must be attached to that
  group (or `attach_all_groups`). Users supply the group via a pod annotation
  and are responsible for matching the usage-group name exactly.
- **Feasibility is the same analysis as a web booking**: the three capacity
  tiers (physical / cohort / group, including borrowing when the group's
  resolved relaxation flag is on, clamped by the class's `relax_min_available`
  buffer), the per-member/team SU budget and group SU pool (skipped when the
  user is an admin or a manager of the group — as if they booked themselves),
  `max_gpus_per_reservation`, and the group's validity dates (±90-day grace for
  those privileged users).
- **Timing policy does not apply**: no 15-minute lead requirement, no whole-hour
  alignment, no 48-hour cap, no `min/max_days_ahead` window.
- The lease is charged Service Units exactly like a booking (`su_cost_user` /
  `su_cost_group` stored at creation).
- No confirmation email is sent and nothing is pushed to the controller — the
  caller receives the reservation synchronously.

**Responses**

| Code | Condition |
|------|-----------|
| 201 | Lease created — full [ReservationResponse](#reservationresponse), `kind: "on_demand"` |
| 200 | `idempotency_key` matched an existing reservation — that original reservation is returned unchanged (idempotent retry) |
| 403 | Missing/`read_only` key, or a human JWT sent `on_demand: true` |
| 404 | Unknown `username` / `group_name` / `gpu_class_id` (or inactive) |
| 409 | Denied — insufficient capacity **or** a policy gate (membership, class access, SU budget, GPU cap, validity dates). Human-readable JSON `detail` explains which |
| 422 | Malformed body (e.g. `duration_seconds` out of bounds) |

Send a fresh `idempotency_key` per lease attempt (e.g. derived from the pod
UID); replaying the same key returns the original reservation even after it was
cancelled. A `409` means "not feasible right now" — the controller may retry
later as demand/capacity changes.

### `POST /api/reservations/{id}/cancel`

Cancel a reservation on the controller's behalf, recording a machine-readable
reason. Requires a `read_write` service key (or an admin JWT).

```json
{ "reason": "no-show" }
```

| `reason` | Meaning |
|----------|---------|
| `"no-show"` | The reservation holder never ran pods (applies to leases and to already-started bookings) |
| `"controller-revoked"` | The controller released a lease grant it never admitted a pod under (e.g. a budget race, or controller shutdown) |
| `"pod-terminated"` | The lease's pod finished, crashed, or was removed, so the controller released the now-unneeded lease |

Scope:

- a `kind: "on_demand"` lease may be cancelled at any time;
- a `kind: "booking"` row may be cancelled only once it has **started**
  (`start_dt <= now`) — the controller's no-show path for a user who reserved
  capacity but never used it. A not-yet-started booking returns 403 (users and
  admins cancel those via `DELETE /api/reservations/{id}`).

The retained SU charge is the standard, unwaived late-cancellation charge —
identical to the member cancelling at that instant (time already consumed in
full, plus the fraction-of-cost penalty on the unused remainder inside the next
24 h; short remainders are forgiven by the exemption). The reason is stored in
`cancel_reason` and appears in all reservation responses.

**Responses**

| Code | Condition |
|------|-----------|
| 200 | Cancelled — the updated [ReservationResponse](#reservationresponse). **Idempotent**: cancelling an already-cancelled reservation returns 200 with its current state and changes nothing |
| 403 | Read-only key / non-admin JWT, or a not-yet-started `booking` |
| 404 | Reservation not found |
| 422 | Unknown `reason` |

### `POST /api/reservations/{id}/continue`

Continue a **still-running job** under a fresh guaranteed reservation. Mints a
new `kind="booking"` reservation for the source's user/group/GPU class, anchored
at the app's own "now" with an arbitrary second-granularity `duration_seconds`
(so it can cover a job that never aligned to the whole-hour grid), then returns
it. The sibling controller re-links (adopts) the running pod onto the new
reservation, so the job keeps running with no relaunch. Callable by the source's
**owner** (JWT), an admin, or a manager of its group — **not** service keys.

```json
{ "duration_seconds": 7200, "gpu_count": 2, "notes": "extending training run" }
```

| Field | Meaning |
|-------|---------|
| `duration_seconds` | Length of the new guaranteed window from now (60 … 604 800). Required. |
| `gpu_count` | GPUs for the new reservation; defaults to the source's count. Optional. |
| `notes` | Stored on the new reservation; defaults to the source's notes. Optional. |

Eligible sources (must be `status="active"`):

- a `kind="on_demand"` lease — **during** its window or **after** it lapses (an
  overstaying pod); and
- a `kind="booking"` that is **in progress** (started, not yet ended) — a cleaner
  path than re-running the booking wizard. A not-yet-started or already-ended
  booking is rejected.

Semantics:

- **Off-grid, anchored at "now"** — like an on-demand lease, `start_dt` is the
  app's `local_now()` and `end_dt = start + duration_seconds`; whole-hour
  alignment and the 15-minute lead do **not** apply. The resulting
  `kind="booking"` row may therefore be off the hourly grid.
- **Supersede** — when the source still holds future time (`end_dt > now`) it is
  cancelled with the standard unwaived cancellation penalty (charging only the
  time already consumed and freeing its remaining capacity), then set
  `cancel_reason="superseded"`, so the new reservation is admitted against freed
  resources and the two never double-count. An already-ended on-demand overstay
  is left untouched. The controller **re-links the pod instead of evicting it**
  when it observes the `superseded` cancellation alongside the new open booking
  (requires `POD_ADOPTION_ENABLED`).
- **Same admission analysis as a booking** — the three capacity tiers (+
  borrowing), the per-member/team SU budget and group SU pool, and
  `max_gpus_per_reservation`, with privilege judged **as-if-self-booked**.
- The new row records `continued_from_id` (the source reservation's id). No
  confirmation email is sent. Both the new booking and any superseded source are
  pushed to the controller (best-effort) so the pod is carried forward promptly.

**Responses**

| Code | Condition |
|------|-----------|
| 201 | Created — the new [ReservationResponse](#reservationresponse), `kind="booking"`, `continued_from_id` set |
| 400 | Source not active / not an eligible kind or phase, or the window can't be admitted on budget |
| 403 | Caller is not the owner, an admin, or a manager of the group |
| 404 | Reservation, group, or GPU class not found / inactive |
| 409 | Capacity would be exceeded |

### `POST /api/reservations/preemption-victims`

Choose which overstay pods the controller should preempt. Requires a
`read_write` service key (or an admin JWT) — the same gate as the on-demand and
cancel endpoints. Purely advisory and **read-only**: it inspects nothing it
mutates and returns a decision; the controller performs the deletions.

The controller has already determined *which* pods are eligible (live, past
their runtime guarantee, admitted by the controller, of the class in question)
and how many GPUs it must reclaim per class near an upcoming reservation
boundary. This endpoint decides *which* of those eligible pods to sacrifice, so
that prioritisation policy lives in the app rather than the controller.

```json
{
  "needed_by_class": { "h100": 2 },
  "candidates": [
    { "pod_uid": "abc-123", "namespace": "alice", "pod_name": "train-7c9",
      "gpu_class": "h100", "gpu_count": 1, "reservation_id": 4412 },
    { "pod_uid": "def-456", "namespace": "bob", "pod_name": "notebook-2",
      "gpu_class": "h100", "gpu_count": 1, "reservation_id": 4380 }
  ]
}
```

| Field | Meaning |
|-------|---------|
| `needed_by_class` | GPUs still to reclaim, keyed by the GPU-class **label value** (e.g. `"h100"`) |
| `candidates[].pod_uid` | Opaque pod identifier; echoed back verbatim in the response |
| `candidates[].gpu_class` | GPU-class label value the candidate belongs to |
| `candidates[].gpu_count` | GPUs the candidate holds (used for greedy coverage) |
| `candidates[].reservation_id` | The candidate's booking-reference — the app's handle to the reservation (owner/group/kind) for prioritisation |

Per class, the app orders that class's candidates by its selection policy
(**uniform-random for now**) and greedily accepts them until the class's
`needed` GPU shortfall is covered — which may overshoot when a chosen pod holds
more GPUs than the residual need. A class whose candidate pool cannot cover its
shortfall contributes every candidate it has (the controller records the
remainder as unmet). The response lists the chosen `pod_uid`s:

```json
{ "victim_pod_uids": ["abc-123", "def-456"] }
```

The controller kills only pods it offered — a `pod_uid` in the response that was
not in the request is ignored. An **empty** list is a deliberate "spare
everyone" decision and is respected (the controller does **not** fall back to
its own selection); the controller only reverts to local random selection when
the call itself fails (network error, non-2xx, or the endpoint being absent on
an older app).

**Responses**

| Code | Condition |
|------|-----------|
| 200 | The response body `{ "victim_pod_uids": [...] }` (`PreemptionSelectionResponse`) |
| 403 | Read-only key / non-admin JWT |
| 422 | Malformed body (e.g. `gpu_count <= 0`) |

### `POST /api/reservations/ondemand-admission`

Choose which pending pods the controller should admit on-demand this round.
Requires a `read_write` service key (or an admin JWT) — the same gate as the
on-demand create, cancel, and preemption-victims endpoints. Advisory and
**read-only**: it creates nothing and returns only a selection; the controller
then creates a real lease for each granted pod via `POST /api/reservations` (see
**Creating on-demand reservations**).

This is the delegation point for **LAS (least-attained-service) prioritization**
and any future admission policy. The controller has already determined *which*
pending pods are eligible for a JIT lease this round (GPU-only-pending, not
matched by an open reservation, past their retry cooldown, of a class that is
not under the stuck-holder safety interlock). This endpoint decides *which* of
those eligible pods to admit now, so prioritisation policy lives in the app
rather than the controller. Each candidate is the exact "ask" a
`POST /api/reservations` create would carry, so the app can weigh it against
priority **and** the same feasibility analysis a create performs.

```json
{
  "candidates": [
    { "pod_uid": "abc-123", "username": "alice", "group_name": "cse142",
      "gpu_class_id": 10, "gpu_count": 1, "duration_seconds": 1800 },
    { "pod_uid": "def-456", "username": "bob", "group_name": null,
      "gpu_class_id": 10, "gpu_count": 2, "duration_seconds": 1200 }
  ]
}
```

| Field | Meaning |
|-------|---------|
| `candidates[].pod_uid` | Opaque pod identifier; echoed back verbatim in the response (equals the create's `idempotency_key`) |
| `candidates[].username` | Reservation owner the lease would be created for (the pod's namespace) |
| `candidates[].group_name` | Usage group the lease would be created under, or `null` when group matching is disabled |
| `candidates[].gpu_class_id` | Numeric GPU-class id the lease would target |
| `candidates[].gpu_count` | GPUs the pod requests |
| `candidates[].duration_seconds` | Lease duration the controller would request (pod minimum-runtime + buffer) |

The app returns the subset of `pod_uid`s it grants admission this round:

```json
{ "granted_pod_uids": ["abc-123"] }
```

The controller admits only pods it offered — a `pod_uid` in the response that was
not in the request is ignored. An **empty** list is a deliberate "grant none this
round" decision and is respected (the non-granted pods simply retry on a later
tick). The controller falls back to granting **every** offered candidate (its
prior greedy per-pod behaviour) only when the call itself fails (network error,
non-2xx, or the endpoint being absent on an older app), or when
`ONDEMAND_DELEGATE_ADMISSION` is disabled controller-side.

For each granted pod the controller then issues an idempotent
`POST /api/reservations` (keyed by `pod_uid`); a `409` there still applies — a
grant this endpoint returns is an admission *decision*, and the subsequent create
remains the authoritative feasibility check.

**Responses**

| Code | Condition |
|------|-----------|
| 200 | The response body `{ "granted_pod_uids": [...] }` (`OnDemandAdmissionResponse`) |
| 403 | Read-only key / non-admin JWT |
| 422 | Malformed body (e.g. `gpu_count <= 0`) |

### Reading the reservation time window

Every `ReservationResponse` includes pre-computed UTC timestamps:

| Field | Type | Description |
|-------|------|-------------|
| `start_utc` | string (ISO 8601, `Z`) | Reservation start in UTC |
| `end_utc` | string (ISO 8601, `Z`) | Reservation end in UTC |

Use these directly — no timezone knowledge required:

```python
# Python example — compute activeDeadlineSeconds for the k8s controller
from datetime import datetime, timezone

end = datetime.strptime(reservation["end_utc"], "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=timezone.utc)
active_deadline_seconds = max(0, int((end - datetime.now(timezone.utc)).total_seconds()))
```

**How they are computed.** The server converts the stored local-time `start_dt` /
`end_dt` to UTC using the `TIMEZONE` environment variable configured at deployment time:

```
start_utc = start_dt converted to UTC via TIMEZONE
end_utc   = end_dt   converted to UTC via TIMEZONE
```

**Example** — server configured with `TIMEZONE=America/Los_Angeles` (PDT, UTC−7),
`start_dt = "2026-06-11T20:00:00"`, `end_dt = "2026-06-11T22:00:00"`:

```
start_utc = "2026-06-12T03:00:00Z"
end_utc   = "2026-06-12T05:00:00Z"
```

### Kubernetes node targeting

`gpu_class.name` names the hardware tier.  Each GPU class may also carry a
`label_value` field — the Kubernetes node-label value for the tier (e.g.
`h100`, `a100-80gb`).  It is not embedded in reservation responses; retrieve
it with the endpoint below.

### `GET /api/gpu-classes/{class_id}`

Fetch a single GPU class, including its `label_value`.  Accessible with
either service-key scope.

**Path parameter:** `class_id` — integer (from `reservation.gpu_class_id`)

**Response** `200`

```json
{
  "id": 1,
  "name": "H100",
  "description": "NVIDIA H100 80 GB SXM5",
  "total_gpus": 8,
  "label_value": "h100",
  "su_rate_per_hour": 4,
  "max_gpus_per_reservation": 2,
  "relax_min_available": null,
  "attach_all_groups": false,
  "is_active": true,
  "created_at": "2026-01-15T09:00:00Z"
}
```

`su_rate_per_hour` is the base Service Units charged per GPU per hour (before
discount-schedule multipliers). `max_gpus_per_reservation` caps a single booking's
GPU count (`null` = no cap). `relax_min_available` is an admission-control
buffer for borrowing (limit relaxation; `null` = no buffer); it does not affect the
controller and can be ignored by API clients.
`attach_all_groups` makes the class bookable by every group without an explicit
attachment.

`label_value` is `null` when the class has no Kubernetes mapping; the
controller skips reservations for such classes.

**Errors**

| Code | Condition |
|------|-----------|
| 404 | GPU class not found |

`GET /api/gpu-classes` (the full list) is service-key accessible as well.
All gpu-class **write** endpoints (`POST`/`PUT`/`DELETE` and the
`/overrides` sub-resource) require an admin JWT.

### `GET /api/settings`

Public (no authentication required). The response carries UI-oriented fields
(site title, announcement content, the borrowing default `relax_limits`, …) and
`controller_push_enabled` (whether the app is configured to push reservation
changes to the controller in real time). Nothing in it is required by the
controller: every reservation it needs arrives via `GET /api/reservations` and
the on-demand endpoints above. (The pre-lease-model `reclaim_window_minutes` /
`reclaim_preempt_guard_minutes` fields are gone along with the reclaim
subsystem.)

---

## 5. Roster sync endpoints

The roster sync daemon provisions accounts and keeps group membership in sync
with an institutional directory. All write operations are idempotent or
explicitly noted otherwise.

### Users

#### `GET /api/users`

List all user accounts (including inactive/deactivated), ordered by username.

**Response** `200` — array of [UserResponse](#userresponse)

---

#### `POST /api/users`

Create a new user account.

**Request body**

| Field | Type | Required | Constraints | Description |
|-------|------|----------|-------------|-------------|
| `username` | string | yes | 3–64 chars, unique | Login name |
| `email` | string | yes | unique | Email address |
| `password` | string | only for local accounts | min 8 chars | Initial password (Argon2id-hashed). Required when `auth_provider` is `"local"`; **must be omitted** for any other provider. |
| `role` | string | no | `"admin"` \| `"auditor"` \| `"user"`; default `"user"` | App-wide privilege tier: `admin` = full read/write, `auditor` = read-only access to all admin surfaces, `user` = ordinary account. **Service keys may set only `"user"`** — a key requesting `admin` or `auditor` receives 403. |
| `is_admin` | boolean | no | default `false` | **Deprecated alias** for `role` (maps `true`→`admin`, `false`→`user`). Honoured only when `role` is omitted; prefer `role`. Same service-key restriction applies. |
| `auth_provider` | string | no | default `"local"` | `"local"`, an OAuth provider name (`"jupyterhub"`, `"google"`, `"oidc"`), or `"saml"`. Set the matching provider name when pre-provisioning accounts that will log in via SSO. |
| `external_id` | string | no | unique | External identity for the provider (JupyterHub username, Google email, OIDC subject, or domain-stripped SAML NameID). Set it for non-local accounts so the SSO callback matches the pre-created row. |

**Response** `201` — [UserResponse](#userresponse)

**Errors**

| Code | Condition |
|------|-----------|
| 400 | `username` already exists |
| 400 | `email` already registered |
| 400 | `external_id` already registered |
| 403 | Service key attempted to create a privileged user (`role` other than `"user"`) |

**Sync pattern:** query `GET /api/users` first and index by `username` or
`email` to avoid duplicate-creation errors.

---

#### `PUT /api/users/{user_id}`

Partial update of a user account. Supply only the fields to change; omitted
fields are left unchanged.

**Path parameter:** `user_id` — integer, user's database ID

**Request body** (all fields optional)

| Field | Type | Description |
|-------|------|-------------|
| `email` | string | New email address (must be unique) |
| `password` | string (min 8 chars) | New password. **Human callers only** — a service key sending this field receives 403. |
| `role` | string | Set the privilege tier (`"admin"` \| `"auditor"` \| `"user"`). **Admin JWT only** — a service key sending this field receives 403. |
| `is_admin` | boolean | **Deprecated alias** for `role` (maps `true`→`admin`, `false`→`user`); applied only when `role` is omitted. **Admin JWT only.** |
| `is_active` | boolean | Reactivate (`true`) or deactivate (`false`) a user |

For a service-key caller (the roster-sync case) the usable fields are
therefore `email` and `is_active` — a leaked key must not be able to take
over an account by resetting its password or changing its role.

**Response** `200` — [UserResponse](#userresponse)

**Errors**

| Code | Condition |
|------|-----------|
| 400 | New email already in use by another account |
| 400 | Change would demote/deactivate the last active administrator |
| 403 | Service key attempted to set `password`, `role`, or `is_admin` |
| 404 | User not found |

**Deactivation note:** prefer `DELETE /api/users/{user_id}` for departing
users; `PUT` with `is_active: false` is equivalent but also allows reactivation
via `is_active: true`.

---

#### `DELETE /api/users/{user_id}`

Soft-deactivate a user (`is_active → false`). The account record and all
reservation history are preserved. Deactivated users cannot log in.

**Path parameter:** `user_id` — integer

**Response** `204` No Content

**Errors**

| Code | Condition |
|------|-----------|
| 404 | User not found |

---

### Groups

#### `GET /api/groups`

List all usage groups with full member and attached-GPU-class details, ordered by name.

**Response** `200` — array of [GroupResponse](#groupresponse)

**Sync pattern:** call this once per sync run to build a `name → id` map,
then use IDs for all membership operations below.

---

#### `GET /api/groups/{group_id}/members`

List all members of a group, ordered by username.

**Path parameter:** `group_id` — integer

**Response** `200` — array of [UserBrief](#userbrief)

```json
[
  { "id": 7,  "username": "jsmith" },
  { "id": 12, "username": "mlee" }
]
```

**Note:** this endpoint returns `UserBrief` (id, username) without
the role. To see roles, use the `members` array inside `GET /api/groups/{id}`
which returns [GroupMemberBrief](#groupmemberbrief) objects that include `role`.

**Errors**

| Code | Condition |
|------|-----------|
| 404 | Group not found |

---

#### `POST /api/groups/{group_id}/members`

Add a user to a group, or update their role if they are already a member
(idempotent upsert).

**Authorization** (applies to `POST`, `PATCH`, and `DELETE` on `.../members`):
an admin JWT or a `read_write` service key (the controller path) may curate any
group. In the human UI, a **group manager** may also curate a group whose
`provisioning_source` is `"manager"` (full parity — add/remove members and
appoint/demote co-managers); other callers get `403`. These endpoints only
attach existing accounts — they never create users.

**Path parameter:** `group_id` — integer

**Request body**

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `user_id` | integer | yes | Database ID of the user to add |
| `role` | `"member"` \| `"manager"` | no | Default `"member"` |

**Response** `204` No Content

**Errors**

| Code | Condition |
|------|-----------|
| 403 | Caller may not curate this group's membership |
| 404 | Group not found |
| 404 | User not found |

**Role mapping suggestion** (adapt to your directory schema):

| Directory role | API role |
|---------------|----------|
| instructor | `"manager"` |
| TA | `"manager"` |
| student | `"member"` |

---

#### `PATCH /api/groups/{group_id}/members/{user_id}`

Change the role of an existing group member. Fails if the user is not
currently a member (use `POST` to add-or-update instead).

**Path parameters:** `group_id`, `user_id` — integers

**Request body**

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `role` | `"member"` \| `"manager"` | yes | New role |

**Response** `204` No Content

**Errors**

| Code | Condition |
|------|-----------|
| 403 | Caller may not curate this group's membership |
| 404 | Group not found, or user is not a member of the group |

---

#### `DELETE /api/groups/{group_id}/members/{user_id}`

Remove a user from a group. Existing reservations made under this group
membership are not affected.

**Path parameters:** `group_id`, `user_id` — integers

**Response** `204` No Content

**Errors**

| Code | Condition |
|------|-----------|
| 403 | Caller may not curate this group's membership |
| 404 | Group not found, or membership record not found (user was not in the group) |

---

### Recommended sync algorithm

```
groups_by_name = { g.name: g.id  for g in GET /api/groups }
users_by_uname = { u.username: u for u in GET /api/users   }

for each user in directory:
    if user.username not in users_by_uname:
        POST /api/users   →  record new id
    else if user attributes changed:
        PUT  /api/users/{id}

    for each (group_name, role) the user should belong to:
        group_id = groups_by_name[group_name]
        POST /api/groups/{group_id}/members  { user_id, role }
        # idempotent: also corrects the role if it changed

for each user in system who is NOT in directory:
    DELETE /api/users/{id}   # soft-deactivates

for each (group, user) membership in system NOT in directory:
    DELETE /api/groups/{group_id}/members/{user_id}
```

`POST /api/groups/{group_id}/members` is an upsert, so a single pass covers
both new additions and role corrections without a separate `PATCH` call.

---

## 6. Type reference

### UserResponse

```json
{
  "id":            7,
  "username":      "jsmith",
  "email":         "jsmith@example.edu",
  "role":          "user",
  "is_admin":      false,
  "is_active":     true,
  "auth_provider": "local",
  "external_id":   null,
  "created_at":    "2026-01-15T09:00:00Z"
}
```

| Field | Type | Notes |
|-------|------|-------|
| `id` | integer | Stable primary key; use this in membership calls |
| `username` | string | 3–64 chars, unique, immutable after creation |
| `email` | string | Unique |
| `role` | string | App-wide privilege tier: `"admin"`, `"auditor"` (read-only admin), or `"user"` |
| `is_admin` | boolean | Derived from `role` (`true` iff `role == "admin"`); retained for backward compatibility |
| `is_active` | boolean | `false` = soft-deleted |
| `auth_provider` | string | `"local"`, an OAuth provider name (`"jupyterhub"`, `"google"`, `"oidc"`), or `"saml"` |
| `external_id` | string \| null | External identity (JupyterHub username / Google email / OIDC subject / domain-stripped SAML NameID) for SSO accounts |
| `created_at` | datetime | UTC |

---

### GroupResponse

```json
{
  "id": 2,
  "name": "CS151B-FA26",
  "description": "Deep Learning — Fall 2026",
  "valid_from": "2026-09-22",
  "valid_until": "2026-12-12",
  "min_days_ahead": 0,
  "max_days_ahead": 14,
  "su_budget": 200,
  "su_anchor_mode": "open",
  "provisioning_source": "admin",
  "sync_with_sicad": false,
  "sicad_course_id": null,
  "is_active": true,
  "created_at": "2026-06-01T10:00:00Z",
  "members": [
    { "id": 7, "username": "jsmith", "role": "manager" },
    { "id": 9, "username": "bwang",  "role": "member"  }
  ],
  "gpu_classes": [ ... ]
}
```

`gpu_classes` lists the GPU classes the group may book — explicit
`UsageGroupGpuClass` attachments plus any class flagged `attach_all_groups`. Each
entry is a full GpuClassResponse.

| Field | Type | Notes |
|-------|------|-------|
| `id` | integer | |
| `name` | string | Unique |
| `description` | string \| null | |
| `valid_from` | date \| null | Group bookable on or after this date; admins and group managers get a 90-day grace window before this date |
| `valid_until` | date \| null | Group bookable on or before this date; admins and group managers get a 90-day grace window after this date |
| `min_days_ahead` | integer \| null | Members must book at least N days in advance (ignored for admins and group managers) |
| `max_days_ahead` | integer \| null | Members cannot book more than N days out (ignored for admins and group managers) |
| `su_budget` | number \| null | Per-member Service Unit budget: the sum of stored `su_cost` over a member's open reservations (within the window set by `su_anchor_mode`) may not exceed this (ignored for admins and group managers). `null` = unlimited |
| `su_anchor_mode` | string | How far back the SU budget window reaches: `"open"` (only currently-open reservations; renewable ceiling), `"weekly"`, `"monthly"`, `"quarterly"`, or `"since_creation"` (cumulative, never resets). See SCHEDULING.md §5 for full semantics. |
| `provisioning_source` | string | Who may curate this group's membership: `"admin"` (administrators only; default), `"manager"` (the group's managers **and** admins), or `"sicad"` (the built-in SICAD roster sync **and** admins). Administrators may always curate regardless. Settable only by an admin via `POST`/`PUT /api/groups`. |
| `sync_with_sicad` | boolean | Read-only derived flag (`provisioning_source == "sicad"`). When `true`, the app's built-in SICAD roster sync keeps this group's membership in sync with the course roster (add-only). Retained for backward compatibility; set `provisioning_source` to change it |
| `sicad_course_id` | string \| null | Remote SICAD/AWSEd courseID backing the roster sync. `null` = fall back to `name`, letting the group name differ from the SICAD courseID. Only meaningful when `provisioning_source` is `"sicad"` |
| `is_active` | boolean | Inactive groups cannot accept new reservations |
| `created_at` | datetime | UTC |
| `members` | array of [GroupMemberBrief](#groupmemberbrief) | |

---

### GroupMemberBrief

Appears in the `members` array of `GroupResponse`.

| Field | Type | Notes |
|-------|------|-------|
| `id` | integer | User's database ID |
| `username` | string | |
| `role` | `"member"` \| `"manager"` | Per-group role |

---

### UserBrief

Returned by `GET /api/groups/{group_id}/members`.

| Field | Type |
|-------|------|
| `id` | integer |
| `username` | string |

---

### ReservationResponse

```json
{
  "id": 42,
  "user_id": 7,
  "user": { "id": 7, "username": "jsmith" },
  "group_id": 2,
  "group": { "id": 2, "name": "CS151B-FA26" },
  "gpu_class_id": 1,
  "gpu_class": { "id": 1, "name": "H100", "label_value": "h100" },
  "start_dt": "2026-09-01T08:00:00",
  "end_dt": "2026-09-01T16:00:00",
  "date": "2026-09-01",
  "start_utc": "2026-09-01T15:00:00Z",
  "end_utc": "2026-09-01T23:00:00Z",
  "gpu_count": 4,
  "su_cost_user": 32,
  "su_cost_group": 32,
  "su_cost_original": 32,
  "kind": "booking",
  "status": "active",
  "notes": null,
  "submitted_by_id": 7,
  "submitted_by": { "id": 7, "username": "jsmith" },
  "created_at": "2026-08-20T10:15:00Z",
  "updated_at": "2026-08-20T10:15:00Z",
  "cancelled_at": null,
  "cancelled_by_id": null,
  "cancel_reason": null
}
```

| Field | Type | Notes |
|-------|------|-------|
| `id` | integer | |
| `user_id` | integer \| null | Reservation holder; always set on current rows (nullable only for pre-lease-model data) |
| `user` | UserBrief \| null | |
| `group_id` | integer \| null | Always set on current rows (nullable only for pre-lease-model data) |
| `group` | GroupBrief \| null | |
| `gpu_class_id` | integer | |
| `gpu_class` | `{id, name, label_value}` | |
| `start_dt` | datetime (local, no suffix) | Reservation start in site-local wall-clock; may cross midnight. Whole-hour for `kind="booking"`; arbitrary for `kind="on_demand"` |
| `end_dt` | datetime (local, no suffix) | Reservation end; ≤ 48h after `start_dt` for non-privileged members' bookings; no server-side cap for admins/managers; `start + duration_seconds` for leases |
| `date` | date | Calendar date of `start_dt` (convenience for filtering) |
| `start_utc` | string (ISO 8601, `Z`) | Reservation start converted to UTC; use this for time comparisons |
| `end_utc` | string (ISO 8601, `Z`) | Reservation end converted to UTC; use this for `activeDeadlineSeconds` |
| `gpu_count` | integer | Number of GPUs reserved |
| `su_cost_user` | number | Service Units charged to the individual user (zeroed when a manager waives a cancellation penalty) |
| `su_cost_group` | number | Service Units charged against the group pool (only zeroed on an admin waive) |
| `su_cost_original` | number | Service Units charged at creation time; never altered by later penalty rewrites or waives |
| `kind` | `"booking"` \| `"on_demand"` | `"booking"` = user-scheduled reservation; `"on_demand"` = controller-requested lease (see §"Reservation kinds") |
| `status` | `"active"` \| `"cancelled"` | |
| `notes` | string \| null | Free-text note from the user (or the controller, for leases) |
| `submitted_by_id` | integer \| null | User ID of the authenticated caller (differs from `user_id` when a manager books on behalf of a member; `null` for leases) |
| `submitted_by` | UserBrief \| null | Brief info for the submitter |
| `created_at` | datetime (UTC, `Z`) | |
| `updated_at` | datetime (UTC, `Z`) | |
| `cancelled_at` | datetime \| null (UTC, `Z`) | |
| `cancelled_by_id` | integer \| null | User ID of whoever cancelled; `null` for a controller (service-key) cancellation |
| `cancel_reason` | `"no-show"` \| `"controller-revoked"` \| `"pod-terminated"` \| `"superseded"` \| null | Machine-readable reason recorded by `POST /api/reservations/{id}/cancel` (or `"superseded"` when a source was continued via `POST /api/reservations/{id}/continue`); `null` for human cancellations |
| `continued_from_id` | integer \| null | Set on a booking minted via `POST /api/reservations/{id}/continue`: the id of the superseded source reservation whose pod it carries forward; `null` otherwise |

---

### ServiceKeyResponse

```json
{
  "id": 3,
  "name": "k8s-controller-prod",
  "key_prefix": "gpures_a3f9c012",
  "scope": "read_only",
  "is_active": true,
  "created_at": "2026-06-07T14:22:00",
  "last_used_at": "2026-06-07T18:05:00"
}
```

| Field | Type | Notes |
|-------|------|-------|
| `id` | integer | Use this ID for revocation |
| `name` | string | Human label |
| `key_prefix` | string | First 15 chars of raw key — safe to log for audit |
| `scope` | `"read_only"` \| `"read_write"` | Key's permission level |
| `is_active` | boolean | `false` = revoked |
| `created_at` | datetime | UTC |
| `last_used_at` | datetime \| null | UTC; updated on every authenticated request |

---

### ServiceKeyCreateResponse

Extends `ServiceKeyResponse` with one additional field returned only at creation:

| Field | Type | Notes |
|-------|------|-------|
| `raw_key` | string | The full 71-character key. Store immediately — never retrievable again. |
