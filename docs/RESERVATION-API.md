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
    "su_cost": 32,
    "kind": "booking",
    "status": "active",
    "notes": null,
    "submitted_by_id": 7,
    "submitted_by": { "id": 7, "username": "jsmith" },
    "created_at": "2026-08-20T10:15:00Z",
    "updated_at": "2026-08-20T10:15:00Z",
    "cancelled_at": null,
    "cancelled_by_id": null,
    "cancelled_by": null
  }
]
```

`start_dt` / `end_dt` are the booking's site-local wall-clock interval (naive, no
timezone). A reservation is an arbitrary whole-hour range that **may cross midnight**
(`end_dt` on the next calendar day). Non-privileged members are limited to 48 hours;
admins and group managers have no server-side cap. `date` mirrors `start_dt`'s date
and is provided for convenience filtering. `su_cost` is the total Service Units the
booking consumes, computed at creation from the GPU class base rate
(`su_rate_per_hour`) and the active discount schedules.

### Reservation kinds and the reclaim filter

Every reservation has a `kind` field: `"booking"` (a normal user reservation) or
`"reclaim"` (an admin-only capacity hold created by the GPU recovery task or
manually by an admin). Reclaim reservations have `user_id = null` and
`group_id = null` — they carry no user or group attribution.

By default `GET /api/reservations` **excludes** reclaim reservations for all
`status` values except `"all"`:

| `status` query param | booking rows | reclaim rows |
|----------------------|-------------|--------------|
| `active` (default)   | ✓           | ✗            |
| `cancelled`          | ✓           | ✗            |
| `all`                | ✓           | ✓            |

The Kubernetes controller can use reclaim reservations to detect idle capacity
that the recovery task has claimed for opportunistic scheduling. To see them,
add `status=all` to the query; then filter on `kind == "reclaim"` to separate
them from cancelled user bookings.

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
  "management_buffer": 1,
  "label_value": "h100",
  "su_rate_per_hour": 4,
  "max_gpus_per_reservation": 2,
  "attach_all_groups": false,
  "is_active": true,
  "created_at": "2026-01-15T09:00:00Z"
}
```

`su_rate_per_hour` is the base Service Units charged per GPU per hour (before
discount-schedule multipliers). `max_gpus_per_reservation` caps a single booking's
GPU count (`null` = no cap).
`attach_all_groups` makes the class bookable by every group without an explicit
attachment. `management_buffer` is the number of GPUs within `total_gpus`
reserved for admin/manager use and invisible to regular members.

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

Public (no authentication required). Besides the UI fields, the response carries
two read-only, env-driven values the controller can use to interpret reclaim
capacity holds:

| Field | Type | Description |
|-------|------|-------------|
| `reclaim_window_minutes` | integer | How far ahead (minutes) the app tiles reclaim holds onto idle capacity (`0` = recovery disabled). |
| `reclaim_preempt_guard_minutes` | integer | Lead time (minutes) before a reclaim hold's start within which the app treats it as **hard** capacity: a hold whose start is inside this guard will not be preempted by a new booking, so it is safe for the controller to schedule onto. A hold further out than the guard may be cancelled (preempted) by a real booking at any time. |

How the controller should read these: a reclaim row (`kind == "reclaim"`, see
§"Reservation kinds and the reclaim filter") whose `start_utc` is more than
`reclaim_preempt_guard_minutes` away may still be preempted and should be treated
as opportunistic/cancellable; once it is within the guard it is committed capacity.
The guard is sized to comfortably exceed the controller's reservation poll
interval, so a preempted hold always leaves the controller's view before it would
be scheduled onto — no explicit coordination is needed.

A within-guard block is committed *by default*, but not unconditionally: a
controller that implements the take-back API (`POST /api/reservations/take-back`)
may cede a specific idle within-guard block on request, at which point the app is
free to book over it. This turns the guard's one-way promise into a handshake —
see the take-back API's own documentation for the request/response contract. A
controller that does not implement take-back is unaffected: it never receives
such a request, and the guard's original one-way promise still holds.

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
| `is_admin` | boolean | no | default `false` | Administrator flag. **Service keys may not set this** — a key sending `is_admin: true` receives 403. |
| `auth_provider` | string | no | default `"local"` | `"local"` or an OAuth provider name (`"jupyterhub"`, `"google"`, `"oidc"`). Set the matching provider name when pre-provisioning accounts that will log in via OAuth. |
| `external_id` | string | no | unique | External identity for the provider (JupyterHub username, Google email, or OIDC subject). Set it for non-local accounts so the OAuth callback matches the pre-created row. |

**Response** `201` — [UserResponse](#userresponse)

**Errors**

| Code | Condition |
|------|-----------|
| 400 | `username` already exists |
| 400 | `email` already registered |
| 400 | `external_id` already registered |
| 403 | Service key attempted to create an admin user |

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
| `is_admin` | boolean | Grant or revoke admin flag. **Admin JWT only** — a service key sending this field receives 403. |
| `is_active` | boolean | Reactivate (`true`) or deactivate (`false`) a user |

For a service-key caller (the roster-sync case) the usable fields are
therefore `email` and `is_active` — a leaked key must not be able to take
over an account by resetting its password or promoting it to admin.

**Response** `200` — [UserResponse](#userresponse)

**Errors**

| Code | Condition |
|------|-----------|
| 400 | New email already in use by another account |
| 400 | Change would demote/deactivate the last active administrator |
| 403 | Service key attempted to set `password` or `is_admin` |
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
| 404 | User is not a member of the group |

---

#### `DELETE /api/groups/{group_id}/members/{user_id}`

Remove a user from a group. Existing reservations made under this group
membership are not affected.

**Path parameters:** `group_id`, `user_id` — integers

**Response** `204` No Content

**Errors**

| Code | Condition |
|------|-----------|
| 404 | Membership record not found (user was not in the group) |

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
| `is_admin` | boolean | |
| `is_active` | boolean | `false` = soft-deleted |
| `auth_provider` | string | `"local"` or an OAuth provider name (`"jupyterhub"`, `"google"`, `"oidc"`) |
| `external_id` | string \| null | External identity (JupyterHub username / Google email / OIDC subject) for OAuth accounts |
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
  "sync_with_sicad": false,
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
| `sync_with_sicad` | boolean | When `true`, the app's built-in SICAD roster sync keeps this group's membership in sync with the course roster (add-only) |
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
  "su_cost": 32,
  "kind": "booking",
  "status": "active",
  "notes": null,
  "submitted_by_id": 7,
  "submitted_by": { "id": 7, "username": "jsmith" },
  "created_at": "2026-08-20T10:15:00Z",
  "updated_at": "2026-08-20T10:15:00Z",
  "cancelled_at": null,
  "cancelled_by_id": null,
  "cancelled_by": null
}
```

| Field | Type | Notes |
|-------|------|-------|
| `id` | integer | |
| `user_id` | integer \| null | Booking user; `null` for `kind="reclaim"` |
| `user` | UserBrief \| null | `null` for `kind="reclaim"` |
| `group_id` | integer \| null | `null` for `kind="reclaim"` |
| `group` | GroupBrief \| null | `null` for `kind="reclaim"` |
| `gpu_class_id` | integer | |
| `gpu_class` | `{id, name, label_value}` | |
| `start_dt` | datetime (local, no suffix) | Reservation start in site-local wall-clock; may cross midnight |
| `end_dt` | datetime (local, no suffix) | Reservation end; ≤ 48h after `start_dt` for non-privileged members; no server-side cap for admins/managers |
| `date` | date | Calendar date of `start_dt` (convenience for filtering) |
| `start_utc` | string (ISO 8601, `Z`) | Reservation start converted to UTC; use this for time comparisons |
| `end_utc` | string (ISO 8601, `Z`) | Reservation end converted to UTC; use this for `activeDeadlineSeconds` |
| `gpu_count` | integer | Number of GPUs reserved |
| `su_cost` | number | Total Service Units consumed (stored at creation) |
| `kind` | `"booking"` \| `"reclaim"` | `"booking"` = normal user reservation; `"reclaim"` = admin-only capacity hold |
| `status` | `"active"` \| `"cancelled"` | |
| `notes` | string \| null | Free-text note from the user |
| `submitted_by_id` | integer \| null | User ID of the authenticated caller (differs from `user_id` when a manager books on behalf of a member) |
| `submitted_by` | UserBrief \| null | Brief info for the submitter |
| `created_at` | datetime (UTC, `Z`) | |
| `updated_at` | datetime (UTC, `Z`) | |
| `cancelled_at` | datetime \| null (UTC, `Z`) | |
| `cancelled_by_id` | integer \| null | User ID of whoever cancelled |
| `cancelled_by` | UserBrief \| null | Brief info for whoever cancelled (mirrors `submitted_by`); populated on cancelled records |

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
