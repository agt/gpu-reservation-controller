# GPU Reservation System — External Daemon API

This document covers every endpoint needed to build two external daemons:

- **Kubernetes controller** — reads active reservations and manages
  `ResourceQuota` / `PriorityClass` objects in user namespaces.
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

Service keys are admin-equivalent: they see all data and may perform all
write operations the daemons need. They are separate from user accounts and
do not expire, but can be revoked instantly (§3).

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

| Type | Format | Example |
|------|--------|---------|
| Date | `YYYY-MM-DD` | `2026-09-01` |
| Datetime | ISO 8601 UTC, no timezone suffix | `2026-09-01T08:00:00` |
| Time-of-day | `HH:MM:SS` | `08:00:00` |

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

**Response** `201` — [ServiceKeyCreateResponse](#servicekeycreateresponse)

```json
{
  "id": 3,
  "name": "k8s-controller-prod",
  "key_prefix": "gpures_a3f9c012",
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
    "user": { "id": 7, "username": "jsmith", "full_name": "Jane Smith" },
    "group_id": 2,
    "group": { "id": 2, "name": "CS151B-FA26" },
    "gpu_class_id": 1,
    "gpu_class": { "id": 1, "name": "H100" },
    "policy_id": 3,
    "slot_index": 0,
    "policy": {
      "id": 3,
      "name": "H100 Morning (8h)",
      "start_time": "08:00:00",
      "duration_minutes": 480,
      "repeat_count": 1
    },
    "date": "2026-09-01",
    "gpu_count": 4,
    "status": "active",
    "notes": null,
    "created_at": "2026-08-20T10:15:00",
    "updated_at": "2026-08-20T10:15:00",
    "cancelled_at": null,
    "cancelled_by_id": null
  }
]
```

### Computing the reservation time window

Each reservation maps to an exact clock window using fields from the nested
`policy` object:

```
slot_start = policy.start_time  +  slot_index × policy.duration_minutes
slot_end   = slot_start         +  policy.duration_minutes
```

Both are offsets in minutes from midnight on `date`, expressed in the cluster's
local timezone (the server does not store a timezone — coordinate with the
cluster operator).

**Example:** `start_time = "08:00:00"`, `duration_minutes = 240`,
`slot_index = 1`, `date = "2026-09-01"`:

```
slot_start = 480 + 1 × 240 = 720 min  →  2026-09-01 12:00
slot_end   = 720 + 240     = 960 min  →  2026-09-01 16:00
```

### Kubernetes node targeting

`gpu_class.name` names the hardware tier.  Each GPU class may also carry a
`label_value` field (see [GpuClassBrief](#gpuclassbriefextended) note) — this
is the Kubernetes node-label value for the tier (e.g. `h100`, `a100-80gb`).
Retrieve it with `GET /api/gpu-classes/{id}` if needed; it is not embedded in
reservation responses.

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
| `full_name` | string | no | — | Display name |
| `password` | string | yes | min 8 chars | Initial password (Argon2id-hashed) |
| `is_admin` | boolean | no | default `false` | Administrator flag |

**Response** `201` — [UserResponse](#userresponse)

**Errors**

| Code | Condition |
|------|-----------|
| 400 | `username` already exists |
| 400 | `email` already registered |

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
| `full_name` | string | New display name |
| `password` | string (min 8 chars) | New password |
| `is_admin` | boolean | Grant or revoke admin flag |
| `is_active` | boolean | Reactivate (`true`) or deactivate (`false`) a user |

**Response** `200` — [UserResponse](#userresponse)

**Errors**

| Code | Condition |
|------|-----------|
| 400 | New email already in use by another account |
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

List all usage groups with full member and policy details, ordered by name.

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
  { "id": 7,  "username": "jsmith",  "full_name": "Jane Smith" },
  { "id": 12, "username": "mlee",    "full_name": "Michael Lee" }
]
```

**Note:** this endpoint returns `UserBrief` (id, username, full_name) without
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
  "id":         7,
  "username":   "jsmith",
  "email":      "jsmith@example.edu",
  "full_name":  "Jane Smith",
  "is_admin":   false,
  "is_active":  true,
  "created_at": "2026-01-15T09:00:00"
}
```

| Field | Type | Notes |
|-------|------|-------|
| `id` | integer | Stable primary key; use this in membership calls |
| `username` | string | 3–64 chars, unique, immutable after creation |
| `email` | string | Unique |
| `full_name` | string \| null | Optional display name |
| `is_admin` | boolean | |
| `is_active` | boolean | `false` = soft-deleted |
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
  "max_reservations_per_user": null,
  "max_reservations_total": null,
  "is_active": true,
  "created_at": "2026-06-01T10:00:00",
  "members": [
    { "id": 7, "username": "jsmith", "full_name": "Jane Smith", "role": "manager" },
    { "id": 9, "username": "bwang",  "full_name": "Bo Wang",    "role": "member"  }
  ],
  "policies": [ ... ]
}
```

| Field | Type | Notes |
|-------|------|-------|
| `id` | integer | |
| `name` | string | Unique |
| `description` | string \| null | |
| `valid_from` | date \| null | Group bookable on or after this date |
| `valid_until` | date \| null | Group bookable on or before this date |
| `min_days_ahead` | integer \| null | Users must book at least N days in advance |
| `max_days_ahead` | integer \| null | Users cannot book more than N days out |
| `max_reservations_per_user` | integer \| null | Per-user simultaneous cap |
| `max_reservations_total` | integer \| null | Group-wide simultaneous cap |
| `is_active` | boolean | Inactive groups cannot accept new reservations |
| `created_at` | datetime | UTC |
| `members` | array of [GroupMemberBrief](#groupmemberbrief) | |
| `policies` | array of PolicyWithClass | Booking time-window definitions |

---

### GroupMemberBrief

Appears in the `members` array of `GroupResponse`.

| Field | Type | Notes |
|-------|------|-------|
| `id` | integer | User's database ID |
| `username` | string | |
| `full_name` | string \| null | |
| `role` | `"member"` \| `"manager"` | Per-group role |

---

### UserBrief

Returned by `GET /api/groups/{group_id}/members`.

| Field | Type |
|-------|------|
| `id` | integer |
| `username` | string |
| `full_name` | string \| null |

---

### ReservationResponse

```json
{
  "id": 42,
  "user_id": 7,
  "user": { "id": 7, "username": "jsmith", "full_name": "Jane Smith" },
  "group_id": 2,
  "group": { "id": 2, "name": "CS151B-FA26" },
  "gpu_class_id": 1,
  "gpu_class": { "id": 1, "name": "H100" },
  "policy_id": 3,
  "slot_index": 0,
  "policy": {
    "id": 3,
    "name": "H100 Morning (8h)",
    "start_time": "08:00:00",
    "duration_minutes": 480,
    "repeat_count": 1
  },
  "date": "2026-09-01",
  "gpu_count": 4,
  "status": "active",
  "notes": null,
  "created_at": "2026-08-20T10:15:00",
  "updated_at": "2026-08-20T10:15:00",
  "cancelled_at": null,
  "cancelled_by_id": null
}
```

| Field | Type | Notes |
|-------|------|-------|
| `id` | integer | |
| `user_id` | integer | |
| `user` | UserBrief | |
| `group_id` | integer \| null | |
| `group` | GroupBrief \| null | |
| `gpu_class_id` | integer | |
| `gpu_class` | `{id, name}` | |
| `policy_id` | integer | |
| `slot_index` | integer | 0-based index into repeating slots |
| `policy` | PolicyBrief | Embedded; see time-window calculation in §4 |
| `date` | date | Calendar date of the reservation |
| `gpu_count` | integer | Number of GPUs reserved |
| `status` | `"active"` \| `"cancelled"` | |
| `notes` | string \| null | Free-text note from the user |
| `created_at` | datetime | UTC |
| `updated_at` | datetime | UTC |
| `cancelled_at` | datetime \| null | UTC |
| `cancelled_by_id` | integer \| null | User ID of whoever cancelled |

---

### ServiceKeyResponse

```json
{
  "id": 3,
  "name": "k8s-controller-prod",
  "key_prefix": "gpures_a3f9c012",
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
| `is_active` | boolean | `false` = revoked |
| `created_at` | datetime | UTC |
| `last_used_at` | datetime \| null | UTC; updated on every authenticated request |

---

### ServiceKeyCreateResponse

Extends `ServiceKeyResponse` with one additional field returned only at creation:

| Field | Type | Notes |
|-------|------|-------|
| `raw_key` | string | The full 71-character key. Store immediately — never retrievable again. |
