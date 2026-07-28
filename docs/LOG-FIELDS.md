# Log fields

The canonical field dictionary for **both** the reservation app and the
`gpu-reservation-controller` daemon. An identical copy lives at this same path
in the sibling repo — update both together, the same way `API.md` /
`docs/RESERVATION-API.md` are kept in step. Sharing the dictionary is the point:
a line from either side can be joined on the same key.

`app/log_fields.py` (also duplicated in both repos) is the only thing that
renders this grammar.

> **Status: phase 2.** Converted so far — app: `auth`, `reservations`,
> `availability`; controller: pod admission, preemption, and the Kubernetes
> client. Still emitting the original prose (phase 3): app admin CRUD, settings,
> SICAD, email, data-io, controller push; controller JIT lease requests, no-show
> handling, reservation fetch, capacity audit, the API client, and the watch
> stream. A field marked *(planned)* below has a dictionary entry but no emitter
> yet.

---

## 1. Grammar

A line is:

```
<ts> <LEVEL> <logger>: actor=<actor> trace=<trace> event=<noun>.<verb> [k=v ...]
```

1. **`event=` is always the first message field** and is the only prose-derived
   one. Lower-snake noun, dot, verb: `event=reservation.created`,
   `event=pod.admitted`, `event=preempt.boundary`.
2. **One concept per field.** No parentheses, slashes, arrows or `a..b` ranges
   inside a value.
   - `pod ns/name` → `ns=jdoe pod=notebook-0`
   - `class=H100(id=2)` → `cid=2 class=H100`
   - `requested=2026-08-01..2026-08-07` → `req_from=2026-08-01 req_to=2026-08-07`
   - `user_id: 3 -> 7` → `old.uid=3 new.uid=7`
   - `reservation #42` → `rid=42`
3. **Absent means omitted.** `kv()` drops a `None` or empty value entirely
   rather than emitting `key=-` or `key=None`. A key that is not on the line was
   not known — which is how a fail-open guard stays distinguishable from one
   that measured zero. The two **envelope** fields are the documented exception:
   a formatter cannot omit a field, so `actor=-` / `trace=-` mean "not known".
4. **Values are quoted only when they need it.** A value containing a space,
   `=` or `"` is double-quoted with `\` and `"` escaped. Free text (`reason`,
   `err`, `detail`) therefore stays on one field without breaking the parse.
5. **Lists are comma-joined with no spaces** (`ids=3,7,9`). **Maps are fanned
   out to one line per key** — the preemption sweep emits one line per GPU
   class rather than one line carrying two dicts.
6. **Booleans** render `true` / `false`. **Timestamps** render ISO-8601;
   attach `tzinfo` before logging a wall-clock value so the offset is carried
   (the controller's datetimes are always UTC-aware; the app should convert via
   `timezone_utils.site_tz()`).
7. **Every value is scrubbed** of non-printable characters by `kv()` — newlines
   above all. Many values are attacker-influenced (submitted usernames, OAuth
   identities, SAML NameIDs, SICAD roster names, pod names and annotations), and
   the chokepoint is the helper so no call site can forget.

---

## 2. Key naming

Two tiers, so the short keys stay memorable rather than becoming a dozen
cryptic abbreviations.

**Tier 1 — typed short ids, for referring to *other* entities.** These recur
across many message families and are the only abbreviations to learn:

| key | entity |
|---|---|
| `uid` | user |
| `gid` | usage group |
| `cid` | GPU class |
| `rid` | reservation |
| `cohid` | cohort |
| `tid` | team |

**Tier 2 — `id=` is the primary key of whatever `event=` names.** The long-tail
entities each appear in one message family, so the event name already
disambiguates and no new abbreviation is needed:

```
event=date_range.updated id=7 from=2026-09-21 to=2026-12-11 propagated=14
event=su_boost.created id=3 uid=91 gid=12 su_offset=250
```

Corollary: on `event=reservation.*` the reservation is `id=`, not `rid=`. `rid=`
appears only where a non-reservation event points at one
(`event=overstay.recorded id=8 rid=42`, and every controller pod line).

**Names travel beside ids as separate fields**, never inside them:
`uid=91 user=jdoe`, `gid=12 group=cse151b`, `cid=2 class=H100`.

**GPU class has three distinct values** and therefore three keys: `cid` (numeric
app id), `class` (display name), `clabel` (Kubernetes node-label value, which is
all the controller could resolve before). Emitting them separately is what makes
an app line and a controller line joinable on class.

**`reason=` is scoped by `event=`.** Three unrelated enums share the key; the
event name says which. A bare `grep reason=` mixes them — scope to the event.

---

## 3. Envelope

Rendered by the logging formatter, not by call sites.

| field | app | controller |
|---|---|---|
| timestamp / level / logger | `%(asctime)s %(levelname)s %(name)s` | same, level padded to 8 |
| `actor` | from the **unverified** Bearer JWT `username` claim: a username, `imp:<target>:<impersonator>`, or `-` | constant `controller` |
| `trace` | the `X-Client-Trace` header, whitelist-matched `[A-Za-z0-9_-]{1,36}`, else `-` | *(planned)* one per sweep/tick/batch, propagated outbound |

The actor is read before signature verification (a rejected login still logs),
so it goes through `_sanitize_log_token`, which additionally replaces space,
`=` and `"` — a formatter cannot decide to quote, so the value must be safe
bare. `kv()` quotes instead, but only because it renders the message body.

Third-party loggers (uvicorn access lines, httpx, the Kubernetes client) do not
follow this grammar and are not expected to.

---

## 4. Dictionary

### Identity and entities

| key | type | meaning |
|---|---|---|
| `id` | int | primary key of the entity `event=` names |
| `uid` | int | user id |
| `user` | string | username (controller: also the pod's namespace) |
| `gid` | int | usage group id |
| `group` | string | usage group name |
| `cid` | int | GPU class id |
| `class` | string | GPU class display name |
| `clabel` | string | GPU class Kubernetes node-label value |
| `cohid` | int | cohort id |
| `cohort` | string | cohort name |
| `rid` | int | reservation id, referenced from a non-reservation event |
| `tid` | int | team id |
| `extid` | string | SSO provider's stable identity |
| `poduid` | string | Kubernetes pod UID; also a lease's idempotency key |
| `ns` | string | Kubernetes namespace |
| `pod` | string | Kubernetes pod name |
| `node` | string | Kubernetes node name |

### Outcome

| key | type | meaning |
|---|---|---|
| `status` | int | HTTP status code |
| `reason` | enum, scoped by `event=` | why — see §2 |
| `detail` | quoted string | free-text denial detail (`HTTPException.detail`) |
| `err` | quoted string | exception text |
| `kind` | `booking` \| `on_demand` | reservation flavour |
| `provider` | `local` \| `jupyterhub` \| `google` \| `oidc` \| `saml` | auth path |
| `role` | `admin` \| `auditor` \| `user`, or `member` \| `manager` | scoped by `event=` |
| `scope` | `read_only` \| `read_write` | service-key privilege |

`reason` enums by event: `auth.*` → `no_such_user`, `inactive`,
`wrong_provider`, `account_locked`, `bad_password`, `deactivated` ·
`reservation.cancelled` → `no-show`, `controller-revoked`, `pod-terminated`,
`superseded` · `overstay.recorded` → `pod-terminated`, `preempted`, `deleted` ·
`k8s.event` → `RuntimeGuaranteed`, `Preempted`, `ReservationCancelled`,
`ReservationReassigned`, `OverstayRelinked`.

### Time

| key | type | meaning |
|---|---|---|
| `start` / `end` | ISO-8601 | reservation window |
| `from` / `to` | ISO-8601 date | inclusive span (date ranges, dated admin surfaces) |
| `req_from` / `req_to` | ISO-8601 date | availability span asked for |
| `srv_from` / `srv_to` | ISO-8601 date | availability span served after clipping |
| `boundary` | ISO-8601 UTC | reservation `slot_start` driving a sweep |
| `deadline` | ISO-8601 UTC | no-show deadline |
| `until` | ISO-8601 UTC | runtime-guarantee end instant |
| `at` | ISO-8601 UTC | projected termination-warning kill instant |
| `locked_until` | ISO-8601 | account lockout expiry |
| `dur_s` | int seconds | duration — disambiguate when two share a line: `min_runtime_s`, `lease_dur_s`, `waited_s`, `guarantee_s` |

### Capacity

| key | type | meaning |
|---|---|---|
| `gpus` | int | GPU count requested or held |
| `reserved` | int | GPUs reserved on the reservation |
| `free` | int | GPUs free |
| `used` | int | GPUs in use |
| `total` | int | physical GPUs allocatable |
| `app_gpus` / `phys_gpus` | int | capacity-audit counts |
| `node_free` | int | largest single-node free GPUs for a class |
| `demand` | int | GPUs demanded at a boundary, per class |
| `kills` | int | victims selected at a boundary |
| `short` | int | GPUs still short after preempting every eligible overstayer |
| `sweep` | `A` \| `B` | preemption sweep phase (`phase=` is the pod lifecycle phase) |
| `phase` | `Pending` \| `Running` \| `Succeeded` \| `Failed` \| `Unknown` | pod lifecycle phase |
| `risk` | float 0..1 | termination-warning risk score |
| `gstatus` | `guaranteed` \| `overstay` | live guarantee standing |

### Service Units

| key | type | meaning |
|---|---|---|
| `su` | float | SU cost at creation |
| `su_user` / `su_group` | float | SU charged to the member / the group pool |
| `su_retained` | float | SU kept for already-consumed time |
| `su_offset` | float | signed per-member budget offset |
| `waived` | bool | whether a cancellation penalty was waived |

### Updates

| key | type | meaning |
|---|---|---|
| `chg` | comma list | which fields an update changed |
| `old.<field>` / `new.<field>` | scalar | before/after value of one changed field |

A redacted field (password, Quill-managed HTML, `logo_data`, SMTP password)
appears in `chg=` with **no** `old.`/`new.` pair — this one rule replaces the
older `password changed` and `[field changed]` conventions. A no-op update omits
`chg=` entirely rather than emitting the literal `no-op`.

### Availability, bulk and counts

| key | type | meaning |
|---|---|---|
| `buckets` | int | hourly buckets returned (`0` = the window clipped everything) |
| `privileged` | bool | whether admin/manager booking rules applied |
| `n` | int | rows written by a bulk operation |
| `ids` | comma list of int | ids affected in bulk |
| `count` | int | fallback count — prefer a specific key (`active`, `cancelled`, `upserts`, `evicted`, `relinked`, `watched`, `preserved`) wherever the event distinguishes them |
| `fails` | int | consecutive failure counter (failed logins, watch-stream reconnects) |

### Auth, SICAD and Kubernetes traces

| key | type | meaning |
|---|---|---|
| `identity` | string | full OAuth identity before domain stripping |
| `nameid` | string | full SAML NameID before domain stripping |
| `domain` | string | domain part checked against the allowlist |
| `allowed` | comma list | configured domain allowlist |
| `errors` | comma list | python3-saml validation error codes |
| `acct_provider` | string | the account's *actual* `auth_provider`, on a `reason=wrong_provider` denial (`provider` stays the attempted path) |
| `course_id` | string | SICAD/AWSEd courseID backing a roster sync |
| `team` | string | SICAD team display name |
| `team_key` | string | SICAD `uniqueName`, the stable team upsert key |
| `src_role` | string | SICAD's upstream role value |
| `booking_ref` | string | `res-<id>` annotation value, where the raw string is the thing asserted |
| `gate` | string | SchedulingGate name |
| `selector` / `rv` / `timeout_s` | string / string / int | Kubernetes LIST+WATCH parameters |
| `watch_event` | `ADDED` \| `MODIFIED` \| `DELETED` | raw watch event type |
| `path` | string | filesystem path |
| `patch` | string | which patch a `k8s.patch_pod` is applying (`toleration`, `runtime_guarantee`, `guarantee_status`, `termination_warning`, `termination_warning_clear`, `gate_remove`) |
| `purpose` | string | why a LIST was issued (`tolerated_snapshot`, `gpu_inventory`) |
| `tol_key` / `tol_value` | string | the toleration being applied |
| `resource` / `value` | string / string | the Kubernetes resource name and the malformed value, on an unparseable allocatable |
| `nodes` | int | nodes carrying a GPU class, in a node-inventory line |

### Selection, counts and diagnostics

| key | type | meaning |
|---|---|---|
| `candidates` | int | size of a pool offered for selection (preemption victims, on-demand admission) |
| `selected` / `granted` | int | how many of them were chosen |
| `reservations` | int | reservations in the occupancy map |
| `fallback` | string | what was used instead when a delegated call was unavailable |
| `target` | string | which snapshot failed (`pods`, `node_capacity`) |
| `source_id` / `source_kind` | int / `booking` \| `on_demand` | the reservation a *continue* was carried forward from |
| `superseded` | bool | whether that source still held future time and was cancelled penalty-exempt |
| `guarantee_s` | int seconds | guaranteed duration, where it shares a line with another duration |

---

## 5. Adding a field

1. Add it here first — this file is the dictionary, not a description of what
   the code happens to do.
2. Reuse an existing key if the concept already has one. Two keys for one
   concept is the failure mode this document exists to prevent.
3. If the value is a map, fan it out to one line per key rather than inventing
   a nested encoding.
4. If the value is attacker-influenced, it needs no special handling — `kv()`
   scrubs everything. Do **not** pre-format it into the message string, which
   would bypass the chokepoint.
