# GPU Reservation System — Scheduling Capabilities Summary

A FastAPI/SQLite single-lab GPU reservation app. Students book GPU time to build
and run assignments. This document captures the full set of scheduling
constraints, control levers, and allocation logic — written for review against
operations-research literature for configuration guidance.

## 1. The resource model (what is being scheduled)

- **GPU classes** (`gpu_classes`) — hardware tiers, each with a fixed integer
  `total_gpus` (the cluster pool for that tier). This is the supply. Multiple
  independent classes (e.g. H100, A100) are scheduled separately; there is no
  cross-class substitution. Each class carries a `su_rate_per_hour` (base Service
  Units per GPU per hour, default 0), an optional `max_gpus_per_reservation`
  cap, an optional `min_su_per_gpu_hour` floor (hard minimum effective rate —
  see **SU pricing** below), and a `management_buffer` (INT, default 0) —
  see **Management buffer** below.
- **Capacity overrides** (`gpu_class_day_overrides`) — per-date-span capacity
  adjustments (holidays, maintenance, surge). Inclusive `date_start`/`date_end`,
  either bound nullable = unbounded. Overlaps resolve **narrowest-span-wins**;
  ties → largest `available_gpus`. This lets a short maintenance window override
  a long-standing baseline. Each override also carries a `management_buffer` that
  replaces the class-level buffer for covered dates.

### Management buffer

`GpuClass.management_buffer` (and `GpuClassDayOverride.management_buffer` for
date-scoped periods) reserves a slice of GPU capacity that is invisible to
ordinary members:

- **Non-privileged users** see `total_gpus − management_buffer` as their
  effective pool in availability queries and are capped to that ceiling when
  booking.
- **Admins and group managers** see and book -- for themselves or on behalf of users/group members -- against the true `total_gpus`
  (the buffer does not reduce their visible or bookable capacity).
- The day-override `management_buffer` follows the same narrowest-span-wins
  resolution as `available_gpus`, so a short maintenance-mode override can
  raise or lower the buffer relative to the class default.

Use this to hold headroom for unplanned hardware maintenance, or to allow 
instructors to make student accommodations even when the cluster otherwise appears full.

## 2. The time structure (how reservations are expressed)

Time is **continuous at whole-hour granularity** — not a fixed slot grid.
A reservation specifies an arbitrary `start_dt`/`end_dt` pair, both snapped to
whole hours (no minutes/seconds), site-local wall-clock time:

- `start_dt` must be at least 15 minutes in the future (propagation lead time
  for the Kubernetes controller).
- `end_dt > start_dt`; may cross midnight (e.g. 22:00 → 06:00).
- Non-privileged members are subject to a **48-hour server-side cap** (the API
  returns 400 for requests exceeding 48 h). Admins and group managers are exempt
  and may create reservations longer than 48 h, typically via the admin
  reservation interface. The frontend booking wizard also enforces a 48-hour
  ceiling for non-privileged users.
- The `date` column mirrors `start_dt.date()` for filtering.

Capacity is evaluated **per hour** across the booked interval, so cross-midnight
ranges correctly see each day's day-override capacity.

## 3. Service Unit (SU) pricing

Every GPU class has a `su_rate_per_hour` (base rate). System-wide
**SU discount schedules** (`su_discount_schedules`) define time-of-day multipliers
that reduce the effective rate during off-peak windows:

- Each schedule has `days_of_week` (JSON int list, 0=Mon), `start_time`/`end_time`
  (wall-clock; `end_time ≤ start_time` wraps past midnight), `multiplier`
  (0 ≤ m ≤ 1; effective rate = base × m; `0` = free window), optional inclusive
  `date_start`/`date_end`, and `is_active`.
- Each schedule must be **explicitly attached to one or more GPU classes** via the
  `su_discount_schedule_gpu_classes` join table (`gpu_class_ids` field on create/update).
  A schedule with no attached classes has no effect on any class, regardless of its
  `is_active` state.
- Among overlapping schedules covering the same GPU class at any instant, the same
  narrowest-span-wins resolution applies: narrowest date-window for discount; ties →
  highest multiplier (smallest discount).
- When no attached schedule covers the instant, the multiplier is **1.0** (full price).

The booking's **total SU cost** is computed at creation time and stored on the
reservation row:

```
effective_rate(hour) = max(base_rate × multiplier(hour), min_su_per_gpu_hour)
su_cost = Σ (effective_rate(hour) × gpu_count)  for each hour in [start_dt, end_dt)
```

`GpuClass.min_su_per_gpu_hour` is a hard floor on the effective per-GPU-hour rate
regardless of which discount schedule applies.  It ensures a minimum cost even when
a zero-multiplier free window is active.  `NULL` means no floor (any multiplier,
including 0, applies directly).

This stored cost is then used for budget enforcement and reporting.

`app/pricing.py` exposes `effective_multiplier`, `compute_su_cost`, and
`hourly_breakdown` (used by the availability timeline so the frontend can preview
cost before booking).

### Cancellation penalties

The stored `su_cost` is adjusted at cancellation time based on lead time:

| Lead time at cancellation | SU cost outcome |
|---|---|
| ≥ 24 h before `start_dt` | `su_cost` set to **0** (full waiver) |
| < 24 h before `start_dt` | `su_cost` set to **50 %** of the cost attributable to the within-24h portion of the reservation |

A cancelled reservation with a non-zero `su_cost` (a late-cancel penalty)
continues to count against the member's open SU balance until its `end_dt`
passes — the same renewable-ceiling model as active reservations. This prevents
gaming the budget by booking then cancelling at the last minute.

**Waiving penalties.** Admins and group managers can waive the penalty:
- **At cancellation time** — the cancel modal shows the computed penalty and
  offers a waive checkbox.
- **After the fact** — `POST /api/reservations/{id}/waive-penalty` clears the
  `su_cost` on an already-cancelled reservation. The admin reservations page shows
  an amber indicator on cancelled rows with a non-zero penalty.

## 4. Access scoping & shares (who can book what)

Allocation of the cluster to "courses" (as well as projects, labs, etc.) is done through **usage groups**
(`usage_groups`):

- **GPU class attachment** (`usage_group_gpu_classes`) — a group can only book
  GPU classes explicitly linked to it, or any class flagged `attach_all_groups`.
  This controls *which hardware tiers* a course sees.
- **Per-group GPU ceiling** (`usage_group_gpu_limits`) — `max_gpus` cap per
  (group, class) over an optional date span. `max_gpus=0` disables a class for
  that group. Same narrowest-span-wins overlap resolution → supports a temporary
  "deadline-week boost" overriding a semester-long baseline cap. This is the
  **course's share** of the cluster.
  - Effective hourly availability for a member =
    `min(cluster_capacity − peak_reserved, group_ceiling − group_peak_reserved)`.
    "Peak" = maximum concurrent GPUs across any one-hour bucket in the requested
    interval (not a sum of all overlapping reservations).

## 5. Quotas & booking-window constraints (per group, enforced at booking time)

All on `UsageGroup`, enforced for regular members in both
`POST /api/reservations` and surfaced in `GET /api/availability`:

| Lever | Meaning | Type |
|---|---|---|
| `valid_from` / `valid_until` | Date range the group can book within | Activation window |
| `min_days_ahead` | Earliest lead time before a start date (booking opens) | Booking horizon (lower) |
| `max_days_ahead` | Furthest ahead a start date can be booked | Booking horizon (upper) |
| `su_budget` | Cap on a member's total **open Service Units** within the configured window (see `su_anchor_mode`); `NULL` = unlimited | SU budget limit |
| `su_anchor_mode` | Determines how far back the SU budget window reaches: `open` (default), `weekly`, `monthly`, `quarterly`, or `since_creation` — see table below | Budget window |

Additional hard constraints:
- `start_dt` must be at least 15 minutes in the future.
- Start date must be today or future.
- No duplicate-booking unique key (there is no `(group, user, date, policy, slot_index)`
  constraint; any number of reservations for the same user on the same day are
  allowed, subject to capacity and SU budget).
- Availability queries are capped at a 90-day date range.

### SU budget window (`su_anchor_mode`)

`su_anchor_mode` on `UsageGroup` selects how far back the budget window reaches:

| Mode | Window start | Character |
|---|---|---|
| `open` (default) | `end_dt > now` — only currently-open reservations | Renewable ceiling: SUs are freed when a reservation ends or is cancelled (no penalty) |
| `weekly` | Monday 00:00 local of the current week | Resets each week |
| `monthly` | 1st of the current month 00:00 local | Resets each month |
| `quarterly` | Jan 1 / Apr 1 / Jul 1 / Oct 1 00:00 local | Resets each calendar quarter |
| `since_creation` | `UsageGroup.created_at` | Cumulative — never resets |

For windowed modes (`weekly`, `monthly`, `quarterly`, `since_creation`) the
balance counts all `su_cost` accrued since the anchor date regardless of whether
the reservation has ended — effectively a **depleting quota** over the window
period rather than a renewable concurrent ceiling. The `GET /api/groups/su-status`
endpoint returns per-group breakdown fields: `su_used` (active SUs in window),
`su_open` (active future reservations in window), `su_cancelled` (late-cancel
penalties still counting), `used_count`, `open_count`, and `su_remaining`.

The `since_creation` mode is the strictest: once SUs are spent they are never
recovered (except by the cancellation-penalty waiver mechanic above or by the
admin increasing `su_budget`).

Note: late-cancel penalties (`su_cost > 0` on cancelled rows) count against the
budget until `end_dt` passes, regardless of anchor mode.

## 6. Privilege tiers (constraint bypass)

- **Members** — all constraints above apply strictly.
- **Group managers & admins** ("privileged") — bypass `min/max_days_ahead` and
  `su_budget`; get a **±90-day grace window** around `valid_from`/`valid_until`.
  **Group membership is not bypassed**: admins and group managers must still be
  members of a group to book under it or query its availability. They still face
  hardware capacity and per-reservation GPU limits. Additionally:
  - **Management buffer bypass** — see the full `total_gpus` pool (not the
    member-visible reduced ceiling) in availability and booking.
  - **Penalty waiver** — can waive late-cancellation SU penalties at cancel
    time or after the fact via `POST /api/reservations/{id}/waive-penalty`.

## 7. Allocation logic

**Booking is greedy/first-come-first-served by users**, validated under a
serialized critical section (SQLite `BEGIN IMMEDIATE` via `write_intent()`) so
concurrent bookings for the last GPU(s) in a slot cannot both succeed. There is
**no optimization, priority, or fairness algorithm** in the booking path — it's
pure admission control: a request is accepted iff every constraint passes and
capacity remains at every hour in the requested interval.

The on-demand / auto-fill sweep and `app/scheduling.py` were **removed** in the
time-range redesign; there is no background loop that fills idle capacity.

### GPU capacity recovery (reclaim reservations)

An optional background task (`app/gpu_recovery.py`) fills otherwise-idle capacity
with **reclaim reservations** (`Reservation.kind = 'reclaim'`).  These are
admin-only capacity holds with no user or group attribution (`user_id = NULL`,
`group_id = NULL`).  The Kubernetes controller can use them to schedule
opportunistic background workloads into unbooked hours, filling the cluster
rather than leaving idle GPUs unused.

- Enabled by setting `SiteSettings.gpu_recovery_window_hours` to a positive
  integer (number of hours ahead to fill); `0` or `NULL` disables the loop.
- Runs hourly (30-second startup delay); idempotent because existing reclaims
  count as used capacity in the per-hour profile.
- A greedy merge algorithm walks each active GPU class's availability profile and
  emits one single-GPU reclaim spanning each contiguous free run.
- Reclaim reservations are **excluded from default API views** (`status=active`
  or `status=cancelled`) and **excluded from reports** — they only appear when
  the caller explicitly requests `status=all`.
- They still count against cluster capacity for the purposes of user booking
  availability checks (a reclaim hold prevents a regular user from booking the
  same GPU-hour).
- Admins can also create reclaim reservations manually via `POST /api/reservations`
  with `kind='reclaim'`.

## 8. What is NOT modeled (gaps for OR guidance)

- **No priorities, weights, or preemption** between users or groups **at
  booking time** — the reservation app itself is pure FCFS within static
  ceilings. (The separate Kubernetes controller *does* preempt at the pod
  level, independent of this app's booking logic: a pod running past its
  guaranteed window may be deleted to free capacity for an incoming
  reservation, with random — not priority-based — victim selection among
  overstayers. See the controller's README/CLAUDE.md for that mechanism; it
  has no visibility into or effect on booking admission here.)
- **No fairness mechanism** (no proportional sharing, max-min fairness, lottery,
  or aging) — purely FCFS within static per-group ceilings.
- **No dynamic pricing or quota adjustment** — SU rates and discount schedules
  are static admin-set values; group GPU ceilings are static (date-span overrides
  aside).
- **No backfill/optimization of user bookings** — idle capacity is simply
  unavailable until another user claims it.
- **No waitlist / queue** — a full interval returns 409; no demand signal is
  captured.
- **No per-user-per-day GPU cap** (`max_gpus_per_user_per_day` is a documented
  deferred feature in CLAUDE.md).
- Concurrency limits are on **peak instantaneous GPU count** and **SU budget**
  (renewable or windowed depending on `su_anchor_mode`), not on throughput over
  time or fairness of access to scarce peak-hour windows.
- Duration cap is **48 hours for non-privileged members** (server-side 400);
  admins and group managers have no server-side upper bound.

## 9. Configuration levers an operator actually turns

Per **course (group)**: validity dates, booking horizon (`min/max_days_ahead`),
per-member SU budget (`su_budget`), SU budget window mode (`su_anchor_mode`),
per-class GPU ceiling (with date-span boosts), and which GPU classes are visible.

Per **GPU class**: total GPUs, `su_rate_per_hour` (base SU rate),
`min_su_per_gpu_hour` (effective rate floor; `NULL` = no floor), optional
per-reservation GPU cap, `management_buffer` (GPUs hidden from non-privileged
users), date-span capacity overrides (each with their own `available_gpus` and
`management_buffer`).

Per **SU discount schedule**: days-of-week, start/end time-of-day window
(midnight-wrap supported), multiplier (0 = free, 1 = full price), optional date
bounds, active flag, and the list of GPU classes the schedule applies to
(`gpu_class_ids`; a schedule with no attached classes has no effect).

Site-wide: timezone, `gpu_recovery_window_hours` (hours-ahead to fill with
reclaim capacity-hold reservations; `0`/`NULL` disables recovery).

## 10. Open questions for OR review

1. How to set per-group GPU ceilings and open-SU budgets to balance utilization
   vs. fair student access under contention (especially near assignment deadlines).
2. How the booking-horizon window (`min/max_days_ahead`) interacts with
   peak-hour scarcity when multiple courses share a GPU class.
3. Whether an off-peak discount multiplier schedule effectively redistributes
   load, or whether deadline-driven demand is inelastic to pricing signals.
4. Whether the first-come-first-served model with per-group ceilings achieves
   adequate fairness, or whether a max-min fair share or lottery mechanism would
   better serve a multi-course lab environment.
5. Whether `su_anchor_mode = since_creation` or quarterly gives better incentive
   alignment near assignment deadlines compared to the renewable-ceiling (`open`)
   default, and how the 50 % late-cancel penalty rate affects no-show rates in
   practice.
6. Appropriate sizing of `management_buffer` to balance instructor/maintenance
   headroom against the utilization visible to students under the reduced ceiling.
