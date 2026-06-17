# Group Manager Guide — DSMLP/Research Cluster GPU Reservation System

*Audience: course instructors and research-group leads who have been assigned the
**group manager** role for one or more usage groups.*

---

## Contents

1. [Overview](#overview)
2. [Logging In](#logging-in)
3. [Your Dashboard](#your-dashboard)
4. [Navigation](#navigation)
5. [Making a Reservation for Yourself](#making-a-reservation-for-yourself)
6. [Group Reservations Page](#group-reservations-page)
7. [Making a Reservation on Behalf of a Student](#making-a-reservation-on-behalf-of-a-student)
8. [My Reservations](#my-reservations)
9. [Reports](#reports)
10. [Privileged Booking Rules](#privileged-booking-rules)
11. [System Architecture](#system-architecture)
12. [Kubernetes Enforcement](#kubernetes-enforcement)

---

## Overview

The GPU Reservation System lets instructors and researchers pre-claim GPUs on the
shared cluster for defined time windows.  It is organized around **usage groups**
(one per course section, project, or cohort) that control *who* may book, *which*
GPU tiers are available, and *when* bookings are permitted.

A **group manager** has elevated privileges relative to ordinary members:

| Capability | Member | Group Manager |
|---|---|---|
| Reserve GPUs for themselves | ✓ | ✓ |
| View own reservations | ✓ | ✓ |
| View all reservations in their groups | — | ✓ |
| Cancel any reservation in their groups | — | ✓ |
| Book on behalf of another group member | — | ✓ |
| Bypass `min_days_ahead` / `max_days_ahead` booking-window restrictions | — | ✓ |
| Bypass per-member Service Unit budget (`su_budget`) | — | ✓ |
| Pre/post-semester booking grace period (±90 days) | — | ✓ |
| See and book the management-buffer headroom (full `total_gpus`) | — | ✓ |
| Waive late-cancellation SU penalties | — | ✓ |
| Create or configure groups, GPU classes, discount schedules | — | admin only |

> **Note:** group membership itself is **not** bypassed — an admin or group
> manager must still be a member of a group to book under it or view its
> availability.

The test scenario used in this guide is **TEST123_S126_A00** — a hypothetical
TEST 123 course, Spring 2026, Section A00 — managed by instructor account
**tbluefin** (Taylor Bluefin).  The course has 50 enrolled students and books
Medium-tier (20–24 GB) GPUs as whole-hour time ranges, with reservations ramping
up toward a **June 24, 2026** assignment deadline.

---

## Logging In

Navigate to the application root (`https://<cluster-hostname>/`).  If you are not
already authenticated you will be redirected to the login page.

```
https://<cluster-hostname>/login.html
```

<!-- Screenshot placeholder: 01_login.png
     A centered card on a light-grey background.  Fields: "Username" and
     "Password" (required, marked with a red asterisk).  Blue "Sign in" button. -->

> **Screenshot: Login page**
> *(A centered card shows the site title "DSMLP/Research Cluster — GPU Reservation",
> a subtitle "Sign in to manage your GPU reservations", and Username / Password
> fields above a blue Sign in button.)*

Enter your institutional username (e.g. `tbluefin`) and password, then click
**Sign in**.  The system issues an 8-hour JWT and stores it in `localStorage`;
you will remain logged in across page navigations until it expires or you click
**Log out** in the sidebar.

---

## Your Dashboard

After login you land on **Dashboard** (`/dashboard.html`).

<!-- Screenshot placeholder: 02_dashboard_manager.png
     Shows the tbluefin dashboard.  Sidebar (left) lists: Dashboard, New Reservation,
     My Reservations, Group Manager, Reports.  Username "tbluefin / User" appears at
     the bottom.  Main area has a "Your Upcoming Reservations" panel, an "SU Budget"
     panel (per group, used/open vs. budget), and a GPU Classes table showing
     Medium (20–24gb) with an 80-GPU pool and a Reserve button. -->

> **Screenshot: Dashboard — tbluefin view**
> *(The "Your Upcoming Reservations" panel is empty on first login.  An "SU Budget"
> panel summarises Service Unit usage for each group that has a budget.  The GPU
> Classes table lists every tier accessible through the manager's groups.)*

Key elements:

- **Your Upcoming Reservations** — your own next reservations, each showing
  class name, GPU count, date, time window, and SU cost.  A **Manage**
  button links to the My Reservations page.
- **SU Budget** — for each group that sets a `su_budget`, a bar showing the SUs
  you have committed (used + open) against the budget, plus reservation counts.
  This panel is hidden when none of your groups cap Service Units.  As a manager
  your own bookings are exempt from the budget, but the panel still reflects the
  configured limits.
- **GPU Classes table** — one row per accessible tier, showing its GPU pool.
  Clicking **Reserve** pre-selects that class on the New Reservation wizard.

---

## Navigation

The left sidebar lists your available pages.  For a group manager the links are:

| Link | Page | Purpose |
|---|---|---|
| **Dashboard** | `/dashboard.html` | Personal overview and upcoming reservations |
| **New Reservation** | `/reserve.html` | Four-step wizard to book a time range |
| **My Reservations** | `/my-reservations.html` | Your own past and upcoming reservations |
| **Group Manager** | `/group-manager.html` | All reservations in your groups; book/cancel on behalf of members |
| **Reports** | `/admin/reports.html` | GPU-hours heat-map across all groups and GPU classes |

Admin-only links (Groups, Users, GPU Classes, Group GPU Limits, SU Discount
Schedules, Settings, etc.) are **not** visible to group managers.

Your username and role (`User`) appear at the bottom of the sidebar.  Click
**Log out** to end your session.

---

## Making a Reservation for Yourself

From the sidebar choose **New Reservation** (`/reserve.html`).  The page guides
you through four sequential steps.

### Step 1 — Select Group

<!-- Screenshot placeholder: 04_new_reservation.png
     Step indicator at top: "1 Group → 2 GPU Class → 3 Date & Slot → 4 Confirm".
     Step 1 card shows a grid of group tiles.  TEST123_S126_A00 is one tile with
     its name, validity dates, and a radio-style selection ring. -->

> **Screenshot: New Reservation — Step 1 (Group)**
> *(The step indicator highlights Step 1.  Below it, a card titled "Select Group"
> shows tiles for every group the user belongs to.  Each tile shows the group name
> and its active date range.  For managers the tile also shows a small "Manager"
> badge.)*

Select the group your reservation should be charged against (e.g. **TEST123_S126_A00**).
Click **Next**.

### Step 2 — Select GPU Class

<!-- Screenshot placeholder: 04b_reservation_group_selected.png
     Step 2 card titled "Select GPU Class" with a context badge showing the
     selected group.  A tile for "Medium (20-24gb)" shows total_gpus=80 and a
     description listing example hardware. -->

> **Screenshot: New Reservation — Step 2 (GPU Class)**
> *(A "Select GPU Class" card shows tiles for each GPU tier attached to the
> chosen group.  Each tile shows the tier's available GPUs and its base SU rate
> per GPU·hour.  For TEST123 only the Medium tier is available.)*

Select the GPU class.  Click **Next**.

### Step 3 — Select Date & Time

There are no fixed slots. You pick a **date** and then drag a **range slider**
over an hourly timeline to choose an arbitrary start and end time (whole-hour
granularity).

<!-- Screenshot placeholder: 04c_reservation_date_selected.png and
     04d_reservation_range_selected.png
     A date picker, then an hourly timeline bar for that day.  Each hour cell is
     shaded by utilization and by its SU discount multiplier (off-peak hours are
     muted/cheaper, past hours greyed out).  Two draggable handles set the start
     and end of the reservation.  A live readout shows the selected window, the
     duration, the per-hour SU rate range, and the total SU cost. -->

> **Screenshot: New Reservation — Step 3 (Date & Time)**
> *(After picking a date, an hourly timeline for that day appears.  Hour cells
> are shaded by how booked they are and by the active SU discount multiplier;
> past hours are greyed out.  Drag the two handles to set the start and end of
> the window — it may extend into the next day for an overnight range.  A live
> summary shows the chosen window, duration, and total SU cost.)*

Pick the date, drag the handles to the window you need, then click **Next**.
The default selection is a 1-hour span starting at the next bookable hour.
Non-privileged members are limited to a 48-hour maximum range.

> **Group manager note:** As a manager, the `min_days_ahead` / `max_days_ahead`
> booking-window restrictions are lifted — you can book same-day or as far out
> as needed, and you are exempt from the 48-hour duration cap.  Dates within the
> ±90-day grace window around the group's active span are also unlocked for you
> (see [Privileged Booking Rules](#privileged-booking-rules)).

### Step 4 — Confirm

<!-- Screenshot placeholder: (no separate screenshot taken)
     Step 4 card "Confirm Reservation" shows a definition list with Group, GPU
     Class, Date, Time window, Duration, and total SU cost.  A numeric GPU count
     input (default 1; max determined by the GPU class) and an optional Notes
     textarea are below the summary. -->

> **Screenshot: New Reservation — Step 4 (Confirm)**
> *(A summary definition list shows every detail of the pending booking,
> including the total SU cost.  The GPU Count field lets you claim up to the
> per-reservation maximum set on the GPU class (1 for Medium-tier TEST123).  An
> optional Notes field is available for project names, PI, etc.  Notes are
> visible to group staff.)*

Review the details — including the **SU cost**, which is charged against the
member's group Service Unit budget.  Adjust the **GPU Count** if the class allows
more than one (the cost scales with GPU count).  Add an optional note and click
**Confirm Reservation**.  A green success screen confirms the booking.

---

## Group Reservations Page

The **Group Manager** page (`/group-manager.html`) gives a bird's-eye view of
all reservations in every group you manage.

<!-- Screenshot placeholder: 03_group_manager.png
     Top of page shows a blue info banner "Group manager booking privileges"
     listing the four privilege bullet points.  Below is a "Filters" card with
     fields for Username, Group (dropdown), From Date, To Date, Status (Active /
     All / Cancelled), and Search / Reset buttons. -->

> **Screenshot: Group Reservations — top of page**
> *(A blue info banner at the top summarises the manager's booking privileges.
> Below it, a Filters card lets you narrow results by username, group, date
> range, and status.)*

### Privilege banner

The info banner at the top of the page reminds you of your elevated booking
rights:

- You can use **New Reservation** to book for any group member, including yourself.
- `min_days_ahead` / `max_days_ahead` restrictions are waived.
- You may book up to **90 days before or after** a group's scheduled active
  span (useful for pre-semester setup or late-grade wrap-up).
- Hardware capacity limits and per-reservation / per-group GPU ceilings still apply.

### Reservation table

<!-- Screenshot placeholder: 03b_group_manager_table.png
     Results table with columns: User, Group, GPU Class, Date, Time, GPUs,
     SU Cost, Status, and a Cancel button for future active bookings.  Rows are
     colour-coded by status (active = default, cancelled = muted). -->

> **Screenshot: Group Reservations — reservation table**
> *(Each row shows the owning user's username, the group, GPU class, date, time
> window, GPU count, SU cost, and status badge.  Future active reservations
> have a red Cancel button allowing the manager to cancel them — the cancel
> dialog shows any late-cancellation SU penalty and offers a manager waiver.)*

Use the **Group** dropdown to limit results to a single course section.  The
**Username** text field accepts a partial username for quick student lookups.
Filter by date range to see the run-up to a deadline.

### Assessing student reservations against cluster utilisation

To understand how your course's bookings relate to overall cluster load, switch
to the **Reports** page (described below).  On the Group Reservations page you
can spot individual students who have not yet made reservations — filter by the
group name and check whether every enrolled student appears in the table.

---

## Making a Reservation on Behalf of a Student

Group managers can book any slot for any member of their groups without logging
in as that student.

**Method 1 — from the Group Manager page (modal form)**

1. On `/group-manager.html`, click **New Reservation** (top-right button or
   sidebar link).  A modal dialog opens, distinct from the full wizard.
2. In the **User** field, type the student's username (autocomplete is offered
   from group membership).  Select the correct entry from the dropdown.
3. Choose the **Group**, **GPU Class**, **Date**, and a **start/end time** for the
   booking window.
4. Set **GPU Count** (1 for Medium-tier in TEST123) and optional **Notes**.
5. Click **Create**.

<!-- Screenshot placeholder: 07_privileged_reserve.png
     Modal dialog "New Reservation" overlaying the group-manager page.  The "User"
     field is a typeahead showing a student suggestion.  Other fields: Group
     (TEST123_S126_A00), GPU Class (Medium 20-24gb), Date (date picker), Start /
     End time, GPU Count (1), Notes (textarea).  A blue "Create" button at
     the bottom right. -->

> **Screenshot: New Reservation modal (on behalf of a student)**
> *(The "User" typeahead is pre-populated with a student username suggestion.
> The remaining fields cascade from the group and GPU class selections and let
> the manager set the time window directly.  The manager submits on behalf of
> the student; the resulting reservation record shows the student as owner, with
> the manager recorded as the submitter.)*

**Method 2 — from the New Reservation wizard**

Navigate to `/reserve.html`.  Because you are a manager, Step 1 shows your groups
and the member list attached to each group is available server-side.  After
selecting the group you can append `?on_behalf_of=<user_id>` to the URL, or use
the group manager modal method above for a guided experience.

> **Important:** The reservation is **owned by the student** (their `user_id` is
> stored on the record).  It appears in the student's "My Reservations" page and
> its SU cost counts against the student's Service Unit budget for the group.
> The manager is recorded as the submitter (`submitted_by_id`), so on-behalf-of
> bookings are distinguishable from the student's own.

---

## My Reservations

The **My Reservations** page (`/my-reservations.html`) shows reservations
*owned by you personally* (i.e. `tbluefin`), not your students' bookings.

<!-- Screenshot placeholder: 05_my_reservations.png
     Page titled "My Reservations" with a "+ New" button in the top-right corner.
     A Filters card allows filtering by Status (Upcoming/Active, All, Cancelled),
     Group, and GPU Class.  Below, reservation cards are listed in chronological
     order.  Each card shows GPU class name, GPU count, date, time window, SU
     cost, group name, and an optional notes line.  Upcoming reservations
     have a red Cancel button. -->

> **Screenshot: My Reservations**
> *(Each reservation appears as a card with an icon (server for active, ban for
> cancelled).  Key details — class, date, time, group, notes — are shown on each
> card.  Future active reservations display an "Upcoming" badge and a Cancel
> button.  Past reservations show a "Past" badge; cancelled ones show a red
> "Cancelled" badge.)*

To cancel one of your own upcoming reservations, click the red **Cancel** button
on its card.  A confirmation modal will ask you to confirm before the cancellation
is applied.

---

## Reports

The **Reports** page (`/admin/reports.html`) is accessible to both administrators
and group managers.  It provides a heat-map view of GPU utilisation across all
groups and GPU tiers for a chosen date range.

<!-- Screenshot placeholder: 06_reports.png
     Page titled "Reports".  A date-range bar at top (From / To inputs, "Load
     Report" button).  Three stacked cards follow:
       1. "Reservations by Group" — table with rows per group, columns per date,
          cells show reservation count.  Heat-map coloring: higher counts are
          darker blue; zero entries are shown as "—".
       2. "Max Simultaneous Reservations by GPU Class" — same layout, peak
          concurrency per class per day.
       3. "Reserved Hours by GPU Class" — GPU-hours booked per class per day.
     Dates in TEST123 columns peak around June 21-26 as the assignment deadline
     approaches. -->

> **Screenshot: Reports page**
> *(Three heat-map tables are shown.  The "Reservations by Group" table has one
> row per group (CSE 151B, TEST123_S126_A00, XL Pilot…) and one column per date
> in the selected range.  Cells are coloured on a blue scale — deeper blue
> indicates heavier bookings.  Zero counts are represented by "—".  The peak
> for TEST123 falls on June 21–23, reflecting the deadline-clustered reservation
> distribution generated for this course.)*

### Reading the report

**Reservations by Group** — counts active reservations per group per day.  Use
this to gauge whether TEST123 students are booking early enough and whether
activity is concentrated in a dangerous last-minute spike.

**Max Simultaneous Reservations by GPU Class** — the peak number of concurrent
jobs running at any single instant on each GPU tier.  Compare this against the
total GPU count for the class (e.g. Medium has 80 GPUs) to understand headroom.
If the peak for a single day approaches the pool size, students who haven't
booked yet may be locked out.

**Reserved Hours by GPU Class** — total GPU-hours reserved per class per day.
Useful for budgeting and capacity planning.

### Adjusting the date range

The default window is today through today + 13 days.  Click the **From** and
**To** date fields to extend the range to, for example, the full semester.  Then
click **Load Report** to refresh all three tables.

---

## Privileged Booking Rules

Group managers (and administrators) are subject to a set of relaxed booking
constraints.  These rules are enforced symmetrically on both the reservation
creation endpoint and the availability endpoint, so the slot picker only shows
dates the caller is actually allowed to book.

| Rule | Regular member | Group manager |
|---|---|---|
| Group membership (must be a member to book under the group) | enforced | **enforced** |
| `min_days_ahead` (e.g. must book ≥ 2 days in advance) | enforced | **waived** |
| `max_days_ahead` (e.g. cannot book more than 60 days out) | enforced | **waived** |
| `valid_from` / `valid_until` group active dates | strict boundary | ±90-day grace window |
| Service Unit budget (`su_budget`, per member: Σ stored `su_cost` over the budget window) | enforced | **waived** |
| 48-hour maximum reservation length | enforced | **waived** |
| Management buffer (members capped to `total_gpus − management_buffer`) | enforced | **waived** (see full pool) |
| Late-cancellation SU penalty | applies | **can be waived** |
| Per-reservation GPU ceiling (`max_gpus_per_reservation`) | enforced | enforced |
| Hardware capacity (pool size) | enforced | enforced |
| Group GPU ceiling (`UsageGroupGpuLimit`) | enforced | enforced |

The ±90-day grace window means a manager can begin testing resources 3 months
before the official semester start date, and can continue making late bookings up
to 3 months after the semester end date — without requiring the system
administrator to extend the group's `valid_until` date.

> **Example:** TEST123_S126_A00 is active June 2 – July 15.  As `tbluefin` you
> can book slots as early as March 3 or as late as October 13, while an enrolled
> student would be blocked outside the June 2–July 15 window.

---

## System Architecture

This section gives a high-level overview of the application for instructors who
want to understand how their reservations are stored and served.

### Components

```
Browser (HTML/JS SPA)
        │  REST API calls  (JSON over HTTPS)
        ▼
┌──────────────────────────────────────────────────────────────────┐
│  FastAPI application  (Python 3.13, uvicorn)                     │
│                                                                  │
│  ┌──────────────┐  ┌──────────────────┐  ┌──────────────────┐  │
│  │  Auth layer  │  │  REST routers    │  │  Static files    │  │
│  │  (Argon2id,  │  │  /api/auth       │  │  served at /     │  │
│  │   JWT HS256) │  │  /api/groups     │  │  (HTML, CSS, JS) │  │
│  └──────────────┘  │  /api/reservations│  └──────────────────┘  │
│                    │  /api/availability│                         │
│  ┌──────────────┐  │  /api/reports    │                         │
│  │  Email svc   │  │  /api/users      │                         │
│  │  (reminder   │  │  …               │                         │
│  │   loop,      │  └──────────────────┘                         │
│  │   asyncio)   │                                               │
│  └──────────────┘                                               │
│                                                                  │
│  SQLAlchemy 2 ORM                                               │
│        │                                                         │
│        ▼                                                         │
│  SQLite database  (/data/gpu_reservations.db)                    │
└──────────────────────────────────────────────────────────────────┘
```

### Frontend

The UI is a single-page application built with **plain HTML, CSS, and ES modules**
— no framework or build step.  Every page imports shared utilities from
`/js/api.js` (auth helpers, `apiFetch`, navigation, formatting).  Authentication
state is kept in `localStorage` as a signed JWT; on every page load `requireAuth()`
checks for its presence and redirects to `/login.html` if absent.

The sidebar is populated dynamically by `initSidebar()`: admin links are injected
for administrators, and the **Group Manager** and **Reports** links appear
automatically for any user who manages at least one active group.

### Backend

The FastAPI application exposes all functionality under `/api/…`.  Key design
decisions:

- **No Alembic** — schema migrations are handled by an `_apply_migrations()` function
  in `app/main.py` that issues `ALTER TABLE … ADD COLUMN` statements at startup.
- **SQLite with WAL mode** — sufficient for the single-node, moderate-concurrency
  workload of a university cluster (<2 000 users).  The database file is mounted
  from a persistent volume at `/data/gpu_reservations.db` in the Docker deployment.
- **Argon2id password hashing** — OWASP-recommended parameters (m=19456 KiB,
  t=2 passes, p=1 thread).
- **JWT auth, 8-hour lifetime** — tokens are signed with HS256 using a
  `SECRET_KEY` environment variable.  The server re-reads the user row from the
  database on every privileged request; claims in the token are never trusted for
  authorisation decisions.

### Data model (summary)

| Table | Purpose |
|---|---|
| `users` | All accounts; `is_admin` flag; `auth_provider` (local or an OAuth provider) |
| `usage_groups` | Course sections or research groups; booking-window settings plus `su_budget` and `su_anchor_mode` |
| `usage_group_members` | Many-to-many join with `role` = `member` or `manager` |
| `usage_group_gpu_classes` | Links a group to the GPU classes its members may book |
| `usage_group_gpu_limits` | Optional per-group GPU ceiling per hardware tier and date range |
| `gpu_classes` | Hardware tiers: total pool size, `management_buffer`, base `su_rate_per_hour`, `min_su_per_gpu_hour`, per-reservation GPU cap, Kubernetes label |
| `gpu_class_day_overrides` | Date-span capacity / buffer overrides per GPU class |
| `su_discount_schedules` | System-wide time-of-day SU discount windows, attached to GPU classes |
| `reservations` | Core booking record: user, group, GPU class, `start_dt`/`end_dt` time range, GPU count, stored `su_cost`, `kind` (`booking` or `reclaim`), status |
| `site_settings` | Singleton: site title, announcement HTML, `gpu_recovery_window_hours` |
| `email_settings` | Singleton: SMTP config, Jinja2 templates, reminder offsets |

### Deployment

The application ships as a Docker image built from a `python:3.13-slim` base.
A GitHub Actions workflow publishes the image to GHCR on every push.  In
production a single container serves both the REST API and the static SPA.
The SQLite database is bind-mounted from persistent storage.

```
docker run -d \
  -p 8000:8000 \
  -v /srv/gpu-res-data:/data \
  -e SECRET_KEY=<strong-random-key> \
  ghcr.io/<org>/gpu-reservation-app:latest
```

For JupyterHub OAuth2 login add the five `JUPYTERHUB_*` environment variables
described in the project README.

---

## Kubernetes Enforcement

Reservations are enforced on the cluster by a companion service, the
**GPU Reservation Controller** (separate `gpu-reservation-controller`
repository), deployed by the cluster operators.  In day-to-day use you should
not need to interact with it, but knowing how it behaves helps you explain
session behaviour to students:

1. **Reserved nodes are fenced off.**  A share of each GPU tier's nodes
   carries a `gpu-class-reservation` taint, so ordinary (unreserved) sessions
   cannot land there.

2. **Holding a reservation unlocks them.**  When a student with an active
   reservation launches a session (their pod runs in their per-student
   namespace and is labelled with the GPU class), the controller grants that
   pod permission (a *toleration*) to schedule onto the reserved nodes — up to
   the reservation's GPU count.

3. **Sessions are capped to the reserved window.**  The controller limits the
   session's maximum runtime to the end of the reservation window using the
   reservation's `start_utc`/`end_utc` (via Kubernetes `activeDeadlineSeconds`).
   A Kubernetes event records the applied limit, and the remaining time is
   surfaced inside Jupyter / VS Code.

4. **Idle capacity can be back-filled with reclaim holds.**  The app has an
   optional **GPU capacity recovery** loop (enabled by setting the site-wide
   `gpu_recovery_window_hours`) that fills otherwise-idle GPU-hours with
   **reclaim reservations** (`kind = 'reclaim'`) — admin-only capacity holds with
   no user or group.  The controller can use these to schedule opportunistic
   background workloads into unbooked hours so GPUs are not left idle.  Reclaim
   holds are hidden from the normal reservation views and excluded from reports.

> The earlier **on-demand / no-show reclaim** and **ad-hoc block** mechanics were
> **removed** in the time-range redesign.  Reservations are no longer treated as
> no-shows, and capacity is not loaned out to walk-up users mid-window; the
> reclaim-hold loop above is the only background capacity filler.

Students still launch sessions through the normal JupyterHub path; the
controller works in the background.  The instructor-facing takeaway: a student
**with** a reservation is guaranteed capacity for their window, while idle
capacity outside reservations may be back-filled with low-priority reclaim
workloads.

---

*Last updated: 2026-06-17.  For issues or questions contact the cluster
administrators at [help@dsmlp.ucsd.edu](mailto:help@dsmlp.ucsd.edu).*
