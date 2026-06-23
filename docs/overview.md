**GPU Reservations for the Instructional Cluster**

Design and Operations Plan

*Draft for technical and academic stakeholders · June 2026*

**Contents**

> 1\. Executive summary
>
> 2\. The problem we are solving
>
> 3\. Goals and design principles
>
> 4\. How the system works
>
> 5\. Keeping reserved capacity from sitting idle
>
> 6\. Fairness and the student experience
>
> 7\. How this compares with peer institutions
>
> Appendix A. Reservation-block lifecycle

1\. Executive summary

The Horae cluster provides usersw with exclusive, interactive
access to a GPU --- most typically via a Jupyter notebook or a VS Code session
connected to the cluster. With courses of 300 or more students sharing
150 GPUs, demand spikes sharply around assignment deadlines. When two
courses recently shared a deadline, first-come, first-served (FCFS) wait
times for an interactive session measure in hours.

This plan introduces a **calendar-based reservation system** so that a
student can hold a known, time-bound block of GPU access instead of
submitting a job and waiting an indeterminate time.   During this time block,
and subject to existing quotas (often 1 GPU), the student's job would jump to the head of the cluster scheduling queue.

The aim is **predictability** for the student without sacrificing overall
utilization: any capacity a reservation does not actually use ---
because the holder never shows up, or later becomes idle for an extended period --- is
returned back to the FCFS pool.

The system has two parts: a **web application** where students book
blocks and staff manage policy, and a **Kubernetes controller** that
grants reserved access and recycles unused capacity. 

2\. The problems we are solving include

-   **Interactive, exclusive workloads.** Almost all usage is through
    interactive sessions, each holding a GPU for the duration of a
    working session.  Traditional batch-oriented scheduling systems
    excel at efficiently organizing jobs, but they can't ensure the
    human operator is present at job launch.

-   **Structural scarcity at deadlines.** 150 GPUs cannot simultaneously
    serve the 300-plus students in a single large course, let alone
    several courses at once. 

-   **Synchronized bursts.** Demand is concentrated in the evenings
    before deadlines, and worsens sharply when multiple courses share a
    deadline.

-   **No predictability under FCFS.** Today a student submits and waits
    an unknown length of time. They cannot plan their work around a
    guaranteed access window.

3\. Goals and design principles

1.  **Predictability.** A reservation guarantees a student a specific,
    time-bound block of access.

2.  **High utilization.** Reserved capacity that is not actively used is
    returned to on-demand users quickly, so reservations never leave
    GPUs idle while others wait.

3.  **Fairness.** Per-user and per-group limits prevent any one student
    or course from monopolizing the cluster.

4\. How the system works

4.1 Two components

At a high level, the reservation app decides who is entitled to what and
when; the controller enforces those entitlements inside Kubernetes.

  ------------------- ---------------------------------------------------
  **Component**       **Role**

  Reservation web app Students book an arbitrary whole-hour time range
                      by course/group and GPU class, each carrying a
                      Service-Unit (SU) cost; staff define groups, SU
                      budgets, GPU ceilings and capacity. Serves a
                      read-only API to the controller.

  Kubernetes          Reads active reservations and grants reserved Pods
  controller          access to reserved nodes; caps runtime to the
                      reserved window; converts unused reserved capacity
                      to on-demand and recycles it.

  ------------------- ---------------------------------------------------

4.2 Booking model

A reservation is an arbitrary time range at whole-hour granularity --- a
student picks a start and end hour rather than a fixed slot from a fixed
plan. A range may cross midnight (for example 22:00 to 06:00). Ordinary
members are capped at a 48-hour maximum length; group managers (instructors, TAs) and admins
are exempt and may book longer sessions on behalf of their students.

Every booking is priced in **Service Units (SU)** --- a single
currency that meters consumption per GPU-hour and underpins the per-member
budgets described in Section 6.

4.3 Reserved and on-demand capacity

A configurable share of GPU nodes is marked ("tainted") so that ordinary
Pods cannot schedule on them. These form the reserved pool; the
remaining nodes serve on-demand FCFS users. When a student has a valid,
active reservation, the controller grants that student's Pod permission
(a "toleration") to run on a reserved node, within the GPU budget of the
reservation. On-demand Pods never receive this permission on their own
--- only the controller grants it, and only under the conditions in
Section 5. This keeps the controller in full control of who reaches
reserved hardware.

4.4 Runtime caps and in-session warnings

When the controller admits a Pod to a reserved block it also caps the
session's maximum runtime to the end of the reserved window (extended
automatically across directly back-to-back blocks held by the same
student for the same GPU class and GPU count). The student is warned of their
current-session maximum runtime inside both Jupyter and VS Code, so the
end of a window is never a surprise.


5\. Keeping reserved capacity from sitting idle

The central risk of any reservation system is that reserved-but-unused
capacity sits idle while other students wait. The controller serves
on-demand users from capacity set aside for them up front and from
reserved capacity a holder does not use --- the latter converted to
on-demand one-way, as described below.

5.1 Sources of on-demand capacity

On-demand users are served from three sources --- one designated as
on-demand from the outset, and two reclaimed from reservations that go
unused:

-   **Reclaim capacity holds.** The reservation app periodically marks
    unused short-term capacity (<~6hrs) as available for on-demand use.

-   **No-shows.** If a reservation window opens and the holder has not
    launched a matching Pod within 15 minutes, the block is converted to
    on-demand duty. The conversion is irrevocable for the remainder of
    the window --- the original holder has forfeited that block, which
    removes any ambiguity about late arrivals reclaiming a slot already
    in use. 

-   **Idle sessions.**  If a holder's session is running but making no use
    of its GPU for an extended period, the existing idle-culling process terminates it
    with a notification to the the controller. The controller then re-arms a short claim
    deadline (15 minutes by default); if the holder does not relaunch within it, the remaining block is
    converted to on-demand as above.

5.2 How on-demand Pods are placed (the guards)

The controller attempts to place only on-demand Pods that are Pending
specifically for lack of GPU resources. Before placing one onto an
on-demand block, all of the following must hold:

1.  **GPU-only Pending.** The Pod is Pending solely because no GPU is
    available --- not for any other reason (memory, other constraints).

2.  **Minimum runtime fits.** On-demand jobs are annotated with a
    minimum acceptable runtime; the controller will not place a job into
    a window shorter than that, so no one is handed a stub of a slot
    that is killed moments later.

3.  **Safety interlock.** No reservation-holder Pod **for the same GPU
    class** is Pending for lack of node resources; on-demand placement
    for that class is held until the stuck Pod is resolved, while other
    GPU classes continue unaffected. A stuck reservation Pod should be
    rare and is treated as an anomaly warranting human investigation;
    Splunk alerting is configured to surface it.

5.3 Recycling on-demand slots

When an on-demand Pod on such a block finishes or is idle-culled, the
block returns to the on-demand pool for the next eligible Pending Pod,
and so on until the window ends. 

6\. Fairness and the student experience

Booking limits in the reservation app prevent monopolization and let
course and computing staff shape access for deadline weeks. The primary lever is a
per-member **Service Unit (SU) budget** set on each group: every booking
costs SU (priced per GPU-hour by the GPU class, discounted during
off-peak windows), and a member cannot exceed that set budget. 

By default the budget window refreshes weekly on Mondays: the SU budget must cover
all of the student's SU consumption for that week as well as any future reservations in place.

Alongside the SU budget, course-wide limits on GPU usage ensure that resources are fairly
divided between courses. A limited **management buffer** holds back a slice of GPU capacity that is invisible to students, giving staff
headroom for maintenance or last-minute student accommodations even when the cluster otherwise looks full.  Instructors and TAs may book within this buffer, students may not.

To discourage speculative booking (reserving "just in case" and not
showing up, which manufactures the very scarcity we are trying to
relieve), the app charges a **late-cancellation SU penalty**: cancelling
within 24 hours of the window keeps part of the booking's SU cost against
the member's budget, while cancelling earlier is fully waived. 

Group managers (course staff) can manage their groups' reservations
without full administrative access: they book on behalf of students (the
reservation is owned by the student, with the manager recorded as the
submitter), and they are exempt from the booking-window, SU-budget, and
management-buffer limits, get a ±90-day grace window around their group's
active dates, and can waive late-cancellation penalties.

7\. How this compares with peer institutions

Most instructional clusters do not hand-build reservation logic; they
lean on a scheduler that already provides queueing, quotas and fair
access. Slurm-based teaching clusters (for example at Pitt and Arizona
State) give each course its own scheduler account with a class-specific
quality-of-service and fair-share scoring, so students who have used
less recently gain priority --- a better queue, but still an
indeterminate wait. Some sites (such as Hannover's LUIS cluster) arrange
coarse advance reservations for a course manually with operations staff.
Kubernetes-native options such as Kueue add quota borrowing and
reclamation across teams to absorb bursts. ETH Zurich's student cluster
is a useful reference for the interactive case: per-course time budgets,
auto-terminating sessions, and an in-UI countdown.

What none of these provide cleanly is a student-facing, calendar-style
guarantee of a specific time --- which is precisely the predictability
goal here. That justifies the custom layer, while we deliberately reuse
proven ideas from the field e.g. idle-culling and runtime caps for
utilization.

Appendix A --- Reservation-block lifecycle

The state machine below tracks a single reservation block as the
controller moves it through its life. Reserved-phase states are shown in
blue, on-demand-phase states in green; three red transitions mark the
irrevocable conversions of unused or reclaimed reserved capacity to
on-demand capacity: no-show, idle-cull, and mid-window cancellation.
Reclaim capacity holds (Section 5.1) --- the renamed scheduled
on-demand blocks --- have no reserved phase and enter the diagram directly
at the On-Demand --- Available state. Note: the diagram image predates
the mid-window cancellation path; see walkthrough step 4b below.

![States from Scheduled through Awaiting Claim, Reserved In Use,
On-Demand Available/In Use, to Window Ended, with no-show, idle-cull,
and cancellation conversions.](./media/dbedd404ff0049ba26540ae53449dab333012f9f.png "Reservation-block lifecycle state machine"){width="6.25in"
height="6.083333333333333in"}

Walkthrough

1.  **Scheduled → Awaiting Claim.** The controller watches for pods
    continuously from startup. When a reservation is fetched it
    pre-registers a no-show deadline of `slot_start + 15 min`; no
    separate "start watching" action occurs at window-open time. On
    controller startup, any reservation whose window is already open
    receives a grace deadline of `now + NOSHOWN_GRACE_MINUTES` (default
    30 min) instead, to avoid falsely declaring a live in-flight session
    as a no-show across a restart.

2.  **Awaiting Claim → Reserved In Use.** The holder's Pod is matched;
    the controller adds the toleration and caps runtime to the end of the
    reserved window (extended automatically across directly back-to-back
    blocks held by the same student for the same GPU class and count).

3.  **Awaiting Claim → On-Demand Available.** Fifteen minutes pass with
    no claiming Pod: the block is irrevocably released to the FCFS pool.

4a. **Reserved In Use → On-Demand Available (idle-cull).** The session
    is found idle and culled; once the holder's Pod is deleted the
    controller re-arms the no-show deadline (`now +
    NOSHOWN_GRACE_MINUTES`, default 30 min). If the holder does not
    relaunch within that window the remaining block is irrevocably
    released to the FCFS pool.

4b. **Reserved In Use → On-Demand Available (cancellation).** If the
    reservation is cancelled mid-window, the controller evicts the
    holder's Pod (emitting a `ReservationCancelled` event and deleting
    it), releases the GPU capacity, and immediately opens the remaining
    window for on-demand placement. Like the no-show and idle-cull paths,
    this conversion is irrevocable for the remainder of the window.

5.  **On-Demand Available → In Use.** An eligible Pending Pod is placed,
    subject to the three guards in Section 5.2. The controller caps the
    pod's runtime to the end of the on-demand block's window.

6.  **On-Demand In Use → Available.** That Pod finishes or is culled,
    and the block is recycled to the next waiter. When an on-demand block
    is open, the controller also checks for abutting future reclaim blocks
    that have entered the reservation app's non-preemptible guard window;
    if found, the open block's end time is extended to cover them and
    those blocks are stubbed so they cannot be independently placed,
    giving a longer effective runtime to any job admitted near the end of
    the original block.

7.  **→ Window Ended.** When the window closes, the capacity that was on
    loan --- the GPU or MIG instance, not the whole node --- returns to
    serving reservations.
