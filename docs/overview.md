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
> 7\. Operations and monitoring
>
> 8\. Risks and open items
>
> 9\. How this compares with peer institutions
>
> Appendix A. Reservation-block lifecycle

1\. Executive summary

The instructional cluster gives each student exclusive, interactive
access to a GPU --- typically a Jupyter notebook or a VS Code session
connected to the cluster. With courses of 300 or more students sharing
150 GPUs, demand spikes sharply around assignment deadlines. When two
courses recently shared a deadline, first-come, first-served (FCFS) wait
times for an interactive session exceeded five hours.

This plan introduces a **calendar-based reservation system** so that a
student can hold a known, time-bound block of GPU access instead of
submitting a job and waiting an indeterminate time. The aim is
**predictability** for the student without sacrificing overall
utilization: any capacity a reservation does not actually use ---
because the holder never shows up, or leaves the GPU idle --- is
returned to the FCFS pool within minutes.

The system has two parts: a **web application** where students book
blocks and staff manage policy, and a **Kubernetes controller** that
grants reserved access and recycles unused capacity. Physical GPUs are
partitioned with NVIDIA Multi-Instance GPU (MIG) so that one student's
memory use or a crashed CUDA kernel cannot disrupt a neighbour.

2\. The problem we are solving

-   **Interactive, exclusive workloads.** Almost all usage is
    interactive sessions, each holding a GPU for the duration of a
    working session rather than a short batch job.

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

4.  **Isolation.** Students are protected from each other's crashes and
    memory contention.

5.  **Transparency.** On-demand users can see their position in the
    queue rather than waiting blind.

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

  MIG partitioning    Splits physical GPUs into isolated instances so a
                      student\'s memory use or a crashed CUDA kernel
                      cannot affect a neighbour.
  ------------------- ---------------------------------------------------

4.2 Booking model

A reservation is an arbitrary time range at whole-hour granularity --- a
student picks a start and end hour rather than a fixed slot from a fixed
plan. A range may cross midnight (for example 22:00 to 06:00). Ordinary
members are capped at a 48-hour maximum length; group managers and admins
are exempt. Every booking is priced in **Service Units (SU)** --- a single
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

4.5 GPU isolation with MIG

Physical GPUs are divided into isolated MIG instances wherever the
hardware allows. This isolation is a requirement rather than a
convenience for this workload: many frameworks do not share GPU memory
reliably, and students frequently test CUDA code that can crash the
device. MIG ensures one student's failure cannot take down another's
session. The trade-off is that isolation, unlike time-slicing, fixes the
number of shareable instances per GPU; capacity planning (Section 8)
accounts for this.

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

-   **Reclaim capacity holds.** The reservation app can set capacity
    aside as on-demand from the start: an admin schedules a hold
    manually, or the app's GPU-recovery loop fills otherwise-unbooked
    hours as they near. These appear in the API as `kind="reclaim"`
    reservations --- admin-only holds with no user or group --- and carry
    no reservation holder, so the controller serves on-demand users from
    them for the whole block. (Reclaim is the current name for what were
    previously called on-demand/ad-hoc blocks; the GPU-recovery loop
    replaces the earlier auto-fill sweep.)

-   **No-show.** If a reservation window opens and the holder has not
    launched a matching Pod within 15 minutes, the block is converted to
    on-demand duty. The conversion is irrevocable for the remainder of
    the window --- the original holder has forfeited that block, which
    removes any ambiguity about late arrivals reclaiming a slot already
    in use. (One operational caveat: the controller tracks this state in
    memory only, so a controller restart re-opens a short grace window
    --- 30 minutes by default --- during which a late holder could still
    claim.)

-   **Idle session.** If a holder's session is running but making no use
    of its GPU, the existing idle-culling process terminates it. The
    controller notices the vacated window on its next reservation
    refresh and re-arms a short claim deadline (30 minutes by default);
    if the holder does not relaunch within it, the remaining block is
    converted to on-demand in the same way.

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

5.3 Recycling and transparency

When an on-demand Pod on such a block finishes or is idle-culled, the
block returns to the on-demand pool for the next eligible Pending Pod,
and so on until the window ends. At that point only the capacity that
was temporarily serving on-demand --- the specific GPU or MIG instance,
a portion of the node rather than the whole node --- returns to handling
reservations; the node itself stays in the reserved pool throughout.
Because interactive sessions rarely end on their own, idle-culling is
the main driver of this recycling, not natural completion. Showing
waiting on-demand users their position in the queue --- through events
surfaced in JupyterHub and on the shell login node --- is planned but
not yet implemented; today the controller emits events only when it
caps a session's runtime.

6\. Fairness and the student experience

Booking limits in the reservation app prevent monopolization and let
course staff shape access for deadline weeks. The primary lever is a
per-member **Service Unit (SU) budget** set on each group: every booking
costs SU (priced per GPU-hour by the GPU class, discounted during
off-peak windows), and a member cannot exceed their group's budget. The
budget window is configurable --- a renewable ceiling that frees SU as
reservations end, or a depleting weekly/monthly/quarterly/cumulative
quota --- so staff can choose between "as much as fits at once" and "this
much for the term." Alongside the SU budget, per-group and per-GPU-class
GPU ceilings (with date-span boosts for deadline weeks) bound concurrent
hardware use. A **management buffer** holds back a slice of each GPU
class's capacity that is invisible to ordinary members, giving staff
headroom for maintenance or last-minute student accommodations even when
the cluster otherwise looks full.

To discourage speculative booking (reserving "just in case" and not
showing up, which manufactures the very scarcity we are trying to
relieve), the app charges a **late-cancellation SU penalty**: cancelling
within 24 hours of the window keeps part of the booking's SU cost against
the member's budget, while cancelling earlier is fully waived. A further
soft no-show penalty --- a temporary reduction in booking priority after
an unclaimed reservation --- is still under consideration.

Group managers (course staff) can manage their groups' reservations
without full administrative access: they book on behalf of students (the
reservation is owned by the student, with the manager recorded as the
submitter), and they are exempt from the booking-window, SU-budget, and
management-buffer limits, get a ±90-day grace window around their group's
active dates, and can waive late-cancellation penalties.

7\. Operations and monitoring

-   **Least privilege.** The controller authenticates to the reservation
    app with a `read_only`-scoped service key (the app issues scoped keys,
    `read_only` or `read_write`); it only reads reservation data and never
    modifies it.

-   **Reclaim-hold integration.** Reclaim capacity holds (Section 5.1) are
    integral to operation, not an add-on: they are how the reservation app
    hands idle, unbooked capacity to the controller, and together with the
    controller's no-show and idle-cull reclamation they form the single
    on-demand management system that keeps reserved hardware from sitting
    idle.

-   **Anomaly alerting.** Splunk alerts fire on stuck reservation-holder
    Pods, which also gate on-demand placement (Section 5.2).

-   **Queue visibility (planned).** On-demand queue position will be
    emitted as events visible in JupyterHub and on the login node; not
    yet implemented in the controller.

-   **Per-student namespaces.** JupyterHub (via KubeSpawner) places each
    student in their own Kubernetes namespace, which the controller
    relies on to match Pods to reservations.

8\. Risks and open items

-   **Booking-time integrity.** When reservations open, hundreds of
    students may book at once. The booking path must check capacity and
    record the reservation atomically so the last available slot cannot
    be double-booked, and the datastore must tolerate the write burst.

-   **Controller availability.** A single controller is a single point
    of failure; while it is down, new reserved Pods cannot be admitted.
    Production hardening (fast restart, and eventually a standby) is
    planned.

-   **Capacity sizing.** The reserved share of nodes is set by hand and
    is not yet linked to the reservation app's view of capacity, and MIG
    fixes the number of instances per GPU. The reservation app's
    per-GPU-class **management buffer** is a related hand-set lever ---
    capacity held back from ordinary members for maintenance and
    accommodations --- that likewise has to be sized by judgement.
    Reserved-pool sizing, management buffers, and MIG profiles should be
    reviewed against observed demand.

-   **On-demand remains best-effort.** Students without a reservation
    still depend on available and recycled capacity; the predictability
    guarantee applies to reservation holders. Oversubscription and queue
    transparency mitigate, but do not eliminate, on-demand waits.

9\. How this compares with peer institutions

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
proven ideas from the field: idle-culling and runtime caps for
utilization, MIG for isolation, and queue transparency for the on-demand
cohort.

Appendix A --- Reservation-block lifecycle

The state machine below tracks a single reservation block as the
controller moves it through its life. Reserved-phase states are shown in
blue, on-demand-phase states in green; the two red transitions are the
irrevocable conversions of unused reserved capacity into on-demand
capacity. Reclaim capacity holds (Section 5.1) --- the renamed scheduled
on-demand blocks --- have no reserved phase and enter the diagram directly
at the On-Demand --- Available state.

![States from Scheduled through Awaiting Claim, Reserved In Use,
On-Demand Available/In Use, to Window Ended, with no-show and idle-cull
conversions.](./media/dbedd404ff0049ba26540ae53449dab333012f9f.png "Reservation-block lifecycle state machine"){width="6.25in"
height="6.083333333333333in"}

Walkthrough

1.  **Scheduled → Awaiting Claim.** At the start of the reserved window
    the controller begins watching for the holder's Pod and starts a
    15-minute no-show timer.

2.  **Awaiting Claim → Reserved In Use.** The holder's Pod is matched;
    the controller adds the toleration and caps runtime to the window.

3.  **Awaiting Claim → On-Demand Available.** Fifteen minutes pass with
    no claiming Pod: the block is irrevocably released to the FCFS pool.

4.  **Reserved In Use → On-Demand Available.** The session is found idle
    and culled; the remaining block is released the same way.

5.  **On-Demand Available → In Use.** An eligible Pending Pod is placed,
    subject to the three guards in Section 5.2.

6.  **On-Demand In Use → Available.** That Pod finishes or is culled,
    and the block is recycled to the next waiter.

7.  **→ Window Ended.** When the window closes, the capacity that was on
    loan --- the GPU or MIG instance, not the whole node --- returns to
    serving reservations.