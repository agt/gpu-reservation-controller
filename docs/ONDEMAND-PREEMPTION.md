# On-demand (JIT) jobs — lease, overstay, and preemption

How the controller handles **just-in-time (JIT) on-demand jobs** — pods with no
reservation open now or soon, for which the controller books a short on-demand
reservation on the pod's behalf — and what happens once such a job **overstays**
its runtime guarantee and becomes subject to **demand-driven preemption**.

The diagram shows every call-out to the GPU Reservation App (the app's interior
is not modelled) and the other parts of the controller that directly affect
on-demand jobs: routing in `pod_watch_loop`, the shared admission path, the
queue-processor tick, the preemption sweep with its adoption rescue pass, and
the reservation-state sync (fetch loop + inbound push).

Code anchors: `app/main.py` (`_try_request_lease`, `_try_apply_toleration`,
`_run_preemption_sweep`, `_adopt_pods`), `app/controller.py`
(`find_admittable_reservation`, `guarantee_end`, `plan_pod_adoptions`,
`plan_boundary_candidates`), `app/reservation_client.py` (all HTTP call-outs).

## Why a flowchart

Three Mermaid types were considered. A **state diagram** models a single pod's
lifecycle well but hides *who* acts (four concurrent loops) and reduces the app
call-outs to transition-label footnotes. A **sequence diagram** is the natural
shape for call-outs, but this logic is dominated by branching — routing checks,
guards, grant/deny, delegate/fallback — and by hour-scale gaps between admission
and overstay, which nested `alt` frames and a single time axis handle poorly. A
**flowchart with subgraphs** shows all three things at once: the decision logic,
which background loop owns each step, and every app call-out as a labelled
dashed edge to a purple endpoint node. Flowchart it is.

## The diagram

```mermaid
---
title: "JIT on-demand jobs: lease → admission → guarantee → overstay → adoption / preemption"
config:
  flowchart:
    nodeSpacing: 35
    rankSpacing: 45
---
flowchart TB

    %% ── pod routing ──
    subgraph WATCH["pod_watch_loop — routing, re-evaluated on every attempt"]
        W0["Pod ADDED / MODIFIED: gpu-class label,<br/>no gpu-class-reservation toleration"]
        W1{"find_admittable_reservation —<br/>match open now or within<br/>ONDEMAND_HORIZON_MINUTES (30), with budget?"}
        W2["Reserved queue (enqueue_pod);<br/>fast-path attempt if window already open;<br/>budget-full / error → retry in 2–5 min;<br/>queued windows admitted by tick once open"]
        W3{"JIT-eligible? ONDEMAND_PLACEMENT_ENABLED<br/>∧ Pending ∧ horae/minimum-runtime-seconds<br/>∧ group label if REQUIRED_GROUP_LABEL"}
        W4["Not JIT-eligible: queue for any future<br/>match (find_best_reservation),<br/>else left Pending"]
        C0["OnDemandCandidate — attempted now,<br/>again on MODIFIED events and every tick<br/>(FIFO), honouring next_attempt_at"]
    end

    %% ── JIT lease request ──
    subgraph JIT["_try_request_lease — JIT lease request (same coroutine from watch loop and tick)"]
        L1{"Re-read pod:<br/>gone / terminal / Unknown?"}
        L2{"Routing re-check: a matching<br/>reservation became admittable?"}
        LG{"Guard 1 — Pending only for GPU capacity?<br/>Guard 3 — gpu-class free of<br/>stuck-holder interlock?"}
        L6["Request lease: gpu_class_id via<br/>state.gpu_class_ids, duration = min-runtime +<br/>ONDEMAND_LEASE_BUFFER_MINUTES (10),<br/>idempotency_key = pod UID"]
        L8["Lease granted → upsert into<br/>state.reservations (under reservation_lock)"]
        L9["Compensating cancel — admission failed<br/>after grant; lease dropped from state"]
        DROP["Candidate dropped"]
    end
    EP_CREATE(["app: POST /api/reservations"])
    EP_CANCEL_R(["app: POST /api/reservations/{id}/cancel"])

    %% ── shared admission ──
    subgraph ADMIT["_try_apply_toleration — the one shared admission path"]
        A1{"Budget check: gpu_requested ≤<br/>available(reservation)?"}
        A2["Patch pod: toleration gpu-class-reservation=<br/>&lt;label&gt;:NoSchedule + annotation<br/>horae/booking-reference = res-&lt;id&gt;;<br/>record guarantee + RuntimeGuaranteed event"]
    end

    %% ── holder lifecycle ──
    H0["ADMITTED HOLDER — an on-demand job is an<br/>ordinary reservation holder under res-&lt;id&gt;"]
    G0{"Live guarantee (guarantee_end):<br/>lease slot_end + back-to-back chain,<br/>recomputed on every check — can grow"}
    OS["OVERSTAY — past guarantee (guarantee_end<br/>None or ≤ now); pod keeps running freely"]

    %% ── adoption rescue ──
    subgraph ADOPT["_adopt_pods — overstay rescue (POD_ADOPTION_ENABLED)"]
        AD1{"plan_pod_adoptions: same user now holds an<br/>OPEN booking with spare budget that chaining<br/>cannot reach (gap / starts now / other gpu_count)?"}
        AD2["Re-annotate booking-reference → res-&lt;new id&gt;;<br/>relink occupancy; OverstayRelinked event;<br/>guarantee re-recorded"]
    end

    %% ── preemption sweep ──
    subgraph SWEEP["preemption_loop — every PREEMPTION_CHECK_INTERVAL (60 s)"]
        S1{"Booking slot_start within<br/>PREEMPTION_LEAD_MINUTES (15)?<br/>phase A = lead-time, B = at-boundary;<br/>each boundary/phase fires once"}
        S2["Snapshot tolerated pods + node GPU capacity —<br/>either fails → skip sweep, kill nothing"]
        S4{"Per class at the boundary: demand<br/>(remaining budget − chained-holder GPUs)<br/>&gt; free (physical − in-use)?"}
        S5["Eligible pool (plan_boundary_candidates):<br/>same gpu-class, controller-admitted, live,<br/>not terminating, PAST GUARANTEE only"]
        S6{"PREEMPTION_DELEGATE_SELECTION?"}
        S7{"App returned a victim list?"}
        S8["Local fallback: uniform-random pick<br/>(select_victims_locally)"]
        S9["Preempt victims: Preempted event → delete pod<br/>→ release occupancy; unmet shortfall → warning"]
        IDLE["Nothing to reclaim this sweep"]
        GONE["Pod deleted — capacity freed; DELETED<br/>watch event releases occupancy"]
    end
    EP_VICTIMS(["app: POST /api/reservations/preemption-victims"])

    %% ── queue-processor tick ──
    subgraph TICK["queue_processor_loop — every POD_LIST_TICK_INTERVAL (300 s)"]
        T1["Snapshot tolerated pods → rebuild occupancy +<br/>claimed windows, refresh guard-3 interlock"]
        T3["Re-attempt due on-demand candidates (FIFO)"]
        T4["Vacated lease (holder pod gone mid-window):<br/>no-show tracking re-arms; declared after grace"]
    end
    EP_CANCEL_N(["app: POST /api/reservations/{id}/cancel"])

    %% ── reservation-state sync ──
    subgraph SYNC["Reservation-state sync (reconciles under reservation_lock)"]
        F1["reservation_fetch_loop — full pull every<br/>RESERVATION_FETCH_INTERVAL (300 s)"]
        P1["Inbound POST /api/reservations/push —<br/>bearer token; partial delta upsert"]
        RS["state.reservations +<br/>gpu-class label ↔ id maps"]
        EVICT["Lease cancelled mid-window → evict holder<br/>(ReservationCancelled), release capacity"]
    end
    EP_LIST(["app: GET /api/reservations"])
    EP_CLASSES(["app: GET /api/gpu-classes (+ /{id})"])
    APPP(["app: push client"])

    %% ── routing edges ──
    W0 --> W1
    W1 -->|"yes — reserved path"| W2
    W1 -->|no| W3
    W3 -->|no| W4
    W3 -->|yes| C0
    C0 -->|attempt| L1

    %% ── lease request edges ──
    L1 -->|yes| DROP
    L1 -->|no| L2
    L2 -->|"yes — route to reserved queue<br/>(pre-admission analogue of adoption)"| W2
    L2 -->|no| LG
    LG -->|"not yet knowable / interlocked<br/>→ retry ~30 s"| C0
    LG -->|"Pending for non-GPU reasons"| DROP
    LG -->|pass| L6
    L6 -.->|"on_demand = true"| EP_CREATE
    L6 -->|"denied (409 / error) or unknown<br/>class id → cool down 2–5 min"| C0
    L6 -->|granted| L8
    L8 --> A1
    L9 -.->|"reason = controller-revoked"| EP_CANCEL_R
    L9 -->|"pod still viable → fresh<br/>attempt in 2–5 min"| C0
    L9 -->|"pod went terminal"| DROP

    %% ── admission edges ──
    W2 -->|"window open (fast path or tick)"| A1
    A1 -->|yes| A2
    A1 -->|"budget full — on a JIT grant"| L9
    A2 -->|"patch error — on a JIT grant"| L9
    A2 -->|admitted| H0

    %% ── lifecycle edges ──
    H0 --> G0
    G0 -->|"now &lt; guarantee end — protected,<br/>never a preemption candidate"| H0
    G0 -->|"guarantee over"| OS

    %% ── adoption edges ──
    OS -->|"examined for rescue"| AD1
    AD1 -->|yes| AD2
    AD1 -->|"no rescue — still overstay: preemptible<br/>once a boundary needs the capacity"| S5
    AD2 -->|"protected again — boundary<br/>demand credited"| H0

    %% ── sweep edges ──
    S1 -->|no| IDLE
    S1 -->|yes| S2
    S2 -->|"① rescue pass first — a re-booked pod<br/>can never be picked as a victim"| AD1
    S2 -->|"② then per boundary, ascending<br/>(doomed set prevents double-selection)"| S4
    S4 -->|"no — free capacity covers it"| IDLE
    S4 -->|"yes — kills_needed = demand − free"| S5
    S5 --> S6
    S6 -.->|"eligible pool + kills_needed"| EP_VICTIMS
    S6 -->|delegated| S7
    S6 -->|disabled| S8
    S7 -->|"uid list — unknown uids ignored;<br/>empty list respected (spare everyone)"| S9
    S7 -->|"call failed / endpoint absent"| S8
    S8 --> S9
    S9 --> GONE

    %% ── tick edges ──
    T1 -->|"adoption tidy-up each tick,<br/>even with no boundary near"| AD1
    T3 -.->|"wakes due candidates"| C0
    T4 -.->|"reason = no-show"| EP_CANCEL_N

    %% ── sync edges ──
    F1 -.-> EP_LIST
    F1 -.-> EP_CLASSES
    APPP -.->|"pushes deltas within seconds"| P1
    F1 --> RS
    P1 --> RS
    RS -->|"in-window cancellation detected"| EVICT
    RS -.->|"drives matching and<br/>lease id resolution"| W1
    RS -.->|"drives guarantees<br/>and boundaries"| G0

    %% ── styling ──
    classDef app fill:#6f42c1,stroke:#3d2478,color:#ffffff
    classDef k8s fill:#1f6feb,stroke:#0d419d,color:#ffffff
    classDef ok fill:#2da44e,stroke:#1a7f37,color:#ffffff
    classDef warn fill:#bf8700,stroke:#7d4e00,color:#ffffff
    classDef kill fill:#cf222e,stroke:#82071e,color:#ffffff
    classDef aux fill:#57606a,stroke:#24292f,color:#ffffff

    class EP_LIST,EP_CLASSES,EP_CREATE,EP_VICTIMS,EP_CANCEL_R,EP_CANCEL_N,APPP app
    class A2,S2,T1 k8s
    class H0,AD2 ok
    class OS,L9 warn
    class S9,GONE,EVICT kill
    class DROP,IDLE,RS aux
```

### Legend

| Convention | Meaning |
|---|---|
| Solid arrow | Control flow inside the controller |
| Dashed arrow | HTTP call-out to / from the app, or a state feed |
| Purple stadium node (`app: …`) | GPU Reservation App endpoint — external, interior not modelled |
| Blue node | Step whose work is Kubernetes API calls (patch / snapshot / events / delete) |
| Green node | Pod in a protected state (admitted holder, freshly adopted) |
| Amber node | Overstay state and compensating actions |
| Red node | Pod-terminating outcome |
| Grey node | Terminal / shared-state box |

## App call-outs at a glance

| Endpoint | Direction | Called from | Role for on-demand jobs | On failure |
|---|---|---|---|---|
| `GET /api/reservations` | controller → app | fetch loop (every 300 s) | Active set that drives routing, guarantees, and boundaries | Refresh cycle aborts; previous state kept |
| `GET /api/gpu-classes` (+ `/{id}` fallback) | controller → app | fetch / push reconcile | `label ↔ id` maps — a lease request needs the numeric `gpu_class_id` | Previous cycle's maps kept |
| `POST /api/reservations` | controller → app | `_try_request_lease` | Create the JIT lease (`on_demand=true`, sized min-runtime + buffer, idempotent by pod UID) | Treated as a denial → 2–5 min cool-down, retry |
| `POST /api/reservations/{id}/cancel` | controller → app | lease-grant cleanup; no-show cancels on the tick | `reason=controller-revoked` when admission fails after a grant; `reason=no-show` when a vacated lease times out | Logged; a dangling lease is later reaped by no-show tracking |
| `POST /api/reservations/preemption-victims` | controller → app | preemption sweep | App chooses victims **among** the controller's eligible pool | Local uniform-random fallback (`select_victims_locally`) |
| `POST /api/reservations/push` | app → controller | inbound API (bearer token) | Pushes reservation deltas within seconds — a pushed lease cancellation evicts its holder | Next full pull remains the source of truth |

## Reading notes

- **An on-demand job is an ordinary reservation holder.** Once admitted under
  `res-<id>` it is charged SU, protected by the same runtime guarantee, and
  displaces overstayers via the same boundary preemption as any booking — there
  is no separate on-demand admission or preemption code path.
- **A pod within its guarantee is never a preemption candidate**, however
  severe the shortfall. Only past-guarantee pods are ever offered to the app.
- **The guarantee is recomputed live and can grow.** `guarantee_end` returns
  lease `slot_end` plus any back-to-back chain (same user, class, `gpu_count`,
  abutting windows). An abutting re-book therefore extends the guarantee with
  no controller action; **adoption** covers what chaining cannot reach — a gap,
  a window starting "now", or a different `gpu_count`.
- **Adoption runs before victim planning** inside the sweep (and lazily every
  tick), so a pod whose user just re-booked is re-homed, credits the boundary's
  demand, and can never be selected as a victim in the same sweep.
- **Victim authority is split**: the controller alone decides *eligibility*
  (past-guarantee pods it admitted); the app decides *which* of those die. An
  unknown UID in the app's response is ignored, an empty list ("spare
  everyone") is respected, and any call failure falls back to local
  uniform-random selection so preemption still works.
- **Phase A (lead-time) may preempt on behalf of a reservation that later
  no-shows** — an accepted trade-off, not a bug. Phase B (at-boundary)
  additionally catches pods whose own guarantee ends exactly at the boundary.
  Each boundary/phase pair fires at most once (`preemption_fired`).
- **The sweep never acts on unknown physical state**: if either the pod
  snapshot or the node-capacity snapshot fails, the whole sweep is skipped
  with a warning and nothing is killed.
- **Failure containment on the lease path**: a grant whose admission fails
  (budget race, patch error, pod went terminal) is compensating-cancelled
  (`reason=controller-revoked`) so it is never left dangling; the request
  itself is idempotent by pod UID, so a retry after a lost response returns
  the original lease instead of double-booking.
- **Serialisation**: the lease grant + admission, the adoption pass, and sweep
  planning/kills all run under `ControllerState.reservation_lock`, so a
  concurrent fetch or push cannot swap the reservation set mid-decision.
