# GPU Reservation Controller

A Kubernetes controller daemon that enforces time-bound GPU reservations by
patching pods with Kubernetes tolerations.  

Pods that belong to an active reservation — and fit within its GPU budget — 
are permitted to schedule on the additional nodes during the reservation window.

---

## How it works

A configurable fraction of our GPU nodes will carry a taint:

```
gpu-class-reservation=<gpu-class-label>:NoSchedule
```

This blocks all ordinary pods from scheduling there.  (Note that this is 
distinct from and in addition to "gpu-class=&lt;gpu-class-label&gt;:NoSchedule" 
which ensures that only jobs intended for that GPU type end up there.)

The controller's job is to add the matching **toleration** to pods
that have a valid, active reservation, subject to the GPU budget 
for that reservation.  (Current budgets are always 1 unit, but the 
system is designed to accommodate greater values in the future.)

### Control loop

```
┌──────────────────────────────────────────────────────┐
│ 1. Reservation fetch (every RESERVATION_FETCH_INTERVAL s)
│    GET /api/reservations?status=all                   │
│        &date_start=today&date_end=today+LOOKAHEAD     │
│        (paginated, 200/page)                          │
│    GET /api/gpu-classes  → refresh label_value maps   │
└──────────────────────────────────────────────────────┘
           │ updates in-memory reservation list
           ▼
┌──────────────────────────────────────────────────────┐
│ 2. Pod watch  (LIST at startup, then WATCH stream)   │
│    Pods with label gpu-class=<X> are routed:         │
│      a. A reservation is open now, or opens within   │
│         ONDEMAND_HORIZON_MINUTES, with budget         │
│           → enter the reserved work queue             │
│      b. Else, if JIT-eligible (min-runtime annotation,│
│         group label if required)                     │
│           → become an on-demand candidate; attempt a  │
│             lease request immediately                 │
│      c. Else, if some future reservation matches      │
│         (beyond horizon / no budget)                  │
│           → enter the reserved work queue anyway       │
│      d. Else → left Pending                            │
│                                                       │
│    Fast path: if a new pod (ADDED) arrives while its │
│    window is already open, the toleration is applied │
│    immediately — no wait for the queue processor.    │
└──────────────────────────────────────────────────────┘
           │ task queue / on-demand candidates (in memory)
           ▼
┌──────────────────────────────────────────────────────┐
│ 3. Queue processor  (every 300 s)                    │
│    Reserved path — pods queued before their window   │
│    opened, and retries for pods over-budget:          │
│      a. Count nvidia.com/gpu already in use by other │
│         sibling pods that hold the toleration for    │
│         the same booking (matched via the            │
│         horae/booking-reference annotation)          │
│      b. If pod_gpus + sibling_gpus ≤ reserved_gpus:  │
│           PATCH pod → add toleration + annotations   │
│           PATCH pod → annotate runtime guarantee      │
│           Create RuntimeGuaranteed Event on pod      │
│      c. Otherwise: retry in 2–5 min                  │
│                                                      │
│    No-show → cancel: for each reservation declared   │
│      no-show, re-verify no pod raced in, then         │
│      POST /api/reservations/{id}/cancel (no-show)     │
│                                                      │
│    JIT on-demand path (ONDEMAND_PLACEMENT_ENABLED):  │
│      d. Safety interlock (guard 3): if a reservation │
│         holder is stuck Pending for a GPU class,     │
│         hold lease requests for that class            │
│      e. For each candidate whose retry cooldown has  │
│         passed: guard 1 (GPU-only-pending), resolve  │
│         gpu-class → gpu_class_id, then                │
│           POST /api/reservations (on_demand=true,     │
│             idempotency_key=pod UID)                  │
│           Granted → admit under it (same as 3a/3b)    │
│           Admission failed → compensating cancel      │
│             (reason=controller-revoked)                │
│           Denied → retry in 2–5 min                    │
└──────────────────────────────────────────────────────┘
           │ (independent of the task queue)
           ▼
┌──────────────────────────────────────────────────────┐
│ 4. Preemption sweep  (every PREEMPTION_CHECK_INTERVAL)│
│    No activeDeadlineSeconds is ever set — a pod may   │
│    run past its guarantee until capacity is needed:   │
│      a. For each upcoming reservation start ("boundary│
│         ") within PREEMPTION_LEAD_MINUTES of now:     │
│           demand = incoming bookings' unclaimed GPUs  │
│           free   = node capacity − live pod usage     │
│      b. If demand > free, delete random past-guarantee│
│         pods of that GPU class until covered:         │
│           Create Preempted Event, then delete pod     │
│      c. Phase A runs before the boundary (proactive); │
│         phase B runs at the boundary itself and also  │
│         makes pods whose own window just ended eligible│
└──────────────────────────────────────────────────────┘
```

### Just-in-time (JIT) on-demand leases

The reservation app schedules only real bookings — there is no ad-hoc
capacity-hold type.  A pod with no reservation open now or opening soon does
not wait indefinitely and is not placed onto ad-hoc spare capacity: the
controller requests a **real reservation** on its behalf, just-in-time.
On-demand jobs are therefore ordinary reservations — charged SU, protected by
the same runtime guarantee as any booking, and able to displace overstayers
via the existing boundary preemption.

**Routing** (re-evaluated on every attempt, in `pod_watch_loop`):

1. A reservation matching the pod's namespace/GPU-class (and usage group, if
   `REQUIRED_GROUP_LABEL` is set) that is open now or opens within
   `ONDEMAND_HORIZON_MINUTES` **and** has spare budget → the pod is queued for
   it, same as any reserved-path pod.
2. Otherwise, if the pod is **JIT-eligible** — `Pending`, carries
   `horae/minimum-runtime-seconds`, and carries the group label when
   `REQUIRED_GROUP_LABEL` is set — it becomes an on-demand candidate and a
   lease request is attempted immediately (and again on later pod updates,
   respecting a retry cooldown).
3. Otherwise, if some future reservation matches at all (beyond the horizon,
   or currently over budget), the pod is queued for it anyway — the plain
   wait-for-window behaviour, preserved for a pod that isn't JIT-eligible.
4. Otherwise the pod is left **Pending** (a pod missing the group label or the
   minimum-runtime annotation is not guessed at).

**Requesting a lease** — the controller resolves the pod's `gpu-class` label
to a numeric `gpu_class_id`, then calls `POST /api/reservations` with
`duration_seconds = minimum-runtime + ONDEMAND_LEASE_BUFFER_MINUTES * 60` and
`on_demand=true` (the app relaxes policy limits — SU, caps, minimum duration
— never physical calendar capacity).  The request is **idempotent by the
pod's Kubernetes UID**: a retry after a prior grant returns the same
reservation rather than creating a duplicate.

- **Denied** (409/error): the candidate cools down 2–5 min and retries.
- **Granted**: the pod is admitted under the new reservation immediately,
  through the same admission path as any reserved-path pod (stamps
  `res-<id>`, records the guarantee, emits `RuntimeGuaranteed`).  If
  admission does **not** succeed — a transient patch error, or the pod
  vanishing in the interim — the controller issues a compensating cancel
  (`POST /api/reservations/{id}/cancel`, `reason=controller-revoked`) so the
  grant is never left dangling.

**Safety interlock (guard 3)** — if any reservation-holder pod for a given
GPU class is stuck in Pending (admitted but the scheduler cannot place it),
lease requests are suspended for that class until the stuck pod is resolved.
Other GPU classes are unaffected.

**No-show → cancel** — if a reservation holder fails to launch a pod within
`NOSHOW_TIMEOUT_MINUTES` of the window opening, the controller durably
cancels the reservation (`POST /api/reservations/{id}/cancel`,
`reason="no-show"`) so the app can re-book the window immediately.  The
cancel is re-verified against a fresh pod snapshot first (a pod that raced in
at the last second is never cancelled out from under it) and retried next
tick if it fails.  A reservation a live holder is still occupying — directly
or via a chained runtime guarantee — is **claimed** and is never declared a
no-show.  The declaration itself is in-memory and does not survive a
restart, but once the cancel actually lands, the reservation is durably gone
from the app's active set, so there is nothing to re-arm.

**Occupancy tracking across restarts** — capacity for every admitted pod is
tracked in one occupancy map keyed by reservation id.  The map is rebuilt
from the cluster — the reservation id parsed from each pod's
`horae/booking-reference` — by the startup pod LIST and, on every
queue-processor tick, from a live snapshot, so a missed event or a restart
self-heals within one tick.

### Toleration applied

```yaml
key:      gpu-class-reservation
operator: Equal
value:    <pod's gpu-class label value>   # e.g. "h100"
effect:   NoSchedule
```

### Annotations stamped on managed pods

| Annotation | Written when | Purpose |
|------------|--------------|---------|
| `horae/booking-reference` | toleration applied | Identifies the reservation the pod was admitted under (`res-<id>` — the only prefix, since every admitted pod is tied to a real reservation, JIT or otherwise); the id is the key for the per-reservation GPU budget and for rebuilding occupancy from the cluster |
| `horae/pod-runtime-limit-seconds` | guarantee recorded | The runtime guarantee's duration in seconds at admission time, for operator visibility and in-pod notification widgets. Legacy key name — no longer backs a hard cap; see *Runtime guarantees and demand-driven preemption* |
| `horae/guaranteed-until` | guarantee recorded | The same guarantee as an absolute UTC ISO-8601 instant |

(`horae/minimum-runtime-seconds` is the one annotation **consumed** rather
than written — see *Just-in-time (JIT) on-demand leases* above.  Both
guarantee annotations are **informational only**: nothing in the controller
reads them back to make a decision, and a guarantee can technically shrink
after being written — e.g. a window shortened server-side — so treat them as
best-effort, not authoritative.)

### Runtime guarantees and demand-driven preemption

When a pod is admitted, the controller records how long its GPU access is
**guaranteed** — but does **not** enforce that with `spec.activeDeadlineSeconds`.
A pod may run past its guarantee freely; the controller reclaims capacity
from an overstaying pod only when a new reservation actually needs it. This
replaced an earlier hard-cap design because users, unable to predict their
runtime accurately, consistently over-booked "just in case."

**Guarantee calculation** — the guaranteed instant is:

- The **end** of the pod's current reservation window, plus
- The **full duration** of any directly back-to-back future reservations with
  the same owner, GPU class, and GPU count (no gap between consecutive
  windows).

Unlike the old cap, this is an absolute instant **recomputed live** on every
check rather than frozen at admission — so a pod's guarantee can *grow*
after admission (an abutting follow-on booking), something a Kubernetes
deadline could never do.

**Recording the guarantee** — after applying the toleration, the controller
annotates the pod (see table above) and creates a Kubernetes **Event** with
reason `RuntimeGuaranteed` explaining when the guarantee ends and that the
pod may later be preempted.  Recording is best-effort: if the PATCH or Event
creation fails, a warning is logged but the toleration that was already
applied is not revoked.

**Recovering capacity** happens in a separate, periodic sweep
(`PREEMPTION_CHECK_INTERVAL`, default 60s) — see step 4 of the control-loop
diagram above. In short: for each upcoming reservation start ("boundary")
within `PREEMPTION_LEAD_MINUTES` (default 15) of now, the controller computes
demand (incoming bookings' unclaimed GPUs) against free physical capacity
(from a node LIST); if demand exceeds free capacity, it deletes **random**
past-guarantee pods of the same GPU class until the shortfall is covered. A
pod still within its guarantee is never selected, however severe the
shortfall — that's logged as an unmet-demand warning instead. Victim
selection is uniform-random for now; priority ranking among overstayers is a
deferred future design. Each boundary is evaluated at most once per phase; a
pod snapshot or node-capacity-snapshot failure skips the sweep entirely
rather than risk a kill based on unknown physical state. Preempted pods get a
Kubernetes Event with reason `Preempted` before deletion.

**Adopting overstay pods into a re-booked reservation**
(`POD_ADOPTION_ENABLED`, default on). Because pods overrun, a user may book a
*fresh* reservation (a new, distinct id) while their pod from the previous
window is still running. If the new window abuts the old one (same owner,
GPU class, and GPU count), the guarantee chaining above already extends the
pod's guarantee onto it — no action needed. For the cases chaining cannot
reach (a non-abutting follow-on window, or a different GPU count), and only
once the pod is **past its runtime guarantee**, the controller instead
*re-links* the pod: it re-annotates the pod's `horae/booking-reference` to
the new reservation and moves the pod's occupancy accordingly, so it is
credited against the new reservation. This runs before each preemption sweep
plans any kills (so a just-re-booked pod is never a victim) and once per
queue-processor tick. Re-linked pods get a Kubernetes Event with reason
`OverstayRelinked`.

**Renewing on-demand leases** (`LEASE_RENEWAL_ENABLED`, default on). An
on-demand lease is fixed-length; left alone its pod would run out its window
and be preempted or expire. As a lease nears the end of its guaranteed block —
within `LEASE_RENEWAL_LEAD_MINUTES` (default 15) of now — the `renewal_loop`
(every `LEASE_RENEWAL_CHECK_INTERVAL` s, default 60) asks the app to renew it
in place via `POST /api/reservations/{id}/renew`. A grant extends the lease to
`ceil_to_hour(now) + 1h` (1–2 h of hour-aligned runway) keeping the **same
reservation id**, so pod matching and occupancy are unchanged and the runtime
guarantee grows with the new end; the pod gets a `LeaseRenewed` Kubernetes
Event. If the app can't renew for lack of capacity or budget (HTTP 409), the
controller emits a `RenewalDenied` Event **once** — the user's cue to
checkpoint before the lease ends — and does not keep retrying that lease. Only
on-demand leases with a live pod are renewed; ordinary bookings are untouched.

---

## Prerequisites

| Requirement | Notes |
|-------------|-------|
| Kubernetes cluster | 1.24+ recommended |
| GPU nodes tainted | `kubectl taint node <node> gpu-class-reservation=<label>:NoSchedule` |
| Reservation management API | Running instance; service key pre-created |
| Python 3.11+ | For local / out-of-cluster use |

---

## Installation

### Docker (recommended)

```bash
docker build -t gpu-reservation-controller:latest .
```

Or pull from your registry if you use the provided GitHub Actions workflow
(`.github/workflows/docker.yml`).

### Local (development / out-of-cluster)

```bash
uv venv .venv
source .venv/bin/activate
uv pip install -r requirements.txt
```

---

## Configuration

All settings are supplied via environment variables.

| Variable | Required | Default | Description |
|----------|----------|---------|-------------|
| `RESERVATION_API_URL` | **yes** | — | Base URL of the reservation management app, e.g. `https://gpures.example.edu` |
| `RESERVATION_API_KEY` | **yes** | — | Service key (`gpures_…`) — mount from a Kubernetes Secret |
| `RESERVATION_FETCH_INTERVAL` | no | `300` | Seconds between reservation refresh cycles |
| `RESERVATION_LOOKAHEAD_DAYS` | no | `7` | How many calendar days ahead to fetch reservations |
| `KUBECONFIG` | no | *(absent)* | Path to a kubeconfig file; if unset, in-cluster service-account credentials are used |
| `HEALTH_PORT` | no | `8000` | Port for the `GET /health` liveness endpoint |
| `TZ` | no | system default | Affects log timestamp display only; reservation window arithmetic is UTC-based and does not depend on it |
| `ONDEMAND_PLACEMENT_ENABLED` | no | `true` | Set to `false` to disable the JIT on-demand lease path entirely |
| `ONDEMAND_HORIZON_MINUTES` | no | `30` | JIT routing horizon: a pod is queued for a reservation opening within this many minutes (with budget) instead of requesting a lease |
| `ONDEMAND_LEASE_BUFFER_MINUTES` | no | `10` | Minutes added to a pod's `horae/minimum-runtime-seconds` when sizing a requested JIT lease's duration |
| `NOSHOW_TIMEOUT_MINUTES` | no | `15` | Minutes after a reservation window opens before declaring a no-show and cancelling it app-side (legacy alias `NOSHOWN_TIMEOUT_MINUTES` still accepted) |
| `NOSHOW_GRACE_MINUTES` | no | `30` | Grace period (minutes) after controller startup before no-shows are declared for windows already in progress (legacy alias `NOSHOWN_GRACE_MINUTES` still accepted) |
| `POD_LIST_TICK_INTERVAL` | no | `300` | Seconds between queue-processor ticks (pod LIST frequency) |
| `POD_SCHEDULING_GATE_NAME` | no | *(absent)* | Name of a SchedulingGate to remove from a pod after admitting it; unset disables scheduling-gate removal |
| `INBOUND_API_TOKEN` | no | *(absent)* | Bearer token for the inbound push API (`POST /api/reservations/push`); mount from a Kubernetes Secret. Unset leaves the endpoint **disabled** (returns 503) |
| `PREEMPTION_LEAD_MINUTES` | no | `15` | Minutes before a reservation slot boundary that phase-A preemption runs, proactively freeing capacity from overstaying pods |
| `PREEMPTION_CHECK_INTERVAL` | no | `60` | Seconds between preemption sweeps |
| `POD_ADOPTION_ENABLED` | no | `true` | Re-link an overstay pod to a reservation its user has since booked. Set to `false` to disable |
| `LEASE_RENEWAL_ENABLED` | no | `true` | Renew (chain) on-demand leases as they near expiry so a running job keeps its GPUs, or is warned to checkpoint when capacity is unavailable. Set to `false` to disable |
| `LEASE_RENEWAL_LEAD_MINUTES` | no | `15` | Renew an on-demand lease whose guaranteed block ends within this many minutes of now |
| `LEASE_RENEWAL_CHECK_INTERVAL` | no | `60` | Seconds between lease-renewal sweeps |
| `REQUIRED_GROUP_LABEL` | no | *(absent)* | Pod label naming the usage group a pod belongs to (e.g. `dsmlp/course`). When set, the pod's value for this label must equal the reservation's group name — an additional match constraint alongside `gpu-class` — before the controller admits it, adopts it, or chain-extends its guarantee; a pod without the label is also never JIT-eligible. Unset disables the group constraint |
| `LOG_LEVEL` | no | `INFO` | Python logging level for the controller |

> **Security note:** The controller needs a **`read_write`**-scoped service
> key: besides the read endpoints (`/api/reservations`, `/api/gpu-classes`),
> it calls `POST /api/reservations` (JIT lease requests),
> `POST /api/reservations/{id}/cancel` (no-show / controller-revoked cancels),
> and `POST /api/reservations/{id}/renew` (on-demand lease renewals).
> Always inject the key from a Kubernetes Secret rather than baking it into an
> image or ConfigMap.

---

## Running locally (out-of-cluster)

```bash
export RESERVATION_API_URL=https://gpures.example.edu
export RESERVATION_API_KEY=gpures_<your-key>
export KUBECONFIG=~/.kube/config
export TZ=America/Los_Angeles

uvicorn app.main:app --host 0.0.0.0 --port 8000
```

Verify the controller is alive:

```bash
curl http://localhost:8000/health
# {"status":"ok"}
```

---

## Inbound push API

The controller normally learns about reservation changes by **polling** the
reservation app every `RESERVATION_FETCH_INTERVAL` seconds. To propagate changes
faster (e.g. a cancellation or an owner reassignment), the reservation app
can **push** one or more updated reservation entries:

```
POST /api/reservations/push
Authorization: Bearer <INBOUND_API_TOKEN>
Content-Type: application/json

{ "reservations": [ <ReservationResponse>, … ] }
```

- Each entry uses the same shape the app already returns from
  `GET /api/reservations`. Entries are **upserted by id**; an entry whose
  `status` is not `"active"` (e.g. a cancellation) drops that reservation from
  the active set, and an in-window cancellation additionally **evicts the
  admitted pod** and frees its capacity — the same behaviour as a cancellation
  seen on a normal poll.
- This is a **partial delta**; bulk synchronisation remains a controller-initiated
  pull, and the next full poll is always the source of truth.
- The endpoint shares the `HEALTH_PORT` listener (no extra port/Service), and
  responds `200` (with `{"applied", "cancelled", "total_active"}`), `401`
  (missing/invalid bearer), or `503` (endpoint disabled because
  `INBOUND_API_TOKEN` is unset).

```bash
curl -X POST http://localhost:8000/api/reservations/push \
  -H "Authorization: Bearer $INBOUND_API_TOKEN" \
  -H "Content-Type: application/json" \
  -d '{"reservations": [ … ]}'
```

To enable it under Helm, point `inboundApiTokenSecret.name` at a Secret holding
the token (see below). No additional RBAC is required.

---

---

## Kubernetes deployment

### Helm (recommended)

A chart at `helm/gpu-reservation-controller/` renders the ServiceAccount,
ClusterRole/Binding, Deployment, and a Service for `/health`. The API-key
Secret must exist beforehand (step 1 below); everything in steps 2–3 is
covered by the chart:

```bash
helm install gpu-reservation-controller ./helm/gpu-reservation-controller \
  --namespace gpu-system --create-namespace \
  --set reservationApiUrl=https://gpures.example.edu \
  --set config.timezone=America/Los_Angeles
```

See `helm/gpu-reservation-controller/values.yaml` for all options (fetch
interval, lookahead, on-demand toggle, no-show timing, resources). The
manual manifests below remain valid for non-Helm deployments.

To enable the inbound push API, create a Secret holding the bearer token and
point the chart at it (leave it unset to keep the endpoint disabled):

```bash
kubectl create secret generic gpu-reservation-push-token \
  --namespace gpu-system --from-literal=token='<random-token>'
helm upgrade gpu-reservation-controller ./helm/gpu-reservation-controller \
  --reuse-values --set inboundApiTokenSecret.name=gpu-reservation-push-token
```

### 1 — Create the API-key Secret

```bash
kubectl create secret generic gpu-reservation-api-key \
  --namespace gpu-system \
  --from-literal=api-key='gpures_<your-key>'
```

### 2 — RBAC

The controller needs read access to pods across all namespaces, write access
(PATCH) to pods in namespaces where reservations are active, `delete` on pods
so it can evict pods whose reservation is cancelled mid-window or whose
runtime guarantee has elapsed and capacity is needed, and `get`/`list` on
nodes so the preemption sweep can compute physical GPU capacity per class.

```yaml
apiVersion: rbac.authorization.k8s.io/v1
kind: ClusterRole
metadata:
  name: gpu-reservation-controller
rules:
  - apiGroups: [""]
    resources: ["pods"]
    verbs: ["get", "list", "watch", "patch", "delete"]
  - apiGroups: [""]
    resources: ["events"]
    verbs: ["create"]
  - apiGroups: [""]
    resources: ["nodes"]
    verbs: ["get", "list"]
---
apiVersion: rbac.authorization.k8s.io/v1
kind: ClusterRoleBinding
metadata:
  name: gpu-reservation-controller
roleRef:
  apiGroup: rbac.authorization.k8s.io
  kind: ClusterRole
  name: gpu-reservation-controller
subjects:
  - kind: ServiceAccount
    name: gpu-reservation-controller
    namespace: gpu-system
```

### 3 — Deployment

```yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: gpu-reservation-controller
  namespace: gpu-system
spec:
  replicas: 1
  selector:
    matchLabels:
      app: gpu-reservation-controller
  template:
    metadata:
      labels:
        app: gpu-reservation-controller
    spec:
      serviceAccountName: gpu-reservation-controller
      containers:
        - name: controller
          image: gpu-reservation-controller:latest
          ports:
            - containerPort: 8000
          env:
            - name: RESERVATION_API_URL
              value: "https://gpures.example.edu"
            - name: RESERVATION_API_KEY
              valueFrom:
                secretKeyRef:
                  name: gpu-reservation-api-key
                  key: api-key
            - name: TZ
              value: "America/Los_Angeles"
          livenessProbe:
            httpGet:
              path: /health
              port: 8000
            initialDelaySeconds: 30
            periodSeconds: 30
          resources:
            requests:
              cpu: 50m
              memory: 64Mi
            limits:
              cpu: 200m
              memory: 256Mi
```

### 4 — Taint GPU nodes

For each GPU node and GPU class participating in the reservation system:

```bash
kubectl taint node <gpu-node> \
  gpu-class-reservation=<label-value>:NoSchedule
```

`<label-value>` must match the `label_value` field in the reservation API's
GPU class record (e.g. `h100`, `a100-80gb`).

---

## Pod requirements

Pods that should be managed by the controller must:

1. Run in a namespace whose name matches the **reservation owner's username**.
2. Carry the label `gpu-class=<label-value>` matching the reserved GPU class.
3. Request `nvidia.com/gpu` resources in their container spec.

Example pod fragment:

```yaml
metadata:
  namespace: jsmith          # must equal reservation username
  labels:
    gpu-class: h100          # must equal gpu_class.label_value
spec:
  containers:
    - name: trainer
      resources:
        requests:
          nvidia.com/gpu: "2"
        limits:
          nvidia.com/gpu: "2"
```

---

## Service key management

Service keys for the controller are managed via the reservation management app.
See `RESERVATION-API.md` §3 for the key lifecycle API, or use the CLI helper on
the reservation server:

```bash
python manage_service_keys.py create --name k8s-controller-prod
python manage_service_keys.py list
python manage_service_keys.py revoke --name k8s-controller-prod
```

---

## Project layout

```
gpu-reservation-controller/
├── app/
│   ├── __init__.py
│   ├── main.py               FastAPI app + five background asyncio tasks
│   ├── config.py             Config dataclass from environment variables
│   ├── schemas.py            Pydantic models for reservation API responses
│   ├── reservation_client.py httpx async client for the reservation API + JIT lease create/cancel
│   ├── k8s_client.py         Kubernetes client wrapper (watch, patch, occupancy snapshot)
│   └── controller.py         Shared state, queue, matching, window arithmetic
├── tests/                    pytest suite (controller logic, guards, no-show, JIT leases)
├── helm/gpu-reservation-controller/  Helm chart (Deployment, RBAC, Service)
├── docs/
│   ├── RESERVATION-API.md    Reservation management API specification
│   │                         (identical copy of API.md in gpu-reservation-app —
│   │                         update both together)
│   ├── SCHEDULING.md         Reservation-app scheduling/reclaim behaviour reference
│   └── lifecycle.mmd/.png    Pod lifecycle state diagram (Mermaid + rendered)
├── requirements.txt
├── requirements-dev.txt
├── pytest.ini
├── Dockerfile
├── OBSERVABILITY.md          Catalogue of every structured log point
├── CLAUDE.md                 Architecture reference for AI coding assistants
├── AGENTS.md                 Development standards for AI coding agents
└── README.md                 This file
```
