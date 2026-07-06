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
│    GET /api/gpu-classes/{id}  → resolve label_value   │
│    GET /api/settings  → reclaim_preempt_guard_minutes │
└──────────────────────────────────────────────────────┘
           │ updates in-memory reservation list
           ▼
┌──────────────────────────────────────────────────────┐
│ 2. Pod watch  (LIST at startup, then WATCH stream)   │
│    Pods with label gpu-class=<X> are matched against │
│    the reservation list:                             │
│      namespace == reservation.user.username          │
│      gpu-class label == gpu_class.label_value        │
│    Matched pods enter the work queue.                │
│                                                      │
│    Fast path: if a new pod (ADDED) arrives while its │
│    window is already open, the toleration is applied │
│    immediately — no wait for the queue processor.    │
└──────────────────────────────────────────────────────┘
           │ task queue (in memory)
           ▼
┌──────────────────────────────────────────────────────┐
│ 3. Queue processor  (every 300 s)                    │
│    Handles pods queued before their window opened,   │
│    and retries for pods that were over-budget:       │
│      a. Count nvidia.com/gpu already in use by other │
│         sibling pods that hold the toleration for    │
│         the same booking (matched via the            │
│         horae/booking-reference annotation)          │
│      b. If pod_gpus + sibling_gpus ≤ reserved_gpus:  │
│           PATCH pod → add toleration + annotations   │
│           PATCH pod → set activeDeadlineSeconds      │
│           Create RuntimeCapped Event on pod          │
│      c. Otherwise: retry in 2–5 min                  │
│                                                      │
│    On-demand path (when ONDEMAND_PLACEMENT_ENABLED): │
│      d. Reconcile reclaim merges: re-apply persisted │
│         merges + absorb newly-committed future       │
│         reclaim blocks into open subject blocks      │
│      e. Safety interlock (guard 3): if a reservation │
│         holder is stuck Pending for a GPU class,     │
│         hold on-demand placement for that class      │
│      f. For each on-demand candidate whose retry     │
│         cooldown has passed, find a suitable block:  │
│           kind="reclaim", no-show, or cancelled,     │
│           matching GPU class, sufficient free GPUs,  │
│           remaining window >= minimum-runtime-seconds│
│           PATCH pod → add toleration + block-id      │
│           PATCH pod → set activeDeadlineSeconds      │
│           Create RuntimeCapped Event on pod          │
└──────────────────────────────────────────────────────┘
```

### On-demand placement

When `ONDEMAND_PLACEMENT_ENABLED=true` (the default), the controller also
handles pods that have **no matching user reservation** but carry the
`horae/minimum-runtime-seconds` annotation.  These pods are treated as
*on-demand candidates* and are placed onto reclaimable capacity when it is
available (reclaim holds, no-show windows, or cancelled-in-window windows — see
*On-demand capacity sources* below).

**Candidate selection** — a Pending pod becomes an on-demand candidate when:
- It has a `gpu-class` label but no matching user reservation.
- It carries `horae/minimum-runtime-seconds=<N>` (a positive integer).
- Only `ADDED` events create candidates; `MODIFIED` bursts are ignored.

**Block selection** — a block is eligible when:
- It is an on-demand capacity source — a `kind == "reclaim"` hold, a no-show
  reservation, or a cancelled-in-window reservation (see *On-demand capacity
  sources* below).
- Its GPU class matches the pod's `gpu-class` label.
- Its window is currently open and has at least `minimum-runtime-seconds` remaining.
- It has sufficient free GPU capacity for the pod's request.
- It is not currently claimed by a live reservation holder, and it has not been
  absorbed into another block as a merge stub.

Among eligible blocks, the controller prefers the one whose window ends
**latest** (maximising the pod's effective runtime before the block's
`activeDeadlineSeconds` cap kicks in); ties are broken by most free capacity.
A block that has absorbed an abutting future reclaim block (see *Reclaim-block
merging* below) presents here as a single, longer window, so this same
"latest end wins" rule extends the chosen runtime automatically.

**Safety interlock (guard 3)** — if any reservation-holder pod for a given
GPU class is stuck in Pending (admitted but the scheduler cannot place it),
on-demand placement is suspended for that class until the stuck pod is
resolved.  Other GPU classes are unaffected.

**On-demand capacity sources** — the blocks a candidate can land on come from
three places, distinguished by *who* freed the capacity:

| Source | Origin | `kind` / state | booking-reference prefix |
|--------|--------|----------------|--------------------------|
| **Reclaim hold** | The reservation **app's** GPU recovery task tiles idle capacity into explicit reclaim rows; the controller sees them via `GET /api/reservations?status=all` | `kind == "reclaim"` | `ondemand-<id>` |
| **No-show reclaim** | **Controller-derived:** a user reservation with no matching pod by its deadline | `kind == "booking"`, id tracked in-memory as a no-show | `noshow-<id>` |
| **Cancelled-in-window reclaim** | **Controller-derived:** a reservation cancelled *while its window is open* — the controller evicts any admitted pods and retains the freed window | `kind == "booking"`, `status == "cancelled"`, retained in memory until its window ends | `ondemand-<id>` |

Only the first is created upstream; the latter two are reclaim sources the
controller synthesises internally from reservation state.  All three feed the
same `find_ondemand_block` selection and the same unified occupancy map (keyed by
reservation id), so each retains an independent GPU budget regardless of source.
A reservation a live holder is still occupying — directly or via a chained runtime
cap — is **claimed** and is never lent out as on-demand capacity.

**No-show reclaim** — if a reservation holder fails to launch a pod within
`NOSHOWN_TIMEOUT_MINUTES` of the window opening, that reservation is marked
as a no-show and its capacity becomes available for on-demand candidates.
If a holder's pod is later deleted mid-window (e.g. idle-culled), the next
reservation refresh re-arms a fresh deadline of `NOSHOWN_GRACE_MINUTES`, so
a vacated window is also reclaimed for on-demand use if the holder does not
return.  No-show state is **in-memory only**: it is not written back to the
reservation API, and a controller restart clears it — mid-window reservations
then get a fresh `NOSHOWN_GRACE_MINUTES` deadline, so a late holder can
reclaim their window across a restart.

**Cancelled-in-window reclaim** — when a reservation is cancelled while its
window is already open, the controller emits a `ReservationCancelled` Event on
each pod admitted under it, deletes those pods, and retains the cancelled
reservation in memory until its original window ends.  The freed GPUs are offered
to on-demand candidates for the remainder of that window (booking-reference
`ondemand-<id>`).  Like no-show state, this is **in-memory only** and is rebuilt
from a fresh reservation fetch after a restart.

**Reclaim-block merging** — on-demand jobs are normally capped to the end of the
single block they land on (no back-to-back chaining, unlike reserved holders).
To let a job that starts near a block boundary run longer, the controller merges
an open on-demand **subject block** (any of the three sources above) with a
directly **abutting** future `kind="reclaim"` block of the **same GPU class and
equal GPU count**, provided that future block is **committed**.

A reclaim block is committed once its start is within the reservation app's
`reclaim_preempt_guard_minutes` — inside that guard the app will not preempt the
hold with a new booking, so it is safe to schedule onto.  The controller judges
this against the block's start **at the last reservation fetch**, not the
between-fetch clock: a block still preemptible when we last fetched must not be
merged just because the tick clock drifts it into the guard, or it could race a
last-minute booking the controller has not yet seen.  Because the guard is sized
to exceed the poll interval, a block legitimately entering the guard is always
re-confirmed by a fresh fetch (still present, or gone if preempted) before it is
merged.

When a subject abuts a committed future block, the subject's window is extended
to that block's end and the absorbed block becomes a **stub** — excluded from
independent placement so it is never double-booked.  The longest abutting block
is chosen, and further blocks are chained in iteratively as they enter the guard
on later fetches.  Because the merged block presents as one longer window,
`find_ondemand_block` and the `activeDeadlineSeconds` cap extend the job's runtime
automatically.  Merges are **persistent**: they are re-applied to the freshly
loaded reservation list on every refresh and pruned only once the whole merged
span has ended, so a reload never re-exposes an absorbed block while a
deadline-extended job is still running on it.  Merging is skipped entirely when
on-demand placement is disabled or the guard is unknown (settings fetch failed or
recovery disabled).

**Recycling** — when an on-demand pod terminates, the freed capacity is
immediately offered to the next waiting candidate of the same GPU class
without waiting for the next queue-processor tick.

**Occupancy tracking across restarts** — capacity for every admitted pod
(reserved, on-demand, and no-show alike) is tracked in one occupancy map keyed
by reservation id.  The map is rebuilt from the cluster — the reservation id
parsed from each pod's `horae/booking-reference` — by the startup pod LIST and,
on every queue-processor tick, from a live snapshot, so a missed event or a
restart self-heals within one tick.

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
| `horae/booking-reference` | toleration applied | Identifies the reservation the pod was admitted under (`res-<id>`, `ondemand-<id>`, or `noshow-<id>`); the id is the key for the per-reservation GPU budget and for rebuilding occupancy from the cluster |
| `horae/pod-runtime-limit-seconds` | deadline set | Records the applied `activeDeadlineSeconds` value for operator visibility and in-pod notification widgets |

(`horae/minimum-runtime-seconds` is the one annotation **consumed** rather
than written — see On-demand placement above.)

### Runtime capping

When a pod is admitted, the controller also enforces a maximum runtime by
setting `spec.activeDeadlineSeconds` on the pod.  The value is calculated as:

- **Remaining time** in the pod's current reservation window, plus
- **Full duration** of any directly back-to-back future reservations with the
  same owner, GPU class, and GPU count (no gap between consecutive windows).

If the pod's existing `activeDeadlineSeconds` is unset or exceeds this
maximum, it is updated.  A Kubernetes **Event** with reason `RuntimeCapped` is
then created on the pod explaining the change.  If the pod already has an
`activeDeadlineSeconds` within the allowed maximum it is left unchanged.

Deadline enforcement is best-effort: if the PATCH or Event creation fails, a
warning is logged but the toleration that was already applied is not revoked.

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
| `ONDEMAND_PLACEMENT_ENABLED` | no | `true` | Set to `false` to disable on-demand placement and run reserved-path logic only |
| `NOSHOW_TIMEOUT_MINUTES` | no | `15` | Minutes after a reservation window opens before declaring a no-show and opening the block to on-demand pods (legacy alias `NOSHOWN_TIMEOUT_MINUTES` still accepted) |
| `NOSHOW_GRACE_MINUTES` | no | `30` | Grace period (minutes) after controller startup before no-shows are declared for windows already in progress (legacy alias `NOSHOWN_GRACE_MINUTES` still accepted) |
| `POD_LIST_TICK_INTERVAL` | no | `300` | Seconds between queue-processor ticks (pod LIST frequency) |
| `POD_SCHEDULING_GATE_NAME` | no | *(absent)* | Name of a SchedulingGate to remove from a pod after admitting it; unset disables scheduling-gate removal |
| `INBOUND_API_TOKEN` | no | *(absent)* | Bearer token for the inbound push API (`POST /api/reservations/push`); mount from a Kubernetes Secret. Unset leaves the endpoint **disabled** (returns 503) |
| `LOG_LEVEL` | no | `INFO` | Python logging level for the controller |

> **Note:** `reclaim_preempt_guard_minutes` (used by reclaim-block merging) is
> **not** an environment variable — the controller reads it from the reservation
> app's `GET /api/settings` endpoint on each refresh cycle, so it always tracks
> the app's own configuration.  If the settings fetch fails, the previous value
> is kept; until the first successful fetch, reclaim-block merging is skipped.

> **Security note:** The controller requires a **`read_only`**-scoped service
> key — it only calls `GET` endpoints (`/api/reservations`, `/api/gpu-classes`,
> and `/api/settings`).  Do not provision a `read_write` key for this daemon.
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
faster (e.g. a cancellation, or a future standby assignment), the reservation app
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
(PATCH) to pods in namespaces where reservations are active, and `delete` on
pods so it can evict pods whose reservation is cancelled mid-window.

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
│   ├── main.py               FastAPI app + three background asyncio tasks
│   ├── config.py             Config dataclass from environment variables
│   ├── schemas.py            Pydantic models for reservation API responses
│   ├── reservation_client.py httpx async client for the reservation API
│   ├── k8s_client.py         Kubernetes client wrapper (watch, patch, occupancy snapshot)
│   └── controller.py         Shared state, queue, matching, window arithmetic
├── tests/                    pytest suite (controller logic, guards, no-show, on-demand)
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
