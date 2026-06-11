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
│    GET /api/reservations?status=active                │
│        &date_start=today&date_end=today+LOOKAHEAD     │
│        (paginated, 200/page)                          │
│    GET /api/gpu-classes/{id}  → resolve label_value   │
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
│ 3. Queue processor  (every 30 s)                     │
│    Handles pods queued before their window opened,   │
│    and retries for pods that were over-budget:       │
│      a. Count nvidia.com/gpu already in use by other │
│         sibling pods that hold the toleration for    │
│         the same booking (matched via the            │
│         dsmlp/booking-reference annotation)          │
│      b. If pod_gpus + sibling_gpus ≤ reserved_gpus:  │
│           PATCH pod → add toleration + annotations   │
│           PATCH pod → set activeDeadlineSeconds      │
│           Create RuntimeCapped Event on pod          │
│      c. Otherwise: retry in 2–5 min                  │
│                                                      │
│    On-demand path (when ONDEMAND_PLACEMENT_ENABLED): │
│      d. Safety interlock (guard 3): if a reservation │
│         holder is stuck Pending for a GPU class,     │
│         hold on-demand placement for that class      │
│      e. For each on-demand candidate whose retry     │
│         cooldown has passed, find a suitable block:  │
│           kind="ondemand" (or no-show reclaimed),    │
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
`dsmlp/minimum-runtime-seconds` annotation.  These pods are treated as
*on-demand candidates* and are placed onto `kind=ondemand` blocks when
capacity is available.

**Candidate selection** — a Pending pod becomes an on-demand candidate when:
- It has a `gpu-class` label but no matching user reservation.
- It carries `dsmlp/minimum-runtime-seconds=<N>` (a positive integer).
- Only `ADDED` events create candidates; `MODIFIED` bursts are ignored.

**Block selection** — a block is eligible when:
- `kind == "ondemand"` (or it has been reclaimed from a no-show reservation).
- Its GPU class matches the pod's `gpu-class` label.
- Its window is currently open and has at least `minimum-runtime-seconds` remaining.
- It has sufficient free GPU capacity for the pod's request.

Among eligible blocks, the controller prefers the one whose window ends
**latest** (maximising the pod's effective runtime before the block's
`activeDeadlineSeconds` cap kicks in); ties are broken by most free capacity.

**Safety interlock (guard 3)** — if any reservation-holder pod for a given
GPU class is stuck in Pending (admitted but the scheduler cannot place it),
on-demand placement is suspended for that class until the stuck pod is
resolved.  Other GPU classes are unaffected.

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

**Recycling** — when an on-demand pod terminates, the freed capacity is
immediately offered to the next waiting candidate of the same GPU class
without waiting for the next queue-processor tick.

**Occupancy tracking across restarts** — capacity for every admitted pod
(reserved, on-demand, and no-show alike) is tracked in one occupancy map keyed
by reservation id.  The map is rebuilt from the cluster — the reservation id
parsed from each pod's `dsmlp/booking-reference` — by the startup pod LIST and,
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
| `dsmlp/booking-reference` | toleration applied | Identifies the reservation the pod was admitted under (`res-<id>`, `ondemand-<id>`, or `noshow-<id>`); the id is the key for the per-reservation GPU budget and for rebuilding occupancy from the cluster |
| `dsmlp/pod-runtime-limit-seconds` | deadline set | Records the applied `activeDeadlineSeconds` value for operator visibility and in-pod notification widgets |

(`dsmlp/minimum-runtime-seconds` is the one annotation **consumed** rather
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
| `TZ` | no | system default | IANA timezone for reservation window arithmetic, e.g. `America/Los_Angeles` |
| `ONDEMAND_PLACEMENT_ENABLED` | no | `true` | Set to `false` to disable on-demand placement and run reserved-path logic only |
| `NOSHOWN_TIMEOUT_MINUTES` | no | `15` | Minutes after a reservation window opens before declaring a no-show and opening the block to on-demand pods |
| `NOSHOWN_GRACE_MINUTES` | no | `30` | Grace period (minutes) after controller startup before no-shows are declared for windows already in progress |

> **Security note:** The controller requires a **`read_only`**-scoped service
> key — it only calls `GET` endpoints.  Do not provision a `read_write` key for
> this daemon.  Always inject the key from a Kubernetes Secret rather than
> baking it into an image or ConfigMap.

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

### 1 — Create the API-key Secret

```bash
kubectl create secret generic gpu-reservation-api-key \
  --namespace gpu-system \
  --from-literal=api-key='gpures_<your-key>'
```

### 2 — RBAC

The controller needs read access to pods across all namespaces, and write
access (PATCH) to pods in namespaces where reservations are active.

```yaml
apiVersion: rbac.authorization.k8s.io/v1
kind: ClusterRole
metadata:
  name: gpu-reservation-controller
rules:
  - apiGroups: [""]
    resources: ["pods"]
    verbs: ["get", "list", "watch", "patch"]
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
│   ├── k8s_client.py         Kubernetes client wrapper (watch, patch, count)
│   └── controller.py         Shared state, queue, matching, window arithmetic
├── tests/                    pytest suite (controller logic, guards, no-show, on-demand)
├── helm/gpu-reservation-controller/  Helm chart (Deployment, RBAC, Service)
├── docs/overview.md          Design & operations plan (stakeholder-facing)
├── requirements.txt
├── Dockerfile
├── RESERVATION-API.md        Reservation management API specification
│                             (identical copy of API.md in gpu-reservation-app —
│                             update both together)
├── CLAUDE.md                 Architecture reference for AI coding assistants
├── AGENTS.md                 Development standards for AI coding agents
└── README.md                 This file
```
