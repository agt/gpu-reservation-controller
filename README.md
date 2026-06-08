# GPU Reservation Controller

A Kubernetes controller daemon that enforces time-bound GPU reservations by
patching pods with Kubernetes tolerations.  

Pods that belong to an active reservation — and fit within its GPU budget — 
are permitted to schedule on the additional nodes during the reservation window.

---

## How it works

GPU nodes carry a taint:

```
gpu-class-reservation=<gpu-class-label>:NoSchedule
```

This blocks all ordinary pods from scheduling there.  The controller's job is
to add the matching **toleration** to pods that have a valid, active reservation,
subject to the GPU budget for that reservation.

### Control loop

```
┌──────────────────────────────────────────────────────┐
│ 1. Reservation fetch (every RESERVATION_FETCH_INTERVAL s)
│    GET /api/reservations?status=active&date_start=today
│    GET /api/gpu-classes/{id}  → resolve label_value
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
│         sibling pods that have the toleration        │
│      b. If pod_gpus + sibling_gpus ≤ reserved_gpus:  │
│           PATCH pod → add toleration                 │
│           PATCH pod → set activeDeadlineSeconds      │
│           Create RuntimeCapped Event on pod          │
│      c. Otherwise: retry in 2–5 min                  │
└──────────────────────────────────────────────────────┘
```

### Toleration applied

```yaml
key:      gpu-class-reservation
operator: Equal
value:    <pod's gpu-class label value>   # e.g. "h100"
effect:   NoSchedule
```

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
├── requirements.txt
├── Dockerfile
├── RESERVATION-API.md        Reservation management API specification
├── CLAUDE.md                 Architecture reference for AI coding assistants
├── AGENTS.md                 Development standards for AI coding agents
└── README.md                 This file
```
