# Observability reference

This document inventories every structured log point in the controller.
All timestamps are UTC.  Log level is controlled by the `LOG_LEVEL`
environment variable (default `INFO`); set to `DEBUG` to enable the
debug-level entries below.

Entries marked **†** were added during the pod-lifecycle logging review
session and were not present in the original codebase.

---

## Controller lifecycle

Emitted once at process start and stop.  Source: `app/main.py` (`lifespan`).

| Level | Message | Description |
|-------|---------|-------------|
| INFO | `Performing initial reservation fetch…` | First reservation fetch starting synchronously before background loops launch. |
| INFO | `Initial fetch complete: N reservation(s), N GPU class(es) resolved` | Initial fetch succeeded; shows active reservation and resolved GPU-class counts. |
| INFO | `No-show tracking initialised: N reservation(s) watched` | No-show deadline tracking armed for N user reservations. |
| ERROR | `Initial reservation fetch failed (…); controller will retry in N s, pod matching may be delayed` | Startup fetch failed; controller continues but pod matching is degraded until the next retry succeeds. |
| INFO | `GPU reservation controller started` | All four background loops are running. |
| INFO | `Shutting down GPU reservation controller…` | SIGTERM or lifespan exit received; background tasks being cancelled. |
| INFO | `Controller stopped` | All tasks have exited cleanly. |
| INFO | `Kubernetes: loaded kubeconfig from <path>` | Out-of-cluster mode: kubeconfig loaded from the given path. |
| INFO | `Kubernetes: using in-cluster service-account credentials` | In-cluster mode: service-account credentials loaded. |

---

## Reservation fetch

Emitted by the periodic reservation refresh loop.
Sources: `app/main.py` (`reservation_fetch_loop`, `_refresh_reservations`) and
`app/reservation_client.py` (the HTTP client itself).

| Level | Message | Description |
|-------|---------|-------------|
| DEBUG † | `Reservation refresh cycle starting` | A periodic refresh cycle is beginning. Useful to confirm the loop is alive between INFO-level events. |
| INFO | `Fetched N reservations (N active, N cancelled) (today + N days)` | The reservation client completed its paginated `status=all` fetch; emitted by `ReservationClient.fetch_reservations`. |
| WARNING | `Could not fetch GPU classes: HTTP <code>` / `Could not fetch GPU classes: <exception>` | The full-list `GET /api/gpu-classes` fetch failed; the previous cycle's label/id maps are kept rather than losing all resolution. |
| WARNING | `Could not parse GPU classes response: <exception>` | The GPU-class list payload failed schema validation / JSON decoding; `fetch_gpu_classes` returns None rather than aborting the refresh. |
| INFO | `GPU class N (name) → label_value='value'` | The per-id fallback resolved a class referenced by a reservation but missing from the bulk list (e.g. created since the last successful fetch). |
| WARNING | `Could not fetch GPU class N: HTTP <code>` / `Could not fetch GPU class N: <exception>` | The per-class fallback detail fetch failed (HTTP status or network error); the class is skipped this cycle and `fetch_gpu_class` returns None. |
| WARNING | `Could not parse GPU class N response: <exception>` | The fallback GPU-class payload failed schema validation / JSON decoding; `fetch_gpu_class` returns None rather than aborting the refresh. |
| WARNING | `GPU class N has no label_value; pods for this class cannot be matched to reservations` | A GPU class referenced by an active reservation has no `label_value` set; pods for that class cannot be matched until the class is configured. |
| INFO † | `Reservation refresh complete: N active reservation(s), N GPU class(es) resolved` | Periodic refresh completed successfully; current active reservation and GPU-class counts. |
| ERROR | `Reservation refresh failed: <exception>` | An entire refresh cycle failed (network error, API error, etc.); previous state is retained. |

---

## Reservation cancellation

Emitted when an in-window reservation is detected as cancelled.
Source: `app/main.py` (`_handle_cancelled_reservations`).

| Level | Message | Description |
|-------|---------|-------------|
| WARNING | `Could not snapshot pods for cancellation eviction: <exception>` | The cluster LIST needed to evict pods failed; eviction is skipped for this cycle. |
| INFO | `Evicting N pod(s) for cancelled reservation #N (by <user>)` | N pods admitted under a now-cancelled reservation are being evicted. |
| WARNING | `Could not emit ReservationCancelled event for pod ns/name: <exception>` | Best-effort Kubernetes event emission failed; pod deletion will still be attempted. |
| WARNING | `Could not delete pod ns/name: <exception>` | Pod deletion failed; the pod will persist until manually removed or the next eviction cycle. |

---

## No-show tracking

Emitted during no-show deadline management and the resulting controller-issued
cancel.  Source: `app/controller.py` (`update_noshow_tracking`,
`reconcile_noshow`, `check_noshow_deadlines`, `mark_pod_seen_for_noshow`),
`app/main.py` (`_cancel_pending_noshows`), `app/reservation_client.py`
(`cancel_reservation`).

| Level | Message | Description |
|-------|---------|-------------|
| DEBUG | `No-show tracking (init): reservation #N deadline=<time>` | A no-show deadline was set for a reservation during startup initialisation. |
| DEBUG | `No-show tracking (new): reservation #N deadline=<time>` | A no-show deadline was set for a reservation newly seen in a periodic refresh. |
| DEBUG | `No-show deadline pruned: reservation #N left active list` | A tracked deadline was removed because the reservation is no longer in the active list. |
| INFO | `No-show reservation #N removed: left active list` | A previously declared no-show reservation has left the active list and is no longer tracked. |
| DEBUG † | `Pending no-show cancel #N pruned: left active list` | A reservation queued for a no-show cancel left the active list (its cancel already landed, or the app removed it some other way); no further retry is needed. |
| INFO | `Reservation #N declared no-show (user=u, gpu-class=c): no matching pod appeared before deadline; queued for cancellation` | No holder pod appeared within the timeout window; the reservation is queued in `pending_noshow_cancels` for a durable app-side cancel. |
| DEBUG | `No-show deadline(s) cleared for reservation(s) [N, …]: holder pod admitted (namespace=ns, gpu-class=c)` | A reserved-path holder pod's booking reference was recognised; no-show deadlines for all windows in the holder's back-to-back chain are cleared. |
| DEBUG | `No-show deadline cleared for reservation #N: matching pod already admitted (namespace=ns, gpu-class=c)` | Fallback path: a pod without a booking reference was matched by namespace + GPU class, and its reservation's no-show deadline was cleared. |
| INFO † | `No-show cancel for reservation #N skipped this tick: a pod is now admitted under it` | This tick's fresh pod snapshot shows a pod admitted under the id (a last-second arrival); the cancel is skipped rather than issued out from under it. |
| INFO † | `Reservation #N cancelled (no-show)` | `POST /api/reservations/{id}/cancel` (`reason="no-show"`) succeeded; the id is removed from the active set and `pending_noshow_cancels`. |
| WARNING † | `Failed to cancel no-show reservation #N; will retry next tick` | The cancel request failed; the id remains in `pending_noshow_cancels` and is retried on the next queue-processor tick. |
| INFO † | `Cancel request for reservation #N (<reason>): already gone` | The reservation was already gone (404) when cancelling; treated as success (idempotent). |
| WARNING † | `Could not cancel reservation #N (<reason>): HTTP <code>` / `Could not cancel reservation #N (<reason>): <exception>` | The cancel request failed with an HTTP error or network error; `cancel_reservation` returns False. |

---

## Reserved-path pod admission

Emitted as pods are matched to reservations, queued, and admitted.
Sources: `app/controller.py` (`enqueue_pod`, `dequeue_pod`, `reconcile_queue`), `app/main.py` (`pod_watch_loop`, `_try_apply_toleration`, `_record_guarantee`, `queue_processor_loop`), `app/k8s_client.py` (`apply_toleration`, `annotate_runtime_guarantee`, `emit_runtime_guaranteed_event`).

| Level | Message | Description |
|-------|---------|-------------|
| INFO | `Enqueued pod ns/name for reservation #N (window start–end, N GPU(s) reserved, pod requests N)` | Pod has been matched to a reservation and placed in the work queue. |
| INFO | `Pod ns/name arrived inside reservation window; attempting immediate toleration` | A newly-ADDED pod landed during an already-open window; toleration is attempted without waiting for the queue-processor tick. |
| DEBUG | `Pod ns/name: GPU budget full (N requested > N available of N reserved); retry in N s` | Toleration not applied because the reservation's GPU budget is exhausted; will retry after a 2–5 min jitter delay. |
| INFO | `Pod ns/name already has toleration; dequeuing` | The pod was already carrying the toleration (applied externally or by a previous attempt); removed from the queue. |
| INFO | `Applied toleration key=value:NoSchedule to pod ns/name (booking=ref)` | Toleration patch succeeded; pod is now admitted under the given booking reference. |
| INFO | `Recorded runtime guarantee on pod ns/name: Ns (until <time>)` | The pod's runtime guarantee has been annotated (`horae/pod-runtime-limit-seconds`, `horae/guaranteed-until`); the guarantee is informational only and is not enforced by Kubernetes. |
| INFO | `Emitted RuntimeGuaranteed event for pod ns/name (guaranteed=Ns, until=<time>)` | A `RuntimeGuaranteed` Kubernetes event has been created on the pod explaining when the guarantee ends. |
| WARNING | `Failed to record runtime guarantee on pod ns/name: <exception>` | Recording the guarantee failed after the toleration was already applied; best-effort, toleration is not revoked. |
| WARNING | `Error processing pod ns/name: <exception>; retry in N s` | Toleration patch or pod re-read failed with a transient error; the optimistic occupancy record was rolled back and the entry remains in the queue. |
| INFO | `Reservation #N window expired; removing pod ns/name from queue` | The reservation window closed before the pod could be admitted; the entry is dropped from the queue. |
| INFO | `Pod ns/name re-queued: reservation #N cancelled, now targeting reservation #N` | A queue entry's reservation was cancelled; the pod has been re-matched to a new reservation. |
| INFO | `Pod ns/name removed from queue: reservation #N cancelled and no replacement found` | A queue entry's reservation was cancelled and no substitute reservation could be found; the pod is dropped from the queue. |
| DEBUG | `Dequeued pod ns/name (uid=uid)` | Pod was removed from the reserved-path work queue (deletion, toleration already present, or window expiry). |

---

## JIT on-demand candidate watch

Emitted as pods enter and leave the on-demand candidate queue via watch events.
Sources: `app/main.py` (`pod_watch_loop`), `app/controller.py` (`add_ondemand_candidate`, `remove_ondemand_candidate`).

| Level | Message | Description |
|-------|---------|-------------|
| DEBUG † | `Pod ns/name ADDED: no admittable reservation (gpu-class=c); routing to JIT on-demand queue` | A newly-ADDED Pending pod has no reservation open now or opening within `ONDEMAND_HORIZON_MINUTES`; it is JIT-eligible and becomes an on-demand candidate. |
| INFO | `On-demand candidate: pod ns/name (uid=uid, gpu-class=c, gpus=N, min-runtime=Ns)` | Pod has been registered as a JIT on-demand candidate. |
| INFO † | `On-demand candidate ns/name deleted before a lease was granted (gpu-class=c, gpus=N, min-runtime=Ns, submitted=<time>, deleted=<time>, waited=Ns)` | An on-demand candidate was deleted from Kubernetes before the controller could secure a lease for it — unmet demand. |
| DEBUG | `Removed on-demand candidate ns/name (uid=uid)` | Pod was removed from the on-demand candidate list (deletion, terminal phase, routed to the reserved queue instead, or a lease was granted). |

---

## JIT lease request

Emitted during each attempt to secure a lease for an on-demand candidate
(`main._try_request_lease`, called from the pod-watch fast path and the
queue-processor's FIFO retry pass).
Sources: `app/main.py` (`_try_request_lease`), `app/reservation_client.py`
(`create_ondemand_reservation`), `app/k8s_client.py` (`apply_toleration`,
`annotate_runtime_guarantee`, `emit_runtime_guaranteed_event`).

| Level | Message | Description |
|-------|---------|-------------|
| WARNING | `Error reading on-demand candidate ns/name: <exception>; will retry` | The pod could not be re-read at the top of the attempt; candidate is kept, cooldown applied. |
| INFO | `On-demand candidate ns/name is <phase>; dropping` | Pod reached a terminal phase (Succeeded/Failed/Unknown) before a lease was requested; candidate is removed. |
| INFO | `On-demand candidate ns/name: not GPU-only-pending ('…'); dropping` | Pod is Pending for a non-GPU reason; the controller's toleration cannot unblock it, so the candidate is dropped (guard 1). |
| DEBUG | `On-demand candidate ns/name: scheduling conditions not yet set; retry shortly` | The pod's `PodScheduled` condition is not yet populated; the GPU-only-pending check cannot run yet (guard 1). |
| DEBUG | `On-demand candidate ns/name: safety interlock active for gpu-class=c; retry shortly` | Lease requests are held for this GPU class because a reservation-holder pod is stuck Pending (guard 3). |
| WARNING | `On-demand candidate ns/name: gpu-class=c has no known id; retry later` | The pod's `gpu-class` label has no entry in `gpu_class_ids`; cannot build a lease request yet. |
| INFO | `On-demand lease request denied for pod ns/name (gpu-class=c, gpus=N); retrying later` | `POST /api/reservations` returned a denial (409/error); candidate cools down 2–5 min. |
| INFO | `On-demand lease #N granted for pod ns/name (gpu-class=c, gpus=N, duration=Ns)` | A lease was granted; the controller is about to admit the pod under it. |
| WARNING | `Admission failed after granting lease #N for pod ns/name; issuing compensating cancel` | The pod could not actually be admitted under the granted lease (budget race, transient patch error, or the pod going terminal); a compensating cancel is issued and the lease is dropped from state. |
| — | *(shares the reserved-path admission log lines below — "Applied toleration…", "Recorded runtime guarantee…", "Emitted RuntimeGuaranteed event…" — via `_try_apply_toleration`)* | Successful admission under a granted lease reuses the exact same admission path and log lines as any reserved-path pod. |

---

## Scheduling-gate removal

Emitted when `POD_SCHEDULING_GATE_NAME` is configured and the controller removes
the named gate after admitting a pod (both reserved and on-demand paths).
Sources: `app/k8s_client.py` (`remove_scheduling_gate`), `app/main.py`
(`_enforce_scheduling_gate_removal`).

| Level | Message | Description |
|-------|---------|-------------|
| DEBUG | `k8s: scheduling gate 'name' not present on pod ns/name; skipping removal` | The configured gate is not on the pod (already removed, or never set); nothing to patch. |
| DEBUG | `k8s: removing scheduling gate 'name' from pod ns/name` | About to issue the strategic-merge `$patch: delete` for the named gate. |
| INFO | `Removed scheduling gate 'name' from pod ns/name` | The gate was removed successfully; the scheduler can now place the pod. |
| WARNING | `Failed to remove scheduling gate 'name' from pod ns/name: <exception>` | Best-effort gate removal failed after admission; the toleration is not revoked. |

---

## On-demand safety interlock (guard 3)

Emitted when the safety interlock protecting reservation-holder pods is toggled.
Source: `app/main.py` (`queue_processor_loop`).

| Level | Message | Description |
|-------|---------|-------------|
| WARNING | `Safety interlock activated for gpu-class=c: N reservation-holder pod(s) stuck Pending (ns/name, …); on-demand placement for this class held` | One or more holder pods are stuck Pending on this GPU class; JIT lease requests are suspended for the class until they schedule. |
| INFO | `Safety interlock cleared for gpu-class=c: on-demand placement resumed` | All holder pods for this GPU class have scheduled; JIT lease requests are re-enabled. |

---

## Preemption sweep †

Emitted by the periodic capacity-recovery sweep that recovers GPUs from pods
running past their runtime guarantee. Sources: `app/main.py`
(`preemption_loop`, `_run_preemption_sweep`, `_preempt_pod`), `app/k8s_client.py`
(`snapshot_node_gpu_capacity`, `delete_pod`, `emit_preempted_event`).

| Level | Message | Description |
|-------|---------|-------------|
| WARNING | `Preemption sweep: failed to snapshot pods: <exception>` | The pod LIST needed to plan preemption failed; the entire sweep is skipped — no kill is ever made on unknown state. |
| WARNING | `Preemption sweep: failed to snapshot node GPU capacity: <exception>` | The node LIST needed to compute physical capacity failed; the entire sweep is skipped. |
| INFO | `Preemption sweep boundary=<time> phase=A/B: demand={class: N, …} free={class: N, …} kills=N` | A boundary/phase was evaluated; shows the per-class demand and free-capacity maps and how many victims were selected. Only logged when there is nonzero demand at the boundary. |
| WARNING | `Preemption sweep boundary=<time> phase=A/B: N GPU(s) of gpu-class=c still short after preempting all eligible overstayers` | Even after preempting every eligible past-guarantee pod of this class, demand could not be fully covered — a signal that no in-hour recovery exists beyond this sweep (see README's *Runtime guarantees and demand-driven preemption*). |
| INFO | `Preempting pod ns/name (gpu-class=c, gpus=N): <message>` | About to delete a selected victim; `<message>` explains the overstay duration and which boundary's demand triggered it. |
| INFO | `Emitted Preempted event for pod ns/name` | A `Preempted` Kubernetes event has been created on the pod, immediately before deletion. |
| INFO | `Deleted pod ns/name` | Pod deletion succeeded (shared log line with cancellation/owner-change eviction — see *Kubernetes API traces*). |
| WARNING | `Could not emit Preempted event for pod ns/name: <exception>` | Best-effort event emission failed; pod deletion will still be attempted. |
| WARNING | `Could not delete pod ns/name: <exception>` | Pod deletion failed; the pod will persist until manually removed or the next sweep. |

---

## Occupancy

Emitted as the controller tracks GPU utilisation across all admission paths.
Source: `app/controller.py` (`record_placement`, `release_pod`, `reconcile_occupancy`).

| Level | Message | Description |
|-------|---------|-------------|
| DEBUG | `Recorded placement: reservation #N ← pod uid=uid (N GPU(s)); N/N free` | A pod's GPU allocation has been recorded in the in-memory occupancy map; shows remaining free capacity on the reservation. |
| INFO | `Released capacity: reservation #N ← pod uid=uid freed N GPU(s)` | A pod has vacated its slot; GPU capacity on the reservation is freed. |
| INFO † | `Occupancy reconciled: N reservation(s), N GPU(s) in use (was N)` | The occupancy map was rebuilt from the live cluster snapshot and the GPU-in-use count changed — a missed watch event was self-healed. |
| DEBUG † | `Occupancy reconciled: N reservation(s), N GPU(s) in use` | Steady-state occupancy reconciliation; count matches the previous tick. |

---

## Kubernetes API traces

Low-level traces of every outbound Kubernetes API call.  Visible only at
`LOG_LEVEL=DEBUG`.  Source: `app/k8s_client.py`.

| Level | Message | Description |
|-------|---------|-------------|
| DEBUG | `k8s: read_namespaced_pod ns/name` | About to fetch the current pod object before patching. |
| DEBUG | `k8s: list_pod_for_all_namespaces selector=gpu-class (tolerated snapshot)` | About to LIST all GPU-class pods to rebuild the tolerated-pod snapshot. |
| DEBUG | `k8s: tolerated snapshot returned N pod(s)` | LIST completed; N pods carry the controller's toleration. |
| DEBUG | `k8s: patch_namespaced_pod ns/name (add toleration key=value:NoSchedule, booking=ref)` | About to PATCH a pod to add the reservation toleration and booking-reference annotation. |
| DEBUG † | `k8s: patch_namespaced_pod ns/name (runtime guarantee: Ns, until <time>)` | About to PATCH a pod's `horae/pod-runtime-limit-seconds` / `horae/guaranteed-until` annotations; these are informational only and not enforced by Kubernetes. |
| DEBUG | `k8s: create_namespaced_event ns (pod=name, reason=RuntimeGuaranteed)` | About to create a `RuntimeGuaranteed` event on the pod. |
| DEBUG | `k8s: create_namespaced_event ns (pod=name, reason=Preempted)` † | About to create a `Preempted` event on a pod being deleted to recover capacity. |
| DEBUG | `k8s: create_namespaced_event ns (pod=name, reason=ReservationCancelled)` | About to create a `ReservationCancelled` event on the pod. |
| DEBUG † | `k8s: list_node (gpu capacity snapshot)` | About to LIST all nodes to compute physical GPU capacity per class (`snapshot_node_gpu_capacity`). |
| DEBUG † | `k8s: node gpu capacity snapshot: {class: N, …}` | Node LIST completed; shows total allocatable GPUs summed per `gpu-class-reservation` taint value. |
| DEBUG | `k8s: delete_namespaced_pod ns/name` | About to DELETE a pod (reservation cancellation eviction, owner-change eviction, or preemption sweep). |
| INFO | `Deleted pod ns/name` | Pod deletion succeeded. |
| DEBUG | `Pod ns/name already gone (404)` | DELETE returned 404; pod had already been removed. |
| INFO | `Emitted ReservationCancelled event for pod ns/name` | `ReservationCancelled` event created on the pod. |

---

## Pod watch stream

Emitted by the watch reconnection loop inside `PodWatcher`.
Source: `app/k8s_client.py` (`PodWatcher._run_watch`).

| Level | Message | Description |
|-------|---------|-------------|
| DEBUG | `k8s: list_pod_for_all_namespaces selector=gpu-class` | Starting a fresh LIST+WATCH cycle; about to issue the seed LIST. |
| DEBUG | `k8s: list returned N pod(s), resourceVersion=rv` | Seed LIST complete; WATCH stream will resume from this resource version. |
| DEBUG | `k8s: watch list_pod_for_all_namespaces selector=gpu-class resourceVersion=rv timeout_seconds=270` | About to open the incremental WATCH stream. |
| DEBUG | `k8s: watch event <type> pod ns/name` | A raw ADDED / MODIFIED / DELETED event arrived from the API server. |
| WARNING | `Pod watch stream error (failure #N): <exception>; reconnecting in 5 s` | Watch stream failed; logged at WARNING on the first failure and every 120th subsequent failure to avoid log spam during sustained disconnects. |
| DEBUG | `Pod watch stream ended (<exception>); reconnecting in 5 s` | Watch stream ended normally or with a minor error; reconnecting. |

---

## Pod annotation warnings

Emitted when a pod carries a malformed annotation.
Source: `app/k8s_client.py` (`get_pod_min_runtime_seconds`).

| Level | Message | Description |
|-------|---------|-------------|
| WARNING | `Pod ns/name has non-integer horae/minimum-runtime-seconds='value'; ignoring` | The `horae/minimum-runtime-seconds` annotation is not a positive integer; the pod will not be treated as an on-demand candidate. |
