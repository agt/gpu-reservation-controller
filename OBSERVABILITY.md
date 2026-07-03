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
| INFO | `GPU reservation controller started` | All three background loops are running. |
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
| INFO | `GPU class N (name) → label_value='value'` | A previously-unseen GPU class has been resolved to its Kubernetes label value; result is cached. |
| WARNING | `Could not fetch GPU class N: HTTP <code>` / `Could not fetch GPU class N: <exception>` | A per-class detail fetch failed (HTTP status or network error); the class is skipped this cycle and `fetch_gpu_class` returns None. |
| WARNING | `Could not parse GPU class N response: <exception>` | The GPU-class payload failed schema validation / JSON decoding; `fetch_gpu_class` returns None rather than aborting the refresh. |
| WARNING | `Could not fetch app settings: HTTP <code>` / `Could not fetch app settings: <exception>` | The `GET /api/settings` fetch failed; reclaim-merge guard is left unknown for this cycle. |
| WARNING | `Could not parse app settings response: <exception>` | The settings payload failed schema validation / JSON decoding; `fetch_settings` returns None rather than aborting the refresh. |
| WARNING | `GPU class N has no label_value; pods for this class cannot be matched to reservations` | A GPU class referenced by an active reservation has no `label_value` set; pods for that class cannot be matched until the class is configured. |
| INFO † | `Reservation refresh complete: N active reservation(s), N GPU class(es) resolved` | Periodic refresh completed successfully; current active reservation and GPU-class counts. |
| ERROR | `Reservation refresh failed: <exception>` | An entire refresh cycle failed (network error, API error, etc.); previous state is retained. |

---

## Reservation cancellation

Emitted when an in-window reservation is detected as cancelled.
Sources: `app/main.py` (`_handle_cancelled_reservations`), `app/controller.py` (`record_cancelled_reservation`, `cleanup_cancelled_reservations`).

| Level | Message | Description |
|-------|---------|-------------|
| WARNING | `Could not snapshot pods for cancellation eviction: <exception>` | The cluster LIST needed to evict pods failed; eviction is skipped for this cycle. |
| INFO | `Evicting N pod(s) for cancelled reservation #N (by <user>)` | N pods admitted under a now-cancelled reservation are being evicted. |
| WARNING | `Could not emit ReservationCancelled event for pod ns/name: <exception>` | Best-effort Kubernetes event emission failed; pod deletion will still be attempted. |
| WARNING | `Could not delete pod ns/name: <exception>` | Pod deletion failed; the pod will persist until manually removed or the next eviction cycle. |
| INFO | `Reservation #N (user=u, gpu-class=c, N GPU(s)) cancelled mid-window; freed capacity available for on-demand placement until <time>` | Cancelled reservation's GPU capacity is now available to on-demand candidates until the window ends. |
| DEBUG | `Cancelled reservation #N window ended; removed from on-demand pool` | The freed-capacity record for a cancelled reservation has been pruned because its window has closed. |

---

## No-show tracking

Emitted during no-show deadline management.
Source: `app/controller.py` (`initialize_noshow_tracking`, `update_noshow_tracking`, `reconcile_noshow`, `check_noshow_deadlines`, `mark_pod_seen_for_noshow`).

| Level | Message | Description |
|-------|---------|-------------|
| DEBUG | `No-show tracking: reservation #N deadline=<time>` | A no-show deadline was set for a reservation during startup initialisation. |
| DEBUG | `No-show tracking (new): reservation #N deadline=<time>` | A no-show deadline was set for a reservation newly seen in a periodic refresh. |
| DEBUG | `No-show deadline pruned: reservation #N left active list` | A tracked deadline was removed because the reservation is no longer in the active list. |
| INFO | `No-show reservation #N removed: left active list` | A previously declared no-show reservation has left the active list and is no longer tracked. |
| INFO | `Reservation #N declared no-show (user=u, gpu-class=c): no matching pod appeared before deadline; capacity opened for on-demand placement` | No holder pod appeared within the timeout window; the reservation's GPU capacity is released for on-demand use for the rest of its window. |
| DEBUG | `No-show deadline(s) cleared for reservation(s) [N, …]: holder pod admitted (namespace=ns, gpu-class=c)` | A reserved-path holder pod's booking reference was recognised; no-show deadlines for all windows in the holder's back-to-back chain are cleared. |
| DEBUG | `No-show deadline cleared for reservation #N: matching pod already admitted (namespace=ns, gpu-class=c)` | Fallback path: a pod without a booking reference was matched by namespace + GPU class, and its reservation's no-show deadline was cleared. |

---

## Reserved-path pod admission

Emitted as pods are matched to reservations, queued, and admitted.
Sources: `app/controller.py` (`enqueue_pod`, `dequeue_pod`, `reconcile_queue`), `app/main.py` (`pod_watch_loop`, `_try_apply_toleration`, `_enforce_deadline`, `queue_processor_loop`), `app/k8s_client.py` (`apply_toleration`, `set_active_deadline`, `emit_runtime_capped_event`).

| Level | Message | Description |
|-------|---------|-------------|
| INFO | `Enqueued pod ns/name for reservation #N (window start–end, N GPU(s) reserved, pod requests N)` | Pod has been matched to a reservation and placed in the work queue. |
| INFO | `Pod ns/name arrived inside reservation window; attempting immediate toleration` | A newly-ADDED pod landed during an already-open window; toleration is attempted without waiting for the queue-processor tick. |
| DEBUG | `Pod ns/name: GPU budget full (N requested > N available of N reserved); retry in N s` | Toleration not applied because the reservation's GPU budget is exhausted; will retry after a 2–5 min jitter delay. |
| INFO | `Pod ns/name already has toleration; dequeuing` | The pod was already carrying the toleration (applied externally or by a previous attempt); removed from the queue. |
| INFO | `Applied toleration key=value:NoSchedule to pod ns/name (booking=ref)` | Toleration patch succeeded; pod is now admitted under the given booking reference. |
| INFO | `Set activeDeadlineSeconds=N on pod ns/name` | `spec.activeDeadlineSeconds` has been patched to cap the pod's runtime to its reservation window(s). |
| INFO | `Emitted RuntimeCapped event for pod ns/name (deadline=Ns)` | A `RuntimeCapped` Kubernetes event has been created on the pod explaining the deadline. |
| WARNING | `Failed to enforce activeDeadlineSeconds on pod ns/name: <exception>` | Deadline enforcement failed after the toleration was already applied; best-effort, toleration is not revoked. |
| WARNING | `Error processing pod ns/name: <exception>; retry in N s` | Toleration patch or pod re-read failed with a transient error; the optimistic occupancy record was rolled back and the entry remains in the queue. |
| INFO | `Reservation #N window expired; removing pod ns/name from queue` | The reservation window closed before the pod could be admitted; the entry is dropped from the queue. |
| INFO | `Pod ns/name re-queued: reservation #N cancelled, now targeting reservation #N` | A queue entry's reservation was cancelled; the pod has been re-matched to a new reservation. |
| INFO | `Pod ns/name removed from queue: reservation #N cancelled and no replacement found` | A queue entry's reservation was cancelled and no substitute reservation could be found; the pod is dropped from the queue. |
| DEBUG | `Dequeued pod ns/name (uid=uid)` | Pod was removed from the reserved-path work queue (deletion, toleration already present, or window expiry). |

---

## On-demand pod watch

Emitted as pods enter and leave the on-demand candidate queue via watch events.
Sources: `app/main.py` (`pod_watch_loop`), `app/controller.py` (`add_ondemand_candidate`, `remove_ondemand_candidate`).

| Level | Message | Description |
|-------|---------|-------------|
| DEBUG † | `Pod ns/name ADDED: no open reservation window (gpu-class=c); routing to on-demand queue` | A newly-ADDED Pending pod has no matching user reservation; it is being routed to the on-demand placement queue. |
| INFO | `On-demand candidate: pod ns/name (uid=uid, gpu-class=c, gpus=N, min-runtime=Ns)` | Pod has been registered as an on-demand placement candidate. |
| INFO † | `On-demand candidate ns/name deleted before placement (gpu-class=c, gpus=N, min-runtime=Ns, submitted=<time>, deleted=<time>, waited=Ns)` | An on-demand candidate was deleted from Kubernetes before the controller could place it onto a block — unmet demand. |
| DEBUG | `Removed on-demand candidate ns/name (uid=uid)` | Pod was removed from the on-demand candidate list (deletion, terminal phase, or successful placement). |

---

## On-demand placement

Emitted during each attempt to place an on-demand candidate onto a reclaim/no-show block.
Sources: `app/main.py` (`_try_place_ondemand`, `_recycle_ondemand_block`, `queue_processor_loop`), `app/controller.py` (`find_ondemand_block`), `app/k8s_client.py` (`apply_toleration`, `set_active_deadline`, `emit_runtime_capped_event`).

| Level | Message | Description |
|-------|---------|-------------|
| DEBUG † | `Selected on-demand block #N for gpu-class=c (window start–end, N/N free)` | `find_ondemand_block` chose block #N as the best fit for the current candidate; logged before returning to the caller. |
| DEBUG | `On-demand candidate ns/name: no suitable block available; retry in N s` | No block currently meets the class, capacity, and minimum-runtime requirements; candidate remains queued. |
| DEBUG | `On-demand candidate ns/name: safety interlock active for gpu-class=c; retry in 30 s` | On-demand placement is held for this GPU class because a reservation-holder pod is stuck Pending (guard 3). |
| DEBUG | `On-demand candidate ns/name: scheduling conditions not yet set; retry in 30 s` | The pod's `PodScheduled` condition is not yet populated; the GPU-only-pending check cannot run yet. |
| INFO | `On-demand candidate ns/name is <phase>; dropping` | Pod reached a terminal phase (Succeeded/Failed/Unknown) before placement; candidate is removed. |
| INFO | `On-demand candidate ns/name: not GPU-only-pending ('…'); dropping` | Pod is Pending for a non-GPU reason; the controller's toleration cannot unblock it, so the candidate is dropped. |
| INFO | `On-demand pod ns/name already has toleration; recording as placed on block #N` | Pod acquired the toleration by some other means; the controller records the occupancy and removes the candidate. |
| INFO | `Placed on-demand pod ns/name onto block #N (gpu-class=c, gpus=N, block has N/N free after placement)` | Toleration applied successfully; pod is now on-demand-admitted under block #N. |
| WARNING | `Failed to enforce activeDeadlineSeconds on on-demand pod ns/name: <exception>` | Deadline cap after on-demand placement failed; best-effort, toleration is not revoked. |
| WARNING | `Error placing on-demand pod ns/name: <exception>; retry in N s` | On-demand placement failed with a transient error; optimistic occupancy record rolled back, candidate will retry. |
| DEBUG † | `Recycling on-demand block for gpu-class=c: N candidate(s) eligible` | A pod vacated an on-demand block; N waiting candidates of the same GPU class are being considered for immediate placement. |
| DEBUG † | `Queue processor tick: N reserved queue entr(ies), N on-demand candidate(s)` | End-of-tick summary showing how many entries remain in each queue after processing. |

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

## On-demand capacity — reclaim-block merging

Emitted during the reclaim-block merge lifecycle.
Source: `app/controller.py` (`reconcile_reclaim_merges`).

| Level | Message | Description |
|-------|---------|-------------|
| INFO | `Merged reclaim block(s) [N, …] into subject reservation #N (gpu-class=c, gpu_count=N); window extended to <time>` | New future reclaim block(s) have been absorbed into a subject block; the subject's effective window is now longer, extending on-demand pod deadlines. |
| DEBUG † | `Re-applied reclaim merge: subject #N extended to <time> (absorbed: [N, …])` | A previously-created merge survived a reservation reload and has been re-applied to the freshly loaded reservation objects. |
| INFO † | `Reclaim merge for subject #N dropped: reservation no longer active or window ended` | A persisted merge was discarded because its subject reservation is gone or the entire merged span has ended. |
| INFO † | `Reclaim merge for subject #N: absorbed block(s) [N, …] no longer active; merge dropped` | A persisted merge was discarded because one or more of its absorbed reclaim blocks were preempted (no longer in the active list). |

---

## On-demand safety interlock (guard 3)

Emitted when the safety interlock protecting reservation-holder pods is toggled.
Source: `app/main.py` (`queue_processor_loop`).

| Level | Message | Description |
|-------|---------|-------------|
| WARNING | `Safety interlock activated for gpu-class=c: N reservation-holder pod(s) stuck Pending (ns/name, …); on-demand placement for this class held` | One or more holder pods are stuck Pending on this GPU class; on-demand placement is suspended for the class until they schedule. |
| INFO | `Safety interlock cleared for gpu-class=c: on-demand placement resumed` | All holder pods for this GPU class have scheduled; on-demand placement is re-enabled. |

---

## Occupancy

Emitted as the controller tracks GPU utilisation across all admission paths.
Source: `app/controller.py` (`record_placement`, `release_pod`, `reconcile_occupancy`).

| Level | Message | Description |
|-------|---------|-------------|
| DEBUG | `Recorded placement: reservation #N ← pod uid=uid (N GPU(s)); N/N free` | A pod's GPU allocation has been recorded in the in-memory occupancy map; shows remaining free capacity on the reservation. |
| INFO | `Released slot: reservation #N ← pod uid=uid freed N GPU(s)` | A pod has vacated its slot; GPU capacity on the reservation is freed. |
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
| DEBUG | `k8s: patch_namespaced_pod ns/name (activeDeadlineSeconds=N)` | About to PATCH a pod to set its runtime deadline. |
| DEBUG | `k8s: create_namespaced_event ns (pod=name, reason=RuntimeCapped)` | About to create a `RuntimeCapped` event on the pod. |
| DEBUG | `k8s: create_namespaced_event ns (pod=name, reason=ReservationCancelled)` | About to create a `ReservationCancelled` event on the pod. |
| DEBUG | `k8s: delete_namespaced_pod ns/name` | About to DELETE a pod (reservation cancellation eviction). |
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
