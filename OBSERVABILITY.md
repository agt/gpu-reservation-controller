# Observability reference

Every log point the controller emits, grouped by the loop or flow it belongs to.

Line format and field meanings are defined in **`docs/LOG-FIELDS.md`** — the
canonical dictionary, shared verbatim with the reservation app. This file is the
inventory: *which* lines exist, at what level, and what each one tells you. Field
names below are the ones that appear on the line; look them up in the dictionary
for types and semantics.

`LOG_LEVEL` (default `INFO`) sets the threshold. Everything marked DEBUG below is
suppressed unless you set `LOG_LEVEL=DEBUG`. All timestamps and all window
arithmetic are UTC; `TZ` affects only how the timestamp column is rendered.

---

## Reading a line

```
2026-07-28 14:03:11,204 INFO     app.main: actor=controller trace=pod-1a2b3c4d5e event=pod.admitted ns=jdoe pod=notebook-0 rid=42 clabel=h100 gpus=2 free=6 reserved=8 until=2026-07-28T16:00:00+00:00
```

- **`actor=controller`** is constant — this daemon has one principal. The field
  exists so a controller line and an app line parse under the same grammar.
- **`trace=`** is the unit of work — see below.
- **`ns=` / `pod=`** identify the pod; **`poduid=`** is its Kubernetes UID.
- **`rid=`** is the reservation. It is the same id space the app calls `id=` on
  its own `reservation.*` events.

## Trace ids

Every line carries a `trace=` naming the unit of work that produced it. The
prefix says what kind:

| prefix | unit of work |
|---|---|
| `fetch-` | one reservation refresh cycle |
| `queue-` | one queue-processor tick (incl. the merge/adopt/lease work it fans out) |
| `sweep-` | one preemption sweep |
| `audit-` | one capacity audit |
| `jit-` | one JIT admission pass (a coalesced re-run gets its own id) |
| `pod-` | one pod watch event |
| `startup-` | the synchronous startup sequence |
| `push-` / `forecast-` | one inbound API request that supplied no id of its own |
| `-` | no unit of work in scope |

**The id crosses to the app.** It is sent as `X-Client-Trace` on every outbound
call, and the app's request middleware already read that header — so the app
logs its side of a controller-initiated operation under the *same* id. Inbound
is symmetric: a push from the app carrying a trace is logged here under the
app's id, so a user's cancel and the eviction it causes share one trace.

Because a watch stream interleaves, the trace is what separates two concurrent
pods' handling in the log; timestamps cannot.

Cross-repo joins:

```bash
# Everything one operation did, on both sides — including lines sharing no object.
grep 'trace=jit-1a2b3c4d5e' controller.log app.log

# One pod's whole life.
grep 'poduid=8f3a…' controller.log app.log

# A reservation from booking through admission to preemption.
grep -E 'event=reservation\.created id=42|rid=42' app.log controller.log
```

`trace` joins an **operation**; `rid` and `poduid` join an **object**. Both
matter: a JIT batch that grants three leases has one trace across six lines on
two sides, while `rid=42` follows that one reservation for its whole life.
`poduid` is the strongest object join — a JIT lease's idempotency key **is** the
pod UID, so `event=reservation.lease_created … poduid=X` in the app and
`event=lease.granted … ns=… pod=…` here refer to the same grant.

---

## 1. Startup and shutdown — `app/main.py` (`lifespan`)

| Level | `event=` | Fields | Notes |
|---|---|---|---|
| INFO | `startup.initial_fetch` | — | Synchronous first fetch, before the loops launch. |
| INFO | `startup.initial_fetch_complete` | `reservations classes` | |
| INFO | `startup.noshow_armed` | `watched` | No-show deadlines armed. |
| ERROR | `startup.initial_fetch_failed` | `err retry_s` | Controller continues degraded; pod matching is delayed until a retry succeeds. |
| WARNING | `startup.capacity_audit_failed` | `err` | The synchronous first audit failed. |
| INFO | `startup.ready` | — | All five background loops running. |
| INFO | `shutdown.start` / `shutdown.complete` | — | |
| INFO | `k8s.auth` | `mode` (+ `path`) | `mode=kubeconfig` or `in_cluster`. |

---

## 2. Reservation fetch — `app/main.py`, `app/reservation_client.py`

| Level | `event=` | Fields | Notes |
|---|---|---|---|
| DEBUG | `fetch.start` | — | Confirms the loop is alive between INFO events. |
| INFO | `api.reservations_fetched` | `count active cancelled lookahead_days` | Paginated `status=all` fetch. |
| INFO | `class.resolved` | `cid class clabel` | Per-id fallback resolved a class missing from the bulk list. |
| WARNING | `class.unresolvable` | `cid reason` | No `label_value` — **pods for this class can never be matched** until it is configured. |
| INFO | `lease.preserved` | `count ids reason` | Locally-granted leases re-added because the snapshot predated the grant. |
| INFO | `fetch.complete` | `reservations classes` | |
| ERROR | `fetch.failed` | `err` | Whole cycle failed; previous state retained. |
| WARNING | `api.gpu_class_fetch_failed` / `api.gpu_class_parse_failed` | `cid` + `status` \| `err` | Per-class fallback. |
| WARNING | `api.gpu_classes_fetch_failed` / `api.gpu_classes_parse_failed` | `status` \| `err` | Bulk list; the previous cycle's maps are kept rather than losing all resolution. |

---

## 3. Reserved-path pod admission — `app/main.py`, `app/controller.py`, `app/k8s_client.py`

| Level | `event=` | Fields | Notes |
|---|---|---|---|
| INFO | `pod.enqueued` | `ns pod rid start end reserved gpus` | Matched to a reservation, queued. |
| INFO | `pod.fast_path` | `ns pod rid` | ADDED inside an already-open window — toleration attempted immediately. |
| DEBUG | `pod.budget_full` | `ns pod rid gpus free reserved` | Retry in 2–5 min. |
| INFO | `pod.toleration_applied` | `ns pod tol_key tol_value booking_ref` | The patch landed. |
| INFO | `pod.admitted` | `ns pod rid clabel gpus free reserved until` | The one line to grep for a successful admission. |
| INFO | `pod.guarantee_recorded` | `ns pod guarantee_s until` | Informational annotations; Kubernetes enforces nothing. |
| INFO | `k8s.event_emitted` | `ns pod reason` (+ `guarantee_s until` \| `rid until`) | `reason=RuntimeGuaranteed` \| `Preempted` \| `ReservationCancelled` \| `ReservationReassigned` \| `OverstayRelinked`. |
| INFO | `pod.dequeued` | `ns pod reason` | e.g. `toleration_already_present`. |
| INFO | `pod.queue_dropped` | `ns pod` + `rid reason` \| `phase reason` | Window expired, reservation cancelled with no replacement, or the pod went terminal. |
| INFO | `pod.requeued` | `ns pod reason old.rid new.rid` | Re-matched after its reservation was cancelled. |
| WARNING | `pod.admission_error` | `ns pod rid err` | Optimistic placement rolled back; entry stays queued. |
| WARNING | `pod.guarantee_record_failed` | `ns pod err` | Best-effort — the toleration is **not** revoked. |
| INFO | `pod.gate_removed` / DEBUG `pod.gate_absent` | `ns pod gate` | `POD_SCHEDULING_GATE_NAME` handling. |
| WARNING | `pod.gate_remove_failed` | `ns pod gate err` | Also best-effort. |
| DEBUG | `pod.routed_jit` | `ns pod clabel reason` | No admittable reservation → JIT queue. |
| DEBUG | `pod.left_pending` | `ns pod reason` | No match and not JIT-eligible (missing group label or minimum-runtime annotation). |

---

## 4. Occupancy — `app/controller.py`

| Level | `event=` | Fields | Notes |
|---|---|---|---|
| DEBUG | `occupancy.placed` | `rid poduid gpus free reserved` | |
| INFO | `occupancy.released` | `rid poduid gpus` | |
| INFO | `occupancy.reconciled` | `reservations old.used new.used` | **INFO only when the count changed** — i.e. a missed watch event just self-healed. |
| DEBUG | `occupancy.reconciled` | `reservations used` | Steady state. |

---

## 5. JIT on-demand leases — `app/main.py`, `app/reservation_client.py`

| Level | `event=` | Fields | Notes |
|---|---|---|---|
| INFO | `ondemand.candidate_added` | `ns pod poduid clabel gpus min_runtime_s group` | |
| DEBUG | `ondemand.candidate_removed` | `ns pod poduid` | |
| INFO | `ondemand.candidate_dropped` | `ns pod reason` (+ `phase` \| `detail`) | Terminal phase, or Pending for a non-GPU reason. |
| DEBUG/INFO/WARNING | `ondemand.candidate_held` | `guard reason ns pod` (+ `clabel gpus node_free`) | **The guard number is the field** — see below. |
| DEBUG | `ondemand.schedule_verdict` | `ns pod` | Scheduler verdict arrived; re-attempting immediately. |
| INFO | `lease.denied` | `ns pod clabel gpus` | App refused (409); cooldown 2–5 min. |
| INFO | `lease.granted` | `rid ns pod clabel gpus lease_dur_s` | |
| WARNING | `lease.admission_failed` | `rid ns pod detail` | Grant landed but admission did not — a compensating cancel follows. |
| INFO | `lease.teardown` | `rid class reason` | The lease's pod went away; the lease is cancelled. |
| INFO | `ondemand.unmet_demand` | `ns pod clabel gpus min_runtime_s submitted deleted waited_s` | **Demand the controller never satisfied** — the pod was deleted before any lease. |
| WARNING | `ondemand.selection_unavailable` | `fallback candidates` | Delegation call failed; granting all. |
| WARNING | `ondemand.unknown_grant` | `poduid` | App returned a uid that was never offered; ignored. |
| WARNING | `ondemand.pod_read_failed` | `ns pod err` | |
| INFO/WARNING | `api.lease_denied` / `api.lease_failed` / `api.lease_parse_failed` | `poduid` + `status` \| `err` | Client-side view of the same request. |

**Guards** (`guard=`), all short-retry rather than drop:

| `guard=` | `reason=` | Meaning |
|---|---|---|
| 1 | `schedule_verdict_pending` | `PodScheduled` not yet set; GPU-only-pending is indeterminate. |
| 3 | `stuck_holder_interlock` | A reservation holder is stuck Pending on this class. |
| 4 | `class_overcommitted` | App-side capacity exceeds physical (see §8). |
| 5 | `no_single_node_fit` | No single node has enough free GPUs for a ≥2-GPU ask. Fail-open when unknown. |
| — | `class_id_unknown` | The `gpu-class` label has no numeric id yet. |

`grep 'event=ondemand.candidate_held guard=4'` answers "how often is the capacity
audit blocking admission" without matching on message text.

---

## 6. Preemption sweep — `app/main.py`

| Level | `event=` | Fields | Notes |
|---|---|---|---|
| INFO | `preempt.boundary` | `boundary sweep clabel demand free kills` | **One line per GPU class**, not one carrying two dicts. |
| WARNING | `preempt.unmet` | `boundary sweep clabel short` | Demand uncovered after preempting every eligible overstayer. |
| INFO | `pod.preempting` | `ns pod clabel gpus detail` | `detail` explains the overstay and the triggering boundary. |
| INFO | `pod.deleted` | `ns pod` | |
| WARNING | `preempt.snapshot_failed` | `target err` | `target=pods` or `node_capacity`. **The whole sweep is skipped** — never kill on unknown physical state. |
| WARNING | `preempt.selection_unavailable` | `fallback` | App delegation failed; local uniform-random used. |
| WARNING | `preempt.unknown_victim` | `poduid` | App named a pod that was never offered; ignored. |
| ERROR | `preempt.sweep_failed` | `err` | |
| DEBUG | `preempt.demand_skipped` | `rid reason` | Reservation's class label unresolvable. |

`sweep=A` is the lead-time phase (proactive, `boundary > now`); `sweep=B` is
at-boundary. `phase=` on other lines means the **pod lifecycle phase** — the two
were deliberately given different keys.

---

## 7. Guarantee status, warnings, adoption and merge — `app/main.py`, `app/k8s_client.py`

| Level | `event=` | Fields | Notes |
|---|---|---|---|
| INFO | `pod.guarantee_status` | `ns pod gstatus` | `gstatus=guaranteed|overstay`; patched only on a real transition. |
| WARNING | `pod.guarantee_status_failed` | `ns pod err` | Retried next tick. |
| INFO | `pod.termination_warned` | `ns pod at risk` | `at` is the projected kill instant. |
| INFO | `pod.termination_warning_cleared` | `ns pod` | Pod left the at-risk pool. |
| WARNING | `pod.termination_warning_failed` | `ns pod err` | |
| INFO | `pod.relinked` | `ns pod rid until reason` (+ the retired lease id) | `reason=adoption` (past-guarantee rescue) or `ondemand_merge` (a lease folded into a now-open booking, carrying an `old.` field for the lease it retired). |
| WARNING | `pod.relink_failed` / `pod.merge_failed` | `ns pod rid err` | |

---

## 8. Capacity audit — `app/main.py`

| Level | `event=` | Fields | Notes |
|---|---|---|---|
| WARNING | `capacity_audit.mismatch` | `clabel app_gpus phys_gpus overcommitted` | **`overcommitted=true` is the direction that pauses admission**; `false` is under-provisioning, logged but harmless. |
| INFO | `capacity_audit.paused` / `capacity_audit.resumed` | `clabels` | Classes entering/leaving the JIT pause set. |
| WARNING | `capacity_audit.snapshot_failed` | `target err` | Audit skipped; **the existing pause set is left unchanged** — a transient failure must never silently lift a pause. |
| ERROR | `capacity_audit.failed` | `err` | |

---

## 9. Queue processor tick — `app/main.py`

| Level | `event=` | Fields | Notes |
|---|---|---|---|
| DEBUG | `queue.tick` | `queued candidates` | |
| WARNING | `queue.snapshot_failed` | `target err` | `target=pods` or `node_inventory`; prior state kept. |
| WARNING | `interlock.activated` | `clabel guard count pods` | Guard-3 interlock on; JIT held for that class. |
| INFO | `interlock.cleared` | `clabel guard` | |
| DEBUG | `queue.node_feasibility` | `clabel node_free` | One line per class. |

---

## 10. No-show tracking — `app/controller.py`, `app/main.py`

| Level | `event=` | Fields | Notes |
|---|---|---|---|
| DEBUG | `noshow.armed` | `rid reason deadline` | `reason=init|new`. |
| INFO | `noshow.declared` | `rid user clabel reason` | Queued for a durable app-side cancel. |
| INFO | `noshow.cancelled` | `rid` | The cancel landed — durably gone from the app's active set. |
| WARNING | `noshow.cancel_failed` | `rid` | Retried next tick. |
| INFO | `noshow.cancel_skipped` | `rid reason` | A pod appeared at the last second. |
| DEBUG | `noshow.cleared` | `rid` \| `ids` + `ns clabel reason` | Holder pod admitted — chained windows are cleared together. |
| INFO | `noshow.removed` / DEBUG `noshow.deadline_pruned` / `noshow.cancel_pruned` | `rid reason` | Left the active list. |
| DEBUG | `noshow.skipped` | `rid reason` | Window already ended; never declared. |

---

## 11. Cancellation and owner-change eviction — `app/main.py`

| Level | `event=` | Fields | Notes |
|---|---|---|---|
| INFO | `cancel.pods_relinked` | `rid count` | Adoption rescued pods instead of evicting them. |
| INFO | `cancel.evicting` | `rid count detail` | |
| INFO | `owner_change.evicting` | `rid count detail old.user` | Reservation reassigned to a teammate. |
| WARNING | `cancel.snapshot_failed` / `owner_change.snapshot_failed` | `target err` | Eviction skipped this cycle. |
| WARNING | `k8s.event_failed` | `ns pod reason err` | Best-effort; deletion still attempted. |
| WARNING | `pod.delete_failed` | `ns pod err` | |

---

## 12. Inbound APIs — `app/main.py`

| Level | `event=` | Fields | Notes |
|---|---|---|---|
| INFO | `push.applied` | `upserts cancellations owner_changes reservations` | `POST /api/reservations/push`. |
| WARNING | `forecast.snapshot_failed` | `target err` | `GET /api/forecast/preemption-risk` → 503; never report risk from unknown physical state. |

---

## 13. Outbound API failures — `app/reservation_client.py`

All best-effort: every one of these returns `None`/`False` rather than raising.

| Level | `event=` | Fields |
|---|---|---|
| INFO | `api.cancel_already_gone` | `rid reason status` (404 treated as success) |
| WARNING | `api.cancel_failed` | `rid reason` + `status` \| `err` |
| WARNING | `api.victim_selection_failed` / `api.victim_selection_parse_failed` | `status` \| `err` |
| WARNING | `api.admission_selection_failed` / `api.admission_selection_parse_failed` | `status` \| `err` |
| WARNING | `api.overstay_report_failed` | `rid poduid` + `status` \| `err` |

---

## 14. Kubernetes API traces — `app/k8s_client.py`

DEBUG only, unless noted.

| Level | `event=` | Fields |
|---|---|---|
| DEBUG | `k8s.read_pod` | `ns pod` |
| DEBUG | `k8s.list_pods` / `k8s.list_pods_done` | `selector purpose` / `purpose count` (+ `rv`) |
| DEBUG | `k8s.list_nodes` | `purpose` |
| DEBUG | `k8s.node_inventory` | `clabel nodes total` — one line per class |
| DEBUG | `k8s.patch_pod` | `ns pod patch` + the patch's own fields |
| DEBUG | `k8s.create_event` / `k8s.delete_pod` | `ns pod` (+ `reason`) |
| DEBUG | `pod.already_gone` | `ns pod status` (404 on delete) |
| WARNING | `k8s.node_allocatable_invalid` | `node resource value` — treated as 0 |
| WARNING | `pod.annotation_invalid` | `ns pod annotation value` — malformed `horae/minimum-runtime-seconds` |
| DEBUG | `k8s.watch_open` / `k8s.watch_event` | `selector rv timeout_s` / `watch_event ns pod` |
| WARNING | `k8s.watch_error` | `fails err retry_s` — **first failure and every 120th**, to bound spam during a sustained disconnect |
| DEBUG | `k8s.watch_ended` | `err retry_s` |

---

## Guarantees this inventory rests on

- **Every line goes through `kv()`**, enforced by `tests/test_log_grammar.py`,
  which also fails if a field is emitted that `docs/LOG-FIELDS.md` does not
  document. That test is what keeps this file from drifting again.
- **Fail-safe paths log and stop**: any snapshot failure skips the work rather
  than acting on unknown physical state. Grep `event=.*snapshot_failed` to find
  every instance of that pattern.
