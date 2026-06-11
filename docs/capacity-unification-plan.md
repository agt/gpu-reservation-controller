# Capacity-tracking unification — design & implementation plan

*Engineering design doc · author: controller team · status: proposed*

## 1. Background & motivation

The controller currently tracks GPU capacity with **two parallel mechanisms**
that never see each other:

| | Reserved path | On-demand / no-show path |
|---|---|---|
| Mechanism | `count_tolerated_gpu_usage` — live namespaced pod LIST, counts pods whose `dsmlp/booking-reference == "res-<id>"` ([k8s_client.py:211](../app/k8s_client.py)) | `ControllerState.ondemand_occupancy` — in-memory `{block_id: {pod_uid: gpu_count}}` ([controller.py:138](../app/controller.py)) |
| Source of truth | the cluster (recomputed each attempt) | the controller's own bookkeeping |
| Restart recovery | nothing to rebuild; re-counts live | reconstructed from `dsmlp/ondemand-block-id` ([main.py:486](../app/main.py)) |
| Scope | one namespace (the holder's) | cross-namespace |
| Race safety | **none** — TOCTOU between count and patch | optimistic reservation before `await` ([main.py:277](../app/main.py)) |

The split is incidental — reserved came first (a live count is the minimal
thing), on-demand was layered on and its needs (cross-namespace counting,
restart reconstruction, race-free reservation) pushed it onto a map. Keeping
both costs us:

- **Bug #3 — double-counting.** Budgets are keyed on the booking-reference
  *string*, so `res-X` and `noshow-X` are disjoint pools over the *same*
  physical reservation `X`. A pod admitted under `res-X` whose deadline is
  **chained** through reservation `X+1` (`compute_max_deadline_seconds`,
  [controller.py:393](../app/controller.py)) physically occupies `X+1`'s window
  while no pod is booked `res-(X+1)`. No-show tracking, which assumes one pod ↔
  one reservation, can then declare `X+1` a no-show and lend its full
  `gpu_count` to `noshow-(X+1)` pods *on top of* the still-running holder.
  Latent with default timings; live when `NOSHOWN_TIMEOUT_MINUTES` is small.
- **Annotation redundancy (#2).** `dsmlp/ondemand-block-id` re-encodes the id
  already embedded in `dsmlp/booking-reference`. Two sources of truth for one
  value.
- **Two mental models, two reconciliation paths, an un-self-healing on-demand
  map, and a racy reserved path.**

## 2. Goals & non-goals

**Goals**

1. One occupancy model keyed by **reservation id**, covering reserved,
   on-demand, and no-show pods alike. `kind` varies only eligibility, deadline
   policy, and the (cosmetic) booking-reference prefix.
2. Structurally close **#3**: a reservation a live holder is chained through is
   never lent out as on-demand/no-show capacity.
3. Retire **`dsmlp/ondemand-block-id`** (#2): reconstruct occupancy from
   `dsmlp/booking-reference`.
4. Preserve the on-demand map's race-free optimistic reservation, and recover
   the live count's self-healing for *all* paths.

**Non-goals**

- No change to matching rules, the three guards, runtime-cap arithmetic, or the
  no-show timeout/grace semantics (beyond the chain-awareness in goal 2).
- `dsmlp/pod-runtime-limit-seconds` stays (consumed by in-pod notification
  widgets).
- No persistent store — still fully in-memory, rebuilt from the cluster + API.

## 3. Design

### 3.1 One occupancy map for all kinds

Generalise `ondemand_occupancy` into a single map keyed by reservation id that
holds **every** admitted pod regardless of kind:

```
occupancy: dict[int, dict[str, int]]     # reservation_id -> {pod_uid: gpu_count}
```

`ControllerState` gains kind-agnostic methods (renames of today's on-demand
ones, with the reserved path now using them too):

- `available(reservation) -> int` → `reservation.gpu_count - sum(occupancy[id])`
- `record_placement(reservation_id, pod_uid, gpu_count)`  *(idempotent by uid)*
- `release_pod(pod_uid) -> Optional[int]`
- `reconcile_occupancy(snapshot)` — see §3.4

`ondemand_available` / `ondemand_available_by_id` / `record_ondemand_placement`
/ `release_ondemand_pod` / `reconcile_ondemand` collapse into the above. The
"set of reservation blocks" is already `state.reservations` (all kinds in one
list); we are giving it one occupancy view to match.

### 3.2 What `kind` varies (and nothing else)

| Concern | Reserved (`kind="user"`) | On-demand (`kind="ondemand"`) / no-show (user id ∈ `noshow_reservation_ids`) |
|---|---|---|
| Eligibility | `find_best_reservation` — namespace == username | `find_ondemand_block` — any namespace |
| Deadline | chain back-to-back (`compute_max_deadline_seconds`) | cap to single window |
| No-show | participates; protected while claimed (§3.3) | never becomes a no-show |
| `booking-reference` prefix | `res-` | `ondemand-` / `noshow-` |

The prefix is **audit-only** after this change — the budget keys on reservation
id, not the string. Kept for humans/Splunk and for reconstruction parsing.

### 3.3 Fix #3 — chain-aware no-show claiming

The root cause is *booked-id ≠ occupied-id*: a chained `res-X` pod occupies
`X+1` but is filed under `X`. Unifying the data structure alone does **not** fix
this — `X+1`'s availability still can't see the holder. We add an explicit
notion of *claimed* reservations.

Factor the back-to-back walk out of `compute_max_deadline_seconds` into a shared
helper and add its set form:

```
_chain_for(reservation) -> list[ReservationResponse]   # the back-to-back chain
reservations_claimed_by(reservation_id) -> set[int]    # {id} ∪ chained ids
```

Maintain a per-tick snapshot set on `ControllerState`:

```
claimed_reservation_ids: set[int]   # union of reservations_claimed_by(X)
                                    # for every live res-X holder pod
```

Three consumers honour it:

- `check_noshow_deadlines` — never declare a no-show for an id in the set.
- `update_noshow_tracking` — never (re-)arm a deadline for an id in the set.
- `find_ondemand_block` — exclude claimed ids from no-show eligibility
  (defense in depth, so a stale declaration can't leak capacity).

And make the existing clear path chain-aware:

- `mark_pod_seen_for_noshow` becomes booking-aware — for a `res-X` holder pod it
  clears the no-show deadlines for **all** of `reservations_claimed_by(X)`, not
  just the soonest by `slot_start` ([controller.py:268](../app/controller.py)).
  On-demand/no-show pods (other prefixes) no longer touch holder deadlines.

Because the claimed set is recomputed every 30 s (§3.4), it stays fresh well
inside the 15-minute default `NOSHOWN_TIMEOUT_MINUTES` margin.

### 3.4 One cluster snapshot drives occupancy, claimed-set, and guard 3

The queue processor **already** LISTs all `gpu-class` pods every 30 s for guard 3
(`list_stuck_reservation_holder_pods`, [main.py:581](../app/main.py)). Generalise
that single LIST into one snapshot pass:

```
snapshot_tolerated_pods(tol_key) -> list[ToleratedPod]
# ToleratedPod: namespace, name, uid, gpu_class, booking_reference,
#               reservation_id (parsed), gpu_count, phase, scheduled_false
```

From this one pass per tick we derive, with no extra API calls:

1. **Occupancy** — rebuild `occupancy` from ground truth, bucketing each pod by
   its parsed `reservation_id`. This makes the map **self-healing**: a pod
   deleted during a watch disconnect (missed DELETE) is pruned on the next tick.
2. **Claimed set** — union of `reservations_claimed_by(X)` over live `res-X`
   pods (§3.3).
3. **Guard 3** — stuck reservation-holder pods (unchanged semantics).

Important: the occupancy/claimed reconcile must run **unconditionally** each
tick; only the guard-3 stuck-detection stays gated on
`ondemand_placement_enabled` (the reserved path needs occupancy even when
on-demand is off). Between ticks the map is kept warm incrementally — record on
`has_tol` ADDED/MODIFIED, release on DELETE in `pod_watch_loop` — so the
fast path reads `state.available()` with **no API round-trip** (faster than
today's per-attempt namespaced LIST).

**Optimistic reservation + rebuild interaction.** Placements still
`record_placement` before any `await` (race-free within the event loop). A
wholesale rebuild could drop an in-flight record whose patch isn't yet visible
in the LIST, but the placement coroutine completes within one event-loop slice
(sub-second) versus a 30 s tick, and the *next* tick captures the committed pod;
worst case is a ≤30 s transient that self-corrects. (Hardening option: carry
over uids recorded within the last few seconds during rebuild.)

### 3.5 Retire `dsmlp/ondemand-block-id` (#2)

Add a pure helper `parse_booking_reference(ref) -> Optional[int]` that strips the
`res-` / `ondemand-` / `noshow-` prefix. Reconstruction (startup LIST and each
reconcile) uses it instead of `get_pod_ondemand_block_id`. Then:

- Delete `get_pod_ondemand_block_id` ([k8s_client.py:111](../app/k8s_client.py)).
- Drop `extra_annotations={"dsmlp/ondemand-block-id": ...}` from the on-demand
  placement ([main.py:345](../app/main.py)) and stop writing the annotation.
- `dsmlp/booking-reference` is now load-bearing for reconstruction (it already
  was for budget). It is written on every admission by `apply_toleration`, so
  there is **no migration gap** — pods placed by the previous controller already
  carry it (`ondemand-X` / `noshow-X` / `res-X`), and reserved pods that the old
  controller never tracked in a map now get counted correctly. The only
  un-reconstructable pods would predate booking-reference entirely.

## 4. Files touched

- **`app/controller.py`** — generalise occupancy methods; `available`,
  `record_placement`, `release_pod`, `reconcile_occupancy`; factor `_chain_for`,
  add `reservations_claimed_by`; add `claimed_reservation_ids` + honour it in
  `check_noshow_deadlines`, `update_noshow_tracking`, `find_ondemand_block`;
  make `mark_pod_seen_for_noshow` chain-aware.
- **`app/k8s_client.py`** — add `parse_booking_reference`; add
  `snapshot_tolerated_pods` (generalises `list_stuck_reservation_holder_pods`);
  delete `count_tolerated_gpu_usage` and `get_pod_ondemand_block_id`; remove the
  ondemand-block-id annotation write.
- **`app/main.py`** — `_try_apply_toleration` uses `state.available` +
  optimistic `record_placement` + rollback; `_try_place_ondemand` uses the
  renamed record/release and drops the extra annotation; `pod_watch_loop`
  `has_tol` branch records all kinds via booking-reference; `queue_processor_loop`
  runs the single snapshot reconcile each tick; startup reconstruction via
  booking-reference.
- **Docs** — CLAUDE.md (annotations + "two systems → one" + remove
  ondemand-block-id), README.md annotation table, and this doc.

## 5. Implementation staging

Land as four small, independently reviewable PRs (each revertible; in-memory
state rebuilds on rollback):

1. **PR1 — pure helpers, no behaviour change.** `parse_booking_reference`,
   `_chain_for`/`reservations_claimed_by`, `claimed_reservation_ids` plumbing
   (computed but not yet consumed). Full unit tests.
2. **PR2 — #3 fix, minimal & shippable on its own.** Compute the claimed set
   from the existing guard-3 LIST and make no-show declaration/suppression/clear
   chain-aware. This closes the double-count *without* the full unification —
   highest-value, lowest-risk step. Tests + canary.
3. **PR3 — unify occupancy.** One map for all kinds; reserved path switches to
   map-based availability with optimistic record + per-tick rebuild; delete
   `count_tolerated_gpu_usage`. Tests + canary.
4. **PR4 — retire the annotation.** Reconstruct from booking-reference; delete
   `get_pod_ondemand_block_id` and the annotation write; update docs.

## 6. Testing

Existing suites to retarget/extend: `test_ondemand.py` (occupancy → generic
methods), `test_noshow.py`, `test_guards.py`, `test_k8s_helpers.py`.

New coverage:

- **`parse_booking_reference`** — `res-`/`ondemand-`/`noshow-`/garbage/None.
- **`reservations_claimed_by`** — single, full back-to-back chain, stops at a
  gap, respects user/class/gpu_count match, excludes already-no-show links
  (mirror `TestComputeMaxDeadlineSkipsNoshow`).
- **#3 regressions** (the point of the change):
  - chained holder `res-42`→`43` ⇒ `43` not declared no-show while the holder
    pod is live;
  - `43` excluded from `find_ondemand_block` while claimed by `42`'s holder;
  - `mark_pod_seen_for_noshow` clears the whole chain — *replaces*
    `test_picks_soonest_slot_start_when_multiple`.
- **Unified occupancy** — reserved pod recorded/available/released via the
  generic methods; budget enforced from the map; reconcile rebuilds from a
  snapshot and prunes stale uids (self-heal).
- **`snapshot_tolerated_pods`** — mock `CoreV1Api.list_pod_for_all_namespaces`,
  assert the derived occupancy / claimed-set / stuck list.
- **Invariant test** — every admission path writes `booking-reference` (it is
  now the sole reconstruction key).

## 7. Risks & mitigations

- **Safety-critical accounting rewrite.** The budget check is the core
  invariant. → Staged PRs, comprehensive tests, canary deploy, watch Splunk for
  stuck-holder and over-budget anomalies before/after.
- **Per-tick rebuild race** (in-flight optimistic record dropped). → Bounded to
  ≤30 s, self-corrects next tick; optional in-flight carry-over.
- **Claimed-set staleness vs no-show timing.** → Recomputed at 30 s, far inside
  the 15-min default timeout. Flag the interaction with very low
  `NOSHOWN_TIMEOUT_MINUTES` in docs.
- **Single reconstruction key.** Heavier reliance on booking-reference. →
  Always written by `apply_toleration`; covered by the invariant test.
- **Per-tick cost.** One cluster LIST per 30 s already exists (guard 3); we
  remove the per-attempt namespaced LISTs, so net API load drops under churn.

## 8. Rollout & rollback

In-memory only, so no data migration. First post-deploy reconcile rebuilds from
live pods via booking-reference (old pods already carry it). The
`ondemand-block-id` annotation is simply ignored, then no longer written.
Rollback = revert the commit(s); the previous controller re-derives state within
one fetch cycle and resumes writing the annotation — at most a one-cycle
accounting blip.
