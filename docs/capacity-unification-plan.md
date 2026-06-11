# Capacity-tracking unification — design & implementation record

*Engineering record · status: **implemented** 2026-06-11 · commits `cf2755e`
(PR1) → `1cd94f3` (PR2) → `d531b1f` (PR3) → `abf4e4f` (PR4)*

> **As built.** The two parallel capacity-tracking systems were merged into one
> occupancy map keyed by reservation id, the res-/noshow- double-count (#3) was
> closed, and the redundant `dsmlp/ondemand-block-id` annotation (#2) was retired.
> Landed as four small commits on `main` (no PR — early development); the test
> suite went 153 → 197 passing. Two deliberate divergences from the original plan
> are flagged inline in §5 and §3.4. Symbol names below are current; code
> references to *removed* symbols are left unlinked because that code no longer
> exists.

## 1. Background & motivation

Before this change, the controller tracked GPU capacity with **two parallel
mechanisms** that never saw each other:

| | Reserved path | On-demand / no-show path |
|---|---|---|
| Mechanism | `count_tolerated_gpu_usage` — live namespaced pod LIST, counted pods whose `dsmlp/booking-reference == "res-<id>"` | `ControllerState.ondemand_occupancy` — in-memory `{block_id: {pod_uid: gpu_count}}` |
| Source of truth | the cluster (recomputed each attempt) | the controller's own bookkeeping |
| Restart recovery | nothing to rebuild; re-counted live | reconstructed from `dsmlp/ondemand-block-id` |
| Scope | one namespace (the holder's) | cross-namespace |
| Race safety | **none** — TOCTOU between count and patch | optimistic reservation before `await` |

The split was incidental — reserved came first (a live count is the minimal
thing), on-demand was layered on and its needs (cross-namespace counting,
restart reconstruction, race-free reservation) pushed it onto a map. Keeping
both cost us:

- **Bug #3 — double-counting.** Budgets were keyed on the booking-reference
  *string*, so `res-X` and `noshow-X` were disjoint pools over the *same*
  physical reservation `X`. A pod admitted under `res-X` whose deadline is
  **chained** through reservation `X+1` (`compute_max_deadline_seconds`)
  physically occupies `X+1`'s window while no pod is booked `res-(X+1)`. No-show
  tracking, which assumed one pod ↔ one reservation, could then declare `X+1` a
  no-show and lend its full `gpu_count` to `noshow-(X+1)` pods *on top of* the
  still-running holder. Latent with default timings; live when
  `NOSHOWN_TIMEOUT_MINUTES` is small.
- **Annotation redundancy (#2).** `dsmlp/ondemand-block-id` re-encoded the id
  already embedded in `dsmlp/booking-reference`. Two sources of truth for one
  value.
- **Two mental models, two reconciliation paths, an un-self-healing on-demand
  map, and a racy reserved path.**

## 2. Goals & non-goals

**Goals (all met)**

1. One occupancy model keyed by **reservation id**, covering reserved,
   on-demand, and no-show pods alike. `kind` varies only eligibility, deadline
   policy, and the (cosmetic) booking-reference prefix.
2. Structurally close **#3**: a reservation a live holder is chained through is
   never lent out as on-demand/no-show capacity.
3. Retire **`dsmlp/ondemand-block-id`** (#2): reconstruct occupancy from
   `dsmlp/booking-reference`.
4. Preserve the on-demand map's race-free optimistic reservation, and recover
   the live count's self-healing for *all* paths.

**Non-goals (held)**

- No change to matching rules, the three guards, runtime-cap arithmetic, or the
  no-show timeout/grace semantics (beyond the chain-awareness in goal 2).
- `dsmlp/pod-runtime-limit-seconds` stays (consumed by in-pod notification
  widgets).
- No persistent store — still fully in-memory, rebuilt from the cluster + API.

## 3. Design (as implemented)

### 3.1 One occupancy map for all kinds

`ondemand_occupancy` became a single map keyed by reservation id holding
**every** admitted pod regardless of kind:

```
occupancy: dict[int, dict[str, int]]     # reservation_id -> {pod_uid: gpu_count}
```

`ControllerState` gained kind-agnostic methods, used by the reserved path too:

- `available(reservation, exclude_uid=None) -> int` → `gpu_count - sum(occupancy[id])`
- `record_placement(reservation_id, pod_uid, gpu_count)`  *(idempotent by uid)*
- `release_pod(pod_uid) -> Optional[int]`
- `available_by_id(reservation_id)` / `_reservation_gpu_count(reservation_id)`
- `reconcile_occupancy(placements)` — see §3.4

`ondemand_available` / `ondemand_available_by_id` / `record_ondemand_placement`
/ `release_ondemand_pod` / `reconcile_ondemand` were removed in favour of the
above. The "set of reservation blocks" was already `state.reservations` (all
kinds in one list); it now has one occupancy view to match.

### 3.2 What `kind` varies (and nothing else)

| Concern | Reserved (`kind="user"`) | On-demand (`kind="ondemand"`) / no-show (id ∈ `noshow_reservation_ids`) |
|---|---|---|
| Eligibility | `find_best_reservation` — namespace == username | `find_ondemand_block` — any namespace |
| Deadline | chain back-to-back (`compute_max_deadline_seconds`) | cap to single window |
| No-show | participates; protected while claimed (§3.3) | never becomes a no-show |
| `booking-reference` prefix | `res-` | `ondemand-` / `noshow-` |

The prefix is now **audit-only** — the budget keys on reservation id, not the
string. Kept for humans/Splunk and as the reconstruction parse input.

### 3.3 Fix #3 — chain-aware no-show claiming

The root cause is *booked-id ≠ occupied-id*: a chained `res-X` pod occupies
`X+1` but is filed under `X`. Unifying the data structure alone does **not** fix
this — `X+1`'s availability still can't see the holder — so an explicit notion
of *claimed* reservations was added.

The back-to-back walk was factored out of `compute_max_deadline_seconds` into a
shared helper, plus its set form (both in `app/controller.py`):

```
_chain_for(reservation, now) -> list[ReservationResponse]   # the back-to-back chain
reservations_claimed_by(reservation_id, now) -> set[int]    # {id} ∪ chained ids
```

A per-tick set lives on `ControllerState`:

```
claimed_reservation_ids: set[int]   # union of reservations_claimed_by(X)
                                    # for every live res-X holder pod
```

Three consumers honour it:

- `check_noshow_deadlines` — never declare a no-show for an id in the set.
- `update_noshow_tracking` — never (re-)arm a deadline for an id in the set.
- `find_ondemand_block` — exclude claimed ids from no-show eligibility
  (defense in depth, so a stale declaration can't leak capacity).

`refresh_claimed_reservations` also **clears** the no-show deadline of each
claimed reservation, so when a holder vacates the reservation drops out of the
set and the existing `update_noshow_tracking` grace re-arm recycles the window.
And `mark_pod_seen_for_noshow` became booking-aware — for a `res-X` holder pod it
clears the deadlines for **all** of `reservations_claimed_by(X)` (fast clear
between ticks), with the soonest-match behaviour preserved as a fallback.

Because the claimed set is recomputed every 30 s (§3.4), it stays fresh well
inside the 15-minute default `NOSHOWN_TIMEOUT_MINUTES` margin.

### 3.4 One cluster snapshot drives occupancy, claimed-set, and guard 3

The queue processor already LISTed all `gpu-class` pods every 30 s for guard 3.
That was generalised into one snapshot pass (`app/k8s_client.py`):

```
snapshot_tolerated_pods(tol_key) -> list[ToleratedPodInfo]
# ToleratedPodInfo: namespace, name, uid, gpu_class, booking_reference,
#                   reservation_id (parsed), gpu_count, phase, scheduled_false
```

From this one pass per tick the controller derives, with no extra API calls:

1. **Occupancy** — `reconcile_occupancy` rebuilds `occupancy` from ground truth,
   bucketing each live (Running/Pending) pod by its parsed `reservation_id`.
   Wholesale rebuild makes the map **self-healing**: a pod deleted during a watch
   disconnect (missed DELETE) is dropped on the next tick.
2. **Claimed set** — union of `reservations_claimed_by(X)` over live `res-X`
   pods (§3.3).
3. **Guard 3** — stuck reservation-holder pods.

The occupancy/claimed reconcile runs **unconditionally** each tick; only the
guard-3 stuck-detection stays gated on `ondemand_placement_enabled` (the reserved
path needs occupancy even when on-demand is off). Between ticks the map is kept
warm incrementally — record on `has_tol` ADDED/MODIFIED, release on DELETE in
`pod_watch_loop` — so the fast path reads `state.available()` with **no API
round-trip** (faster than the old per-attempt namespaced LIST).

> **Divergence from plan (minor):** guard 3 now silently filters pods with an
> empty `gpu-class` label instead of logging the per-pod warning the old
> `list_stuck_reservation_holder_pods` emitted. The watcher's `gpu-class`
> selector guarantees the label key exists; an empty value is rare.

**Optimistic reservation + rebuild interaction.** Placements still
`record_placement` before any `await` (race-free within the event loop). A
wholesale rebuild can drop an in-flight record whose patch isn't yet visible in
the LIST, but the placement coroutine completes within one event-loop slice
(sub-second) versus a 30 s tick, and the next tick captures the committed pod —
worst case a ≤30 s transient that self-corrects. (Documented in
`reconcile_occupancy` and CLAUDE.md; hardening option: carry over uids recorded
within the last few seconds during rebuild.)

### 3.5 Retire `dsmlp/ondemand-block-id` (#2)

A pure helper `parse_booking_reference(ref) -> Optional[int]` strips the
`res-` / `ondemand-` / `noshow-` prefix; reconstruction (startup LIST and each
reconcile) uses it. Then:

- `get_pod_ondemand_block_id` was deleted.
- The `extra_annotations={"dsmlp/ondemand-block-id": ...}` write was dropped from
  on-demand placement, and the now-unused `extra_annotations` parameter was
  removed from `apply_toleration`.
- `dsmlp/booking-reference` is now load-bearing for reconstruction (it already
  was for budget). It is written on every admission by `apply_toleration`, so
  there was **no migration gap** — pods placed by the previous controller already
  carry it (`ondemand-X` / `noshow-X` / `res-X`), and reserved pods the old
  controller never tracked in a map now get counted correctly. The only
  un-reconstructable pods would predate booking-reference entirely.

## 4. Files touched

- **`app/controller.py`** — generalised occupancy methods (`available`,
  `record_placement`, `release_pod`, `available_by_id`, `_reservation_gpu_count`,
  `reconcile_occupancy`); factored `_chain_for`, added `reservations_claimed_by`;
  added `claimed_reservation_ids` and `refresh_claimed_reservations`, honoured in
  `check_noshow_deadlines`, `update_noshow_tracking`, `find_ondemand_block`; made
  `mark_pod_seen_for_noshow` chain-aware.
- **`app/k8s_client.py`** — added `parse_booking_reference`, `ToleratedPodInfo`,
  and `snapshot_tolerated_pods`; deleted `count_tolerated_gpu_usage`,
  `list_stuck_reservation_holder_pods`, `get_pod_ondemand_block_id`; removed the
  `extra_annotations` parameter and the ondemand-block-id write.
- **`app/main.py`** — `_try_apply_toleration` checks `state.available` with an
  optimistic `record_placement` + rollback; `_try_place_ondemand` uses the renamed
  record/release and dropped the extra annotation; `pod_watch_loop` `has_tol`
  branch records all kinds via booking-reference; `queue_processor_loop` runs the
  single snapshot reconcile each tick; startup reconstruction via booking-reference.
- **Docs** — CLAUDE.md (annotations, "two systems → one", chain-aware no-show,
  removed ondemand-block-id), README.md annotation table, and this record.

## 5. As built — the four commits

Landed as four small, independently revertible commits on `main` (in-memory
state rebuilds on rollback):

1. **PR1 `cf2755e` — pure helpers, no behaviour change.** `parse_booking_reference`,
   `_chain_for` / `reservations_claimed_by`, `claimed_reservation_ids` plumbing
   (computed but not yet consumed). Unit tests.
2. **PR2 `1cd94f3` — #3 fix, self-contained.** Claimed-set computation +
   chain-aware no-show declaration/suppression/clear; closes the double-count
   *without* the full unification.
3. **PR3 `d531b1f` — unify occupancy.** One map for all kinds; reserved path
   switched to map-based availability with optimistic record + per-tick snapshot
   rebuild; deleted `count_tolerated_gpu_usage` and the separate scans.
4. **PR4 `abf4e4f` — retire the annotation.** Reconstruct from booking-reference;
   delete `get_pod_ondemand_block_id` and the annotation write; update docs.

> **Divergence from plan (minor):** PR2 computed the claimed set from a dedicated
> `list_reservation_holder_pods` scan rather than the guard-3 LIST, keeping PR2
> small and self-contained; PR3 then folded that scan **and** the guard-3 scan
> into the single `snapshot_tolerated_pods` pass. Same end state, reached in two
> clean steps.

## 6. Testing (as built)

Suite went **153 → 197 passing**. Retargeted: `test_ondemand.py` (occupancy →
generic methods; new `TestReconcileOccupancy`), `test_noshow.py`,
`test_guards.py`. New files: `test_chain_and_booking.py`, `test_noshow_chain.py`.

Coverage added:

- **`parse_booking_reference`** — `res-`/`ondemand-`/`noshow-`/garbage/None.
- **`_chain_for` / `reservations_claimed_by`** — single, full chain, stops at a
  gap, respects user/class/gpu_count match, excludes no-show links.
- **#3 regressions** — chained holder `res-1`→`2` ⇒ `2` not declared no-show and
  not lent via `find_ondemand_block` while the holder is live; converts once the
  holder is gone; `mark_pod_seen_for_noshow` clears the whole chain.
- **Unified occupancy** — `available(exclude_uid=…)`; `reconcile_occupancy`
  rebuilds from a snapshot and prunes stale uids (self-heal).

## 7. Residual behaviours & notes

- **Per-tick rebuild race** (in-flight optimistic record dropped) — bounded to
  ≤30 s, self-corrects next tick; optional in-flight carry-over not implemented.
- **Claimed-set staleness vs no-show timing** — recomputed at 30 s, far inside
  the 15-min default timeout; the interaction with a very low
  `NOSHOWN_TIMEOUT_MINUTES` is noted in CLAUDE.md.
- **Single reconstruction key** — heavier reliance on `booking-reference`, which
  `apply_toleration` always writes.
- **Per-tick cost** — the one cluster LIST per 30 s already existed for guard 3;
  the per-attempt namespaced LISTs are gone, so net API load dropped. No new RBAC
  (the snapshot reuses the all-namespaces list the watcher already requires).

## 8. Rollout & rollback

In-memory only, so no data migration. The first post-deploy reconcile rebuilds
from live pods via booking-reference (old pods already carry it). The
`ondemand-block-id` annotation is simply ignored, then no longer written.
Rollback = revert the commit(s); the previous controller re-derives state within
one fetch cycle and resumes writing the annotation — at most a one-cycle
accounting blip.
