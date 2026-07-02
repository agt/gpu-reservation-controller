# Code Quality & Consistency Review — July 2026

**Date:** 2026-07-02
**Scope:** entire repository — `app/` (controller core and boundary modules), `tests/`,
documentation (CLAUDE.md, AGENTS.md, README.md, OBSERVABILITY.md, `docs/`), and deployment
artifacts (Dockerfile, Helm chart, CI workflow, requirements files).
**Method:** three parallel review passes with distinct lenses — (1) business-logic core
(`controller.py`, `main.py`), (2) boundary/IO modules (`k8s_client.py`,
`reservation_client.py`, `schemas.py`, `config.py`), (3) tests, docs, and deployment drift.
Every finding was verified at the cited lines against the working tree; the test suite was
run once (**232 passed in 0.83 s**, zero warnings even under `-W error::DeprecationWarning`).

The motivating question: after 100+ PRs, where have *outwardly similar workflows* drifted
into different implementations? Part II is the direct answer; Part I lists correctness bugs
found along the way (several of which are direct consequences of that drift).

Severity legend — **High**: correctness bug or breakage under a supported configuration.
**Medium**: divergence likely to cause a future bug, or wrong behavior in edge cases.
**Low**: style/consistency debt.

---

## Executive summary

| # | Finding | Severity | Where |
|---|---------|----------|-------|
| B1 | Occupancy release on pod deletion is gated on the on-demand feature flag — reserved-path budget leaks when `ONDEMAND_PLACEMENT_ENABLED=false` | High | `main.py:602-632` |
| B2 | `date.today()` in the reservation fetch is local-TZ dependent — active reservations drop out of the fetch with an east-of-UTC `TZ` | High | `reservation_client.py:40` |
| B3 | Reserved path can patch `activeDeadlineSeconds: 0`, which Kubernetes rejects → pod runs uncapped | Medium | `controller.py:584-587` |
| B4 | Dropping a reclaim merge for a claimed subject silently re-exposes absorbed blocks for double-booking | Medium | `controller.py:791-792` |
| B5 | `Optional` used in `main.py` but never imported (latent `NameError`, masked by `from __future__ import annotations`) | Medium | `main.py:26,236,255,342,686` |
| D1 | The two admission coroutines (reserved vs on-demand) drifted apart: duplicated deadline logic, opposite gate-removal/cap ordering, asymmetric pod-state guards | Medium | `main.py:253-511` |
| D6 | Docs and code comments still describe a "30 s" queue-processor tick; the default is 300 s — fixed 30 s retries are dead time at that cadence | Medium | CLAUDE.md, README, `main.py:11,642` |
| T1 | No `tests/conftest.py`: six drifted copies of the reservation factory, `_compute_window` byte-identical in five files | High | `tests/*` |
| P1 | README's manual RBAC manifest lacks `delete` on pods — cancellation eviction 403s for manual deployments | High | README.md:360 |
| P2 | `HEALTH_PORT` is parsed but never consumed; changing Helm `healthPort` breaks liveness probes | High | `config.py:46`, Dockerfile:50, Helm |

---

## Part I — Correctness bugs

### B1. Occupancy release on pod deletion is gated on the on-demand feature flag — **High**

`main.py:602-606` (DELETED branch of `pod_watch_loop`) and `main.py:613-618`
(terminal-phase branch):

```python
state.remove_ondemand_candidate(uid)
if config.ondemand_placement_enabled:
    block_id = state.release_pod(uid)
```

`state.occupancy` is the **unified** budget map for all admission paths (reserved,
on-demand, no-show — `controller.py:144-153`), but incremental release on DELETE/terminal
only happens when `ONDEMAND_PLACEMENT_ENABLED` is true. With the flag `false`
(a documented configuration):

- A deleted or completed reserved-path pod keeps consuming budget until the next
  queue-processor reconcile (default `POD_LIST_TICK_INTERVAL=300` s), so a replacement pod
  hits "GPU budget full" and gets a further 2–5 min retry backoff.
- Worse: because the terminal-phase `continue` at `main.py:618` is also skipped, the
  `has_tol` branch (`main.py:620-632`) runs for **Succeeded/Failed** pods and
  `record_placement` re-adds the dead pod to occupancy on every MODIFIED event.
  `reconcile_occupancy` drops it each tick (it filters Running/Pending, `main.py:750`), but
  the next MODIFIED event re-pollutes the map — the two mechanisms fight until the terminal
  pod object is deleted.

The gating appears historical, from when occupancy tracked only on-demand blocks.
**Recommendation:** call `state.release_pod(uid)` unconditionally on DELETED; make the
terminal-phase release unconditional and gate only `_recycle_ondemand_block` on the flag.
Add a phase guard (`phase not in ("Succeeded", "Failed")`) before the `record_placement`
keep-warm at `main.py:632`.

### B2. `date.today()` is local-timezone dependent, contradicting the UTC-everywhere rule — **High**

`reservation_client.py:40-41`:

```python
today = date.today()
end = today + timedelta(days=self._lookahead_days)
```

`date.today()` uses the process's local timezone (`TZ`). CLAUDE.md and AGENTS.md assert
that `TZ` "affects log timestamp display only", and AGENTS.md's dev instructions even tell
developers to `export TZ=America/Los_Angeles`. But this call sets the
`date_start`/`date_end` bounds of the *only* reservation fetch:

- East-of-UTC `TZ` (e.g. `Asia/Tokyo` at 20:00 UTC): local "today" is already tomorrow, so
  `date_start` excludes reservations whose window is open *right now*. Active reservations
  silently drop out of `state.reservations`, breaking pod matching, and `reconcile_noshow`
  (`controller.py:287-300`) prunes their no-show state as "left active list".
- West-of-UTC `TZ`: the lookahead window shifts a day early, so day-7 reservations are
  fetched a day late.

**Recommendation:** use `datetime.now(timezone.utc).date()`, and consider widening
`date_start` by one day since the API filter is date-based while windows are
UTC-instant-based.

### B3. Reserved-path deadline can be patched to 0; on-demand floors at 1 — **Medium**

`ControllerState.compute_max_deadline_seconds` clamps at zero (`controller.py:584,587`:
`max(0.0, ...)` … `return int(total)`). If the window expires between the queue-processor's
`now > end` check (`main.py:811`) and `_enforce_deadline` running (`main.py:217-225`), the
controller attempts `activeDeadlineSeconds: 0`, which the Kubernetes API rejects
(minimum 1). The on-demand path explicitly floors: `remaining = max(remaining, 1)`
(`main.py:479`). The failure is swallowed by `_enforce_deadline`'s best-effort catch, so
the pod is admitted with **no runtime cap at all** — the opposite of the intended behavior.
**Recommendation:** `return max(1, int(total))` in `compute_max_deadline_seconds` (or floor
in `_enforce_deadline`), matching the on-demand path. (Unifying the two paths — finding
D1a — makes this divergence impossible.)

### B4. Dropping a reclaim merge for a claimed subject re-exposes absorbed blocks, silently — **Medium**

`controller.py:791-792` in `reconcile_reclaim_merges` step 1:

```python
if subject_id in self.claimed_reservation_ids:
    continue  # a reserved holder now occupies it — not on-demand
```

Two problems:

- The merge record is dropped from `surviving` and the absorbed ids are **not** added to
  `merged_stub_ids`, so previously-absorbed future reclaim blocks become independently
  placeable again — while an on-demand job whose `activeDeadlineSeconds` was already
  extended across them may still be running (deadlines are never retracted). That is
  exactly the double-booking the persistence mechanism exists to prevent (per the module's
  own docstring, `controller.py:756-760`).
- The other two drop branches log at INFO (`controller.py:786-790`, `800-804`); this one is
  silent — an operator cannot tell why a merge vanished.

**Recommendation:** keep the merge (stub the absorbed ids) until
`now >= merge.extended_end`, or at minimum log the drop at INFO and document the accepted
double-booking window.

### B5. `Optional` used in `main.py` but never imported — **Medium** (latent)

`main.py:26` imports only `AsyncIterator` from `typing`, yet `Optional` appears in
annotations at `main.py:236, 255, 342, 686`. This doesn't crash today solely because of
`from __future__ import annotations` (line 18) — annotations are never evaluated. Any call
to `typing.get_type_hints()` on these functions, or removal of the future import, raises
`NameError`; any type checker flags it. Note the same file uses `str | None` at line 575,
so the style is mixed as well (see H4).
**Recommendation:** switch the four sites to `str | None` (matching line 575) and drop
`Optional` from this file, or add the import.

### B6. `delete_pod`: always-true guard around the 404 check — **Medium** (works by accident)

`k8s_client.py:461-467`:

```python
except Exception as exc:
    if getattr(getattr(exc, "status", None), "__class__", None) is not None:
```

`getattr(None, "__class__", None)` returns `NoneType`, which is not `None`, so the outer
condition is **always true** — a no-op that reads as if it were filtering. Behavior is
correct by accident (non-404 falls through to `raise`).
**Recommendation:** use the idiomatic pattern — `except ApiException as exc: if
exc.status == 404: return; raise` — which also fixes the module-wide absence of
`kubernetes.client.rest.ApiException` (see D3d).

### B7. `PodWatcher` thread: wrong "daemon" comment and shutdown-hang risk — **Medium**

`k8s_client.py:608-611` comments that the watch thread "runs indefinitely (daemon)", but
since Python 3.9 default-executor threads are **non-daemon** and are joined at interpreter
exit. `_run_watch` is `while True` with a blanket `except` (`k8s_client.py:546-606`) and
never returns, so a clean interpreter shutdown can hang joining it (uvicorn's signal
handling usually masks this, but it makes graceful shutdown fragile and breaks test
teardown). The returned future is also discarded — no cancellation path, inconsistent with
every other executor call in the module. It also permanently consumes one slot of the
shared default executor all other K8s calls use.
**Recommendation:** run the watch in an explicit `threading.Thread(daemon=True)`, or add a
`stop()` event checked between reconnects; fix the comment either way.

### B8. Fast path bypasses the retry cooldown for replayed ADDED events — **Low**

The fast path (`main.py:643-655`) fires on every ADDED event whose window is open, checking
only window bounds. On a watch reconnect, `PodWatcher` re-LISTs and replays every pod as
ADDED (`k8s_client.py:562-563`); `enqueue_pod` is idempotent (`controller.py:476-478`), so
the fast path retrieves the **existing** queue entry — including one in budget-full/error
backoff — and retries immediately, ignoring `entry.next_attempt_at` (which the queue
processor honors, `main.py:822`).
**Recommendation:** add `and now >= entry.next_attempt_at` to the fast-path condition at
`main.py:647`.

### B9. `ValidationError` escapes the "return None on error" contract — **Low**

`fetch_gpu_class` (`reservation_client.py:75-92`) and `fetch_settings` (`:94-105`) document
"or None on error" and catch only `httpx.HTTPStatusError` / `httpx.RequestError` — but
`model_validate(resp.json())` can raise `pydantic.ValidationError` (and `resp.json()` can
raise `JSONDecodeError`), which propagate. A malformed `/api/settings` payload then aborts
the *whole* refresh cycle (`main.py:88`) instead of just skipping the guard update as
designed.
**Recommendation:** include `ValidationError` (or a final `except Exception`) in the two
None-returning fetchers so their contract holds.

### B10. Deterministic Event names can 409 on re-emission — **Low**

`k8s_client.py:409` (`name=f"gpu-runcap-{pod.metadata.uid}"`) and `:481`
(`gpu-rescancel-{uid}`): creating the same-named Event twice for one pod (controller
restart, or a second cap when a merged block extends a deadline) raises 409 AlreadyExists,
swallowed as a generic warning by the best-effort wrappers.
**Recommendation:** use `generate_name="gpu-runcap-"`, or catch 409 and patch
`count`/`last_timestamp`.

### B11. `record_placement` logs "0/0 free" for cancelled-reservation blocks — **Low** (cosmetic)

`record_placement`'s availability log goes through `available_by_id` /
`_reservation_gpu_count` (`controller.py:977-978, 1010-1015`), which scan only
`self.reservations`. Cancelled in-window blocks live exclusively in
`self.cancelled_reservations`, so every on-demand placement onto freed cancelled capacity
logs `0/0 free` — misleading during exactly the incidents you'd read these logs for.
**Recommendation:** make `_reservation_gpu_count` also consult `cancelled_reservations`
(mirroring the `by_id` fallback in `reconcile_reclaim_merges`, `controller.py:771-773`).

---

## Part II — Divergent implementations of similar workflows

This is the heart of the review: places where the same conceptual workflow is implemented
two or more ways.

### D1. The two admission coroutines have drifted apart — **Medium**

`_try_apply_toleration` (reserved path, `main.py:253-332`) and `_try_place_ondemand`
(on-demand/no-show path, `main.py:340-511`) implement the same workflow — budget check,
optimistic record, re-read pod, patch toleration, remove gate, cap deadline, emit event,
rollback on failure — each in its own idiom:

**a. Deadline enforcement: shared helper vs inline duplicate.** The reserved path uses
`_enforce_deadline` (`main.py:209-232`); the on-demand path re-implements the identical
compare/patch/emit/warn block inline (`main.py:477-495`), differing only in how the seconds
value is computed — and in the `max(remaining, 1)` floor (bug B3).
*Recommendation:* generalize `_enforce_deadline` to take `max_seconds: int` (callers
compute it) and use it from both paths; the floor divergence disappears for free.

**b. Gate removal and deadline cap applied in opposite order.** Reserved:
`apply_toleration` → `_enforce_deadline` → `_enforce_scheduling_gate_removal`
(`main.py:306-317`). On-demand: `apply_toleration` → gate removal → deadline cap
(`main.py:454-495`). In the on-demand ordering the scheduling gate is lifted **before** the
runtime cap exists, so a pod can start running with no `activeDeadlineSeconds` if the
deadline patch then fails — on a block whose whole premise is "this capacity is only free
until `slot_end`". The reserved ordering (cap first, then ungate) is the safe one.
*Recommendation:* move the on-demand gate removal after the deadline cap.

**c. Pod-state guards: on-demand checks phase and schedulability; reserved checks
nothing.** `_try_place_ondemand` drops `Succeeded/Failed/Unknown` phases
(`main.py:400-409`) and runs the GPU-only-pending guard (`main.py:411-440`);
`_try_apply_toleration` re-reads the pod (`main.py:295`) but patches regardless of phase —
a queued pod that completed while waiting still gets tolerated, annotated, and
deadline-patched (which fails into the warning path). Note also a **third variant** of the
terminal-phase predicate: the watch loop uses `("Succeeded", "Failed")` (`main.py:613`)
while `_try_place_ondemand` uses `("Succeeded", "Failed", "Unknown")` (`main.py:401`).
*Recommendation:* add the same terminal-phase drop to `_try_apply_toleration`, and define
the terminal-phase tuple once (e.g. in `k8s_client.py` next to `get_pod_phase`).

**d. Success logging asymmetry.** On-demand placement logs a rich INFO summary
("Placed on-demand pod … block has %d/%d free", `main.py:465-475`); reserved-path success
relies solely on `apply_toleration`'s generic line (`k8s_client.py:326-333`) — no
reserved-path log mentions the reservation id, window, or remaining budget at INFO. The
"already has toleration" outcomes also diverge (`main.py:300-304` vs `444-450`), and only
the on-demand branch documents the keep-the-optimistic-record decision in a comment.
*Recommendation:* add a symmetric INFO line on reserved-path success; share the comment.

**e. Retry jitter magic numbers duplicated four times.** `random.randint(120, 300)` at
`main.py:276, 323, 366, 502`; the fixed 30 s retry at `main.py:386, 439`. All four jitter
sites also repeat `entry.next_attempt_at = now + timedelta(...)`.
*Recommendation:* module-level constants (`RETRY_JITTER_RANGE`, `SHORT_RETRY_SECONDS`) or a
`_retry_delay()` helper. See also D6 — the 30 s retry was sized to a 30 s tick that no
longer exists.

### D2. Booking-reference format knowledge is split across modules — **Medium**

Parsing lives in `k8s_client.py` (`_BOOKING_REFERENCE_PREFIXES`,
`parse_booking_reference`, `:114-133` — whose docstring warns "it must accept every prefix
`apply_toleration` writes"), but *construction* is scattered as f-strings in `main.py`:
`f"res-{entry.reservation.id}"` (`:272`), `f"noshow-{block.id}"` / `f"ondemand-{block.id}"`
(`:389-392`), plus a raw `.startswith("res-")` test at `main.py:765-766`. A new admission
path (or a renamed prefix) can silently break occupancy reconstruction after restart,
because build and parse live in different modules with no shared constant. (No current
mismatch — the round-trip was verified.)
**Recommendation:** co-locate `make_booking_reference(kind, id)` and
`is_reserved_path(reference)` next to `parse_booking_reference`, and have both admission
paths call them.

### D3. The five K8s write operations each follow their own pattern — **Medium**

Added at different times, each drifted (`k8s_client.py`):

**a. Parameter shape and order** — `apply_toleration(pod_name, namespace, pod, …)` (`:276`)
and `remove_scheduling_gate` (`:336`) put `pod` last; `set_active_deadline(pod_name,
namespace, seconds)` (`:368`) takes no pod; both event emitters put `pod` **first**
(`:392`, `:470`); `read_pod`/`delete_pod` use different arg names (`:205`, `:447`). Every
function that takes `pod` also takes `pod_name`/`namespace`, derivable from `pod.metadata`.
*Recommendation:* one convention — functions needing the object take `pod` only; others
take `(name, namespace)`.

**b. `run_in_executor` boilerplate duplicated seven times** (`:208-211, 244-251, 321-324,
357-361, 375-383, 434-437, 454-459, 504-508`). Executor *discipline* is consistent (no
direct `_core_v1` call from async code; the direct calls at `:555, 577` are correctly
inside the watch thread), but the three-line pattern should be one `_run(fn, *args)`
helper — also a single choke point for future `ApiException` mapping or metrics.

**c. Event emitters ~80 % copy-paste** — `emit_runtime_capped_event` (`:392-444`) and
`emit_reservation_cancelled_event` (`:470-513`) differ only in name prefix, `reason`,
`message`, `action`; ~30 lines of `CoreV1Event` construction are duplicated. *Extract
`_emit_pod_event(pod, *, name_prefix, reason, action, message)`.*

**d. Exception conventions differ per function** — four write ops let everything propagate;
`delete_pod` alone catches internally (with the broken guard, B6). Nowhere is
`ApiException` imported; all error classification is duck-typed or absent.
*Standardize on raising `ApiException` with documented exceptions (delete_pod's 404).*

**e. Re-fetch-before-patch responsibility is split** — CLAUDE.md says "the pod is
re-fetched immediately before patching", but the invariant is enforced by *callers*
(`main.py:295, 397`) while `apply_toleration` trusts the passed pod for the toleration
list it overwrites (`k8s_client.py:300-315`). Nothing is wrong today, but each new call
site must remember it. *Either read inside, or document the "caller must pass a just-read
pod" contract in the docstrings.*

### D4. The two window-extension mechanisms diverge in structure — **Medium**

Reserved back-to-back chaining (`_chain_for` / `compute_max_deadline_seconds` /
`reservations_claimed_by`, `controller.py:513-606`) and on-demand reclaim-block merging
(`reconcile_reclaim_merges`, `controller.py:734-888`) express the same idea — "abutting,
same class, equal gpu_count ⇒ one longer span" — with different mechanics:

**a. Merging mutates `ReservationResponse.end_utc` in place; chaining is pure.** Chaining
sums durations without mutation; merging rewrites the shared Pydantic model
(`subject.end_utc = merge.extended_end`, `controller.py:806`; `= cur_end`, `:871`) so
`slot_end()` transparently returns the extended end everywhere. Clever but global: every
consumer of `slot_end` silently sees the mutated value, and correctness depends on subject
kinds being excluded from all booking-path logic. It also means the "models mirroring
RESERVATION-API.md" are silently required to be mutable — adding
`ConfigDict(frozen=True)` to `schemas.py` (a natural hardening) would break merging far
from the schema file. Any future code consulting `end_utc` on these objects expecting the
API value (metrics, event messages) will be wrong.
*Recommendation:* at minimum document the invariant on `ReclaimMerge` and
`ReservationResponse`; better, keep an `effective_end(r)` overlay (a `reclaim_merges`
lookup falling back to `r.end_utc`) instead of mutating fetched API models — this also
removes the need to re-apply mutations after each wholesale reload.

**b. Tie-breaking differs.** Merge picks the longest abutting block
(`max(targets, key=slot_end)`, `controller.py:867`); chain picks effectively the first in
API order (`controller.py:554-564`) and leaves the others unclaimed/unprotected. With the
current data model duplicates shouldn't exist, but the two mechanisms answer the same
question differently. *Apply the same `max(…, key=slot_end)` rule in `_chain_for`, or
assert/log on multiple candidates.*

**c. Guard-unknown handling wipes persisted merges.** `guard is None` does
`self.reclaim_merges = {}` after clearing `merged_stub_ids` (`controller.py:763-767`) —
CLAUDE.md describes this state as merging being "skipped", but the code *destroys*
persisted merges. Today the guard can only be None before the first successful settings
fetch, so no merge can exist yet — but that invariant is enforced two modules away
(`main.py:88-90`). Contrast step 2's conservative handling when only the fetch timestamp is
missing ("re-apply surviving merges but discover none", `controller.py:820-824`).
*Treat `guard is None` the same way — uniformly conservative.*

**d. The "on-demand subject" predicate is written four times.** `(kind == "reclaim" or id
in noshow_reservation_ids) and id not in claimed_reservation_ids [and not merged_stub] and
window open` appears for the active list and again for `cancelled_reservations` in both
`reconcile_reclaim_merges` (`controller.py:827-839`) and `find_ondemand_block`
(`:919-941`), plus duplicated `_label` / `_label_matches` fallbacks (`:775-779`,
`:910-917`). *One `iter_ondemand_blocks(now)` generator and one `effective_label` helper
feeding both call sites.*

### D5. Fast path vs queue processor: structural duplication — **Low**

Beyond the cooldown bug (B8):

**a. Three idioms for removing a queue entry** — fast path: `task_queue.pop(uid, None)`
(`main.py:655`); processor: accumulate `to_remove` then pop (`main.py:818-830`); watch
DELETED / already-tolerated: `dequeue_pod(uid)` (`main.py:583, 622`), which logs at DEBUG.
Successful admissions therefore never produce the "Dequeued" log line while deletions do —
the log stream under-reports the most common removal reason. Same split on the on-demand
side: `remove_ondemand_candidate` (logging) vs raw `.pop(uid, None)` (`main.py:709, 842`).
*Route all removals through the logging helpers; add a `reason` parameter if useful.*

**b. `_recycle_ondemand_block` re-implements the processor's candidate scan** —
`main.py:695-710` vs `833-842` both sort candidates by `pod_created_at`, filter on
`next_attempt_at`, call `_try_place_ondemand`, pop on success — differing only in class
filter and stop-after-first-success. *One `_place_ondemand_candidates(state, config, *,
gpu_class=None, max_placements=None)` helper.*

### D6. "30-second queue-processor tick" survives in docs and comments; the default is 300 s — **Medium**

`config.py:57-59` defaults `POD_LIST_TICK_INTERVAL` to 300, and the CLAUDE.md env table,
Helm values, and Dockerfile agree — but five prose references still say 30 s:

- `README.md:56` — "Queue processor (every 30 s)"
- `CLAUDE.md:148` — "waiting up to 30 s for the next queue-processor tick"
- `CLAUDE.md:223` — "a window bounded by the 30 s tick interval" (so CLAUDE.md contradicts
  its own env table)
- `main.py:11` and `main.py:642` — "the 30-second queue-processor polling interval"

Consequences beyond stale prose: the fixed 30 s retries at `main.py:386, 439` were
evidently sized to a 30 s tick — at a 300 s tick the candidate becomes eligible after 30 s
but nothing looks at it for up to another 270 s (unless a `_recycle_ondemand_block` event
fires). And the documented "optimistic placement briefly dropped … bounded by the tick
interval" exposure (`controller.py:1090-1093`) is 10× what the doc implies.
**Recommendation:** replace numeric "30 s" with "the `POD_LIST_TICK_INTERVAL` tick
(default 300 s)" everywhere; express the short retry as a function of the tick or as
"next tick" semantics; re-evaluate whether the occupancy-drop caveat is still acceptable
at 300 s.

### D7. `initialize_noshow_tracking` and `update_noshow_tracking` are near-duplicates — **Medium**

`controller.py:215-250` vs `:252-285`: identical bodies except `update_` adds two guard
checks (`noshow_reservation_ids`, `claimed_reservation_ids`) — both provably empty at
startup, so `initialize_` is a strictly weaker special case. Two copies means the next
guard added to one (as `claimed_reservation_ids` evidently was) can be forgotten in the
other.
**Recommendation:** delete `initialize_noshow_tracking` and call `update_noshow_tracking`
from the lifespan (`main.py:876-880`); keep the distinct log tag via a
`reason: str = "new"` parameter if desired.

### D8. Mixed error conventions across the three HTTP endpoints — **Medium**

`fetch_reservations` (`reservation_client.py:38-73`) raises on any HTTP/network error;
`fetch_gpu_class` and `fetch_settings` catch and return `None`. Arguably deliberate (a
failed reservation fetch must abort the refresh; a failed lookup degrades gracefully), but
the asymmetry is undocumented — `fetch_reservations` has no "Raises:" note — and the
None-contract is leaky anyway (B9). Timeouts are also per-request literals (15.0 at `:56`,
10.0 at `:79, :97`) with no client-level default, so a future endpoint added without an
explicit timeout silently gets httpx's 5 s default.
**Recommendation:** document the intended contract per method; set
`timeout=httpx.Timeout(10.0)` on the `AsyncClient` and override only where needed.

### D9. Smaller state-lifecycle inconsistencies in `ControllerState` — **Low**

- **`reconcile_queue` never refreshes the retained reservation object.**
  `state.reservations` is replaced wholesale each cycle (`main.py:116`), yet
  `QueueEntry.reservation` keeps the object captured at enqueue time; `reconcile_queue`
  (`controller.py:608-651`) prunes by id but leaves survivors pointing at the stale
  snapshot. Currently harmless (windows immutable in this API), but it is the only place a
  wholesale-replaced object is retained without re-resolution — contrast
  `reconcile_reclaim_merges`, which re-resolves via `by_id` every cycle precisely because
  reloads replace objects (`controller.py:771-784`). *Swap in the fresh object in
  `reconcile_queue`.*
- **`check_noshow_deadlines` can declare a no-show for an already-ended window**
  (`controller.py:302-329` tests only `now >= deadline`), logging "capacity opened for
  on-demand placement" even though `find_ondemand_block` will never select it. *Skip
  entries whose `slot_end(res) <= now`.*
- **Pruning-log levels differ for parallel prunes** — `reconcile_noshow` prunes deadlines
  at DEBUG but ids at INFO (`controller.py:287-300`); `cleanup_cancelled_reservations` at
  DEBUG (`:1079`); merge drops at INFO except the silent claimed case (B4). *Pick one level
  per significance class (state affecting placement decisions → INFO).*

### D10. `main.py` digs into pod internals that `k8s_client.py` owns — **Medium**

Per AGENTS.md, `k8s_client.py` owns pod-object interpretation, and `main.py` mostly honors
that via accessors (`get_pod_phase`, `get_pod_gpu_count`, `get_pod_booking_reference`).
Exceptions:

- `main.py:416-422` re-implements the `PodScheduled`-condition lookup that
  `is_gpu_only_pending` (`k8s_client.py:174-181`) and `snapshot_tolerated_pods` (`:256-257`)
  already contain — a third copy, outside the owning module.
- Raw reads of `pod.spec.active_deadline_seconds` (`main.py:221, 481`) and
  `pod.metadata.creation_timestamp` (`main.py:664`).

**Recommendation:** expose the scheduling message from `is_gpu_only_pending` (or a sibling
helper) and add trivial accessors for the two raw reads.

---

## Part III — Hygiene: naming, types, logging, dead code

### H1. `LOG_LEVEL` is parsed outside `config.py` and undocumented in the env table — **Medium**

`main.py:62` reads `os.environ.get("LOG_LEVEL", "INFO")` directly — the only env read
outside `Config.from_env` in `app/`, contradicting AGENTS.md's "config.py owns all
environment-variable parsing". CLAUDE.md's env table omits it (OBSERVABILITY.md and the
Helm chart do document/wire it).
**Recommendation:** add `log_level` to `Config`; add the row to CLAUDE.md's table.

### H2. No exception handler preserves tracebacks — **Medium**

Every catch logs only `%s` of the exception (e.g. `main.py:545`, `main.py:324-330`). For a
daemon whose only diagnostic surface is logs, a `TypeError` deep in merge arithmetic and a
transient 500 from the API render identically as one line.
**Recommendation:** `exc_info=True` (or `log.exception`) on at least the top-of-loop
catches (`reservation_fetch_loop`, the queue processor's snapshot catch) where the
unexpected-bug class of error can hide.

### H3. Naming drift — **Low**

- **`noshown_*` (config/env) vs `noshow_*` (state):** `NOSHOWN_TIMEOUT_MINUTES` /
  `noshown_grace_minutes` (`config.py:19-20`) vs `noshow_deadlines` /
  `noshow_reservation_ids` (`controller.py:164-168`). "Noshown" is non-standard and
  grep-hostile. Renaming env vars is breaking; consider `NOSHOW_*` aliases + deprecation.
- **reservation / block / slot / window:** the same `ReservationResponse` is a
  "reservation" on the reserved path, a "block" on the on-demand path, and a "slot" in
  occupancy logs ("Released slot:", `controller.py:991`), while `slot_start`/`slot_end`
  docstrings say "window". CLAUDE.md sanctions reservation-vs-block as path vocabulary;
  "slot" is a third term used nowhere else. Rename the log text; consider
  `window_start`/`window_end` if those functions are ever touched again.

### H4. Type-hint coverage is uneven; `Optional` vs `X | None` mixed — **Low**

`controller.py`, `reservation_client.py`, `config.py`, `schemas.py` are fully and precisely
annotated. `main.py` and `k8s_client.py` are the least-typed part of the codebase: untyped
`pod`/`fresh_pod` parameters throughout (kubernetes' `V1Pod` would state intent),
`cancelled_in_window: list` (`main.py:146`) where `list[ReservationResponse]` is known,
`labels: dict` (`main.py:574`), `health() -> dict` (`main.py:935`), `read_pod` with no
return annotation (`k8s_client.py:205`). `main.py:575` uses `str | None` while lines
236/255/342/686 use (unimported) `Optional[str]` — pick one style per file at minimum
(both files have the future import, so `X | None` is safe).

### H5. Log formatting inconsistencies — **Low**

Broadly consistent (`except Exception as exc` + %-style lazy formatting throughout; no
f-strings in log calls; identifiers usually `namespace/name`). Remaining:

- **Four timestamp formats:** `"%Y-%m-%d %H:%M"` (`controller.py:249, 284, 496, 954`),
  end-only `"%H:%M"` (`:497, 955`), hand-rolled ISO `"%Y-%m-%dT%H:%M:%SZ"`
  (`controller.py:1068`, `main.py:598-599`), and `.isoformat()` (`controller.py:811, 887`).
  The first form prints UTC with no timezone marker — easy to misread as local. *One `ts()`
  helper.*
- **Occupancy logs identify pods by uid only** (`controller.py:972-979, 990-995`) while
  every other line uses `namespace/name`; and the `←` glyph means "placed" in one message
  and "freed" in the other. *Pass namespace/name in; use explicit verbs.*
- **Budget/no-capacity outcomes are DEBUG-only** (`main.py:277-286, 367-372`) while every
  other placement-affecting state change is INFO/WARNING — a user's pod sitting unadmitted
  for budget reasons is invisible at the default `LOG_LEVEL=INFO`. *Log INFO on first entry
  into backoff, DEBUG thereafter.*
- **`now` re-sampling:** `_try_place_ondemand` samples `datetime.now(timezone.utc)` five
  times (`main.py:358, 373, 386, 439, 478`); `_refresh_reservations` mixes a captured `now`
  with a fresh sample at `main.py:135`. *Sample once per invocation and thread it through*
  (the fetch-time stamp predating the per-class awaits deserves a comment either way — the
  merge design hinges on that timestamp).

No naive datetimes were found anywhere in `app/` — every `datetime.now()` uses
`timezone.utc`; the only local-time leak is `date.today()` (B2).

### H6. Dead code and unused parameters — **Low**

- `config.health_port` parsed (`config.py:17, 46`) but never read repo-wide — see P2.
- `_handle_cancelled_reservations(..., now)` — `now` threaded in (`main.py:129`) and never
  used (`main.py:143-201`).
- `is_gpu_only_pending(pod, toleration_key)` — `toleration_key` never referenced
  (`k8s_client.py:153-197`); the docstring's claim about keyed acceptance is true only
  incidentally. Drop the parameter or implement the check.
- Unused `datetime` import in `reservation_client.py:11`.
- `available(reservation, exclude_uid)` (`controller.py:718-732`) vs
  `available_by_id(reservation_id)` (`:1002-1008`) compute the same quantity from different
  keys; the latter exists only for a debug log and silently returns 0 outside
  `self.reservations` (B11). Fold together.
- `mark_pod_seen_for_noshow`'s `booking_reservation_id is None` fallback
  (`controller.py:372-391`) is the last remnant of the pre-annotation heuristic ("clear the
  soonest matching reservation") and can vouch for the wrong reservation for
  externally-tolerated pods. Worth a deliberate keep-or-remove decision.

### H7. Schema smells — **Low**

- `AppSettings` requires `reclaim_window_minutes` (`schemas.py:66`) which nothing consumes —
  if the API renames/omits it, validation fails, `fetch_settings` returns None, and reclaim
  merging silently disables. Drop it or give it a default.
- `GpuClassBrief` (`schemas.py:24-27`) and `GpuClassDetail` (`:70-80`) declare identical
  shapes — two names for one shape invites drift. Subclass or merge.
- Otherwise consistent: plain `BaseModel` throughout, `Optional[...] = None` uniform, no
  consumer reads a field the models don't declare, no dict access bypassing the models.

---

## Part IV — Test suite

**Status: 232 passed in 0.83 s, zero warnings** (including `-W error::DeprecationWarning`);
no skipped/xfailed/dead tests. `pytest.ini` (`testpaths = tests`) is sufficient for what
the suite relies on.

### T1. No `conftest.py`; the reservation factory exists in six drifted copies — **High**

- `_compute_window` is **byte-identical in 5 files** (md5-verified):
  `test_controller_reserved.py:32`, `test_noshow.py:35`, `test_ondemand.py:29`,
  `test_chain_and_booking.py:29`, `test_noshow_chain.py:27`.
- `_user_reservation` exists in 4 of those with **drifted signatures** —
  `test_controller_reserved.py:46-56` accepts `gpu_class_id`/`start_time`/
  `reservation_date`; `test_noshow_chain.py:41-48` dropped all three.
- Two further independent factories: `_user_res` (`test_cancellation.py:41-72`,
  minutes-from-midnight offsets) and `_block` (`test_reclaim_merge.py:24-52`, explicit
  datetimes — arguably the best of the six).
- Behavioral drift: only `test_cancellation.py:61` populates `GpuClassBrief.label_value`,
  so the `_label_matches` fallback path is exercisable from one file only; some factories
  pass naive `created_at=datetime(2024, 1, 1)` (`test_controller_reserved.py:72`) while
  others are tz-aware; the `_state(...)` helper is duplicated in 5 files.

**Recommendation:** create `tests/conftest.py` with one factory (explicit
`start_utc`/`end_utc`, like `_block`) and one state fixture; delete the copies.

### T2. Legacy "slot grid" vocabulary; `TestSlotArithmetic` tests the helper — **Medium**

Shared helpers still simulate the removed slot-based API (`slot_index`, "from policy
fields", `test_controller_reserved.py:32-43`), while the product's `slot_start`/`slot_end`
are trivial accessors (`controller.py:54-61`). `TestSlotArithmetic`
(`test_controller_reserved.py:101-128`, 6 tests) therefore mostly verifies the *test
helper's* arithmetic. Collapse to one accessor test; rename helper params.

### T3. Duplicate coverage across files — **Medium**

- Chain-break conditions tested twice through the same code path:
  `TestComputeMaxDeadline.*_breaks_chain` (`test_controller_reserved.py:306-334`) vs
  `TestChainFor.*_breaks_chain` (`test_chain_and_booking.py:132-155`).
- `test_noshow.py:498-507` is behaviorally identical to `test_ondemand.py:317-323`
  (`reconcile_occupancy` never consults `noshow_reservation_ids`).
- `find_ondemand_block` criteria re-tested three times with three sources
  (`test_ondemand.py:127-226`, `test_noshow.py:433-490`, `test_cancellation.py:185-258`);
  a parametrized "source" fixture would halve it.

### T4. Mixed organization and idioms — **Medium**

Classes vs module-level functions (7 files vs 2) with no convention; parametrize used in
two files while `test_guards.py:106-193` hand-writes ~12 near-identical cases;
`import pytest` unused in 4 files; mid-function imports in `test_guards.py:221-283`; two
different SimpleNamespace mock-pod builders (`test_k8s_helpers.py:46`,
`test_guards.py:41`) for the same collaborator — the latter's docstring even claims to
follow the former while diverging.

### T5. Async testing is one-off and fragile — **Low**

Only `test_guards.py:249-326` tests coroutine code, via manual `asyncio.run` +
monkeypatching + env-var setup needed because importing `app.main` executes `create_app()`
at import time (`main.py:926`). Works without pytest-asyncio (consistent with
requirements-dev.txt), but isn't reusable, and any test importing `app.main` without the
env vars errors. Minor: `_make_main_module_and_state`'s docstring promises a 3-tuple, the
function returns 2 values (`test_guards.py:220, 246`). Also
`test_stuck_holder_gpu_classes_can_be_set` (`test_guards.py:209-216`) asserts that Python
attribute assignment works — no product logic; and `test_cancellation.py:7, 267` misname
`canceller_description` as `_canceller_description` in main.py (it lives in
`controller.py:28`).

### T6. Coverage gaps (by inspection, not by coverage run)

Documented behaviors with no tests: the **fast path** and `_try_apply_toleration` itself
(budget check, optimistic record/rollback, already-tolerated dequeue, `main.py:253-332,
641-655`); **scheduling-gate removal** (`k8s_client.py:336-365`, `main.py:235-250`);
**occupancy rebuild end-to-end** (`snapshot_tolerated_pods` / the tolerated-pod
`record_placement` branch, `main.py:620-632` — `reconcile_occupancy` is only fed pre-parsed
tuples); **runtime-cap enforcement decision** (`_enforce_deadline` patch-only-when-exceeds
and best-effort semantics — the arithmetic *is* tested); **`Config.from_env`**
(required-var errors, falsy parsing); **`ReservationClient`** (pagination, `status=all`,
error handling — AGENTS.md prescribes pytest-httpx, which isn't installed; see P7);
**GPU-class label-cache reuse** (`main.py:97-114`); **cancellation eviction flow**
(event-then-delete ordering, 404 tolerance); **`_recycle_ondemand_block`** FIFO rule.

---

## Part V — Documentation & deployment drift

### P1. README's manual RBAC lacks `delete` on pods — cancellation eviction breaks — **High**

The controller deletes pods on mid-window cancellation (`main.py:190` → `delete_pod`,
`k8s_client.py:447-467`). The Helm ClusterRole is correct
(`helm/gpu-reservation-controller/templates/clusterrole.yaml:13` includes `delete`), but
README's manual manifest has `verbs: ["get", "list", "watch", "patch"]`
(`README.md:360-362`) and the prose says "write access (PATCH)" (`README.md:351-352`).
Manual deployments get 403s on every eviction, degraded to a `Could not delete pod`
warning — pods keep running through cancelled windows.
**Recommendation:** add `delete` to the README ClusterRole; mention eviction in the prose.

### P2. `HEALTH_PORT` is dead config; Helm `healthPort` actively breaks probes — **High**

`Config.health_port` is parsed (`config.py:17,46`) but never read anywhere; the port is
fixed by the uvicorn CLI hardcoded in the Dockerfile CMD (`Dockerfile:50`). Meanwhile Helm
wires the `HEALTH_PORT` env *and* binds `containerPort` and both probes to it
(`deployment.yaml:39,58-59,81-97`), and CLAUDE.md/README advertise it as functional.
Setting `config.healthPort: 9000` makes probes poll :9000 while uvicorn listens on :8000 →
CrashLoop via failed liveness.
**Recommendation:** either consume `health_port` (template the container args / run uvicorn
programmatically) or delete the setting from config.py, Dockerfile ENV, Helm values, and
both doc tables.

### P3. `TZ` still described as affecting window arithmetic — **Medium**

Code does all window math on `start_utc`/`end_utc`; CLAUDE.md and AGENTS.md are correct.
Two places lag: `README.md:282` ("IANA timezone for reservation window arithmetic") and
`helm/.../values.yaml:56` (same stale comment). Update both to "log timestamp display
only". (And note B2 — until `date.today()` is fixed, `TZ` *does* still affect fetch
bounds, which is a bug, not a feature.)

### P4. README config table missing three env vars — **Medium**

`README.md:274-285` omits `POD_LIST_TICK_INTERVAL` (`config.py:57`),
`POD_SCHEDULING_GATE_NAME` (`config.py:60` — README never mentions scheduling gates at all
despite the feature being wired through both admission paths), and `LOG_LEVEL`
(`main.py:62` — also missing from CLAUDE.md's table; see H1).

### P5. `cancelled_by` object missing from RESERVATION-API.md §6 — **Medium**

`schemas.py:54` models `cancelled_by: Optional[UserBrief]` and the controller depends on it
(`canceller_description`, `controller.py:28-42`; `main.py:150-151`), but the API spec
documents only `cancelled_by_id` (`docs/RESERVATION-API.md:797`; JSON examples at 259, 769
likewise omit it). If the API really doesn't send it, the "by <username>" event-message
path is dead code; if it does, the spec lags. Add it to §6, mirroring `submitted_by`.

### P6. OBSERVABILITY.md misses two whole log sources despite claiming completeness — **Medium**

OBSERVABILITY.md:3 claims to inventory "every structured log point". Missing: all of
`reservation_client.py` (INFO fetch summary `:66-72`, two WARNINGs `:84-92, :100-105`) and
all scheduling-gate messages (`k8s_client.py:346-365`, `main.py:246-250`). (Its "retry in
30 s" rows are correct — those retries are literal, see D6.)

### P7. AGENTS.md testing guidance references tools that aren't present — **Low**

`AGENTS.md:83-85` prescribes pytest-httpx and the kubernetes fake client; neither is in
`requirements-dev.txt` (pytest only) and no test uses them. Either adopt them (T6 gives the
candidates) or fix the guidance.

### P8. Assorted low-severity doc/deploy items — **Low**

- **README project layout** (`README.md:485-508`): lists `docs/overview.md` (doesn't
  exist) and `RESERVATION-API.md` at repo root (lives in `docs/`); omits OBSERVABILITY.md,
  docs/SCHEDULING.md, requirements-dev.txt, pytest.ini.
- **Stale "count" vocabulary:** CLAUDE.md's architecture tree ("k8s_client.py … count
  usage") and `README.md:495, 59-63` describe the removed per-attempt GPU counting; the
  design is now occupancy map + per-tick snapshot (`k8s_client.py:239-242` docstring).
- **`reservation_client.py` module docstring** (`:3-5`) omits `/api/settings`.
- **CI workflow is a copy-paste from another repo** (`.github/workflows/docker.yml:1,19,73`
  — name "Build notebook", "Checkout notebook" steps, weekly cron rebuild, ~30 lines of
  disk-space purging for a slim image; dated action versions `checkout@v3`,
  `login-action@v2`). It does work (the build runs the test stage); rename and trim.
- **Dockerfile ENV block** (`Dockerfile:36-38`) lists only three "optional tuning" vars;
  later-added settings are absent — the half-list misleads (and includes dead
  `HEALTH_PORT`, P2).
- **Helm `replicaCount: 1`** (`values.yaml:3`) is dead — the template hardcodes
  `replicas: 1` (`deployment.yaml:11`, intentional per its comment). Remove the value or
  template it with a guard.
- **requirements.txt**: all five entries used, nothing missing; only `pydantic>=2` is
  constrained at all, so image builds are unreproducible — consider pinning or a lock file.

---

## What was checked and found consistent

For balance — these were explicitly audited and are *not* drifted:

- **Executor discipline** in `k8s_client.py`: no direct `_core_v1` call from async code;
  the two direct calls (`:555, 577`) are inside the dedicated watch thread, as required.
- **UTC discipline**: every `datetime.now()` in `app/` passes `timezone.utc`; all window
  math uses `start_utc`/`end_utc`. The single leak is `date.today()` (B2).
- **CLAUDE.md env table ↔ `Config` fields** match 1:1 (modulo `LOG_LEVEL` H1 and inert
  `HEALTH_PORT` P2).
- **Booking-reference round-trip**: every prefix written by `main.py` is accepted by
  `parse_booking_reference` (the split-ownership *risk* is D2; there is no current
  mismatch).
- **Helm chart**: the healthiest artifact — every config.py env var wired, values mirror
  code defaults, RBAC complete (pods get/list/watch/patch/delete + events create),
  single-replica enforced with an explanatory comment.
- **Dockerfile vs docs**: base image, non-root user, healthcheck, port, and in-build test
  stage all match CLAUDE.md/README.
- **Log call style**: uniformly %-style lazy formatting, no f-strings in log calls,
  `except Exception as exc` convention throughout.
- **Test suite health**: 232/232 green, warning-free, no skipped or dead tests.

---

## Suggested remediation order

1. **Bug fixes, small and surgical** (one PR each or one batch): B1, B2, B3, B8, B9, B5 —
   all are few-line changes with existing test files to extend.
2. **Doc/deploy corrections** (no code risk): P1, P3, P4, P5, P6, D6's prose edits, P8.
3. **Unify the admission paths** (D1a–c fixes B3's class of bug structurally; D2 makes the
   booking-reference contract explicit) — the highest-leverage consistency work.
4. **Test-suite consolidation** (T1 conftest first — it unlocks cheaply adding the T6
   coverage for the code being unified in step 3).
5. **Opportunistic hygiene** as touched files come up: D3 (K8s write-op patterns), D4a
   (`effective_end` overlay), D7, H1–H6.
