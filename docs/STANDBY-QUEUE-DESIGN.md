# Standby Queue — Design Note

Status: **exploratory** — architectural direction only, not yet implemented.

## Problem

Today the controller admits pods along two paths:

- **Reserved** (`QueueEntry` / `find_best_reservation`) — a pod matches an
  existing booking by namespace + GPU class.
- **On-demand** (`OnDemandCandidate` / `find_ondemand_block`) — a pod with no
  matching booking claims spare capacity (reclaim holds, declared no-shows,
  in-window cancellations) on a first-pod-first-served basis.

We're considering a third input: an ordered **standby queue**. A user
registers desired GPU class, minimum runtime, and an availability window
*before* launching any pod. When a matching on-demand/reclaim block opens,
the head-of-queue standby entry should get that block held for them (as if a
reservation had been made), be notified ("your table is ready" — delivery
mechanism TBD), and have the normal no-show timer start. Standby entries
outrank plain on-demand pods for block allocation.

## Recommendation

**Do not add a third pod-driven admission path, and do not build a
three-way abstraction over reserved / on-demand / standby.**

1. **Standby is not pod-driven.** Both existing paths are triggered by a pod
   appearing in the cluster and matched against existing state. A standby
   entry precedes any pod — it's a request for future capacity, and its
   fulfillment produces a *held reservation*, not an admitted pod. It belongs
   upstream of both existing paths, not beside them.

2. **Fulfill a standby match by creating a real booking.** When a standby
   entry wins a block, the reservation app converts it into a booking owned
   by that user. Everything downstream then works unchanged:
   - `_iter_ondemand_blocks` (`app/controller.py`) only yields
     reclaim / no-show / cancelled-in-window blocks, so a block converted to
     a booking automatically leaves the on-demand pool. This *is* the
     priority-over-FCFS guarantee — no new precedence logic needed.
   - `update_noshow_tracking` arms the existing no-show timer on the new
     booking.
   - A standby no-show returns the block to the pool via the existing
     no-show declaration path.
   - The user's eventual pod admits through the existing reserved path
     (`find_best_reservation` → `_try_apply_toleration`), with no new admission
     code.

3. **Abstract only the block-allocation step, across exactly two cases.** A
   standby entry and an `OnDemandCandidate` are both "demand for an open
   block" (GPU class, count, min runtime, an ordering key) — standby adds an
   availability window and always outranks pod FIFO. A single allocation
   pass per tick (standby order first, then `pod_created_at`) driving the
   existing `find_ondemand_block` could replace `_place_ondemand_candidates`
   (`app/main.py`). The reserved path should **not** be folded into this
   abstraction: it isn't competing for on-demand blocks, and its guard set is
   genuinely different (fast-path admission, back-to-back chaining, claimed
   reservations — see CODE-REVIEW D1 in `controller.py`).

## Ownership split

- **Reservation app** owns the ordered standby queue, contact info, UI, and
  notification delivery. It exposes a queue-fetch endpoint and a new `claim`
  write endpoint (creates the booking and pages the user).
- **Controller** fetches the standby queue each cycle (alongside reservations
  and settings) and runs the match — only the controller has visibility into
  declared no-shows and reclaim-merge overlays, both of which affect which
  blocks are actually open.

This requires the controller's service key to gain its first write
capability (today it's read-only); the reservation app's key-scope model
(`read_only` / `read_write`, see `docs/RESERVATION-API.md` §1) already
anticipates this distinction.

## Open questions

- How does the app represent a standby-claimed block that overlaps a
  declared no-show whose original booking is technically still "active"? A
  new reservation `kind`, or converting the no-show block in place?
- Should a block be held for a standby user ahead of their availability
  window opening, or claimed only once the block is already open?
- Min-runtime must be checked against the intersection of block-remaining
  time and the user's availability window, not either alone.
