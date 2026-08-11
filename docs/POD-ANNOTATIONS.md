# Pod annotations — in-pod consumer reference

Audience: anyone building an **in-pod** consumer of the GPU reservation
controller's status — a JupyterLab extension, a VS Code status-bar item, a shell
prompt/MOTD, a checkpointing wrapper.

The controller stamps every pod it admits with a small set of annotations
describing **what reservation the pod is running under**, **how long its GPU
access is guaranteed**, and **whether it is currently at risk of being
terminated to free capacity**. Everything below is readable from inside the
container through a downward-API volume — no Kubernetes API access, no service
account, no network call to the reservation app.

> **All of these annotations are informational.** Nothing is enforced through
> Kubernetes (`spec.activeDeadlineSeconds` is never set), and the controller
> never reads them back to make a decision — it recomputes everything live from
> reservation state. Treat them as a best-effort heads-up, not a contract. See
> [Robustness rules](#7-robustness-rules) for what that means in practice.

---

## 1. Wiring: exposing annotations to the container

Project **all** of `metadata.annotations` into a single file. Add to the pod
spec (this is the workload's own spec — JupyterHub `singleuser.storage.extraVolumes`,
a VS Code dev-container pod template, etc. — the controller does not add it):

```yaml
spec:
  volumes:
    - name: podinfo
      downwardAPI:
        items:
          - path: annotations
            fieldRef:
              fieldPath: metadata.annotations
  containers:
    - name: notebook
      volumeMounts:
        - name: podinfo
          mountPath: /etc/podinfo
          readOnly: true
```

**Use a volume, not `env:`.** Downward-API *environment variables* are resolved
once at container start and never change; every annotation here is written
*after* the pod starts and some change during its life. A downward-API *volume*
is refreshed by the kubelet.

### File format

One line per annotation, sorted by key, rendered by the kubelet as Go `%q`:

```
galends/admitted-at="2026-08-11T17:02:11Z"
galends/booking-reference="res-4812"
galends/gpu-class-name="H100"
galends/guarantee-status="guaranteed"
galends/guaranteed-until="2026-08-11T20:00:00Z"
galends/pod-runtime-limit-seconds="10800"
galends/reservation-end="2026-08-11T19:00:00Z"
galends/reservation-gpu-count="4"
galends/reservation-kind="booking"
galends/reservation-start="2026-08-11T17:00:00Z"
kubectl.kubernetes.io/last-applied-configuration="{\"apiVersion\":\"v1\",...}"
```

Every value is double-quoted, and `"`, `\`, newlines and tabs inside a value are
backslash-escaped — so **one annotation is always exactly one line**, even for
values that contain newlines (`last-applied-configuration` routinely does).
Split each line on the *first* `=`, then unquote. A minimal parser:

```python
import re

_ESCAPES = {'"': '"', "\\": "\\", "n": "\n", "t": "\t", "r": "\r"}

def parse_downward_annotations(path="/etc/podinfo/annotations") -> dict[str, str]:
    out = {}
    with open(path, encoding="utf-8") as fh:
        for line in fh:
            key, sep, raw = line.rstrip("\n").partition("=")
            if not sep or len(raw) < 2:
                continue
            out[key] = re.sub(
                r"\\(.)", lambda m: _ESCAPES.get(m.group(1), m.group(1)), raw[1:-1]
            )
    return out
```

### Update semantics

- The kubelet refreshes the file on its sync loop — **up to ~60 s** (the
  kubelet's `syncFrequency`, default `1m`) after an annotation changes. Budget
  for this on top of the controller's own cadence (§6).
- The mount is the usual `..data` symlink swap: `/etc/podinfo/annotations` is a
  symlink, replaced atomically on each update. An `inotify` watcher must watch
  the **directory** (`IN_MOVED_TO` / `IN_CREATE` on `..data`), not the file
  inode, or it will only ever fire once. Polling the file every 15–30 s is
  simpler and perfectly adequate for these fields.
- Reads are atomic — you never see a half-written file — but the set is only
  *eventually* consistent as a group. Re-read the whole file each time and
  recompute state from the snapshot rather than diffing individual keys.

---

## 2. Annotations the controller writes

| Key | Written | Value | Lifecycle |
|-----|---------|-------|-----------|
| `galends/booking-reference` | at admission | `res-<id>`, e.g. `res-4812` — the reservation the pod is running under | Rewritten when the pod is re-linked to another reservation (adoption / lease→booking merge). Its **presence is the signal that the controller manages this pod**. |
| `galends/guarantee-status` | at admission, refreshed | `guaranteed` \| `overstay` | `guaranteed` at admission; flips to `overstay` once the guarantee lapses. Never removed while the pod lives. |
| `galends/guaranteed-until` | at admission, refreshed | absolute UTC instant, `YYYY-MM-DDTHH:MM:SSZ` | Kept *live* while `guaranteed` — it can move **later** if the user books an abutting follow-on window. Once `overstay` it is frozen at its now-past value. |
| `galends/pod-runtime-limit-seconds` | at admission | integer seconds, e.g. `10800` | The guaranteed *duration* at the moment it was recorded. **Not** refreshed as the guarantee grows — for a countdown, use `guaranteed-until`, not this. |
| `galends/reservation-kind` | at admission, on re-link | `booking` \| `on_demand` | Describes the reservation the pod is *currently* linked to, so it changes if the pod is re-linked. |
| `galends/reservation-start` / `galends/reservation-end` | at admission, on re-link | absolute UTC instant, same format | The reservation's **own** window. Same lifecycle. |
| `galends/reservation-gpu-count` | at admission, on re-link | integer, e.g. `4` | GPUs the *reservation* holds, not what the pod requested. Same lifecycle. |
| `galends/gpu-class-name` | at admission, on re-link | display name, e.g. `H100` | Same lifecycle. |
| `galends/admitted-at` | at first admission only | absolute UTC instant, same format | Written once and never rewritten — a re-link is not a new admission. Never removed while the pod lives. |
| `galends/termination-warning-at` | while at risk | absolute UTC instant, same format | **Appears and disappears.** Present only while the pod is in the at-risk pool; all three warning keys are removed together when the risk clears. |
| `galends/termination-warning-risk` | while at risk | decimal string in `(0, 1]`, 2 dp, e.g. `0.33` | Same lifecycle. |
| `galends/termination-warning-message` | while at risk | human-readable English sentence | Same lifecycle. Rendered deterministically from the other two; safe to display verbatim. |

### What each one means

**`galends/reservation-*` — what the pod is running under.** The `booking-reference`
names *which* reservation; these describe it.

`reservation-kind` is the one that changes your copy the most. `booking` means the
user reserved this window themselves, through the reservation app. `on_demand`
means no reservation was open when their pod started, so the controller requested a
short lease on their behalf, just-in-time — that lease is a real reservation, SU is
charged for it, and it is protected by the same runtime guarantee, but the user
never asked for it and will not recognise it from their calendar. Say so plainly
("started on an on-demand lease until 16:10") rather than calling it "your
reservation".

`reservation-start`/`-end` are that reservation's **own** window, which is *not*
the same as `guaranteed-until`: the guarantee runs to the end of the back-to-back
chain, so a user with three abutting bookings has one window here and a
guarantee three windows long. Show the window for "what you booked" and the
guarantee for "how long you are safe".

`reservation-gpu-count` is how many GPUs the *reservation* holds — compare it
against the pod's own `nvidia.com/gpu` request to tell a user they are using 1 of
the 4 GPUs they booked. It is not a per-pod figure: a user running several pods
under one booking sees the same count on each.

**`galends/admitted-at` — when this pod got its GPU.** Written once, on the pod's
first admission, and never rewritten — including when the pod is re-linked to a
different reservation. That makes it the right anchor for a session-elapsed
clock; `reservation-start` is not (it moves on a re-link, and can predate the pod
by hours when the pod started mid-window).

**`galends/guaranteed-until` — the runtime guarantee.** The instant until which
this pod's GPU access is protected. It is the end of the pod's current
reservation window, extended through any directly back-to-back follow-on
reservations by the same owner for the same GPU class and GPU count. Inside the
guarantee the pod is **never** preempted by the controller, however severe the
cluster shortfall.

Past that instant the pod is not killed either — it keeps running until some
*other* reservation actually needs the capacity. That is the `overstay` state:
still running, no longer protected.

**`galends/termination-warning-at` — the projected kill instant.** Present only
when the controller has identified this pod as an eligible victim at an upcoming
reservation boundary where its GPU class is short on capacity. The value is the
**earliest** instant the pod could actually be deleted (the start of the sweep's
kill window, which opens `PREEMPTION_LEAD_MINUTES` — default 15 — *before* the
boundary, but never before the pod's own guarantee ends). Not a scheduled
execution time: the pod may well survive it (§7).

**`galends/termination-warning-risk` — how likely.** The fraction of the
eligible pool that has to be killed at that boundary, `min(1, shortfall / pool_gpus)`.
`1.00` means the whole pool is needed and the pod will almost certainly be
picked; `0.20` means roughly a one-in-five chance. Pool *membership* is exact;
the number models uniform-random victim selection, which is the controller's
local fallback — when victim selection is delegated to the reservation app, the
app's policy may differ. Render it as a coarse band ("possible" / "likely"),
not as a precise probability.

## 3. Annotations the controller *reads* (job inputs)

Set by whoever creates the pod; the controller consumes them and never writes
them. Worth surfacing read-only in a UI, since they explain admission behaviour:

| Key | Purpose |
|-----|---------|
| `galends/minimum-runtime-seconds` | Positive integer. Required for a pod to be eligible for a just-in-time on-demand lease when no reservation is open; also sizes that lease. A pod without it simply waits for a matching reservation. |
| `galends/usage-group` | The usage group a JIT lease is created under. Required for JIT eligibility unless the deployment identifies the group through a pod *label* instead (`REQUIRED_GROUP_LABEL`). |

The pod's `gpu-class` **label** (not an annotation, so it is not in this file
unless you also project `metadata.labels`) names the GPU class.

---

## 4. Deriving a display state

Everything a consumer needs is four fields. Recompute on every read:

```python
from datetime import datetime, timezone

def status(ann: dict[str, str], now=None):
    now = now or datetime.now(timezone.utc)
    ref = ann.get("galends/booking-reference")
    if not ref:
        return "unmanaged"                     # not admitted (yet) by the controller

    until = _parse_utc(ann.get("galends/guaranteed-until"))   # ...Z -> aware datetime
    in_guarantee = until is not None and until > now          # trust the clock, not the label
    at_risk = "galends/termination-warning-at" in ann

    if in_guarantee and not at_risk:
        return "guaranteed"      # protected until `until`
    if in_guarantee and at_risk:
        return "guarantee-ending"# protected now, flagged to be reclaimed when it lapses
    if at_risk:
        return "at-risk"         # past guarantee AND wanted by an incoming reservation
    return "overstay"            # past guarantee, nothing wants the capacity right now
```

Suggested presentation:

| State | Tone | Copy |
|-------|------|------|
| `guaranteed` | neutral / green | "GPU reserved for another 2 h 14 m (until 20:00 UTC)." |
| `guarantee-ending` | amber | "Reservation ends 20:00 UTC; another job is booked to start then — this pod may be stopped from 19:45 UTC. Extend or re-book to keep the GPU." |
| `overstay` | neutral / grey | "Running past your reservation. The GPU is free for now, but this job can be stopped at any time to make room." |
| `at-risk` | red | Show `galends/termination-warning-message` verbatim, plus a countdown to `termination-warning-at`. |
| `unmanaged` | none | Show nothing. |

Countdowns should target `guaranteed-until` (state `guaranteed`) or
`termination-warning-at` (state `at-risk`), computed client-side against the
current time. All timestamps are UTC with an explicit `Z`; parse as
timezone-aware and render in the user's local zone.

The state above is orthogonal to `reservation-kind`, which sets the *noun* in
that copy: a `guaranteed` pod on a `booking` has "your reservation until 20:00",
the same pod on an `on_demand` lease has "an on-demand lease until 20:00". Both
are real reservations charged in SU, so neither is "free" or "best-effort"
capacity — the difference is only whether the user asked for it.

Three things to *avoid* claiming in copy: don't say the job "will be terminated
at" the warning time (it is the earliest possible moment, not a schedule); don't
say an overstaying job is "over its limit" or "in violation" — overstay is a
normal, permitted mode; and don't describe an `on_demand` lease as the user's own
booking, since it will not appear in their calendar as one.

---

## 5. What the controller does when the moment arrives

A preempted pod is **deleted** (`DELETE` on the pod, normal graceful
termination — `SIGTERM`, then `terminationGracePeriodSeconds`). It is not
evicted, not restarted in place, not paused. A consumer that wants to checkpoint
should do it on the warning, not on `SIGTERM` — the grace period is whatever the
pod spec sets, typically 30 s.

The controller also emits Kubernetes **Events** against the pod
(`RuntimeGuaranteed` at admission, `OverstayRelinked` when a pod is re-linked to
a new reservation, `Preempted` immediately before deletion). These are richer
than the annotations but need Kubernetes API access to read, so they are for
operators and dashboards rather than in-pod consumers.

---

## 6. Propagation latency

Annotations are reconciled on the controller's loops, then picked up by the
kubelet. Worst-case in-pod visibility, with default settings:

| Change | Controller cadence | + kubelet | Worst case |
|--------|--------------------|-----------|------------|
| Admission (`booking-reference`, `guaranteed-until`, `guarantee-status`, `admitted-at`, all `reservation-*`, `gpu-class-name`) | immediate, on admission | ~60 s | ~1 min |
| Re-link (`booking-reference` and every `reservation-*` key change together) | every queue-processor tick, `QUEUE_PROCESSOR_INTERVAL` = 300 s, or immediately during a preemption sweep | ~60 s | ~6 min |
| Termination warning appears / changes / clears | every preemption sweep, `PREEMPTION_CHECK_INTERVAL` = 60 s | ~60 s | ~2 min |
| `guarantee-status` flips to `overstay`; `guaranteed-until` extended by a follow-on booking | every queue-processor tick, `QUEUE_PROCESSOR_INTERVAL` = 300 s | ~60 s | ~6 min |

Two consequences worth designing around:

- **Don't trust `guarantee-status` for the in-guarantee test** — it can lag the
  actual expiry by minutes. Compare `guaranteed-until` against the wall clock
  yourself (as in §4) and use `guarantee-status` only as a corroborating hint.
- **Expect roughly 15 minutes of warning, not more.** Warnings look ahead
  `TERMINATION_WARNING_LEAD_MINUTES` (default 30) to the boundary, while the
  projected kill instant is `PREEMPTION_LEAD_MINUTES` (default 15) before it —
  so a warning typically appears ~15 minutes before the time it names, less
  propagation. A pod whose own guarantee ends exactly at the boundary gets up to
  the full 30. Design checkpoint prompts for a ~10-minute usable window.

---

## 7. Robustness rules

1. **Every key is optional.** Handle each one missing, at any time — including
   `booking-reference` on a pod the controller has not admitted yet, and the
   warning trio during the window between sweeps.
2. **Warnings retract.** The three `termination-warning-*` keys are deleted when
   the pod leaves the at-risk pool — the user re-booked, the incoming
   reservation no-showed, demand evaporated, or the pod was re-linked to a new
   reservation. A UI that latches a red banner will show a false alarm
   indefinitely; clear it when the key disappears.
3. **`termination-warning-at` can pass without anything happening.** The
   shortfall it was computed from may be gone by the time it arrives. Never
   count down to zero and declare the job dead; fall back to "may be stopped at
   any time" once the instant passes.
4. **The guarantee can move in both directions.** Usually later (an abutting
   follow-on booking). It can technically shrink — a window shortened
   server-side — so re-read rather than caching the first value you saw.
5. **`pod-runtime-limit-seconds` goes stale by design.** It is the duration at
   admission and is not refreshed. Use `guaranteed-until` for anything the user
   sees.
6. **A pod's reservation can change under it.** `booking-reference` and every
   `reservation-*` key are rewritten together when the controller re-links a pod
   — to a window its user booked after the pod was already running, or from a
   just-in-time lease onto the matching booking once that booking opens. A pod
   can therefore go from `on_demand` to `booking` mid-session, with a new window
   and a new GPU count. Re-read the set rather than caching it at startup, and
   anchor anything cumulative (a session clock, an accrual estimate) on
   `admitted-at`, which does not move.
7. **Parse defensively.** `risk` is a decimal string (`float()` it, and clamp to
   `[0, 1]`); the timestamps are `YYYY-MM-DDTHH:MM:SSZ` (Python's
   `datetime.fromisoformat` accepts the `Z` suffix from 3.11 on); `res-<id>` and
   `reservation-gpu-count` are integers. Treat `reservation-kind` as an open set
   — match `booking` and `on_demand` explicitly and fall back to neutral copy for
   anything else, rather than assuming a value you do not recognise is a lease.
   Ignore a value that does not parse instead of erroring the whole widget.
8. **Nothing here is authoritative.** These are best-effort stamps. For
   authoritative, richer risk data — per-hour buckets, cluster-wide class
   summaries — the controller exposes
   `GET /api/forecast/preemption-risk` (bearer-token guarded, see README), but
   that is a cluster-side API, not something an in-pod widget should call.
