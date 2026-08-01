"""Async HTTP client for the GPU Reservation management API.

Implements only the endpoints the controller needs:
  - GET /api/reservations  — paginated list of all (active + cancelled) reservations
  - GET /api/gpu-classes/{id}  — per-class detail including label_value
  - GET /api/gpu-classes  — full class list (JIT label → id resolution)
  - POST /api/reservations  — create a JIT on-demand booking
  - POST /api/reservations/ondemand-admission  — ask the app which pending pods to admit
  - POST /api/reservations/preemption-victims  — ask the app which overstay pods to preempt
  - POST /api/reservations/{id}/cancel  — cancel a reservation (no-show / revoke)
  - POST /api/reservations/{id}/overstay  — report an ended overstay's duration (analysis-only)
"""

from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from enum import Enum
from typing import Optional

import httpx
from pydantic import ValidationError

from .config import Config
from .schemas import (
    GpuClassDetail,
    OnDemandAdmissionRequest,
    OnDemandAdmissionResponse,
    OnDemandReservationRequest,
    OverstayReportRequest,
    PreemptionSelectionRequest,
    PreemptionSelectionResponse,
    ReservationResponse,
)

from . import trace
from .log_fields import kv

log = logging.getLogger(__name__)


# Bounded retry for the *paginated* reservation fetch only.  A blip on page 3 of
# 5 otherwise aborts the whole refresh cycle, leaving the controller on stale
# reservation state for a full RESERVATION_FETCH_INTERVAL (300 s default).  This
# narrows that blast radius; it deliberately does **not** become a general retry
# policy for the client, and the raise on final failure stays load-bearing (see
# ``fetch_reservations``) — acting on a partial page set would wholesale-replace
# ``state.reservations`` with an under-count.
_FETCH_MAX_ATTEMPTS = 3
_FETCH_RETRY_BACKOFF_SECONDS = (1, 2)


class LeaseOutcome(str, Enum):
    """Why a JIT lease request did or did not produce a lease.

    Splits what used to be a single ``None`` return: the caller's cooldown
    depends on whether the app *decided* against the lease or simply could not
    be reached, and those two want very different retry timing.
    """

    GRANTED = "granted"
    DENIED = "denied"            # the app answered "no" (4xx, typically 409)
    UNAVAILABLE = "unavailable"  # transient: network error or 5xx
    MALFORMED = "malformed"      # 2xx whose body would not parse


@dataclass(frozen=True)
class LeaseResult:
    """Outcome of ``create_ondemand_reservation``, plus the lease when granted."""

    outcome: LeaseOutcome
    lease: Optional[ReservationResponse] = None

    @property
    def granted(self) -> bool:
        return self.outcome is LeaseOutcome.GRANTED

    @property
    def transient(self) -> bool:
        """True when the failure may clear on its own, so retry sooner.

        Only ``UNAVAILABLE`` qualifies.  A ``DENIED`` is a real answer and a
        ``MALFORMED`` body is likely deterministic — hammering either one buys
        nothing, so both keep the slower jittered cooldown.
        """
        return self.outcome is LeaseOutcome.UNAVAILABLE


async def _attach_trace(request: httpx.Request) -> None:
    """Add ``X-Client-Trace`` so the app logs this request under our trace.

    The app's request middleware already reads this header, so nothing on its
    side had to change for a controller-initiated call to become correlatable.
    No header is sent when no unit of work is in scope.
    """
    for header, value in trace.outbound_headers().items():
        request.headers[header] = value


class ReservationClient:
    def __init__(self, config: Config) -> None:
        self._lookahead_days = config.reservation_lookahead_days
        # Client-level default timeout (CODE-REVIEW D8) so a future endpoint added
        # without an explicit timeout doesn't silently inherit httpx's 5 s default;
        # per-request timeouts below override it where a different value is wanted.
        self._client = httpx.AsyncClient(
            base_url=config.reservation_api_url,
            headers={"X-API-Key": config.reservation_api_key},
            timeout=httpx.Timeout(10.0),
            # Stamp the in-scope trace onto every outbound request from one
            # place rather than at each of the eight call sites — the same
            # chokepoint reasoning as kv() scrubbing values. A hook also cannot
            # be forgotten by an endpoint added later.
            event_hooks={"request": [_attach_trace]},
        )

    async def aclose(self) -> None:
        """Close the underlying HTTP connection pool."""
        await self._client.aclose()

    # ------------------------------------------------------------------
    # Public interface
    # ------------------------------------------------------------------

    async def _get_reservations_page(self, params: dict) -> httpx.Response:
        """GET one page of ``/api/reservations``, retrying transient failures.

        Retries a network error or a 5xx up to ``_FETCH_MAX_ATTEMPTS`` times with
        a short fixed backoff.  A **4xx propagates immediately**: a bad key or a
        bad filter is a deliberate answer that will not fix itself, and retrying
        it only delays the cycle's failure.  The final failure still raises, so
        the caller's fail-safe (abort rather than act on partial data) is intact.
        """
        last_exc: Optional[Exception] = None
        for attempt in range(1, _FETCH_MAX_ATTEMPTS + 1):
            try:
                resp = await self._client.get(
                    "/api/reservations", params=params, timeout=15.0
                )
                resp.raise_for_status()
                return resp
            except httpx.HTTPStatusError as exc:
                if exc.response.status_code < 500:
                    raise
                last_exc = exc
            except httpx.RequestError as exc:
                last_exc = exc
            if attempt == _FETCH_MAX_ATTEMPTS:
                break
            delay = _FETCH_RETRY_BACKOFF_SECONDS[attempt - 1]
            log.warning("%s", kv(
                event="api.reservations_fetch_retry", attempt=attempt,
                err=last_exc, retry_s=delay,
            ))
            await asyncio.sleep(delay)
        assert last_exc is not None  # only reachable via an except branch
        raise last_exc

    async def fetch_reservations(self) -> list[ReservationResponse]:
        """Return all reservations (active and cancelled) from today through lookahead window.

        Each page is fetched with a bounded transient retry
        (``_get_reservations_page``) so one blip mid-pagination does not cost a
        whole refresh interval of staleness.

        Raises:
            httpx.HTTPStatusError / httpx.RequestError: on API or network failure
                that outlives the retries.  Unlike ``fetch_gpu_class`` (which
                degrades to ``None``), a failed reservation fetch propagates so
                the refresh cycle aborts rather than acting on an empty or
                partial reservation list.
        """
        # UTC everywhere: date.today() would use the process TZ and drop
        # currently-open reservations east of UTC (see CODE-REVIEW-2026-07 B2).
        # The API filter is date-based while windows are UTC-instant-based, so
        # widen date_start by one day to avoid clipping a window open right now.
        today = datetime.now(timezone.utc).date()
        start = today - timedelta(days=1)
        end = today + timedelta(days=self._lookahead_days)
        results: list[ReservationResponse] = []
        offset = 0
        limit = 200

        while True:
            resp = await self._get_reservations_page({
                "status": "all",
                "date_start": start.isoformat(),
                "date_end": end.isoformat(),
                "limit": limit,
                "offset": offset,
            })
            page = [ReservationResponse.model_validate(r) for r in resp.json()]
            results.extend(page)
            if len(page) < limit:
                break
            offset += limit

        active = sum(1 for r in results if r.status == "active")
        log.info("%s", kv(
            event="api.reservations_fetched", count=len(results), active=active,
            cancelled=len(results) - active, lookahead_days=self._lookahead_days,
        ))
        return results

    async def fetch_gpu_class(self, gpu_class_id: int) -> Optional[GpuClassDetail]:
        """Return detail for a single GPU class, or None on error."""
        try:
            resp = await self._client.get(f"/api/gpu-classes/{gpu_class_id}")
            resp.raise_for_status()
            return GpuClassDetail.model_validate(resp.json())
        except httpx.HTTPStatusError as exc:
            log.warning("%s", kv(
                event="api.gpu_class_fetch_failed", cid=gpu_class_id,
                status=exc.response.status_code,
            ))
            return None
        except httpx.RequestError as exc:
            log.warning("%s", kv(event="api.gpu_class_fetch_failed", cid=gpu_class_id, err=exc))
            return None
        except (ValidationError, ValueError) as exc:
            # Malformed / unparseable payload (ValueError covers JSONDecodeError):
            # honor the documented "None on error" contract instead of letting it
            # abort the whole refresh cycle (B9).
            log.warning("%s", kv(event="api.gpu_class_parse_failed", cid=gpu_class_id, err=exc))
            return None

    async def fetch_gpu_classes(self) -> Optional[list[GpuClassDetail]]:
        """Return the full GPU class list, or None on error.

        Feeds the JIT lease path's label → id reverse map
        (``ControllerState.gpu_class_ids``): a pod carries only its gpu-class
        *label*, but creating an on-demand reservation needs the numeric
        ``gpu_class_id``.  Degrades to ``None`` like ``fetch_gpu_class`` so a
        transient failure does not abort the reservation refresh cycle.
        """
        try:
            resp = await self._client.get("/api/gpu-classes")
            resp.raise_for_status()
            return [GpuClassDetail.model_validate(c) for c in resp.json()]
        except httpx.HTTPStatusError as exc:
            log.warning("%s", kv(event="api.gpu_classes_fetch_failed", status=exc.response.status_code))
            return None
        except httpx.RequestError as exc:
            log.warning("%s", kv(event="api.gpu_classes_fetch_failed", err=exc))
            return None
        except (ValidationError, ValueError) as exc:
            log.warning("%s", kv(event="api.gpu_classes_parse_failed", err=exc))
            return None

    async def create_ondemand_reservation(
        self, req: OnDemandReservationRequest
    ) -> LeaseResult:
        """Request a JIT on-demand booking; a ``LeaseResult`` either way.

        Idempotent by ``req.idempotency_key`` (the admitting pod's UID): the
        app returns the original reservation for a repeated key rather than a
        duplicate, so the caller can safely retry with the same request.  That
        is what makes an ``UNAVAILABLE`` retry safe even when the request did in
        fact reach the app — a lease created behind a lost response is handed
        back on the retry instead of being duplicated.

        Unlike the other degrading calls this does **not** collapse to ``None``:
        a 409 denial and an unreachable app are different situations and get
        different cooldowns at the call site (see ``LeaseResult.transient``).
        """
        try:
            resp = await self._client.post(
                "/api/reservations",
                json=req.model_dump(mode="json"),
                timeout=15.0,
            )
            resp.raise_for_status()
            return LeaseResult(
                LeaseOutcome.GRANTED, ReservationResponse.model_validate(resp.json())
            )
        except httpx.HTTPStatusError as exc:
            status = exc.response.status_code
            # 5xx is the app failing, not the app deciding — treat it as
            # transient so the candidate comes back on the short retry.
            transient = status >= 500
            log.info("%s", kv(
                event="api.lease_denied", poduid=req.idempotency_key,
                status=status,
            ))
            return LeaseResult(
                LeaseOutcome.UNAVAILABLE if transient else LeaseOutcome.DENIED
            )
        except httpx.RequestError as exc:
            log.warning("%s", kv(
                event="api.lease_failed", poduid=req.idempotency_key, err=exc,
            ))
            return LeaseResult(LeaseOutcome.UNAVAILABLE)
        except (ValidationError, ValueError) as exc:
            log.warning("%s", kv(
                event="api.lease_parse_failed", poduid=req.idempotency_key, err=exc,
            ))
            return LeaseResult(LeaseOutcome.MALFORMED)

    async def select_preemption_victims(
        self, req: PreemptionSelectionRequest
    ) -> Optional[list[str]]:
        """Ask the app to choose which overstay candidates to preempt.

        Returns the selected pod UIDs (possibly empty — the app may deliberately
        spare pods), or ``None`` on any failure (endpoint missing on an older
        app, network error, non-2xx, unparseable body).  A ``None`` return tells
        the caller to fall back to local random selection; an empty list is a
        deliberate app decision and is respected.  Degrades like the other
        client calls so a transient failure never aborts the preemption sweep.
        """
        try:
            resp = await self._client.post(
                "/api/reservations/preemption-victims",
                json=req.model_dump(mode="json"),
                timeout=15.0,
            )
            resp.raise_for_status()
            return PreemptionSelectionResponse.model_validate(resp.json()).victim_pod_uids
        except httpx.HTTPStatusError as exc:
            log.warning("%s", kv(
                event="api.victim_selection_failed", status=exc.response.status_code,
            ))
            return None
        except httpx.RequestError as exc:
            log.warning("%s", kv(event="api.victim_selection_failed", err=exc))
            return None
        except (ValidationError, ValueError) as exc:
            log.warning("%s", kv(event="api.victim_selection_parse_failed", err=exc))
            return None

    async def select_ondemand_admissions(
        self, req: OnDemandAdmissionRequest
    ) -> Optional[list[str]]:
        """Ask the app which pending pods to grant JIT on-demand admission.

        Returns the granted pod UIDs (possibly empty — the app may deliberately
        grant none), or ``None`` on any failure (endpoint missing on an older
        app, network error, non-2xx, unparseable body).  A ``None`` return tells
        the caller to fall back to granting every offered candidate (today's
        greedy per-pod behaviour); an empty list is a deliberate app decision and
        is respected.  Degrades like the other client calls so a transient
        failure never strands the admission batch.
        """
        try:
            resp = await self._client.post(
                "/api/reservations/ondemand-admission",
                json=req.model_dump(mode="json"),
                timeout=15.0,
            )
            resp.raise_for_status()
            return OnDemandAdmissionResponse.model_validate(resp.json()).granted_pod_uids
        except httpx.HTTPStatusError as exc:
            log.warning("%s", kv(
                event="api.admission_selection_failed", status=exc.response.status_code,
            ))
            return None
        except httpx.RequestError as exc:
            log.warning("%s", kv(event="api.admission_selection_failed", err=exc))
            return None
        except (ValidationError, ValueError) as exc:
            log.warning("%s", kv(event="api.admission_selection_parse_failed", err=exc))
            return None

    async def cancel_reservation(self, reservation_id: int, reason: str) -> bool:
        """Cancel *reservation_id* (no-show or controller-revoked); True on success.

        Idempotent: cancelling an already-cancelled id is treated as success
        (the app's own idempotent-cancel semantics; any 2xx is a success, and a
        404 means the reservation is already gone, which is the outcome we
        wanted anyway).
        """
        try:
            resp = await self._client.post(
                f"/api/reservations/{reservation_id}/cancel",
                json={"reason": reason},
                timeout=15.0,
            )
            if resp.status_code == 404:
                log.info("%s", kv(
                    event="api.cancel_already_gone", rid=reservation_id,
                    reason=reason, status=404,
                ))
                return True
            resp.raise_for_status()
            return True
        except httpx.HTTPStatusError as exc:
            log.warning("%s", kv(
                event="api.cancel_failed", rid=reservation_id, reason=reason,
                status=exc.response.status_code,
            ))
            return False
        except httpx.RequestError as exc:
            log.warning("%s", kv(
                event="api.cancel_failed", rid=reservation_id, reason=reason, err=exc,
            ))
            return False

    async def report_overstay(
        self, reservation_id: int, req: OverstayReportRequest
    ) -> bool:
        """Record an ended overstay against *reservation_id*; True on success.

        Analysis-only and strictly best-effort: any failure (endpoint absent on
        an older app, network error, non-2xx, unparseable) is logged and
        swallowed so it never blocks pod teardown or preemption.  Idempotent
        app-side on ``req.pod_uid`` — a repeat for the same terminating pod is a
        harmless no-op.
        """
        try:
            resp = await self._client.post(
                f"/api/reservations/{reservation_id}/overstay",
                json=req.model_dump(mode="json"),
                timeout=15.0,
            )
            resp.raise_for_status()
            return True
        except httpx.HTTPStatusError as exc:
            log.warning("%s", kv(
                event="api.overstay_report_failed", rid=reservation_id,
                poduid=req.pod_uid, status=exc.response.status_code,
            ))
            return False
        except httpx.RequestError as exc:
            log.warning("%s", kv(
                event="api.overstay_report_failed", rid=reservation_id,
                poduid=req.pod_uid, err=exc,
            ))
            return False

