"""Async HTTP client for the GPU Reservation management API.

Implements only the endpoints the controller needs:
  - GET /api/reservations  — paginated list of all (active + cancelled) reservations
  - GET /api/gpu-classes/{id}  — per-class detail including label_value
  - GET /api/gpu-classes  — full class list (JIT label → id resolution)
  - POST /api/reservations  — create a JIT on-demand booking
  - POST /api/reservations/{id}/cancel  — cancel a reservation (no-show / revoke)
"""

from __future__ import annotations

import logging
from datetime import datetime, timedelta, timezone
from typing import Optional

import httpx
from pydantic import ValidationError

from .config import Config
from .schemas import (
    GpuClassDetail,
    OnDemandReservationRequest,
    ReservationResponse,
)

log = logging.getLogger(__name__)


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
        )

    async def aclose(self) -> None:
        """Close the underlying HTTP connection pool."""
        await self._client.aclose()

    # ------------------------------------------------------------------
    # Public interface
    # ------------------------------------------------------------------

    async def fetch_reservations(self) -> list[ReservationResponse]:
        """Return all reservations (active and cancelled) from today through lookahead window.

        Raises:
            httpx.HTTPStatusError / httpx.RequestError: on API or network failure.
                Unlike ``fetch_gpu_class`` (which degrades to ``None``), a failed
                reservation fetch propagates so the refresh cycle aborts rather
                than acting on an empty reservation list.
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
            resp = await self._client.get(
                "/api/reservations",
                params={
                    "status": "all",
                    "date_start": start.isoformat(),
                    "date_end": end.isoformat(),
                    "limit": limit,
                    "offset": offset,
                },
                timeout=15.0,
            )
            resp.raise_for_status()
            page = [ReservationResponse.model_validate(r) for r in resp.json()]
            results.extend(page)
            if len(page) < limit:
                break
            offset += limit

        active = sum(1 for r in results if r.status == "active")
        log.info(
            "Fetched %d reservations (%d active, %d cancelled) (today + %d days)",
            len(results),
            active,
            len(results) - active,
            self._lookahead_days,
        )
        return results

    async def fetch_gpu_class(self, gpu_class_id: int) -> Optional[GpuClassDetail]:
        """Return detail for a single GPU class, or None on error."""
        try:
            resp = await self._client.get(f"/api/gpu-classes/{gpu_class_id}")
            resp.raise_for_status()
            return GpuClassDetail.model_validate(resp.json())
        except httpx.HTTPStatusError as exc:
            log.warning(
                "Could not fetch GPU class %d: HTTP %s",
                gpu_class_id,
                exc.response.status_code,
            )
            return None
        except httpx.RequestError as exc:
            log.warning("Could not fetch GPU class %d: %s", gpu_class_id, exc)
            return None
        except (ValidationError, ValueError) as exc:
            # Malformed / unparseable payload (ValueError covers JSONDecodeError):
            # honor the documented "None on error" contract instead of letting it
            # abort the whole refresh cycle (B9).
            log.warning("Could not parse GPU class %d response: %s", gpu_class_id, exc)
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
            log.warning("Could not fetch GPU classes: HTTP %s", exc.response.status_code)
            return None
        except httpx.RequestError as exc:
            log.warning("Could not fetch GPU classes: %s", exc)
            return None
        except (ValidationError, ValueError) as exc:
            log.warning("Could not parse GPU classes response: %s", exc)
            return None

    async def create_ondemand_reservation(
        self, req: OnDemandReservationRequest
    ) -> Optional[ReservationResponse]:
        """Request a JIT on-demand booking; None on denial (409) or error.

        Idempotent by ``req.idempotency_key`` (the admitting pod's UID): the
        app returns the original reservation for a repeated key rather than a
        duplicate, so the caller can safely retry with the same request.
        """
        try:
            resp = await self._client.post(
                "/api/reservations",
                json=req.model_dump(mode="json"),
                timeout=15.0,
            )
            resp.raise_for_status()
            return ReservationResponse.model_validate(resp.json())
        except httpx.HTTPStatusError as exc:
            log.info(
                "On-demand reservation request denied for pod uid=%s: HTTP %s",
                req.idempotency_key,
                exc.response.status_code,
            )
            return None
        except httpx.RequestError as exc:
            log.warning(
                "On-demand reservation request failed for pod uid=%s: %s",
                req.idempotency_key,
                exc,
            )
            return None
        except (ValidationError, ValueError) as exc:
            log.warning(
                "Could not parse on-demand reservation response for pod uid=%s: %s",
                req.idempotency_key,
                exc,
            )
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
                log.info(
                    "Cancel request for reservation #%d (%s): already gone",
                    reservation_id,
                    reason,
                )
                return True
            resp.raise_for_status()
            return True
        except httpx.HTTPStatusError as exc:
            log.warning(
                "Could not cancel reservation #%d (%s): HTTP %s",
                reservation_id,
                reason,
                exc.response.status_code,
            )
            return False
        except httpx.RequestError as exc:
            log.warning(
                "Could not cancel reservation #%d (%s): %s", reservation_id, reason, exc
            )
            return False

