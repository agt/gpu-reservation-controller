"""Async HTTP client for the GPU Reservation management API.

Implements only the endpoints the controller needs:
  - GET /api/reservations  — paginated list of all (active + cancelled) reservations
  - GET /api/gpu-classes/{id}  — per-class detail including label_value
"""

from __future__ import annotations

import logging
from datetime import datetime, timedelta, timezone
from typing import Optional

import httpx

from .config import Config
from .schemas import AppSettings, GpuClassDetail, ReservationResponse

log = logging.getLogger(__name__)


class ReservationClient:
    def __init__(self, config: Config) -> None:
        self._lookahead_days = config.reservation_lookahead_days
        self._client = httpx.AsyncClient(
            base_url=config.reservation_api_url,
            headers={"X-API-Key": config.reservation_api_key},
        )

    async def aclose(self) -> None:
        """Close the underlying HTTP connection pool."""
        await self._client.aclose()

    # ------------------------------------------------------------------
    # Public interface
    # ------------------------------------------------------------------

    async def fetch_reservations(self) -> list[ReservationResponse]:
        """Return all reservations (active and cancelled) from today through lookahead window."""
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
            resp = await self._client.get(
                f"/api/gpu-classes/{gpu_class_id}", timeout=10.0
            )
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

    async def fetch_settings(self) -> Optional[AppSettings]:
        """Return the app settings (reclaim window/guard), or None on error."""
        try:
            resp = await self._client.get("/api/settings", timeout=10.0)
            resp.raise_for_status()
            return AppSettings.model_validate(resp.json())
        except httpx.HTTPStatusError as exc:
            log.warning("Could not fetch app settings: HTTP %s", exc.response.status_code)
            return None
        except httpx.RequestError as exc:
            log.warning("Could not fetch app settings: %s", exc)
            return None
