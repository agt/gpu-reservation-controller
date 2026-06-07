"""Async HTTP client for the GPU Reservation management API.

Implements only the endpoints the controller needs:
  - GET /api/reservations  — paginated list of active reservations
  - GET /api/gpu-classes/{id}  — per-class detail including label_value
"""

from __future__ import annotations

import logging
from datetime import date, timedelta
from typing import Optional

import httpx

from .config import Config
from .schemas import GpuClassDetail, ReservationResponse

log = logging.getLogger(__name__)


class ReservationClient:
    def __init__(self, config: Config) -> None:
        self._base_url = config.reservation_api_url
        self._headers = {"X-API-Key": config.reservation_api_key}
        self._lookahead_days = config.reservation_lookahead_days

    # ------------------------------------------------------------------
    # Public interface
    # ------------------------------------------------------------------

    async def fetch_reservations(self) -> list[ReservationResponse]:
        """Return all active reservations from today through lookahead window."""
        today = date.today()
        end = today + timedelta(days=self._lookahead_days)
        results: list[ReservationResponse] = []
        offset = 0
        limit = 200

        async with httpx.AsyncClient(
            base_url=self._base_url, headers=self._headers
        ) as client:
            while True:
                resp = await client.get(
                    "/api/reservations",
                    params={
                        "status": "active",
                        "date_start": today.isoformat(),
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

        log.info(
            "Fetched %d active reservations (today + %d days)",
            len(results),
            self._lookahead_days,
        )
        return results

    async def fetch_gpu_class(self, gpu_class_id: int) -> Optional[GpuClassDetail]:
        """Return detail for a single GPU class, or None on error."""
        async with httpx.AsyncClient(
            base_url=self._base_url, headers=self._headers
        ) as client:
            try:
                resp = await client.get(
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
