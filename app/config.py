"""Runtime configuration sourced entirely from environment variables."""

from __future__ import annotations

import os
from dataclasses import dataclass
from typing import Optional


@dataclass(frozen=True)
class Config:
    reservation_api_url: str
    reservation_api_key: str
    reservation_fetch_interval: int   # seconds between refresh cycles
    reservation_lookahead_days: int   # how many calendar days ahead to fetch
    kubeconfig_path: Optional[str]    # None → use in-cluster service account
    health_port: int
    ondemand_placement_enabled: bool  # enable/disable on-demand pod placement
    noshown_timeout_minutes: int   # minutes after slot_start before no-show is declared
    noshown_grace_minutes: int     # grace period when controller starts mid-window

    @classmethod
    def from_env(cls) -> "Config":
        url = os.environ.get("RESERVATION_API_URL", "").rstrip("/")
        key = os.environ.get("RESERVATION_API_KEY", "")
        if not url:
            raise RuntimeError(
                "RESERVATION_API_URL environment variable is required"
            )
        if not key:
            raise RuntimeError(
                "RESERVATION_API_KEY environment variable is required"
            )
        return cls(
            reservation_api_url=url,
            reservation_api_key=key,
            reservation_fetch_interval=int(
                os.environ.get("RESERVATION_FETCH_INTERVAL", "300")
            ),
            reservation_lookahead_days=int(
                os.environ.get("RESERVATION_LOOKAHEAD_DAYS", "7")
            ),
            kubeconfig_path=os.environ.get("KUBECONFIG") or None,
            health_port=int(os.environ.get("HEALTH_PORT", "8000")),
            ondemand_placement_enabled=os.environ.get(
                "ONDEMAND_PLACEMENT_ENABLED", "true"
            ).lower()
            not in ("false", "0", "no"),
            noshown_timeout_minutes=int(
                os.environ.get("NOSHOWN_TIMEOUT_MINUTES", "15")
            ),
            noshown_grace_minutes=int(
                os.environ.get("NOSHOWN_GRACE_MINUTES", "30")
            ),
        )
