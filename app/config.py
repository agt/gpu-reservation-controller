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
    pod_list_tick_interval: int    # seconds between queue-processor ticks
    scheduling_gate_name: Optional[str]  # SchedulingGate to remove on admission; None = disabled
    inbound_api_token: Optional[str]  # bearer token for the inbound push API; None = endpoint disabled
    preemption_lead_minutes: int   # lead time before a slot boundary for phase-A preemption
    preemption_check_interval: int  # seconds between preemption sweeps
    pod_adoption_enabled: bool = True  # re-link overstay/on-demand pods to a user's new booking
    log_level: str = "INFO"        # root log level (LOG_LEVEL)

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

        def _noshow(canonical: str, legacy: str, default: str) -> str:
            # Prefer the NOSHOW_* spelling; fall back to the legacy grep-hostile
            # NOSHOWN_* name so existing deployments keep working (CODE-REVIEW H3).
            return os.environ.get(canonical) or os.environ.get(legacy, default)

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
                _noshow("NOSHOW_TIMEOUT_MINUTES", "NOSHOWN_TIMEOUT_MINUTES", "15")
            ),
            noshown_grace_minutes=int(
                _noshow("NOSHOW_GRACE_MINUTES", "NOSHOWN_GRACE_MINUTES", "30")
            ),
            pod_list_tick_interval=int(
                os.environ.get("POD_LIST_TICK_INTERVAL", "300")
            ),
            scheduling_gate_name=os.environ.get("POD_SCHEDULING_GATE_NAME") or None,
            inbound_api_token=os.environ.get("INBOUND_API_TOKEN") or None,
            preemption_lead_minutes=int(
                os.environ.get("PREEMPTION_LEAD_MINUTES", "15")
            ),
            preemption_check_interval=int(
                os.environ.get("PREEMPTION_CHECK_INTERVAL", "60")
            ),
            pod_adoption_enabled=os.environ.get(
                "POD_ADOPTION_ENABLED", "true"
            ).lower()
            not in ("false", "0", "no"),
            log_level=os.environ.get("LOG_LEVEL", "INFO"),
        )
