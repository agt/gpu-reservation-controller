"""Runtime configuration sourced entirely from environment variables."""

from __future__ import annotations

import os
from dataclasses import dataclass
from typing import Optional

# Boolean env-var vocabulary, shared with the reservation app's
# ``config_utils`` (keep the two in step): a recognised truthy/falsy word wins,
# anything else — including junk — falls back to the flag's default.
_TRUTHY = frozenset({"1", "true", "yes", "on"})
_FALSY = frozenset({"0", "false", "no", "off"})


def _env_bool(name: str, default: bool) -> bool:
    """Read a boolean-ish environment variable with an explicit default.

    A recognised truthy/falsy word (see ``_TRUTHY`` / ``_FALSY``) wins;
    anything else — including junk — falls back to ``default``.  This keeps the
    controller's boolean vocabulary in step with the reservation app's
    ``config_utils``.
    """
    raw = os.environ.get(name)
    if raw is None:
        return default
    value = raw.strip().lower()
    if value in _TRUTHY:
        return True
    if value in _FALSY:
        return False
    return default


@dataclass(frozen=True)
class Config:
    reservation_api_url: str
    reservation_api_key: str
    reservation_fetch_interval: int   # seconds between refresh cycles
    reservation_lookahead_days: int   # how many calendar days ahead to fetch
    kubeconfig_path: Optional[str]    # None → use in-cluster service account
    http_port: int                    # bind port for the whole FastAPI listener
    ondemand_lease_enabled: bool      # enable/disable the JIT on-demand lease path
    noshow_timeout_minutes: int    # minutes after slot_start before no-show is declared
    noshow_grace_minutes: int      # grace period when controller starts mid-window
    queue_processor_interval: int  # seconds between queue-processor ticks
    scheduling_gate_name: Optional[str]  # SchedulingGate to remove on admission; None = disabled
    inbound_api_token: Optional[str]  # bearer token for the inbound push API; None = endpoint disabled
    preemption_lead_minutes: int   # lead time before a slot boundary for phase-A preemption
    preemption_check_interval: int  # seconds between preemption sweeps
    pod_adoption_enabled: bool = True  # re-link overstay pods to a user's new booking
    ondemand_merge_enabled: bool = True  # merge a JIT lease's pod into a now-open matching booking
    termination_warning_enabled: bool = True  # annotate pods at risk of demand-driven preemption
    termination_warning_lead_minutes: int = 30  # warning look-ahead, decoupled from PREEMPTION_LEAD_MINUTES
    preemption_delegate_selection: bool = True  # ask the app to choose victims; local random fallback
    ondemand_delegate_admission: bool = False  # ask the app which pending pods to admit; grant-all fallback
    required_group_label: Optional[str] = None  # pod label naming the usage group to match; None = disabled
    ondemand_horizon_minutes: int = 30    # JIT trigger: reserved-match horizon before requesting a lease
    ondemand_lease_buffer_minutes: int = 10  # added to a pod's minimum-runtime when sizing a JIT lease
    capacity_check_interval: int = 3600  # seconds between app-side vs physical capacity audits
    overstay_report_enabled: bool = False  # report overstay durations to the app for analysis (ships dark)
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
            http_port=int(os.environ.get("HTTP_PORT", "8000")),
            ondemand_lease_enabled=_env_bool("ONDEMAND_LEASE_ENABLED", True),
            noshow_timeout_minutes=int(
                os.environ.get("NOSHOW_TIMEOUT_MINUTES", "15")
            ),
            noshow_grace_minutes=int(
                os.environ.get("NOSHOW_GRACE_MINUTES", "30")
            ),
            queue_processor_interval=int(
                os.environ.get("QUEUE_PROCESSOR_INTERVAL", "300")
            ),
            scheduling_gate_name=os.environ.get("POD_SCHEDULING_GATE_NAME") or None,
            required_group_label=os.environ.get("REQUIRED_GROUP_LABEL") or None,
            inbound_api_token=os.environ.get("INBOUND_API_TOKEN") or None,
            preemption_lead_minutes=int(
                os.environ.get("PREEMPTION_LEAD_MINUTES", "15")
            ),
            preemption_check_interval=int(
                os.environ.get("PREEMPTION_CHECK_INTERVAL", "60")
            ),
            pod_adoption_enabled=_env_bool("POD_ADOPTION_ENABLED", True),
            ondemand_merge_enabled=_env_bool("ONDEMAND_MERGE_ENABLED", True),
            termination_warning_enabled=_env_bool("TERMINATION_WARNING_ENABLED", True),
            termination_warning_lead_minutes=int(
                os.environ.get("TERMINATION_WARNING_LEAD_MINUTES", "30")
            ),
            preemption_delegate_selection=_env_bool(
                "PREEMPTION_DELEGATE_SELECTION", True
            ),
            ondemand_delegate_admission=_env_bool(
                "ONDEMAND_DELEGATE_ADMISSION", False
            ),
            ondemand_horizon_minutes=int(
                os.environ.get("ONDEMAND_HORIZON_MINUTES", "30")
            ),
            ondemand_lease_buffer_minutes=int(
                os.environ.get("ONDEMAND_LEASE_BUFFER_MINUTES", "10")
            ),
            capacity_check_interval=int(
                os.environ.get("CAPACITY_CHECK_INTERVAL", "3600")
            ),
            overstay_report_enabled=_env_bool("OVERSTAY_REPORT_ENABLED", False),
            log_level=os.environ.get("LOG_LEVEL", "INFO"),
        )
