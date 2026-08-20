"""Runtime configuration sourced entirely from environment variables."""

from __future__ import annotations

import logging
import os
from dataclasses import dataclass
from typing import Optional

from .log_fields import kv

log = logging.getLogger(__name__)

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


def _env_int(
    name: str,
    default: int,
    *,
    minimum: int = 1,
    maximum: Optional[int] = None,
) -> int:
    """Read an integer environment variable, falling back on junk or out-of-range.

    The numeric settings used to be parsed with a bare ``int()``, which is the
    opposite posture to ``_env_bool`` above: junk raised ``ValueError`` at
    startup, and — worse — a ``0`` or a negative was accepted silently.  A
    ``PREEMPTION_CHECK_INTERVAL`` or ``QUEUE_PROCESSOR_INTERVAL`` of ``0`` is a
    busy loop hammering the Kubernetes API, which is a far worse outcome than
    ignoring the value.

    This mirrors the reservation app's ``config_utils._env_positive_float`` —
    "the ``env_bool`` tolerance posture, applied to numbers" — and keeps the two
    repos' vocabularies in step.  *minimum* is per-setting rather than uniform:
    a zero interval is a busy loop, but a zero grace/lead genuinely means "no
    grace", so only the settings where zero is meaningless floor at 1.

    A rejected value logs at WARNING naming the variable, because an operator
    who set it and saw no effect needs to know it was ignored.
    """
    raw = (os.environ.get(name) or "").strip()
    if not raw:
        return default
    try:
        value = int(raw)
    except ValueError:
        log.warning("%s", kv(
            event="config.invalid", name=name, value=raw,
            reason="not_an_integer", detail=f"using default {default}",
        ))
        return default
    if value < minimum or (maximum is not None and value > maximum):
        log.warning("%s", kv(
            event="config.invalid", name=name, value=value,
            reason="out_of_range", detail=f"using default {default}",
        ))
        return default
    return value


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
    # Fallbacks for the two things a pod must name itself before the controller
    # can mint a JIT lease on its behalf.  Both ship disabled, so an unconfigured
    # deployment keeps the "a pod that doesn't say is left Pending" behaviour.
    default_min_runtime_seconds: int = 0  # stand-in for a missing galends/minimum-runtime-seconds; 0 = disabled
    default_usage_group: Optional[str] = None  # stand-in for a missing group label/annotation; None = disabled
    ondemand_horizon_minutes: int = 30    # JIT trigger: reserved-match horizon before requesting a lease
    ondemand_lease_buffer_minutes: int = 10  # added to a pod's minimum-runtime when sizing a JIT lease
    capacity_check_interval: int = 3600  # seconds between app-side vs physical capacity audits
    headroom_target_percent: int = 0  # % of each class's physical GPUs to hold free; 0 = disabled
    headroom_notice_minutes: int = 15  # notice a headroom victim gets before it becomes killable
    headroom_check_interval: int = 600  # seconds between headroom evaluations (throttles the sweep)
    overstay_report_enabled: bool = False  # report overstay durations to the app for analysis (ships dark)
    singleton_lease_enabled: bool = True  # hold a coordination Lease so a duplicate instance refuses to run
    k8s_tls_strict_verify: bool = True  # OpenSSL strict X.509 checks on the Kubernetes API connection
    pod_name: Optional[str] = None  # this pod's name (downward API); lease holder identity
    pod_namespace: Optional[str] = None  # this pod's namespace (downward API); where the Lease lives
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

        # Floors are per-setting on purpose.  An interval of 0 is a busy loop,
        # so those floor at 1; a lead/grace/horizon of 0 is a meaningful "off",
        # so those floor at 0 and only reject negatives.
        return cls(
            reservation_api_url=url,
            reservation_api_key=key,
            reservation_fetch_interval=_env_int("RESERVATION_FETCH_INTERVAL", 300),
            reservation_lookahead_days=_env_int("RESERVATION_LOOKAHEAD_DAYS", 7),
            kubeconfig_path=os.environ.get("KUBECONFIG") or None,
            http_port=_env_int("HTTP_PORT", 8000, maximum=65535),
            ondemand_lease_enabled=_env_bool("ONDEMAND_LEASE_ENABLED", True),
            noshow_timeout_minutes=_env_int("NOSHOW_TIMEOUT_MINUTES", 15, minimum=0),
            noshow_grace_minutes=_env_int("NOSHOW_GRACE_MINUTES", 30, minimum=0),
            queue_processor_interval=_env_int("QUEUE_PROCESSOR_INTERVAL", 300),
            scheduling_gate_name=os.environ.get("POD_SCHEDULING_GATE_NAME") or None,
            required_group_label=os.environ.get("REQUIRED_GROUP_LABEL") or None,
            # 0 is the meaningful "off" here — get_pod_min_runtime_seconds
            # already rejects a non-positive annotation, so a zero default is
            # indistinguishable from having no default at all.
            default_min_runtime_seconds=_env_int(
                "DEFAULT_MINIMUM_RUNTIME_SECONDS", 0, minimum=0
            ),
            default_usage_group=os.environ.get("DEFAULT_USAGE_GROUP") or None,
            inbound_api_token=os.environ.get("INBOUND_API_TOKEN") or None,
            preemption_lead_minutes=_env_int("PREEMPTION_LEAD_MINUTES", 15, minimum=0),
            preemption_check_interval=_env_int("PREEMPTION_CHECK_INTERVAL", 60),
            pod_adoption_enabled=_env_bool("POD_ADOPTION_ENABLED", True),
            ondemand_merge_enabled=_env_bool("ONDEMAND_MERGE_ENABLED", True),
            termination_warning_enabled=_env_bool("TERMINATION_WARNING_ENABLED", True),
            termination_warning_lead_minutes=_env_int(
                "TERMINATION_WARNING_LEAD_MINUTES", 30, minimum=0
            ),
            preemption_delegate_selection=_env_bool(
                "PREEMPTION_DELEGATE_SELECTION", True
            ),
            ondemand_delegate_admission=_env_bool(
                "ONDEMAND_DELEGATE_ADMISSION", False
            ),
            ondemand_horizon_minutes=_env_int(
                "ONDEMAND_HORIZON_MINUTES", 30, minimum=0
            ),
            ondemand_lease_buffer_minutes=_env_int(
                "ONDEMAND_LEASE_BUFFER_MINUTES", 10, minimum=0
            ),
            capacity_check_interval=_env_int("CAPACITY_CHECK_INTERVAL", 3600),
            # A percentage floors at 0 ("hold nothing", the disabled default) and
            # caps at 100; a notice of 0 means "no notice gate, kill on sight";
            # but an evaluation interval of 0 is a busy loop, so that floors at 1.
            headroom_target_percent=_env_int(
                "HEADROOM_TARGET_PERCENT", 0, minimum=0, maximum=100
            ),
            headroom_notice_minutes=_env_int(
                "HEADROOM_NOTICE_MINUTES", 15, minimum=0
            ),
            headroom_check_interval=_env_int("HEADROOM_CHECK_INTERVAL", 600),
            overstay_report_enabled=_env_bool("OVERSTAY_REPORT_ENABLED", False),
            singleton_lease_enabled=_env_bool("SINGLETON_LEASE_ENABLED", True),
            k8s_tls_strict_verify=_env_bool("K8S_TLS_STRICT_VERIFY", True),
            # POD_NAME comes from the downward API in-cluster; HOSTNAME is the
            # pod name inside a container anyway, so it is a natural fallback.
            pod_name=os.environ.get("POD_NAME") or os.environ.get("HOSTNAME") or None,
            pod_namespace=os.environ.get("POD_NAMESPACE") or None,
            log_level=os.environ.get("LOG_LEVEL", "INFO"),
        )
