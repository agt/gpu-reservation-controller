import datetime
from sqlalchemy import (
    Column, Integer, Float, String, Boolean, DateTime, Date, Time,
    ForeignKey, Text, JSON, UniqueConstraint, Index, text,
)
from sqlalchemy.orm import relationship
from .database import Base


def _utcnow() -> datetime.datetime:
    """Return the current UTC time as a naive datetime (tz-info stripped).

    Replaces the deprecated ``datetime.datetime.utcnow()`` in SQLAlchemy
    column ``default``/``onupdate`` callables — the return value must be
    naive so it round-trips correctly through SQLite's TEXT datetime storage.
    """
    return datetime.datetime.now(datetime.UTC).replace(tzinfo=None)


class User(Base):
    """ORM model for a user account.

    Supports soft deletion via ``is_active=False`` — deactivated users cannot log
    in, but their FK references (reservations, group memberships) are preserved.
    Hard deletion is avoided to prevent orphaned reservation records.

    Relationships:
        reservations: Reservations booked by this user (FK: ``user_id``).
        cancellations: Reservations this user cancelled (FK: ``cancelled_by_id``).
        group_memberships: ``UsageGroupMember`` rows carrying the user's role per group.
    """
    __tablename__ = "users"

    id = Column(Integer, primary_key=True, index=True)
    username = Column(String(64), unique=True, nullable=False, index=True)
    email = Column(String(255), unique=True, nullable=False)
    hashed_password = Column(String(255), nullable=False)
    auth_provider = Column(String(16), nullable=False, default="local")
    external_id = Column(String(255), nullable=True, unique=True, index=True)
    is_admin = Column(Boolean, default=False, nullable=False)
    is_active = Column(Boolean, default=True, nullable=False)
    created_at = Column(DateTime, default=_utcnow, nullable=False)

    reservations = relationship(
        "Reservation", back_populates="user", foreign_keys="[Reservation.user_id]"
    )
    cancellations = relationship(
        "Reservation", back_populates="cancelled_by", foreign_keys="[Reservation.cancelled_by_id]"
    )
    group_memberships = relationship(
        "UsageGroupMember", back_populates="user", cascade="all, delete-orphan"
    )


class GpuClass(Base):
    """ORM model representing a hardware tier that users can reserve.

    ``is_active=False`` is the soft-delete mechanism — inactive classes are hidden
    from most queries and cannot be targeted by new reservations.

    ``label_value`` is the Kubernetes node label value associated with this
    hardware tier (e.g. ``h100``, ``a100-80gb``); nullable when not applicable.

    ``su_rate_per_hour`` is the base Service Unit cost per GPU per hour; the
    effective rate for any given hour is this value multiplied by the active
    ``SuDiscountSchedule`` multiplier covering that hour (smallest discount wins).

    ``max_gpus_per_reservation`` caps the GPU count of any single booking against
    this class (``None`` = no per-reservation cap).

    ``attach_all_groups`` makes the class bookable by **every** group with no
    explicit ``UsageGroupGpuClass`` row (mirrors the old plan-level flag).

    Relationships:
        day_overrides: Per-date capacity overrides (cascade-deleted with class).
        group_links: UsageGroupGpuClass rows attaching this class to groups.
        reservations: All reservations ever made against this class.
    """
    __tablename__ = "gpu_classes"

    id = Column(Integer, primary_key=True, index=True)
    name = Column(String(64), unique=True, nullable=False)
    description = Column(Text)
    total_gpus = Column(Integer, nullable=False)
    management_buffer = Column(Integer, nullable=False, default=0, server_default="0")
    label_value = Column(String(128), nullable=True)
    # Base Service Units per GPU per hour (before discount-schedule multipliers).
    su_rate_per_hour = Column(Float, nullable=False, default=0, server_default="0")
    # Hard floor on the effective SU rate regardless of discount multipliers. NULL = no floor.
    min_su_per_gpu_hour = Column(Float, nullable=True, default=None)
    max_gpus_per_reservation = Column(Integer, nullable=True)
    attach_all_groups = Column(Boolean, default=False, nullable=False, server_default="0")
    is_active = Column(Boolean, default=True, nullable=False)
    created_at = Column(DateTime, default=_utcnow, nullable=False)

    day_overrides = relationship(
        "GpuClassDayOverride", back_populates="gpu_class", cascade="all, delete-orphan"
    )
    group_links = relationship(
        "UsageGroupGpuClass", back_populates="gpu_class", cascade="all, delete-orphan"
    )
    reservations = relationship("Reservation", back_populates="gpu_class")


class GpuClassDayOverride(Base):
    """Date-span GPU capacity override for a GPU class.

    On dates covered by an override the availability engine uses
    ``available_gpus`` (total hardware available in the period) instead of
    ``GpuClass.total_gpus``, and ``management_buffer`` instead of
    ``GpuClass.management_buffer``.  Non-privileged users see
    ``available_gpus - management_buffer``; admins and group managers see
    the full ``available_gpus``.

    Overlap resolution follows the same rule as ``UsageGroupGpuLimit``: the
    override with the narrowest ``(date_end - date_start)`` span wins, unbounded
    spans (either bound ``None``) are treated as infinite, and among
    equally-specific overrides the highest ``available_gpus`` is applied.  See
    ``_narrowest_limit`` in ``routers/reservations.py``.

    ``date_start`` / ``date_end`` are both inclusive; ``None`` means unbounded.
    Multiple overlapping records are allowed; there is intentionally no unique
    constraint.
    """
    __tablename__ = "gpu_class_day_overrides"

    id = Column(Integer, primary_key=True, index=True)
    gpu_class_id = Column(Integer, ForeignKey("gpu_classes.id"), nullable=False, index=True)
    date_start = Column(Date, nullable=True)   # inclusive; None = no lower bound
    date_end = Column(Date, nullable=True)     # inclusive; None = no upper bound
    available_gpus = Column(Integer, nullable=False)
    management_buffer = Column(Integer, nullable=False, default=0, server_default="0")

    gpu_class = relationship("GpuClass", back_populates="day_overrides")


class SuDiscountSchedule(Base):
    """System-wide Service-Unit discount window.

    A discount lowers the effective per-GPU-hour rate of **every** GPU class during
    a recurring weekly time-of-day window, optionally limited to a calendar date
    range.  ``multiplier`` (0 < m ≤ 1) scales the base rate: the effective rate for
    an hour is ``GpuClass.su_rate_per_hour × multiplier``.

    ``days_of_week`` is a JSON list of ints (0=Mon … 6=Sun).  ``start_time`` /
    ``end_time`` bound the time-of-day window; when ``end_time <= start_time`` the
    window **wraps past midnight** (e.g. 22:00→06:00).  ``date_start`` / ``date_end``
    (inclusive, ``None`` = unbounded) optionally restrict the schedule to a span.

    Overlap resolution mirrors ``GpuClassDayOverride`` in spirit but uses a simpler
    rule the user specified: among all schedules covering a given instant the
    **smallest discount wins** — i.e. the **highest** ``multiplier`` (closest to
    1.0) applies.  See ``effective_multiplier`` in ``app/pricing.py``.

    A schedule only applies to explicitly attached GPU classes (via the
    ``su_discount_schedule_gpu_classes`` join table).  A schedule with no
    attached classes has no effect on any class.
    """
    __tablename__ = "su_discount_schedules"

    id = Column(Integer, primary_key=True, index=True)
    name = Column(String(128), nullable=False)
    days_of_week = Column(JSON, nullable=False)   # list[int] 0=Mon … 6=Sun
    start_time = Column(Time, nullable=False)
    end_time = Column(Time, nullable=False)       # <= start_time ⇒ wraps past midnight
    multiplier = Column(Float, nullable=False)    # 0 < m ≤ 1; effective = base × m
    date_start = Column(Date, nullable=True)      # inclusive; None = no lower bound
    date_end = Column(Date, nullable=True)        # inclusive; None = no upper bound
    is_active = Column(Boolean, default=True, nullable=False)
    created_at = Column(DateTime, default=_utcnow, nullable=False)

    gpu_classes = relationship(
        "GpuClass", secondary="su_discount_schedule_gpu_classes", lazy="selectin"
    )

    @property
    def gpu_class_ids(self) -> list[int]:
        return [gc.id for gc in self.gpu_classes]


class SuDiscountScheduleGpuClass(Base):
    """Join table scoping a discount schedule to specific GPU classes.

    A schedule with no rows in this table has no effect on any class.
    Cascade-deletes when either the schedule or the GPU class is removed.
    """
    __tablename__ = "su_discount_schedule_gpu_classes"

    schedule_id = Column(
        Integer, ForeignKey("su_discount_schedules.id", ondelete="CASCADE"), primary_key=True
    )
    gpu_class_id = Column(
        Integer, ForeignKey("gpu_classes.id", ondelete="CASCADE"), primary_key=True
    )


class UsageGroup(Base):
    """A named group that scopes members' access to a subset of GPU classes.

    Quota and booking-window constraints are enforced at reservation-creation time
    for non-privileged users:
      - ``valid_from`` / ``valid_until``: group usable only within this date range.
      - ``min_days_ahead`` / ``max_days_ahead``: how far ahead bookings may be made.
      - ``su_budget``: total Service Units a member's open reservations may consume
        (sum of stored ``Reservation.su_cost`` over the member's active future
        reservations). ``NULL`` = unlimited.

    Admins and group managers bypass these constraints when creating reservations.

    ``is_active=False`` soft-deletes the group; the DELETE endpoint performs a hard
    delete but is blocked when active reservations reference the group.
    """
    __tablename__ = "usage_groups"

    id = Column(Integer, primary_key=True, index=True)
    name = Column(String(128), unique=True, nullable=False)
    description = Column(Text, nullable=True)
    valid_from = Column(Date, nullable=True)
    valid_until = Column(Date, nullable=True)
    min_days_ahead = Column(Integer, nullable=True)
    max_days_ahead = Column(Integer, nullable=True)
    su_budget = Column(Float, nullable=True)  # per-member Service Unit budget; NULL = unlimited
    su_anchor_mode = Column(String(16), nullable=False, default='open', server_default='open')
    is_active = Column(Boolean, default=True, nullable=False)
    sync_with_sicad = Column(Boolean, default=False, nullable=False, server_default=text("0"))
    created_at = Column(DateTime, default=_utcnow, nullable=False)

    members = relationship("UsageGroupMember", back_populates="group", cascade="all, delete-orphan")
    gpu_class_links = relationship("UsageGroupGpuClass", back_populates="group", cascade="all, delete-orphan")
    gpu_limits = relationship("UsageGroupGpuLimit", back_populates="group", cascade="all, delete-orphan")
    reservations = relationship("Reservation", back_populates="group")


class UsageGroupMember(Base):
    """Join table between UsageGroup and User, with an explicit role.

    ``role`` is either ``'member'`` (can make reservations under the group's quota
    rules) or ``'manager'`` (can also book for other members and cancel any
    reservation within the group).  Validated at the schema layer.

    The unique constraint on ``(group_id, user_id)`` is enforced in the DB; the
    POST endpoint uses an upsert pattern (update role if user is already a member).
    """
    __tablename__ = "usage_group_members"

    id = Column(Integer, primary_key=True, index=True)
    group_id = Column(Integer, ForeignKey("usage_groups.id"), nullable=False, index=True)
    user_id = Column(Integer, ForeignKey("users.id"), nullable=False, index=True)
    role = Column(String(32), nullable=False, server_default="member")  # "member" | "manager"

    group = relationship("UsageGroup", back_populates="members")
    user = relationship("User", back_populates="group_memberships")

    __table_args__ = (UniqueConstraint("group_id", "user_id", name="uq_group_user"),)


class UsageGroupGpuClass(Base):
    """Join table granting a UsageGroup access to a GpuClass.

    The availability endpoint and reservation creation both require that the
    requested class is attached to the group being used (or that the class has
    ``attach_all_groups=True``).  The unique constraint on ``(group_id,
    gpu_class_id)`` is enforced in the DB; the POST endpoint is idempotent
    (silently ignores duplicate attach requests).
    """
    __tablename__ = "usage_group_gpu_classes"

    id = Column(Integer, primary_key=True, index=True)
    group_id = Column(Integer, ForeignKey("usage_groups.id"), nullable=False, index=True)
    gpu_class_id = Column(Integer, ForeignKey("gpu_classes.id"), nullable=False, index=True)

    group = relationship("UsageGroup", back_populates="gpu_class_links")
    gpu_class = relationship("GpuClass", back_populates="group_links")

    __table_args__ = (UniqueConstraint("group_id", "gpu_class_id", name="uq_group_gpu_class"),)


class UsageGroupGpuLimit(Base):
    """Per-group GPU ceiling for a GPU class within an optional date range.

    When a reservation is created under ``group_id`` for ``gpu_class_id``, the
    engine looks up active limits for ``(group_id, gpu_class_id, date)`` and
    resolves overlaps using a narrowest-date-span-wins rule: the limit with the
    smallest ``(date_end - date_start)`` span takes precedence, allowing a short
    deadline-week boost to override a semester-long cap.  Unbounded limits
    (either bound ``None``) are treated as infinite span.  Among equally-specific
    limits, the highest ``max_gpus`` (most permissive) is applied.  The effective
    available capacity for the group is
    ``min(cluster_capacity, group_limit) - group_used``.

    ``date_start`` / ``date_end`` are both inclusive; ``None`` means unbounded.
    Multiple overlapping records are allowed; the engine applies narrowest-wins.
    """
    __tablename__ = "usage_group_gpu_limits"

    id = Column(Integer, primary_key=True, index=True)
    group_id = Column(Integer, ForeignKey("usage_groups.id"), nullable=False, index=True)
    gpu_class_id = Column(Integer, ForeignKey("gpu_classes.id"), nullable=False, index=True)
    max_gpus = Column(Integer, nullable=False)
    date_start = Column(Date, nullable=True)   # inclusive; None = no lower bound
    date_end = Column(Date, nullable=True)     # inclusive; None = no upper bound
    created_at = Column(DateTime, default=_utcnow, nullable=False)

    group = relationship("UsageGroup", back_populates="gpu_limits")
    gpu_class = relationship("GpuClass")


class EmailSettings(Base):
    """Singleton SMTP configuration and email template store — always ``id=1``.

    ``smtp_password`` is stored in plain text because this is an internal lab tool
    with no multi-tenant exposure.  It is **never returned via the API**; the
    response schema substitutes a ``smtp_password_set`` boolean instead.

    ``reminder_offsets`` is a JSON list of integers (minutes before slot start) at
    which the reminder loop fires.  An empty list or None disables all reminders.

    Template columns (``confirm_body``, ``reminder_body``, etc.) use Jinja2 syntax.
    A null value means "use the built-in default" — see ``email_service.DEFAULT_*``
    and ``TEMPLATE_VAR_HELP`` for the defaults and available variables.
    """
    __tablename__ = "email_settings"

    id = Column(Integer, primary_key=True, default=1)

    # SMTP
    smtp_host     = Column(String(255), nullable=True)
    smtp_port     = Column(Integer,     nullable=False, default=587)
    smtp_username = Column(String(255), nullable=True)
    smtp_password = Column(String(255), nullable=True)   # stored plain-text (internal tool)
    starttls      = Column(Boolean,     nullable=False, default=True)
    from_address  = Column(String(255), nullable=True)

    # Feature flags
    confirm_enabled  = Column(Boolean, nullable=False, default=False)
    reminder_enabled = Column(Boolean, nullable=False, default=False)

    # Confirmation template
    confirm_subject = Column(String(255), nullable=False,
                             default="Reservation confirmed – {{ site_title }}")
    confirm_body    = Column(Text, nullable=True)

    # Reminder template
    reminder_subject = Column(String(255), nullable=False,
                              default="Reminder: GPU reservation in {{ hours_until }} – {{ site_title }}")
    reminder_body    = Column(Text, nullable=True)

    # Ordered list of minutes-before-start at which to send reminders (JSON list[int])
    reminder_offsets = Column(JSON, nullable=True)

    updated_at = Column(
        DateTime,
        default=_utcnow,
        onupdate=_utcnow,
        nullable=True,
    )


class SentEmail(Base):
    """Audit log and idempotency guard for outbound emails.

    Before sending a reminder, ``_check_reminders_sync`` queries this table to
    confirm the ``(reservation_id, email_type, offset_minutes)`` combination has
    not already been sent.  This prevents duplicate sends across process restarts
    or overlapping check cycles.

    ``success=False`` rows (with ``error_message``) are written even for failed
    attempts so the reminder loop does not endlessly retry a broken delivery.
    """
    __tablename__ = "sent_emails"

    id             = Column(Integer, primary_key=True, index=True)
    reservation_id = Column(Integer, ForeignKey("reservations.id"), nullable=False, index=True)
    email_type     = Column(String(32),  nullable=False)   # "confirmation" | "reminder"
    offset_minutes = Column(Integer,     nullable=True)    # only for reminders
    recipient      = Column(String(255), nullable=False)
    success        = Column(Boolean,     nullable=False, default=True)
    error_message  = Column(Text,        nullable=True)
    sent_at        = Column(DateTime, default=_utcnow, nullable=False)


class SiteSettings(Base):
    """Singleton site-wide configuration — always ``id=1``.

    ``site_title`` propagates to the browser title bar and sidebar branding via
    ``loadSettings()`` in ``api.js``.  ``announcement_html`` is rendered as-is in
    the dashboard banner; ``login_content_html`` is rendered as-is in the login
    page's content pane (blank hides the pane and the login page falls back to
    a single centred card).  Both are produced by the admin Quill editors on
    the Edit Content page.
    """
    __tablename__ = "site_settings"

    id = Column(Integer, primary_key=True, default=1)
    site_title = Column(String(255), nullable=False, default="GPU Reservations")
    announcement_html = Column(Text, nullable=True)
    login_content_html = Column(Text, nullable=True)
    gpu_recovery_window_hours = Column(Integer, nullable=True)
    logo_data = Column(Text, nullable=True)
    updated_at = Column(
        DateTime,
        default=_utcnow,
        onupdate=_utcnow,
        nullable=True,
    )


class Reservation(Base):
    """Core booking record tying a user to a GPU class over a time range.

    A reservation is an arbitrary, user-chosen wall-clock interval stored as the
    naive-local ``start_dt`` / ``end_dt`` pair.  The interval may **cross midnight**
    (``end_dt`` on the following calendar day) and is capped at 24 hours by the
    request schema.  ``date`` mirrors ``start_dt.date()`` and is kept only as an
    indexed convenience for start-date range filters.

    ``su_cost`` is the **total** Service Units the booking consumes, computed at
    creation time by ``app/pricing.compute_su_cost`` from the class base rate and
    the active discount schedules, and stored so reads and budget checks never
    recompute it.

    ``status`` is either ``'active'`` or ``'cancelled'``; rows are never deleted.

    ``cancelled_by_id`` records who performed the cancellation — may differ from
    ``user_id`` when an admin or group manager cancels on behalf of a user.
    ``submitted_by_id`` records the authenticated caller (differs from ``user_id``
    when a manager books on behalf of a member).
    """
    __tablename__ = "reservations"

    id = Column(Integer, primary_key=True, index=True)
    user_id = Column(Integer, ForeignKey("users.id"), nullable=True, index=True)
    group_id = Column(Integer, ForeignKey("usage_groups.id"), nullable=True, index=True)
    gpu_class_id = Column(Integer, ForeignKey("gpu_classes.id"), nullable=False, index=True)
    # Wall-clock interval in site-local time (naive). May cross midnight.
    start_dt = Column(DateTime, nullable=False, index=True)
    end_dt = Column(DateTime, nullable=False)
    # Convenience copy of start_dt.date() for start-date range filtering.
    date = Column(Date, nullable=False, index=True)
    gpu_count = Column(Integer, nullable=False)
    # Total Service Units consumed, computed and stored at creation.
    su_cost = Column(Float, nullable=False, default=0, server_default="0")
    status = Column(String(16), default="active", nullable=False, index=True)
    notes = Column(Text)
    created_at = Column(DateTime, default=_utcnow, nullable=False, index=True)
    updated_at = Column(
        DateTime,
        default=_utcnow,
        onupdate=_utcnow,
        nullable=False,
    )
    cancelled_at = Column(DateTime, nullable=True)
    cancelled_by_id = Column(Integer, ForeignKey("users.id"), nullable=True)
    # Who submitted the API request — always the authenticated caller. Differs
    # from user_id when a manager books on behalf of a member.
    submitted_by_id = Column(Integer, ForeignKey("users.id"), nullable=True)
    # 'booking' = normal user reservation; 'reclaim' = admin-only capacity hold (no user/group).
    kind = Column(String(16), nullable=False, default="booking", server_default="booking")

    user = relationship("User", back_populates="reservations", foreign_keys=[user_id])
    group = relationship("UsageGroup", back_populates="reservations")
    gpu_class = relationship("GpuClass", back_populates="reservations")
    cancelled_by = relationship(
        "User", back_populates="cancellations", foreign_keys=[cancelled_by_id]
    )
    submitted_by = relationship("User", foreign_keys=[submitted_by_id])

    __table_args__ = (
        # Hot path: capacity/availability queries filter on class+status and scan
        # by start_dt; overlap math reads start_dt/end_dt.
        Index("ix_reservations_class_status_start", "gpu_class_id", "status", "start_dt"),
        # Per-user / per-group quota (SU budget) checks at reservation creation.
        Index("ix_reservations_group_user_status_date", "group_id", "user_id", "status", "date"),
    )


class ServiceKey(Base):
    """Long-lived API key for machine-to-machine access (e.g. the Kubernetes controller).

    The raw key is shown exactly once on creation and never stored — only the
    SHA-256 hash is persisted.  ``key_prefix`` stores the first 15 characters
    (``gpures_`` + 8 hex chars) so operators can identify a key in logs without
    reconstructing the secret.
    """
    __tablename__ = "service_keys"

    id           = Column(Integer, primary_key=True, index=True)
    name         = Column(String(128), unique=True, nullable=False, index=True)
    key_prefix   = Column(String(16), nullable=False)
    key_hash     = Column(String(64), unique=True, nullable=False, index=True)
    # "read_only" → GET-only access; "read_write" → may also create users and
    # manage group membership. Never grants admin (creating admins / keys).
    scope        = Column(String(16), nullable=False, server_default="read_only")
    is_active    = Column(Boolean, default=True, nullable=False)
    created_at   = Column(DateTime, default=_utcnow, nullable=False)
    last_used_at = Column(DateTime, nullable=True)
