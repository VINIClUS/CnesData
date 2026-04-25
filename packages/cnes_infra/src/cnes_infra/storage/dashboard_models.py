"""SQLAlchemy ORM models for dashboard.* schema (mapped from migration 015)."""
from datetime import datetime
from uuid import UUID, uuid4

from sqlalchemy import (
    BigInteger,
    CheckConstraint,
    ForeignKey,
    String,
    Text,
    UniqueConstraint,
    func,
)
from sqlalchemy.dialects import postgresql as pg
from sqlalchemy.dialects.postgresql import JSONB, TIMESTAMP
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, relationship


class DashboardBase(DeclarativeBase):
    pass


class DashboardUser(DashboardBase):
    __tablename__ = "users"
    __table_args__ = (
        UniqueConstraint("oidc_issuer", "oidc_subject"),
        CheckConstraint("role IN ('gestor', 'admin')", name="chk_role"),
        {"schema": "dashboard"},
    )

    id: Mapped[UUID] = mapped_column(
        pg.UUID(as_uuid=True), primary_key=True, default=uuid4,
    )
    oidc_subject: Mapped[str] = mapped_column(Text, nullable=False)
    oidc_issuer: Mapped[str] = mapped_column(Text, nullable=False)
    email: Mapped[str] = mapped_column(Text, nullable=False)
    display_name: Mapped[str | None] = mapped_column(Text)
    role: Mapped[str] = mapped_column(Text, nullable=False, default="gestor")
    created_at: Mapped[datetime] = mapped_column(
        TIMESTAMP(timezone=True), server_default=func.now(),
    )
    last_login_at: Mapped[datetime | None] = mapped_column(TIMESTAMP(timezone=True))
    revoked_at: Mapped[datetime | None] = mapped_column(TIMESTAMP(timezone=True))

    tenants: Mapped[list["DashboardUserTenant"]] = relationship(
        back_populates="user", cascade="all, delete-orphan",
    )


class DashboardUserTenant(DashboardBase):
    __tablename__ = "user_tenants"
    __table_args__ = ({"schema": "dashboard"},)

    user_id: Mapped[UUID] = mapped_column(
        pg.UUID(as_uuid=True),
        ForeignKey("dashboard.users.id", ondelete="CASCADE"),
        primary_key=True,
    )
    tenant_id: Mapped[str] = mapped_column(String(6), primary_key=True)

    user: Mapped[DashboardUser] = relationship(back_populates="tenants")


class DashboardAuditLog(DashboardBase):
    __tablename__ = "audit_log"
    __table_args__ = (
        CheckConstraint(
            "action IN ('login','logout','activate_agent',"
            "'view_status','view_runs','view_tenants')",
            name="chk_action",
        ),
        {"schema": "dashboard"},
    )

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True)
    user_id: Mapped[UUID | None] = mapped_column(
        pg.UUID(as_uuid=True), ForeignKey("dashboard.users.id"),
    )
    tenant_id: Mapped[str | None] = mapped_column(String(6))
    action: Mapped[str] = mapped_column(Text, nullable=False)
    metadata_: Mapped[dict | None] = mapped_column("metadata", JSONB)
    request_id: Mapped[UUID | None] = mapped_column(pg.UUID(as_uuid=True))
    timestamp: Mapped[datetime] = mapped_column(
        TIMESTAMP(timezone=True), server_default=func.now(),
    )
