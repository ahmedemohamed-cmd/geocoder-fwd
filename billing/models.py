"""Pydantic request/response schemas for the control plane."""

from datetime import datetime
from typing import Any

from pydantic import BaseModel, Field


# ── auth ─────────────────────────────────────────────────────────────────────
class LoginRequest(BaseModel):
    email: str
    password: str


class TokenResponse(BaseModel):
    access_token: str
    token_type: str = "bearer"
    role: str
    tenant_id: str | None = None


class Identity(BaseModel):
    sub: str
    role: str
    tenant_id: str | None = None


# ── tenants (admin) ──────────────────────────────────────────────────────────
class TenantCreate(BaseModel):
    name: str = Field(min_length=1)
    contact_email: str | None = None
    plan_id: str = "free"
    # initial tenant user provisioned alongside the tenant
    admin_email: str
    admin_password: str = Field(min_length=8)


class TenantUpdate(BaseModel):
    name: str | None = None
    contact_email: str | None = None
    plan_id: str | None = None
    status: str | None = Field(default=None, pattern="^(active|suspended)$")


class TenantUserOut(BaseModel):
    email: str
    role: str
    status: str = "active"


class TenantUserCreate(BaseModel):
    email: str
    password: str = Field(min_length=8)


class TenantUserUpdate(BaseModel):
    status: str = Field(pattern="^(active|disabled)$")


class ResetPasswordRequest(BaseModel):
    email: str
    new_password: str = Field(min_length=8)


class ResetMfaRequest(BaseModel):
    email: str


class AdminOut(BaseModel):
    email: str
    status: str = "active"


class AdminCreate(BaseModel):
    email: str
    password: str = Field(min_length=8)


class AdminUpdate(BaseModel):
    status: str = Field(pattern="^(active|disabled)$")


class TenantOut(BaseModel):
    id: str
    name: str
    contact_email: str | None
    plan_id: str | None
    status: str
    created_at: datetime


# ── plans (admin) — quota in credits, overage in cents per credit ────────────
class PlanCreate(BaseModel):
    id: str = Field(min_length=1, max_length=40, pattern=r"^[a-z0-9_-]+$")
    name: str = Field(min_length=1)
    monthly_quota: int = Field(ge=0)
    base_price_cents: int = Field(ge=0)
    overage_cents_per_unit: float = Field(ge=0)
    hard_cap: bool = False
    rps: int = Field(default=0, ge=0)  # requests/second cap (0 = uncapped)


class PlanUpdate(BaseModel):
    name: str | None = None
    monthly_quota: int | None = Field(default=None, ge=0)
    base_price_cents: int | None = Field(default=None, ge=0)
    overage_cents_per_unit: float | None = Field(default=None, ge=0)
    hard_cap: bool | None = None
    rps: int | None = Field(default=None, ge=0)


class PlanOut(BaseModel):
    id: str
    name: str
    monthly_quota: int
    base_price_cents: int
    overage_cents_per_unit: float
    hard_cap: bool
    rps: int = 0


# ── api keys (tenant) ────────────────────────────────────────────────────────
class KeyCreate(BaseModel):
    name: str = Field(min_length=1)
    scopes: list[str] = Field(default_factory=list)


class KeyUpdate(BaseModel):
    name: str | None = None
    status: str | None = Field(default=None, pattern="^(active|disabled)$")


class KeyOut(BaseModel):
    id: str
    tenant_id: str
    name: str
    key_prefix: str
    scopes: list[str]
    status: str
    created_at: datetime
    # full key, decrypted from key_enc so the owner can view/copy it any time
    # (null only for legacy keys created before encrypted storage)
    api_key: str | None = None


class KeyCreated(KeyOut):
    # plaintext key, also returned (prominently) at creation
    api_key: str


# ── endpoint credit weights (admin) ──────────────────────────────────────────
class WeightUpsert(BaseModel):
    milli_credits: int = Field(ge=0)


class WeightOut(BaseModel):
    endpoint: str
    milli_credits: int
    updated_at: datetime


# ── usage / reports (credits = milli / 1000) ─────────────────────────────────
class KeyUsage(BaseModel):
    key_id: str
    key_name: str
    requests: int
    credits: float = 0


class CurrentUsage(BaseModel):
    tenant_id: str
    period: str
    requests: int
    credits_used: float
    quota: int  # credits
    remaining: float  # credits
    over_quota: bool
    plan_id: str | None
    per_key: list[KeyUsage]


class UsageHistoryRow(BaseModel):
    period: str
    day: str
    endpoint: str
    requests: int
    credits: float = 0


# ── billing ──────────────────────────────────────────────────────────────────
class InvoiceOut(BaseModel):
    id: str
    tenant_id: str
    period: str
    total_requests: int
    total_credits: float = 0
    amount_cents: int
    line_items: list[dict[str, Any]]
    status: str
    generated_at: datetime
    paid_at: datetime | None
