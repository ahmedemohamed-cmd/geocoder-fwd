"""Control-plane (management) API.

Admin: CRUD tenants, view all bills, mark bills paid, run billing.
Tenant user: CRUD their API keys (soft delete), enable/disable, see real-time
usage, history and their own invoices.
"""
from __future__ import annotations

import logging

from fastapi import Depends, FastAPI, HTTPException, Query, Request, Response, status
from fastapi.middleware.cors import CORSMiddleware

from . import (apisix_admin, billing_engine, config, repo, security, usage,
               zitadel_admin)

_log = logging.getLogger("billing.control_plane")


async def _sync_tenant(pool, tenant_id: str) -> None:
    """Reconcile a tenant's APISIX consumer-group + per-key consumers with the DB.
    Best-effort: a gateway hiccup must not fail control-plane CRUD."""
    if not apisix_admin.enabled():
        return
    try:
        tenant = await repo.get_tenant(pool, tenant_id, include_deleted=True)
        keys = await repo.list_keys(pool, tenant_id)  # active + disabled (not deleted)
        if tenant["status"] != "active":
            for k in keys:
                await apisix_admin.delete_consumer(k["id"])
            await apisix_admin.delete_consumer_group(tenant_id)
            return
        plan = await repo.tenant_with_plan(pool, tenant_id)
        in_group = await apisix_admin.ensure_consumer_group(
            tenant_id, quota=int(plan.get("monthly_quota") or 0),
            hard_cap=bool(plan.get("hard_cap")))
        for k in keys:
            if k["status"] == "active" and k.get("key_enc"):
                await apisix_admin.upsert_consumer(
                    k["id"], api_key=security.decrypt_key(k["key_enc"]),
                    in_group=in_group, tenant_id=tenant_id)
            else:
                await apisix_admin.delete_consumer(k["id"])
    except Exception as e:  # noqa: BLE001 - reconciler heals drift later
        _log.warning("APISIX sync for tenant %s failed: %s", tenant_id, e)


async def _apisix_safe(coro) -> None:
    try:
        await coro
    except Exception as e:  # noqa: BLE001
        _log.warning("APISIX op failed: %s", e)
from .auth import (current_identity, get_pool, get_redis, require_admin,
                   require_tenant)
from .models import (AdminCreate, AdminOut, AdminUpdate, CurrentUsage, Identity,
                     InvoiceOut, KeyCreate, KeyCreated,
                     KeyOut, KeyUpdate, KeyUsage, LoginRequest, PlanCreate,
                     PlanOut, PlanUpdate, ResetPasswordRequest, TenantCreate,
                     TenantOut, TenantUpdate, TenantUserCreate, TenantUserOut,
                     TenantUserUpdate, TokenResponse, UsageHistoryRow)

router_tag_admin = "admin"
router_tag_tenant = "tenant"


def _not_found(exc: repo.NotFound) -> HTTPException:
    return HTTPException(status.HTTP_404_NOT_FOUND, str(exc))


async def _invalidate_tenant_cache(pool, redis, tenant_id: str) -> None:
    for h in await repo.list_key_hashes(pool, tenant_id):
        await usage.invalidate_key(redis, h)


def build_app(pool=None, redis=None) -> FastAPI:
    app = FastAPI(title="API Key Management — Control Plane")
    app.state.pool = pool
    app.state.redis = redis
    # Credentialed CORS is incompatible with a wildcard origin (the browser
    # rejects "*" + credentials), so only allow credentials for explicit origins.
    allow_credentials = "*" not in config.CORS_ORIGINS
    app.add_middleware(
        CORSMiddleware, allow_origins=config.CORS_ORIGINS,
        allow_methods=["*"], allow_headers=["*"], allow_credentials=allow_credentials)

    # ── auth ─────────────────────────────────────────────────────────────────
    @app.post("/auth/login", response_model=TokenResponse, tags=["auth"])
    async def login(body: LoginRequest, pool=Depends(get_pool)):
        user = await repo.get_user_by_email(pool, body.email)
        if not user or not security.verify_password(body.password, user["password_hash"]):
            raise HTTPException(status.HTTP_401_UNAUTHORIZED, "invalid credentials")
        if user.get("status") == "disabled":
            raise HTTPException(status.HTTP_403_FORBIDDEN, "user is disabled")
        token = security.issue_token(
            subject=user["email"], role=user["role"], tenant_id=user.get("tenant_id"))
        return TokenResponse(access_token=token, role=user["role"],
                             tenant_id=user.get("tenant_id"))

    @app.get("/auth/me", response_model=Identity, tags=["auth"])
    async def me(ident: Identity = Depends(current_identity)):
        return ident

    # ── admin: platform admins CRUD ──────────────────────────────────────────
    @app.get("/admin/admins", response_model=list[AdminOut], tags=[router_tag_admin])
    async def list_admins(_: Identity = Depends(require_admin), pool=Depends(get_pool)):
        return await repo.list_admins(pool)

    @app.post("/admin/admins", response_model=AdminOut, status_code=201,
              tags=[router_tag_admin])
    async def add_admin(body: AdminCreate, _: Identity = Depends(require_admin),
                        pool=Depends(get_pool)):
        try:
            admin = await repo.create_admin(
                pool, email=body.email, password_hash=security.hash_password(body.password))
        except repo.Conflict as e:
            raise HTTPException(status.HTTP_409_CONFLICT, str(e))
        try:
            await zitadel_admin.provision_admin_user(
                email=body.email, password=body.password, display_name=body.email)
        except zitadel_admin.ZitadelError as e:
            raise HTTPException(status.HTTP_502_BAD_GATEWAY,
                                f"admin created but IdP provisioning failed: {e}")
        return admin

    @app.patch("/admin/admins/{email}", response_model=AdminOut, tags=[router_tag_admin])
    async def modify_admin(email: str, body: AdminUpdate,
                           _: Identity = Depends(require_admin), pool=Depends(get_pool)):
        try:
            admin = await repo.set_admin_status(pool, email=email, status=body.status)
        except repo.NotFound as e:
            raise _not_found(e)
        except repo.Conflict as e:
            raise HTTPException(status.HTTP_409_CONFLICT, str(e))
        try:
            await zitadel_admin.set_user_active(email=email, active=body.status == "active")
        except zitadel_admin.ZitadelError as e:
            raise HTTPException(status.HTTP_502_BAD_GATEWAY, f"IdP update failed: {e}")
        return admin

    @app.delete("/admin/admins/{email}", status_code=204, tags=[router_tag_admin])
    async def remove_admin(email: str, _: Identity = Depends(require_admin),
                           pool=Depends(get_pool)):
        try:
            await repo.delete_admin(pool, email=email)
        except repo.NotFound as e:
            raise _not_found(e)
        except repo.Conflict as e:
            raise HTTPException(status.HTTP_409_CONFLICT, str(e))
        try:
            await zitadel_admin.delete_user(email=email)
        except zitadel_admin.ZitadelError as e:
            raise HTTPException(status.HTTP_502_BAD_GATEWAY, f"IdP delete failed: {e}")
        return Response(status_code=204)

    @app.post("/admin/admins/{email}/reset-password", status_code=204,
              tags=[router_tag_admin])
    async def reset_admin_password(email: str, body: ResetPasswordRequest,
                                   _: Identity = Depends(require_admin), pool=Depends(get_pool)):
        try:
            await repo.set_admin_password(
                pool, email=email, password_hash=security.hash_password(body.new_password))
        except repo.NotFound as e:
            raise _not_found(e)
        try:
            await zitadel_admin.set_user_password(email=email, password=body.new_password)
        except zitadel_admin.ZitadelError as e:
            raise HTTPException(status.HTTP_502_BAD_GATEWAY, f"IdP reset failed: {e}")
        return Response(status_code=204)

    # ── admin: tenants CRUD ──────────────────────────────────────────────────
    @app.post("/admin/tenants", response_model=TenantOut, status_code=201,
              tags=[router_tag_admin])
    async def create_tenant(body: TenantCreate, _: Identity = Depends(require_admin),
                            pool=Depends(get_pool)):
        try:
            tenant = await repo.create_tenant(
                pool, name=body.name, contact_email=body.contact_email,
                plan_id=body.plan_id, admin_email=body.admin_email,
                admin_password_hash=security.hash_password(body.admin_password))
        except repo.NotFound as e:
            raise _not_found(e)
        except repo.Conflict as e:
            raise HTTPException(status.HTTP_409_CONFLICT, str(e))
        # In Zitadel mode, also provision the tenant's login identity in the IdP
        # (no-op in dev mode / when no service token is configured).
        try:
            await zitadel_admin.provision_tenant_user(
                email=body.admin_email, password=body.admin_password,
                tenant_id=tenant["id"], display_name=body.name)
        except zitadel_admin.ZitadelError as e:
            raise HTTPException(status.HTTP_502_BAD_GATEWAY,
                                f"tenant created but IdP provisioning failed: {e}")
        await _sync_tenant(pool, tenant["id"])  # create the APISIX consumer-group
        return tenant

    @app.get("/admin/tenants", response_model=list[TenantOut], tags=[router_tag_admin])
    async def list_tenants(include_deleted: bool = False,
                           _: Identity = Depends(require_admin), pool=Depends(get_pool)):
        return await repo.list_tenants(pool, include_deleted=include_deleted)

    @app.get("/admin/tenants/{tenant_id}", response_model=TenantOut, tags=[router_tag_admin])
    async def get_tenant(tenant_id: str, _: Identity = Depends(require_admin),
                         pool=Depends(get_pool)):
        try:
            return await repo.get_tenant(pool, tenant_id, include_deleted=True)
        except repo.NotFound as e:
            raise _not_found(e)

    @app.patch("/admin/tenants/{tenant_id}", response_model=TenantOut, tags=[router_tag_admin])
    async def update_tenant(tenant_id: str, body: TenantUpdate,
                            _: Identity = Depends(require_admin),
                            pool=Depends(get_pool), redis=Depends(get_redis)):
        fields = body.model_dump(exclude_none=True)
        if "plan_id" in fields:
            if not await pool.fetchval("SELECT 1 FROM plans WHERE id=$1", fields["plan_id"]):
                raise HTTPException(status.HTTP_400_BAD_REQUEST, "unknown plan_id")
        try:
            tenant = await repo.update_tenant(pool, tenant_id, fields)
        except repo.NotFound as e:
            raise _not_found(e)
        # plan/status change affects gateway decisions → drop cached entries
        await _invalidate_tenant_cache(pool, redis, tenant_id)
        await _sync_tenant(pool, tenant_id)  # re-project quota/consumers to APISIX
        return tenant

    @app.delete("/admin/tenants/{tenant_id}", status_code=204, tags=[router_tag_admin])
    async def delete_tenant(tenant_id: str, _: Identity = Depends(require_admin),
                            pool=Depends(get_pool), redis=Depends(get_redis)):
        try:
            await repo.soft_delete_tenant(pool, tenant_id)
        except repo.NotFound as e:
            raise _not_found(e)
        await _invalidate_tenant_cache(pool, redis, tenant_id)
        await _sync_tenant(pool, tenant_id)  # tears down group + consumers in APISIX
        return Response(status_code=204)

    # ── admin: plans CRUD ────────────────────────────────────────────────────
    # ── admin: tenant users + password reset ─────────────────────────────────
    @app.get("/admin/tenants/{tenant_id}/users", response_model=list[TenantUserOut],
             tags=[router_tag_admin])
    async def tenant_users(tenant_id: str, _: Identity = Depends(require_admin),
                           pool=Depends(get_pool)):
        try:
            await repo.get_tenant(pool, tenant_id, include_deleted=True)
        except repo.NotFound as e:
            raise _not_found(e)
        return await repo.list_tenant_users(pool, tenant_id)

    @app.post("/admin/tenants/{tenant_id}/users", response_model=TenantUserOut,
              status_code=201, tags=[router_tag_admin])
    async def add_tenant_user(tenant_id: str, body: TenantUserCreate,
                              _: Identity = Depends(require_admin), pool=Depends(get_pool)):
        try:
            await repo.get_tenant(pool, tenant_id)  # 404 if missing/deleted
            user = await repo.create_tenant_user(
                pool, tenant_id=tenant_id, email=body.email,
                password_hash=security.hash_password(body.password))
        except repo.NotFound as e:
            raise _not_found(e)
        except repo.Conflict as e:
            raise HTTPException(status.HTTP_409_CONFLICT, str(e))
        try:
            await zitadel_admin.provision_tenant_user(
                email=body.email, password=body.password,
                tenant_id=tenant_id, display_name=body.email)
        except zitadel_admin.ZitadelError as e:
            raise HTTPException(status.HTTP_502_BAD_GATEWAY,
                                f"user created but IdP provisioning failed: {e}")
        return user

    @app.patch("/admin/tenants/{tenant_id}/users/{email}", response_model=TenantUserOut,
               tags=[router_tag_admin])
    async def modify_tenant_user(tenant_id: str, email: str, body: TenantUserUpdate,
                                 _: Identity = Depends(require_admin), pool=Depends(get_pool)):
        try:
            user = await repo.set_user_status(pool, tenant_id=tenant_id, email=email,
                                              status=body.status)
        except repo.NotFound as e:
            raise _not_found(e)
        try:
            await zitadel_admin.set_user_active(email=email, active=body.status == "active")
        except zitadel_admin.ZitadelError as e:
            raise HTTPException(status.HTTP_502_BAD_GATEWAY, f"IdP update failed: {e}")
        return user

    @app.delete("/admin/tenants/{tenant_id}/users/{email}", status_code=204,
                tags=[router_tag_admin])
    async def delete_tenant_user(tenant_id: str, email: str,
                                 _: Identity = Depends(require_admin), pool=Depends(get_pool)):
        try:
            await repo.delete_tenant_user(pool, tenant_id=tenant_id, email=email)
        except repo.NotFound as e:
            raise _not_found(e)
        try:
            await zitadel_admin.delete_user(email=email)
        except zitadel_admin.ZitadelError as e:
            raise HTTPException(status.HTTP_502_BAD_GATEWAY, f"IdP delete failed: {e}")
        return Response(status_code=204)

    @app.post("/admin/tenants/{tenant_id}/reset-password", status_code=204,
              tags=[router_tag_admin])
    async def reset_password(tenant_id: str, body: ResetPasswordRequest,
                             _: Identity = Depends(require_admin), pool=Depends(get_pool)):
        users = await repo.list_tenant_users(pool, tenant_id)
        if not any(u["email"] == body.email for u in users):
            raise HTTPException(status.HTTP_404_NOT_FOUND, "user not found for this tenant")
        # local hash (dev auth) — harmless in zitadel mode
        try:
            await repo.set_user_password(
                pool, email=body.email, tenant_id=tenant_id,
                password_hash=security.hash_password(body.new_password))
        except repo.NotFound as e:
            raise _not_found(e)
        # the real login backend in zitadel mode
        try:
            await zitadel_admin.set_user_password(email=body.email, password=body.new_password)
        except zitadel_admin.ZitadelError as e:
            raise HTTPException(status.HTTP_502_BAD_GATEWAY,
                                f"password reset in IdP failed: {e}")
        return Response(status_code=204)

    @app.get("/admin/plans", response_model=list[PlanOut], tags=[router_tag_admin])
    async def list_plans(_: Identity = Depends(require_admin), pool=Depends(get_pool)):
        return await repo.list_plans(pool)

    @app.post("/admin/plans", response_model=PlanOut, status_code=201, tags=[router_tag_admin])
    async def create_plan(body: PlanCreate, _: Identity = Depends(require_admin),
                          pool=Depends(get_pool)):
        try:
            return await repo.create_plan(pool, **body.model_dump())
        except repo.Conflict as e:
            raise HTTPException(status.HTTP_409_CONFLICT, str(e))

    @app.get("/admin/plans/{plan_id}", response_model=PlanOut, tags=[router_tag_admin])
    async def get_plan(plan_id: str, _: Identity = Depends(require_admin),
                       pool=Depends(get_pool)):
        try:
            return await repo.get_plan(pool, plan_id)
        except repo.NotFound as e:
            raise _not_found(e)

    @app.patch("/admin/plans/{plan_id}", response_model=PlanOut, tags=[router_tag_admin])
    async def update_plan(plan_id: str, body: PlanUpdate,
                          _: Identity = Depends(require_admin), pool=Depends(get_pool)):
        try:
            plan = await repo.update_plan(pool, plan_id, body.model_dump(exclude_none=True))
        except repo.NotFound as e:
            raise _not_found(e)
        # re-project the new quota/cap onto every tenant on this plan
        for tid in await repo.tenant_ids_on_plan(pool, plan_id):
            await _sync_tenant(pool, tid)
        return plan

    @app.delete("/admin/plans/{plan_id}", status_code=204, tags=[router_tag_admin])
    async def delete_plan(plan_id: str, _: Identity = Depends(require_admin),
                          pool=Depends(get_pool)):
        try:
            await repo.delete_plan(pool, plan_id)
        except repo.NotFound as e:
            raise _not_found(e)
        except repo.Conflict as e:
            raise HTTPException(status.HTTP_409_CONFLICT, str(e))
        return Response(status_code=204)

    # ── admin: billing ───────────────────────────────────────────────────────
    @app.post("/admin/billing/run", response_model=list[InvoiceOut], tags=[router_tag_admin])
    async def run_billing(period: str = Query(..., pattern=r"^\d{4}-\d{2}$"),
                          _: Identity = Depends(require_admin),
                          pool=Depends(get_pool), redis=Depends(get_redis)):
        await usage.flush_events(pool, redis)  # ensure rollups reflect buffered usage
        return await billing_engine.run_billing(pool, period)

    @app.get("/admin/invoices", response_model=list[InvoiceOut], tags=[router_tag_admin])
    async def admin_list_invoices(status_filter: str | None = Query(None, alias="status"),
                                  period: str | None = None,
                                  _: Identity = Depends(require_admin), pool=Depends(get_pool)):
        return await repo.list_invoices(pool, status=status_filter, period=period)

    @app.get("/admin/tenants/{tenant_id}/invoices", response_model=list[InvoiceOut],
             tags=[router_tag_admin])
    async def admin_tenant_invoices(tenant_id: str, _: Identity = Depends(require_admin),
                                    pool=Depends(get_pool)):
        return await repo.list_invoices(pool, tenant_id=tenant_id)

    @app.post("/admin/invoices/{invoice_id}/pay", response_model=InvoiceOut,
              tags=[router_tag_admin])
    async def mark_paid(invoice_id: str, _: Identity = Depends(require_admin),
                        pool=Depends(get_pool)):
        try:
            return await repo.mark_invoice_paid(pool, invoice_id)
        except repo.NotFound as e:
            raise _not_found(e)
        except repo.Conflict as e:
            raise HTTPException(status.HTTP_409_CONFLICT, str(e))

    # ── tenant: API keys CRUD ────────────────────────────────────────────────
    @app.post("/keys", response_model=KeyCreated, status_code=201, tags=[router_tag_tenant])
    async def create_key(body: KeyCreate, ident: Identity = Depends(require_tenant),
                         pool=Depends(get_pool), redis=Depends(get_redis)):
        rec, full = await repo.create_key(
            pool, tenant_id=ident.tenant_id, name=body.name, scopes=body.scopes)
        # warm the legacy gateway cache
        await _refresh_cache(pool, redis, rec["key_hash"], rec["tenant_id"])
        # register the key as an APISIX consumer in its tenant's group
        if apisix_admin.enabled():
            plan = await repo.tenant_with_plan(pool, ident.tenant_id)
            in_group = await apisix_admin.ensure_consumer_group(
                ident.tenant_id, quota=int(plan.get("monthly_quota") or 0),
                hard_cap=bool(plan.get("hard_cap")))
            await _apisix_safe(apisix_admin.upsert_consumer(
                rec["id"], api_key=full, in_group=in_group, tenant_id=ident.tenant_id))
        return KeyCreated(api_key=full, **_key_public(rec))

    @app.get("/keys", response_model=list[KeyOut], tags=[router_tag_tenant])
    async def list_keys(ident: Identity = Depends(require_tenant), pool=Depends(get_pool)):
        return [_key_with_secret(k) for k in await repo.list_keys(pool, ident.tenant_id)]

    @app.get("/keys/{key_id}", response_model=KeyOut, tags=[router_tag_tenant])
    async def get_key(key_id: str, ident: Identity = Depends(require_tenant),
                      pool=Depends(get_pool)):
        try:
            return _key_with_secret(await repo.get_key(pool, key_id, tenant_id=ident.tenant_id))
        except repo.NotFound as e:
            raise _not_found(e)

    @app.patch("/keys/{key_id}", response_model=KeyOut, tags=[router_tag_tenant])
    async def update_key(key_id: str, body: KeyUpdate,
                         ident: Identity = Depends(require_tenant),
                         pool=Depends(get_pool), redis=Depends(get_redis)):
        fields = body.model_dump(exclude_none=True)
        try:
            rec = await repo.update_key(pool, key_id, ident.tenant_id, fields)
        except repo.NotFound as e:
            raise _not_found(e)
        await _refresh_cache(pool, redis, rec["key_hash"], rec["tenant_id"])
        # enable → (re)create the APISIX consumer from the stored key; disable → remove it
        if apisix_admin.enabled():
            if rec["status"] == "active" and rec.get("key_enc"):
                plan = await repo.tenant_with_plan(pool, ident.tenant_id)
                in_group = await apisix_admin.ensure_consumer_group(
                    ident.tenant_id, quota=int(plan.get("monthly_quota") or 0),
                    hard_cap=bool(plan.get("hard_cap")))
                await _apisix_safe(apisix_admin.upsert_consumer(
                    rec["id"], api_key=security.decrypt_key(rec["key_enc"]),
                    in_group=in_group, tenant_id=ident.tenant_id))
            else:
                await _apisix_safe(apisix_admin.delete_consumer(rec["id"]))
        return rec

    @app.delete("/keys/{key_id}", status_code=204, tags=[router_tag_tenant])
    async def delete_key(key_id: str, ident: Identity = Depends(require_tenant),
                         pool=Depends(get_pool), redis=Depends(get_redis)):
        try:
            rec = await repo.soft_delete_key(pool, key_id, ident.tenant_id)
        except repo.NotFound as e:
            raise _not_found(e)
        await usage.invalidate_key(redis, rec["key_hash"])
        await _apisix_safe(apisix_admin.delete_consumer(rec["id"]))
        return Response(status_code=204)

    # ── tenant: usage & reports ──────────────────────────────────────────────
    @app.get("/usage/current", response_model=CurrentUsage, tags=[router_tag_tenant])
    async def current_usage(ident: Identity = Depends(require_tenant),
                            pool=Depends(get_pool), redis=Depends(get_redis)):
        # Read DURABLE usage from Postgres rollups (not the ephemeral Redis
        # counters, which the shared geocoder Redis can LRU-evict / lose on
        # restart). Flush buffered events first so a refresh reflects fresh calls.
        await usage.flush_events(pool, redis)
        period, _day = usage.now_parts()
        plan = await repo.tenant_with_plan(pool, ident.tenant_id)
        quota = int(plan.get("monthly_quota") or 0)
        total = await repo.usage_total_for_period(pool, ident.tenant_id, period)
        by_key = await repo.usage_by_key_for_period(pool, ident.tenant_id, period)
        per_key = [
            KeyUsage(key_id=k["id"], key_name=k["name"], requests=by_key.get(k["id"], 0))
            for k in await repo.list_keys(pool, ident.tenant_id)
        ]
        return CurrentUsage(
            tenant_id=ident.tenant_id, period=period, requests=total, quota=quota,
            remaining=max(0, quota - total), over_quota=total > quota,
            plan_id=plan.get("plan_id"), per_key=per_key)

    @app.get("/usage/history", response_model=list[UsageHistoryRow], tags=[router_tag_tenant])
    async def usage_history(
        period_from: str = Query(..., alias="from", pattern=r"^\d{4}-\d{2}$"),
        period_to: str = Query(..., alias="to", pattern=r"^\d{4}-\d{2}$"),
        ident: Identity = Depends(require_tenant), pool=Depends(get_pool),
        redis=Depends(get_redis),
    ):
        await usage.flush_events(pool, redis)  # surface very recent usage
        rows = await repo.usage_history(pool, ident.tenant_id,
                                        period_from=period_from, period_to=period_to)
        return [UsageHistoryRow(period=r["period"], day=r["day"],
                                endpoint=r["endpoint"], requests=r["requests"]) for r in rows]

    @app.get("/invoices", response_model=list[InvoiceOut], tags=[router_tag_tenant])
    async def my_invoices(ident: Identity = Depends(require_tenant), pool=Depends(get_pool)):
        return await repo.list_invoices(pool, tenant_id=ident.tenant_id)

    @app.get("/invoices/{invoice_id}", response_model=InvoiceOut, tags=[router_tag_tenant])
    async def my_invoice(invoice_id: str, ident: Identity = Depends(require_tenant),
                         pool=Depends(get_pool)):
        try:
            return await repo.get_invoice(pool, invoice_id, tenant_id=ident.tenant_id)
        except repo.NotFound as e:
            raise _not_found(e)

    # ── internal: APISIX usage sink ──────────────────────────────────────────
    @app.post("/internal/usage", tags=["ops"])
    async def usage_sink(request: Request, pool=Depends(get_pool), redis=Depends(get_redis)):
        """Receives APISIX http-logger batches and records served requests into
        Redis (live counters + durable event list). Not user-facing."""
        if config.USAGE_SINK_SECRET and \
                request.query_params.get("token") != config.USAGE_SINK_SECRET:
            raise HTTPException(status.HTTP_403_FORBIDDEN, "bad sink token")
        try:
            payload = await request.json()
        except Exception:
            return {"recorded": 0}
        entries = payload if isinstance(payload, list) else [payload]
        recorded = 0
        for e in entries:
            consumer = e.get("consumer")
            try:
                code = int(e.get("status") or 0)
            except (TypeError, ValueError):
                code = 0
            if not consumer or code >= 400:      # only count served requests
                continue
            key_id = apisix_admin.key_id_from_consumer(consumer)
            if not key_id:
                continue
            tenant_id = await _tenant_for_key(pool, key_id)
            if not tenant_id:
                continue
            endpoint = (e.get("uri") or "").strip("/").split("/")[0]
            await usage.record(redis, tenant_id=tenant_id, key_id=key_id, endpoint=endpoint)
            recorded += 1
        return {"recorded": recorded}

    @app.get("/health", tags=["ops"])
    async def health():
        return {"status": "ok"}

    return app


# ── helpers ──────────────────────────────────────────────────────────────────
_tenant_for_key_cache: dict[str, str] = {}


async def _tenant_for_key(pool, key_id: str) -> str | None:
    """key_id → tenant_id (immutable), cached in-process for the hot sink path."""
    tid = _tenant_for_key_cache.get(key_id)
    if tid is None:
        tid = await repo.tenant_id_for_key(pool, key_id)
        if tid:
            _tenant_for_key_cache[key_id] = tid
    return tid


def _key_public(rec: dict) -> dict:
    return {k: rec[k] for k in
            ("id", "tenant_id", "name", "key_prefix", "scopes", "status", "created_at")}


def _key_with_secret(rec: dict) -> dict:
    """Public key fields + the decrypted full key (owner can copy it any time)."""
    out = _key_public(rec)
    out["api_key"] = security.decrypt_key(rec["key_enc"]) if rec.get("key_enc") else None
    return out


async def _refresh_cache(pool, redis, key_hash: str, tenant_id: str) -> None:
    """Rebuild the shared key-cache entry from the DB (called after create/update)."""
    info = await repo.get_key_by_hash(pool, key_hash)
    if info is None:
        await usage.invalidate_key(redis, key_hash)
        return
    plan = await repo.tenant_with_plan(pool, tenant_id)
    await usage.cache_key(redis, key_hash, {
        "key_id": info["key_id"],
        "tenant_id": info["tenant_id"],
        "key_status": info["key_status"],
        "tenant_status": info["tenant_status"],
        "scopes": info["scopes"],
        "quota": int(plan.get("monthly_quota") or 0),
        "hard_cap": bool(plan.get("hard_cap")),
        "plan_id": plan.get("plan_id"),
    })
