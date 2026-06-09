// Map Zitadel ID-token claims to our {role, tenantId} identity — the same
// mapping the backend does (billing/auth.py:identity_from_claims).
const ROLES_CLAIM = "urn:zitadel:iam:org:project:roles";
const METADATA_CLAIM = "urn:zitadel:iam:user:metadata";
const TENANT_KEY = "tenant_id";

export function roleOf(profile) {
  const roles = (profile && profile[ROLES_CLAIM]) || {};
  if ("admin" in roles) return "admin";
  if ("tenant_user" in roles) return "tenant_user";
  return null;
}

export function tenantIdOf(profile) {
  const meta = (profile && profile[METADATA_CLAIM]) || {};
  const raw = meta[TENANT_KEY];
  if (!raw) return null;
  try {
    return atob(raw); // Zitadel base64-encodes metadata values
  } catch (_) {
    return raw;
  }
}
