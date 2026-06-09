import React from "react";
import { useAuth } from "react-oidc-context";
import { roleOf, tenantIdOf } from "./claims.js";
import { makeApi } from "./api.js";
import AdminDashboard from "./pages/AdminDashboard.jsx";
import TenantDashboard from "./pages/TenantDashboard.jsx";

export default function App({ config }) {
  const auth = useAuth();

  if (auth.isLoading) return <Centered>Loading…</Centered>;
  if (auth.error) return <Centered>Auth error: {auth.error.message}</Centered>;

  if (!auth.isAuthenticated) {
    return (
      <Centered>
        <h1>API Console</h1>
        <p className="muted">Keys, real-time usage &amp; billing</p>
        <button className="primary" onClick={() => auth.signinRedirect()}>
          Sign in with Zitadel
        </button>
      </Centered>
    );
  }

  const profile = auth.user?.profile || {};
  const role = roleOf(profile);
  const tenantId = tenantIdOf(profile);
  const token = auth.user?.access_token;
  const api = makeApi(token, config.apiBase);

  return (
    <div className="app">
      <header>
        <strong>API Console</strong>
        <span className="spacer" />
        <span className="muted">
          {profile.email || profile.preferred_username} · <b>{role || "no role"}</b>
        </span>
        <button onClick={() => auth.signoutRedirect()}>Sign out</button>
      </header>
      <main>
        {role === "admin" && <AdminDashboard api={api} />}
        {role === "tenant_user" && <TenantDashboard api={api} tenantId={tenantId} />}
        {!role && (
          <Centered>
            Your account has no billing role assigned. Ask an admin to grant
            <code> admin </code> or <code> tenant_user</code>.
          </Centered>
        )}
      </main>
    </div>
  );
}

function Centered({ children }) {
  return <div className="centered">{children}</div>;
}
