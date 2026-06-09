import React from "react";
import ReactDOM from "react-dom/client";
import { AuthProvider } from "react-oidc-context";
import { WebStorageStateStore } from "oidc-client-ts";
import App from "./App.jsx";
import "./index.css";

// Runtime config is written by the Zitadel provisioner into /runtime/config.json
// so the SPA is built once and configured at deploy time.
async function loadConfig() {
  try {
    const r = await fetch("/runtime/config.json", { cache: "no-store" });
    if (r.ok) return await r.json();
  } catch (_) { /* fall through to defaults */ }
  return {
    issuer: "http://localhost:8085",
    clientId: "REPLACE_AT_DEPLOY_TIME",
    projectId: "",
    scope: "openid profile email urn:zitadel:iam:user:metadata",
    apiBase: "http://localhost:8100",
    gatewayBase: "http://localhost:8080",
  };
}

loadConfig().then((cfg) => {
  window.__APP_CONFIG__ = cfg;
  const oidcConfig = {
    authority: cfg.issuer,
    client_id: cfg.clientId,
    redirect_uri: window.location.origin + "/callback",
    post_logout_redirect_uri: window.location.origin + "/",
    response_type: "code",
    scope: cfg.scope,
    userStore: new WebStorageStateStore({ store: window.localStorage }),
    onSigninCallback: () => {
      window.history.replaceState({}, document.title, "/");
    },
  };
  ReactDOM.createRoot(document.getElementById("root")).render(
    <React.StrictMode>
      <AuthProvider {...oidcConfig}>
        <App config={cfg} />
      </AuthProvider>
    </React.StrictMode>
  );
});
