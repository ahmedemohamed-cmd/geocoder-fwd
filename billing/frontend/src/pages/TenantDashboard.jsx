import React, { useEffect, useState } from "react";
import Tabs from "./Tabs.jsx";

const TABS = [
  { id: "overview", label: "Overview" },
  { id: "pricing", label: "Pricing" },
];

export default function TenantDashboard({ api }) {
  const [tab, setTab] = useState("overview");
  const [keys, setKeys] = useState([]);
  const [usage, setUsage] = useState(null);
  const [invoices, setInvoices] = useState([]);
  const [newName, setNewName] = useState("");
  const [created, setCreated] = useState(null); // plaintext key highlighted on create
  const [err, setErr] = useState(null);
  const [revealed, setRevealed] = useState({}); // key id -> show full secret
  const [copiedId, setCopiedId] = useState(null);
  const [weights, setWeights] = useState([]); // endpoint -> credits per request

  const copy = async (id, text) => {
    if (!text) return;
    try {
      await navigator.clipboard.writeText(text);
      setCopiedId(id);
      setTimeout(() => setCopiedId((c) => (c === id ? null : c)), 1500);
    } catch (_) { /* clipboard blocked (e.g. non-https) — user can select manually */ }
  };
  const mask = (k) => (k.api_key ? `${k.key_prefix}_${"•".repeat(12)}` : `${k.key_prefix} (unavailable)`);

  const loadKeys = () => api.get("/keys").then(setKeys).catch((e) => setErr(e.message));
  const loadInvoices = () => api.get("/invoices").then(setInvoices).catch(() => {});
  const loadUsage = () => api.get("/usage/current").then(setUsage).catch(() => {});
  const loadWeights = () => api.get("/weights").then(setWeights).catch(() => {});

  // Usage is loaded on open and on the manual Refresh button (no live polling).
  useEffect(() => { loadUsage(); loadKeys(); loadInvoices(); loadWeights(); }, []);

  const createKey = async (e) => {
    e.preventDefault();
    setErr(null);
    try {
      const k = await api.post("/keys", { name: newName, scopes: [] });
      setCreated(k.api_key);
      setNewName("");
      loadKeys();
    } catch (e2) { setErr(e2.message); }
  };

  const toggle = async (k) => {
    const status = k.status === "active" ? "disabled" : "active";
    try { await api.patch(`/keys/${k.id}`, { status }); loadKeys(); }
    catch (e) { setErr(e.message); }
  };

  const del = async (k) => {
    if (!confirm(`Delete key "${k.name}"? (soft delete)`)) return;
    try { await api.del(`/keys/${k.id}`); loadKeys(); }
    catch (e) { setErr(e.message); }
  };

  // credits_used drives the meter; fall back to requests for a pre-credits API
  const used = usage ? (usage.credits_used ?? usage.requests) : 0;
  const pct = usage && usage.quota ? Math.min(100, (used / usage.quota) * 100) : 0;

  return (
    <>
      <Tabs tabs={TABS} active={tab} onChange={setTab} />
      {tab === "pricing" && (
        <div className="grid single">
          <section className="card">
            <h2>Credit pricing</h2>
            <p className="muted">
              Each API request is billed in credits by endpoint (first path
              segment — e.g. <code>/deep/forward</code> bills as <code>/deep</code>).
              Endpoints not listed here cost 1 credit per request; free endpoints
              (health, feedback, probe uploads) are never billed.
            </p>
            <table>
              <thead><tr><th>Endpoint</th><th>Credits / request</th></tr></thead>
              <tbody>
                {weights.map((w) => (
                  <tr key={w.endpoint}>
                    <td><code>/{w.endpoint}</code></td>
                    <td>{w.milli_credits / 1000}</td>
                  </tr>
                ))}
                {weights.length === 0 && <tr><td colSpan="2" className="muted">Loading…</td></tr>}
              </tbody>
            </table>
          </section>
        </div>
      )}
      {tab === "overview" && (
    <div className="grid">
      <section className="card">
        <div className="row">
          <h2>Usage · {usage?.period}</h2>
          <span className="spacer" />
          <button onClick={loadUsage}>Refresh</button>
        </div>
        {usage ? (
          <>
            <div className="bignum">{used.toLocaleString()}
              <span className="muted"> / {usage.quota.toLocaleString()} credits</span></div>
            <div className="bar"><div className={`fill ${usage.over_quota ? "over" : ""}`}
              style={{ width: `${pct}%` }} /></div>
            <p className="muted">
              {usage.over_quota ? "Over quota — overage billed" : `${usage.remaining.toLocaleString()} credits remaining`}
              {" · "}{usage.requests.toLocaleString()} requests
              {" · plan "}{usage.plan_id}
            </p>
            <table>
              <thead><tr><th>Key</th><th>Credits (month)</th><th>Requests</th></tr></thead>
              <tbody>
                {usage.per_key.map((k) => (
                  <tr key={k.key_id}>
                    <td>{k.key_name}</td>
                    <td>{(k.credits ?? k.requests).toLocaleString()}</td>
                    <td>{k.requests.toLocaleString()}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </>
        ) : <p className="muted">Loading usage…</p>}

        <h2 style={{ marginTop: 24 }}>Invoices</h2>
        <table>
          <thead><tr><th>Period</th><th>Credits</th><th>Amount</th><th>Status</th></tr></thead>
          <tbody>
            {invoices.map((i) => (
              <tr key={i.id}>
                <td>{i.period}</td><td>{(i.total_credits ?? i.total_requests).toLocaleString()}</td>
                <td>${(i.amount_cents / 100).toFixed(2)}</td>
                <td><span className={`pill ${i.status}`}>{i.status}</span></td>
              </tr>
            ))}
            {invoices.length === 0 && <tr><td colSpan="4" className="muted">No invoices yet.</td></tr>}
          </tbody>
        </table>
      </section>

      <section className="card">
        <h2>API keys</h2>
        {err && <p className="error">{err}</p>}
        {created && (
          <div className="banner">
            Copy your key now — it won’t be shown again:
            <code className="key">{created}</code>
            <button onClick={() => setCreated(null)}>Dismiss</button>
          </div>
        )}
        <table>
          <thead><tr><th>Name</th><th>Key</th><th>Status</th><th></th></tr></thead>
          <tbody>
            {keys.map((k) => (
              <tr key={k.id}>
                <td>{k.name}</td>
                <td>
                  <code className="key">{revealed[k.id] ? (k.api_key || mask(k)) : mask(k)}</code>
                  {k.api_key && (
                    <>
                      <button onClick={() => setRevealed((r) => ({ ...r, [k.id]: !r[k.id] }))}>
                        {revealed[k.id] ? "Hide" : "Show"}
                      </button>
                      <button onClick={() => copy(k.id, k.api_key)}>
                        {copiedId === k.id ? "Copied!" : "Copy"}
                      </button>
                    </>
                  )}
                </td>
                <td><span className={`pill ${k.status}`}>{k.status}</span></td>
                <td>
                  <button onClick={() => toggle(k)}>
                    {k.status === "active" ? "Disable" : "Enable"}
                  </button>
                  <button className="danger" onClick={() => del(k)}>Delete</button>
                </td>
              </tr>
            ))}
            {keys.length === 0 && <tr><td colSpan="4" className="muted">No keys yet.</td></tr>}
          </tbody>
        </table>

        <h3>Create key</h3>
        <form onSubmit={createKey} className="row">
          <input placeholder="Key name (e.g. production)" value={newName}
            onChange={(e) => setNewName(e.target.value)} required />
          <button className="primary" type="submit">Create</button>
        </form>
      </section>
    </div>
      )}
    </>
  );
}
