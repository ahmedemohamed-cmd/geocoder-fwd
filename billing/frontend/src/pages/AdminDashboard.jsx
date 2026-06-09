import React, { useEffect, useState } from "react";
import PlansCard from "./PlansCard.jsx";

export default function AdminDashboard({ api }) {
  const [tenants, setTenants] = useState([]);
  const [plans, setPlans] = useState([]);
  const [sel, setSel] = useState(null);
  const [invoices, setInvoices] = useState([]);
  const [err, setErr] = useState(null);
  const [form, setForm] = useState({
    name: "", plan_id: "starter", admin_email: "", admin_password: "",
  });

  const load = () =>
    api.get("/admin/tenants").then(setTenants).catch((e) => setErr(e.message));
  useEffect(() => { load(); api.get("/admin/plans").then(setPlans).catch(() => {}); }, []);

  const openTenant = (t) => {
    setSel(t);
    api.get(`/admin/tenants/${t.id}/invoices`).then(setInvoices).catch((e) => setErr(e.message));
  };

  const createTenant = async (e) => {
    e.preventDefault();
    setErr(null);
    try {
      await api.post("/admin/tenants", form);
      setForm({ name: "", plan_id: "starter", admin_email: "", admin_password: "" });
      load();
    } catch (e2) { setErr(e2.message); }
  };

  const runBilling = async () => {
    const period = new Date().toISOString().slice(0, 7);
    try { await api.post(`/admin/billing/run?period=${period}`); if (sel) openTenant(sel); }
    catch (e) { setErr(e.message); }
  };

  const pay = async (id) => {
    try { await api.post(`/admin/invoices/${id}/pay`); openTenant(sel); }
    catch (e) { setErr(e.message); }
  };

  const del = async (t) => {
    if (!confirm(`Deactivate tenant ${t.name}?`)) return;
    try { await api.del(`/admin/tenants/${t.id}`); load(); setSel(null); }
    catch (e) { setErr(e.message); }
  };

  return (
    <div className="grid">
      <section className="card">
        <h2>Tenants</h2>
        {err && <p className="error">{err}</p>}
        <table>
          <thead><tr><th>Name</th><th>Plan</th><th>Status</th><th></th></tr></thead>
          <tbody>
            {tenants.map((t) => (
              <tr key={t.id} className={sel?.id === t.id ? "active" : ""}>
                <td><a onClick={() => openTenant(t)}>{t.name}</a></td>
                <td>{t.plan_id}</td>
                <td>{t.status}</td>
                <td><button className="danger" onClick={() => del(t)}>Deactivate</button></td>
              </tr>
            ))}
          </tbody>
        </table>

        <h3>Add tenant</h3>
        <form onSubmit={createTenant} className="stack">
          <input placeholder="Tenant name" value={form.name}
            onChange={(e) => setForm({ ...form, name: e.target.value })} required />
          <select value={form.plan_id}
            onChange={(e) => setForm({ ...form, plan_id: e.target.value })}>
            {plans.map((p) => <option key={p.id} value={p.id}>{p.name}</option>)}
          </select>
          <input placeholder="Owner email" type="email" value={form.admin_email}
            onChange={(e) => setForm({ ...form, admin_email: e.target.value })} required />
          <input placeholder="Owner password (min 8)" type="password" value={form.admin_password}
            onChange={(e) => setForm({ ...form, admin_password: e.target.value })} required />
          <button className="primary" type="submit">Create tenant</button>
        </form>
      </section>

      <section className="card">
        <div className="row">
          <h2>Bills {sel ? `· ${sel.name}` : ""}</h2>
          <span className="spacer" />
          <button onClick={runBilling}>Run billing (this month)</button>
        </div>
        {!sel && <p className="muted">Select a tenant to view its bills.</p>}
        {sel && (
          <table>
            <thead><tr><th>Period</th><th>Requests</th><th>Amount</th><th>Status</th><th></th></tr></thead>
            <tbody>
              {invoices.map((i) => (
                <tr key={i.id}>
                  <td>{i.period}</td>
                  <td>{i.total_requests}</td>
                  <td>${(i.amount_cents / 100).toFixed(2)}</td>
                  <td><span className={`pill ${i.status}`}>{i.status}</span></td>
                  <td>{i.status === "pending" &&
                    <button className="primary" onClick={() => pay(i.id)}>Mark paid</button>}</td>
                </tr>
              ))}
              {invoices.length === 0 && <tr><td colSpan="5" className="muted">No invoices yet.</td></tr>}
            </tbody>
          </table>
        )}
      </section>

      <PlansCard api={api} onChange={setPlans} />
    </div>
  );
}
