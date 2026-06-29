import React, { useEffect, useState } from "react";
import PlansCard from "./PlansCard.jsx";
import AdminsCard from "./AdminsCard.jsx";
import AccountCard from "./AccountCard.jsx";

export default function AdminDashboard({ api }) {
  const [tenants, setTenants] = useState([]);
  const [plans, setPlans] = useState([]);
  const [sel, setSel] = useState(null);
  const [invoices, setInvoices] = useState([]);
  const [users, setUsers] = useState([]);
  const [err, setErr] = useState(null);
  const [form, setForm] = useState({
    name: "", plan_id: "starter", admin_email: "", admin_password: "",
  });
  const [newUser, setNewUser] = useState({ email: "", password: "" });

  const load = () =>
    api.get("/admin/tenants").then(setTenants).catch((e) => setErr(e.message));
  useEffect(() => { load(); api.get("/admin/plans").then(setPlans).catch(() => {}); }, []);

  const loadUsers = (tid) =>
    api.get(`/admin/tenants/${tid}/users`).then(setUsers).catch(() => setUsers([]));

  const openTenant = (t) => {
    setSel(t);
    api.get(`/admin/tenants/${t.id}/invoices`).then(setInvoices).catch((e) => setErr(e.message));
    loadUsers(t.id);
  };

  const resetPw = async (email) => {
    const pw = prompt(`New password for ${email}\n(min 8, with upper + lower + number + symbol):`);
    if (!pw) return;
    try {
      await api.post(`/admin/tenants/${sel.id}/reset-password`, { email, new_password: pw });
      alert(`Password reset for ${email}.`);
    } catch (e) { setErr(e.message); }
  };

  const resetMfa = async (email) => {
    if (!confirm(`Reset MFA for ${email}? They'll set up a new authenticator at next sign-in.`)) return;
    try {
      await api.post(`/admin/tenants/${sel.id}/reset-mfa`, { email });
      alert(`MFA reset for ${email}.`);
    } catch (e) { setErr(e.message); }
  };

  const addUser = async (e) => {
    e.preventDefault();
    setErr(null);
    try {
      await api.post(`/admin/tenants/${sel.id}/users`, newUser);
      setNewUser({ email: "", password: "" });
      loadUsers(sel.id);
    } catch (e2) { setErr(e2.message); }
  };
  const toggleUser = async (u) => {
    const status = u.status === "active" ? "disabled" : "active";
    try { await api.patch(`/admin/tenants/${sel.id}/users/${encodeURIComponent(u.email)}`, { status }); loadUsers(sel.id); }
    catch (e) { setErr(e.message); }
  };
  const delUser = async (u) => {
    if (!confirm(`Delete user ${u.email}?`)) return;
    try { await api.del(`/admin/tenants/${sel.id}/users/${encodeURIComponent(u.email)}`); loadUsers(sel.id); }
    catch (e) { setErr(e.message); }
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

  const setTenantStatus = async (t, status) => {
    try { await api.patch(`/admin/tenants/${t.id}`, { status }); load(); if (sel?.id === t.id) openTenant({ ...t, status }); }
    catch (e) { setErr(e.message); }
  };
  const changePlan = async (t, plan_id) => {
    if (!plan_id || plan_id === t.plan_id) return;
    try { await api.patch(`/admin/tenants/${t.id}`, { plan_id }); load(); if (sel?.id === t.id) openTenant({ ...t, plan_id }); }
    catch (e) { setErr(e.message); }
  };
  const del = async (t) => {
    if (!confirm(`Delete tenant ${t.name}? This is permanent (soft delete).`)) return;
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
                <td>
                  <select value={t.plan_id || ""} onChange={(e) => changePlan(t, e.target.value)}
                    disabled={t.status === "deleted"} title="Change subscription plan">
                    {!plans.some((p) => p.id === t.plan_id) && (
                      <option value={t.plan_id || ""}>{t.plan_id || "—"}</option>
                    )}
                    {plans.map((p) => <option key={p.id} value={p.id}>{p.name}</option>)}
                  </select>
                </td>
                <td><span className={`pill ${t.status}`}>{t.status}</span></td>
                <td>
                  {t.status === "active" && (
                    <button onClick={() => setTenantStatus(t, "suspended")}>Suspend</button>
                  )}
                  {t.status === "suspended" && (
                    <button className="primary" onClick={() => setTenantStatus(t, "active")}>Reactivate</button>
                  )}
                  {t.status !== "deleted" && (
                    <button className="danger" onClick={() => del(t)}>Delete</button>
                  )}
                </td>
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
        {!sel && <p className="muted">Select a tenant to view its users &amp; bills.</p>}
        {sel && (
          <>
            <h3>Users</h3>
            <table>
              <thead><tr><th>Email</th><th>Role</th><th>Status</th><th></th></tr></thead>
              <tbody>
                {users.map((u) => (
                  <tr key={u.email}>
                    <td>{u.email}</td>
                    <td>{u.role}</td>
                    <td><span className={`pill ${u.status === "active" ? "active" : "disabled"}`}>{u.status}</span></td>
                    <td>
                      <button onClick={() => resetPw(u.email)}>Reset password</button>
                      <button onClick={() => resetMfa(u.email)}>Reset MFA</button>
                      {u.role !== "admin" && (
                        <>
                          <button onClick={() => toggleUser(u)}>
                            {u.status === "active" ? "Disable" : "Enable"}
                          </button>
                          <button className="danger" onClick={() => delUser(u)}>Delete</button>
                        </>
                      )}
                    </td>
                  </tr>
                ))}
                {users.length === 0 && <tr><td colSpan="4" className="muted">No users.</td></tr>}
              </tbody>
            </table>
            <form onSubmit={addUser} className="row" style={{ marginTop: 8 }}>
              <input placeholder="new user email" type="email" value={newUser.email}
                onChange={(e) => setNewUser({ ...newUser, email: e.target.value })} required />
              <input placeholder="password (min 8)" type="password" value={newUser.password}
                onChange={(e) => setNewUser({ ...newUser, password: e.target.value })} required />
              <button className="primary" type="submit">Add user</button>
            </form>
            <h3>Invoices</h3>
          </>
        )}
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
      <AdminsCard api={api} />
      <AccountCard api={api} />
    </div>
  );
}
