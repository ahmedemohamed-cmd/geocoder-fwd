import React, { useEffect, useState } from "react";

export default function AdminsCard({ api }) {
  const [admins, setAdmins] = useState([]);
  const [form, setForm] = useState({ email: "", password: "" });
  const [err, setErr] = useState(null);

  const load = () => api.get("/admin/admins").then(setAdmins).catch((e) => setErr(e.message));
  useEffect(() => { load(); }, []);

  const add = async (e) => {
    e.preventDefault();
    setErr(null);
    try {
      await api.post("/admin/admins", form);
      setForm({ email: "", password: "" });
      load();
    } catch (e2) { setErr(e2.message); }
  };
  const toggle = async (a) => {
    const status = a.status === "active" ? "disabled" : "active";
    try { await api.patch(`/admin/admins/${encodeURIComponent(a.email)}`, { status }); load(); }
    catch (e) { setErr(e.message); }
  };
  const del = async (a) => {
    if (!confirm(`Remove admin ${a.email}?`)) return;
    try { await api.del(`/admin/admins/${encodeURIComponent(a.email)}`); load(); }
    catch (e) { setErr(e.message); }
  };
  const resetPw = async (a) => {
    const pw = prompt(`New password for ${a.email}\n(min 8, with upper + lower + number + symbol):`);
    if (!pw) return;
    try {
      await api.post(`/admin/admins/${encodeURIComponent(a.email)}/reset-password`,
                     { email: a.email, new_password: pw });
      alert(`Password reset for ${a.email}.`);
    } catch (e) { setErr(e.message); }
  };

  return (
    <section className="card">
      <h2>Platform admins</h2>
      {err && <p className="error">{err}</p>}
      <table>
        <thead><tr><th>Email</th><th>Status</th><th></th></tr></thead>
        <tbody>
          {admins.map((a) => (
            <tr key={a.email}>
              <td>{a.email}</td>
              <td><span className={`pill ${a.status === "active" ? "active" : "disabled"}`}>{a.status}</span></td>
              <td>
                <button onClick={() => resetPw(a)}>Reset password</button>
                <button onClick={() => toggle(a)}>{a.status === "active" ? "Disable" : "Enable"}</button>
                <button className="danger" onClick={() => del(a)}>Remove</button>
              </td>
            </tr>
          ))}
          {admins.length === 0 && <tr><td colSpan="3" className="muted">No admins.</td></tr>}
        </tbody>
      </table>
      <h3>Add admin</h3>
      <form onSubmit={add} className="row">
        <input placeholder="admin email" type="email" value={form.email}
          onChange={(e) => setForm({ ...form, email: e.target.value })} required />
        <input placeholder="password (min 8)" type="password" value={form.password}
          onChange={(e) => setForm({ ...form, password: e.target.value })} required />
        <button className="primary" type="submit">Add admin</button>
      </form>
      <p className="muted">The last active admin can't be disabled or removed.</p>
    </section>
  );
}
