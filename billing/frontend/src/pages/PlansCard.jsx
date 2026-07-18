import React, { useEffect, useState } from "react";

const BLANK = {
  id: "", name: "", monthly_quota: 1000,
  base_dollars: 0, overage_cents_per_unit: 0, hard_cap: false, rps: 0,
};

export default function PlansCard({ api, onChange }) {
  const [plans, setPlans] = useState([]);
  const [form, setForm] = useState(BLANK);
  const [editing, setEditing] = useState(false);
  const [err, setErr] = useState(null);

  const load = () =>
    api.get("/admin/plans").then((p) => { setPlans(p); onChange && onChange(p); })
       .catch((e) => setErr(e.message));
  useEffect(() => { load(); }, []);

  const edit = (p) => {
    setEditing(true);
    setForm({
      id: p.id, name: p.name, monthly_quota: p.monthly_quota,
      base_dollars: (p.base_price_cents / 100),
      overage_cents_per_unit: p.overage_cents_per_unit, hard_cap: p.hard_cap,
      rps: p.rps ?? 0,
    });
  };
  const reset = () => { setEditing(false); setForm(BLANK); setErr(null); };

  const submit = async (e) => {
    e.preventDefault();
    setErr(null);
    const payload = {
      name: form.name,
      monthly_quota: Number(form.monthly_quota),
      base_price_cents: Math.round(Number(form.base_dollars) * 100),
      overage_cents_per_unit: Number(form.overage_cents_per_unit),
      hard_cap: !!form.hard_cap,
      rps: Number(form.rps),
    };
    try {
      if (editing) {
        await api.patch(`/admin/plans/${form.id}`, payload);
      } else {
        await api.post("/admin/plans", { id: form.id, ...payload });
      }
      reset();
      load();
    } catch (e2) { setErr(e2.message); }
  };

  const del = async (p) => {
    if (!confirm(`Delete plan "${p.id}"?`)) return;
    try { await api.del(`/admin/plans/${p.id}`); load(); }
    catch (e) { setErr(e.message); }
  };

  return (
    <section className="card">
      <h2>Plans</h2>
      {err && <p className="error">{err}</p>}
      <table>
        <thead><tr>
          <th>ID</th><th>Name</th><th>Quota</th><th>Base</th><th>Overage</th><th>Cap</th><th>RPS</th><th></th>
        </tr></thead>
        <tbody>
          {plans.map((p) => (
            <tr key={p.id} className={editing && form.id === p.id ? "active" : ""}>
              <td><code>{p.id}</code></td>
              <td>{p.name}</td>
              <td>{p.monthly_quota.toLocaleString()}</td>
              <td>${(p.base_price_cents / 100).toFixed(2)}</td>
              <td>{p.overage_cents_per_unit}¢/credit</td>
              <td>{p.hard_cap ? "hard" : "soft"}</td>
              <td>{p.rps > 0 ? p.rps : "—"}</td>
              <td>
                <button onClick={() => edit(p)}>Edit</button>
                <button className="danger" onClick={() => del(p)}>Delete</button>
              </td>
            </tr>
          ))}
        </tbody>
      </table>

      <h3>{editing ? `Edit plan · ${form.id}` : "Add plan"}</h3>
      <form onSubmit={submit} className="stack">
        {!editing && (
          <input placeholder="id (e.g. enterprise)" value={form.id}
            onChange={(e) => setForm({ ...form, id: e.target.value })} required
            pattern="[a-z0-9_\-]+" title="lowercase letters, digits, - and _" />
        )}
        <input placeholder="Display name" value={form.name}
          onChange={(e) => setForm({ ...form, name: e.target.value })} required />
        <label className="field">Monthly quota (included credits)
          <input type="number" min="0" value={form.monthly_quota}
            onChange={(e) => setForm({ ...form, monthly_quota: e.target.value })} required />
        </label>
        <label className="field">Base price (USD / month)
          <input type="number" min="0" step="0.01" value={form.base_dollars}
            onChange={(e) => setForm({ ...form, base_dollars: e.target.value })} required />
        </label>
        <label className="field">Overage (¢ per credit over quota — 0.04 = $0.40/1k credits)
          <input type="number" min="0" step="0.001" value={form.overage_cents_per_unit}
            onChange={(e) => setForm({ ...form, overage_cents_per_unit: e.target.value })} required />
        </label>
        <label className="field">Rate limit (requests/second, 0 = uncapped)
          <input type="number" min="0" value={form.rps}
            onChange={(e) => setForm({ ...form, rps: e.target.value })} required />
        </label>
        <label className="check">
          <input type="checkbox" checked={form.hard_cap}
            onChange={(e) => setForm({ ...form, hard_cap: e.target.checked })} />
          Hard cap (reject requests over quota with 429)
        </label>
        <div className="row">
          <button className="primary" type="submit">{editing ? "Save" : "Create plan"}</button>
          {editing && <button type="button" onClick={reset}>Cancel</button>}
        </div>
      </form>
    </section>
  );
}
