import React, { useEffect, useState } from "react";

// Weights are stored server-side as integer milli-credits (1 credit = 1000);
// the form works in credits and converts on submit, like dollars↔cents in PlansCard.
const BLANK = { endpoint: "", credits: 1 };

export default function WeightsCard({ api }) {
  const [weights, setWeights] = useState([]);
  const [form, setForm] = useState(BLANK);
  const [editing, setEditing] = useState(false);
  const [err, setErr] = useState(null);

  const load = () =>
    api.get("/admin/weights").then(setWeights).catch((e) => setErr(e.message));
  useEffect(() => { load(); }, []);

  const edit = (w) => {
    setEditing(true);
    setForm({ endpoint: w.endpoint, credits: w.milli_credits / 1000 });
  };
  const reset = () => { setEditing(false); setForm(BLANK); setErr(null); };

  const submit = async (e) => {
    e.preventDefault();
    setErr(null);
    try {
      await api.put(`/admin/weights/${form.endpoint}`, {
        milli_credits: Math.round(Number(form.credits) * 1000),
      });
      reset();
      load();
    } catch (e2) { setErr(e2.message); }
  };

  const del = async (w) => {
    if (!confirm(`Reset "${w.endpoint}" to the default weight (1 credit per request)?`)) return;
    try { await api.del(`/admin/weights/${w.endpoint}`); load(); }
    catch (e) { setErr(e.message); }
  };

  return (
    <section className="card">
      <h2>Endpoint credit weights</h2>
      <p className="muted">
        Each request is billed at its endpoint's weight in credits (matched on the
        first path segment, e.g. <code>deep/forward</code> → <code>deep</code>).
        Unlisted endpoints cost 1 credit per request.
      </p>
      {err && <p className="error">{err}</p>}
      <table>
        <thead><tr>
          <th>Endpoint</th><th>Credits / request</th><th>Updated</th><th></th>
        </tr></thead>
        <tbody>
          {weights.map((w) => (
            <tr key={w.endpoint} className={editing && form.endpoint === w.endpoint ? "active" : ""}>
              <td><code>{w.endpoint}</code></td>
              <td>{w.milli_credits / 1000}</td>
              <td>{new Date(w.updated_at).toLocaleDateString()}</td>
              <td>
                <button onClick={() => edit(w)}>Edit</button>
                <button className="danger" onClick={() => del(w)}>Reset</button>
              </td>
            </tr>
          ))}
          {weights.length === 0 && <tr><td colSpan="4" className="muted">No weights configured.</td></tr>}
        </tbody>
      </table>

      <h3>{editing ? `Edit weight · ${form.endpoint}` : "Add weight"}</h3>
      <form onSubmit={submit} className="stack">
        {!editing && (
          <input placeholder="endpoint (e.g. autocomplete)" value={form.endpoint}
            onChange={(e) => setForm({ ...form, endpoint: e.target.value })} required
            pattern="[a-z0-9_\-]+" title="lowercase letters, digits, - and _" />
        )}
        <label className="field">Credits per request
          <input type="number" min="0" step="0.001" value={form.credits}
            onChange={(e) => setForm({ ...form, credits: e.target.value })} required />
        </label>
        <div className="row">
          <button className="primary" type="submit">{editing ? "Save" : "Create weight"}</button>
          {editing && <button type="button" onClick={reset}>Cancel</button>}
        </div>
      </form>
    </section>
  );
}
