import React, { useState } from "react";

// Self-service security panel shown to every authenticated user (admin or
// tenant). Lets them change their own password and reset their own MFA (TOTP).
export default function AccountCard({ api }) {
  const [pw, setPw] = useState({ current_password: "", new_password: "", confirm: "" });
  const [msg, setMsg] = useState(null);
  const [err, setErr] = useState(null);
  const [busy, setBusy] = useState(false);

  const changePw = async (e) => {
    e.preventDefault();
    setErr(null);
    setMsg(null);
    if (pw.new_password !== pw.confirm) {
      setErr("New passwords don't match.");
      return;
    }
    setBusy(true);
    try {
      await api.post("/me/password", {
        current_password: pw.current_password,
        new_password: pw.new_password,
      });
      setPw({ current_password: "", new_password: "", confirm: "" });
      setMsg("Password changed.");
    } catch (e2) {
      setErr(e2.message);
    } finally {
      setBusy(false);
    }
  };

  const resetMfa = async () => {
    if (!confirm("Reset your MFA? You'll set up your authenticator again at your next sign-in.")) return;
    setErr(null);
    setMsg(null);
    try {
      await api.post("/me/mfa/reset");
      setMsg("MFA reset. Sign out and back in to enroll a new authenticator.");
    } catch (e) {
      setErr(e.message);
    }
  };

  return (
    <section className="card">
      <h2>Account &amp; security</h2>
      {err && <p className="error">{err}</p>}
      {msg && <p className="muted">{msg}</p>}

      <h3>Change password</h3>
      <form onSubmit={changePw} className="stack">
        <input type="password" placeholder="Current password" autoComplete="current-password"
          value={pw.current_password}
          onChange={(e) => setPw({ ...pw, current_password: e.target.value })} required />
        <input type="password" placeholder="New password (min 8)" autoComplete="new-password"
          value={pw.new_password}
          onChange={(e) => setPw({ ...pw, new_password: e.target.value })} required />
        <input type="password" placeholder="Confirm new password" autoComplete="new-password"
          value={pw.confirm}
          onChange={(e) => setPw({ ...pw, confirm: e.target.value })} required />
        <button className="primary" type="submit" disabled={busy}>Change password</button>
      </form>

      <h3 style={{ marginTop: 16 }}>Multi-factor authentication</h3>
      <p className="muted">Reset your authenticator (TOTP) if you changed or lost your device.</p>
      <button onClick={resetMfa}>Reset my MFA</button>
    </section>
  );
}
