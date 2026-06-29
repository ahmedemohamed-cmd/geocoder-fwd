import React from "react";

// Simple top-tab navigation. `tabs` is [{ id, label }]; `active` is the current
// id; `onChange(id)` switches. State lives in the parent (no routing needed).
export default function Tabs({ tabs, active, onChange }) {
  return (
    <nav className="tabs">
      {tabs.map((t) => (
        <button
          key={t.id}
          className={`tab ${active === t.id ? "active" : ""}`}
          onClick={() => onChange(t.id)}
        >
          {t.label}
        </button>
      ))}
    </nav>
  );
}
