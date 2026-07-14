// Minimal fetch wrapper that attaches the Zitadel access token.
export function makeApi(token, base) {
  const headers = () => ({
    Authorization: `Bearer ${token}`,
    "Content-Type": "application/json",
  });
  async function req(method, path, body) {
    const res = await fetch(base + path, {
      method,
      headers: headers(),
      body: body === undefined ? undefined : JSON.stringify(body),
    });
    if (res.status === 204) return null;
    const text = await res.text();
    const data = text ? JSON.parse(text) : null;
    if (!res.ok) throw new Error(data?.detail || data?.error || res.statusText);
    return data;
  }
  return {
    get: (p) => req("GET", p),
    post: (p, b) => req("POST", p, b),
    put: (p, b) => req("PUT", p, b),
    patch: (p, b) => req("PATCH", p, b),
    del: (p) => req("DELETE", p),
  };
}
