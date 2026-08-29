function headers(): HeadersInit {
  const h: Record<string, string> = {};
  const t = localStorage.getItem("dex_api_token");
  if (t) h.Authorization = "Bearer " + t;
  return h;
}

function briefHttpError(status: number, body: string): string {
  let text = String(body || "")
    .replace(/<[^>]+>/g, " ")
    .replace(/\s+/g, " ")
    .trim();
  if (text.length > 120) text = text.slice(0, 120);
  const hint =
    status === 404
      ? " 请用程序地址打开本页（默认 http://127.0.0.1:8090），不要直接打开 html 文件"
      : "";
  return status + (text ? " " + text : "") + hint;
}

export async function apiFetch<T = unknown>(path: string, opts: RequestInit = {}): Promise<T> {
  const r = await fetch(path, {
    ...opts,
    headers: { ...headers(), ...(opts.headers || {}) },
    signal: opts.signal,
  });
  const ct = (r.headers.get("content-type") || "").toLowerCase();
  if (!r.ok) throw new Error(briefHttpError(r.status, await r.text()));
  if (!ct.includes("json")) {
    throw new Error("后端未返回 JSON。请用程序地址打开本页（默认 http://127.0.0.1:8090）");
  }
  return r.json() as Promise<T>;
}
