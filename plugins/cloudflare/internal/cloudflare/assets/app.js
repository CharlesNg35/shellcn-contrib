(async () => {
  const root = document.createElement("main");
  root.className = "cf-shell";
  document.body.appendChild(root);

  const css = document.createElement("style");
  css.textContent = `
    :root { color-scheme: light dark; font-family: Inter, ui-sans-serif, system-ui, sans-serif; }
    body { margin: 0; background: #0f172a; color: #e5e7eb; }
    .cf-shell { min-height: 100vh; padding: 20px; box-sizing: border-box; }
    .hero { display: flex; justify-content: space-between; gap: 16px; align-items: flex-start; margin-bottom: 18px; }
    h1 { margin: 0; font-size: 24px; line-height: 1.2; }
    .muted { color: #94a3b8; font-size: 13px; }
    .grid { display: grid; grid-template-columns: repeat(auto-fit, minmax(180px, 1fr)); gap: 12px; margin-bottom: 18px; }
    .card { background: #111827; border: 1px solid #243244; border-radius: 8px; padding: 14px; }
    .metric { font-size: 28px; font-weight: 700; }
    .section { margin-top: 16px; }
    table { width: 100%; border-collapse: collapse; font-size: 13px; }
    th, td { text-align: left; padding: 10px; border-bottom: 1px solid #243244; vertical-align: top; }
    th { color: #cbd5e1; font-weight: 600; }
    button { border: 1px solid #334155; background: #1f2937; color: #f8fafc; border-radius: 6px; padding: 8px 10px; cursor: pointer; }
    button:hover { background: #273449; }
    .toolbar { display: flex; gap: 8px; flex-wrap: wrap; }
    .ok { color: #86efac; }
    .warn { color: #fbbf24; }
    .err { color: #fca5a5; }
    input { background: #0b1220; border: 1px solid #334155; color: #e5e7eb; border-radius: 6px; padding: 8px; min-width: 280px; }
  `;
  document.head.appendChild(css);

  function route(routeId, params = {}, body) {
    const init = body === undefined ? { method: "GET" } : { method: "POST", body };
    return window.shellcn.route(routeId, params, init);
  }

  function esc(value) {
    return String(value ?? "").replace(/[&<>"']/g, (ch) => ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;" })[ch]);
  }

  function renderLoading() {
    root.innerHTML = `<div class="card">Loading Cloudflare cockpit...</div>`;
  }

  function renderError(err) {
    root.innerHTML = `<div class="card err">${esc(err instanceof Error ? err.message : err)}</div>`;
  }

  function rows(items, cols) {
    if (!items?.length) return `<p class="muted">No rows.</p>`;
    return `<table><thead><tr>${cols.map((c) => `<th>${esc(c.label)}</th>`).join("")}</tr></thead><tbody>${items
      .map((item) => `<tr>${cols.map((c) => `<td>${esc(item[c.key])}</td>`).join("")}</tr>`)
      .join("")}</tbody></table>`;
  }

  async function load() {
    renderLoading();
    try {
      const summary = await route("cloudflare.summary");
      root.innerHTML = `
        <div class="hero">
          <div>
            <h1>Cloudflare Cockpit</h1>
            <div class="muted">Generated ${esc(summary.generated_at)}</div>
          </div>
          <div class="toolbar">
            <button id="refresh">Refresh</button>
          </div>
        </div>
        <div class="grid">
          <div class="card"><div class="muted">Zones</div><div class="metric">${esc(summary.zones || 0)}</div></div>
          <div class="card"><div class="muted">Paused zones</div><div class="metric warn">${esc(summary.paused_zones || 0)}</div></div>
          <div class="card"><div class="muted">Accounts</div><div class="metric">${esc(summary.accounts || 0)}</div></div>
        </div>
        <div class="card section">
          <h2>Zones</h2>
          ${rows(summary.zone_rows || [], [
            { key: "name", label: "Zone" },
            { key: "status", label: "Status" },
            { key: "paused", label: "Paused" },
            { key: "plan", label: "Plan" },
          ])}
        </div>
        <div class="card section">
          <h2>Accounts</h2>
          ${rows(summary.account_rows || [], [
            { key: "name", label: "Account" },
            { key: "id", label: "ID" },
            { key: "type", label: "Type" },
          ])}
        </div>
      `;
      document.getElementById("refresh")?.addEventListener("click", load);
      window.shellcn.hideStatus?.();
    } catch (err) {
      renderError(err);
      window.shellcn.reportError?.(err);
    }
  }

  window.shellcn.onTheme?.((theme) => {
    document.body.dataset.theme = theme;
  });
  await load();
})();
