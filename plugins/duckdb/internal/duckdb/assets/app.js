(function () {
  "use strict";

  var sc = window.shellcn || {};
  var DUCK = null;
  var adb = null;
  var conn = null;
  var cursor = null;
  var pageSize = 100;
  var source = "";
  var refs = {};
  var lastGrid = null;

  function applyTheme(theme, colors) {
    document.body.dataset.theme = theme === "light" ? "light" : "dark";
    var accent = (colors && colors.primary500) || (sc.colors && sc.colors.primary500);
    if (accent) document.documentElement.style.setProperty("--accent", accent);
  }

  function el(tag, attrs, children) {
    var n = document.createElement(tag);
    if (attrs)
      for (var k in attrs) {
        if (k === "class") n.className = attrs[k];
        else if (k === "text") n.textContent = attrs[k];
        else n.setAttribute(k, attrs[k]);
      }
    (children || []).forEach(function (c) {
      if (c == null) return;
      n.appendChild(typeof c === "string" ? document.createTextNode(c) : c);
    });
    return n;
  }

  function button(label, onClick, kind, title) {
    var b = el("button", { class: "btn " + (kind || ""), type: "button", title: title || label }, [label]);
    b.addEventListener("click", onClick);
    return b;
  }

  function buildUI() {
    document.body.appendChild(styleTag());

    refs.file = el("input", { type: "file", accept: ".parquet,.csv,.tsv,.json,.ndjson,.arrow,.duckdb,.db", style: "display:none" });
    refs.file.addEventListener("change", function () {
      if (refs.file.files && refs.file.files[0]) openLocal(refs.file.files[0]);
      refs.file.value = "";
    });

    refs.open = button("Open file", function () { refs.file.click(); }, "primary");
    refs.new = button("New", newDatabase, "", "Create an empty in-memory database");
    refs.dbLabel = el("span", { class: "dbname", text: "No database" });

    var toolbar = el("div", { class: "toolbar" }, [
      refs.open, refs.new,
      el("span", { class: "spacer" }),
      refs.dbLabel,
    ]);

    refs.schema = el("div", { class: "schema" });
    var sidebar = el("aside", { class: "sidebar" }, [
      el("div", { class: "side-head", text: "Schema" }),
      refs.schema,
    ]);

    refs.editor = el("textarea", { class: "editor", spellcheck: "false", placeholder: "SELECT * FROM 'data.parquet' LIMIT 100;" });
    refs.editor.addEventListener("keydown", function (e) {
      if ((e.ctrlKey || e.metaKey) && e.key === "Enter") { e.preventDefault(); runQuery(); }
    });

    refs.pageSize = el("select", { class: "select", title: "Rows per page" });
    [50, 100, 200, 500, 1000].forEach(function (n) {
      var o = el("option", { value: String(n), text: n + " / page" });
      if (n === pageSize) o.selected = true;
      refs.pageSize.appendChild(o);
    });
    refs.pageSize.addEventListener("change", function () {
      pageSize = parseInt(refs.pageSize.value, 10);
      if (cursor) renderPage(0);
    });

    refs.run = button("Run", runQuery, "primary", "Ctrl/⌘ + Enter");
    var runbar = el("div", { class: "runbar" }, [
      refs.run,
      el("span", { class: "kbd", text: "⌘↵" }),
      el("span", { class: "spacer" }),
      refs.pageSize,
      button("Export CSV", exportCSV, "ghost", "Export the current page to CSV"),
    ]);

    refs.results = el("div", { class: "results" });
    var main = el("div", { class: "main" }, [
      el("section", { class: "editor-pane" }, [refs.editor, runbar]),
      el("section", { class: "result-pane" }, [refs.results]),
    ]);

    refs.status = el("div", { class: "status" }, [el("span", { class: "dot" }), el("span", { class: "status-text", text: "Starting…" })]);

    refs.app = el("div", { class: "app" }, [
      toolbar,
      el("div", { class: "body" }, [sidebar, main]),
      refs.status,
    ]);
    document.body.appendChild(refs.file);
    document.body.appendChild(refs.app);

    setupDrop();
    setBusy(true);
    renderLoading("Loading DuckDB engine…");
  }

  function setBusy(busy) {
    [refs.open, refs.new, refs.run].forEach(function (b) { if (b) b.disabled = busy; });
  }

  function renderLoading(text) {
    refs.results.innerHTML = "";
    refs.results.appendChild(el("div", { class: "loading" }, [
      el("div", { class: "spinner" }),
      el("div", { class: "loading-text", text: text || "Loading…" }),
    ]));
  }

  function setupDrop() {
    var overlay = el("div", { class: "drop-overlay", text: "Drop a Parquet / CSV / JSON file to open" });
    refs.app.appendChild(overlay);
    var depth = 0;
    refs.app.addEventListener("dragenter", function (e) { e.preventDefault(); depth++; refs.app.classList.add("dragging"); });
    refs.app.addEventListener("dragover", function (e) { e.preventDefault(); });
    refs.app.addEventListener("dragleave", function () { if (--depth <= 0) { depth = 0; refs.app.classList.remove("dragging"); } });
    refs.app.addEventListener("drop", function (e) {
      e.preventDefault(); depth = 0; refs.app.classList.remove("dragging");
      if (e.dataTransfer && e.dataTransfer.files && e.dataTransfer.files[0]) openLocal(e.dataTransfer.files[0]);
    });
  }

  async function initEngine() {
    var mjsUrl = await sc.assetURL("duckdb.mjs", "text/javascript");
    DUCK = await import(mjsUrl);
    var workerUrl = await sc.assetURL("duckdb-browser-eh.worker.js", "text/javascript");
    var wasmUrl = await sc.assetURL("duckdb-eh.wasm", "application/wasm");
    var worker = new Worker(workerUrl);
    adb = new DUCK.AsyncDuckDB(new DUCK.ConsoleLogger(), worker);
    await adb.instantiate(wasmUrl);
    conn = await adb.connect();
  }

  function baseName(name) {
    var b = String(name).split(/[\\/]/).pop().split("?")[0];
    return b || "data";
  }
  function viewName(name) {
    return baseName(name).replace(/\.[^.]+$/, "").replace(/[^A-Za-z0-9_]/g, "_") || "data";
  }
  function scanExpr(name) {
    var ext = (baseName(name).match(/\.([^.]+)$/) || [, ""])[1].toLowerCase();
    var q = "'" + String(name).replace(/'/g, "''") + "'";
    if (ext === "csv" || ext === "tsv" || ext === "txt") return "read_csv_auto(" + q + ")";
    if (ext === "json" || ext === "ndjson") return "read_json_auto(" + q + ")";
    if (ext === "parquet") return "parquet_scan(" + q + ")";
    return q;
  }

  async function openLocal(file) {
    if (!conn) return;
    try {
      var ext = (baseName(file.name).match(/\.([^.]+)$/) || [, ""])[1].toLowerCase();
      await adb.registerFileHandle(file.name, file, DUCK.DuckDBDataProtocol.BROWSER_FILEREADER, true);
      if (ext === "duckdb" || ext === "db") {
        await conn.query("ATTACH '" + file.name.replace(/'/g, "''") + "' AS attached (READ_ONLY)");
        refs.editor.value = "SHOW ALL TABLES;";
      } else {
        var v = viewName(file.name);
        await conn.query('CREATE OR REPLACE VIEW "' + v + '" AS SELECT * FROM ' + scanExpr(file.name));
        refs.editor.value = 'SELECT * FROM "' + v + '" LIMIT 100;';
      }
      source = file.name;
      refs.dbLabel.textContent = file.name + "  ·  local, in browser";
      await refreshSchema();
      await runQuery();
    } catch (e) {
      renderError(msg(e));
      setStatus("Could not open file", "error");
    }
  }

  async function newDatabase() {
    if (!conn) return;
    source = "in-memory";
    refs.dbLabel.textContent = "in-memory";
    refs.editor.value = "";
    await refreshSchema();
    welcome();
    setStatus("New in-memory database", "ok");
  }

  async function refreshSchema() {
    refs.schema.innerHTML = "";
    var rows = await listRows("SELECT table_catalog, table_schema, table_name, table_type FROM information_schema.tables WHERE table_schema NOT IN ('information_schema','pg_catalog') ORDER BY table_catalog, table_schema, table_name");
    if (!rows.length) { refs.schema.appendChild(el("div", { class: "side-empty", text: "No tables or views yet" })); return; }
    var tables = rows.filter(function (r) { return String(r[3]).indexOf("VIEW") === -1; });
    var views = rows.filter(function (r) { return String(r[3]).indexOf("VIEW") !== -1; });
    if (tables.length) refs.schema.appendChild(schemaGroup("Tables", tables));
    if (views.length) refs.schema.appendChild(schemaGroup("Views", views));
  }

  function qname(cat, sch, name) {
    var q = function (s) { return '"' + String(s).replace(/"/g, '""') + '"'; };
    return q(cat) + "." + q(sch) + "." + q(name);
  }

  function schemaGroup(title, rows) {
    var group = el("div", { class: "schema-group" }, [el("div", { class: "schema-group-head", text: title + " (" + rows.length + ")" })]);
    rows.forEach(function (row) {
      var cat = row[0], sch = row[1], name = row[2];
      var qualified = qname(cat, sch, name);
      var label = sch && sch !== "main" ? sch + "." + name : name;
      var cols = el("div", { class: "cols" });
      cols.style.display = "none";
      var expanded = false;
      var caret = el("span", { class: "caret", text: "▸" });
      var head = el("div", { class: "table-row" }, [caret, el("span", { class: "table-name", text: label, title: cat + "." + sch + "." + name })]);
      caret.addEventListener("click", async function (e) {
        e.stopPropagation();
        expanded = !expanded;
        caret.textContent = expanded ? "▾" : "▸";
        cols.style.display = expanded ? "block" : "none";
        if (expanded && !cols.dataset.loaded) {
          var info = await listRows("DESCRIBE " + qualified);
          info.forEach(function (c) {
            cols.appendChild(el("div", { class: "col" }, [
              el("span", { class: "col-name", text: c[0] }),
              el("span", { class: "col-type", text: c[1] || "" }),
            ]));
          });
          cols.dataset.loaded = "1";
        }
      });
      head.addEventListener("click", function () { refs.editor.value = "SELECT * FROM " + qualified + " LIMIT 100;"; runQuery(); });
      group.appendChild(el("div", {}, [head, cols]));
    });
    return group;
  }

  async function listRows(sql) {
    try {
      var res = await conn.query(sql);
      return res.toArray().map(function (r) { return Object.values(r.toJSON ? r.toJSON() : r); });
    } catch (e) { return []; }
  }

  function isReadQuery(text) {
    var t = text.replace(/^\s*(--[^\n]*\n|\/\*[\s\S]*?\*\/)\s*/g, "").trim().toLowerCase();
    if (!/^(select|with|from|pragma|describe|explain|show|values|table)\b/.test(t)) return false;
    return text.replace(/;+\s*$/, "").indexOf(";") === -1;
  }

  async function runQuery() {
    if (!conn) { setStatus("Engine not ready", "error"); return; }
    var text = refs.editor.value.trim();
    if (!text) return;
    await closeCursor();
    setBusy(true);
    setStatus("Running…");
    var t0 = performance.now();
    try {
      if (isReadQuery(text)) {
        cursor = { reader: await conn.send(text.replace(/;+\s*$/, "")), columns: null, rows: [], done: false };
        await renderPage(0);
        setStatus("Query ran in " + since(t0), "ok");
      } else {
        var res = await conn.query(text);
        renderArrow(res, t0);
        await refreshSchema();
      }
    } catch (e) {
      renderError(msg(e));
      setStatus("Error", "error");
    } finally {
      setBusy(false);
    }
  }

  async function fill(upto) {
    while (cursor.rows.length < upto && !cursor.done) {
      var nx = await cursor.reader.next();
      if (nx.done) { cursor.done = true; break; }
      var batch = nx.value;
      if (!cursor.columns) cursor.columns = batch.schema.fields.map(function (f) { return f.name; });
      var arr = batch.toArray();
      for (var i = 0; i < arr.length; i++) {
        var row = arr[i];
        cursor.rows.push(cursor.columns.map(function (c) { return normalize(row[c]); }));
      }
    }
    if (!cursor.columns && cursor.reader.schema) cursor.columns = cursor.reader.schema.fields.map(function (f) { return f.name; });
  }

  async function renderPage(pageIndex) {
    await fill((pageIndex + 1) * pageSize);
    var start = pageIndex * pageSize;
    var rows = cursor.rows.slice(start, start + pageSize);
    renderGrid(cursor.columns || [], rows, {
      start: start,
      total: cursor.done ? cursor.rows.length : null,
      hasPrev: pageIndex > 0,
      hasNext: cursor.rows.length > start + pageSize || !cursor.done,
      onPrev: function () { renderPage(pageIndex - 1); },
      onNext: function () { renderPage(pageIndex + 1); },
    });
  }

  function renderArrow(res, t0) {
    var columns = res.schema.fields.map(function (f) { return f.name; });
    var all = res.toArray().map(function (row) { return columns.map(function (c) { return normalize(row[c]); }); });
    var page = 0;
    function show(p) {
      page = p;
      var start = p * pageSize;
      renderGrid(columns, all.slice(start, start + pageSize), {
        start: start, total: all.length,
        hasPrev: p > 0, hasNext: start + pageSize < all.length,
        onPrev: function () { show(p - 1); }, onNext: function () { show(p + 1); },
      });
    }
    if (!all.length) { renderMessage("Statement executed."); }
    else show(0);
    setStatus(all.length + " row(s) · " + since(t0), "ok");
  }

  function normalize(v) {
    if (v === null || v === undefined) return null;
    if (typeof v === "bigint") return v.toString();
    if (v instanceof Uint8Array) return { __blob: v.length };
    if (typeof v === "object") { try { return JSON.stringify(v); } catch (e) { return String(v); } }
    return v;
  }

  function renderGrid(columns, rows, nav) {
    refs.results.innerHTML = "";
    var scroll = el("div", { class: "grid-scroll" });
    var table = el("table", { class: "grid" });
    var htr = el("tr", {}, [el("th", { class: "rownum", text: "#" })]);
    columns.forEach(function (c) { htr.appendChild(el("th", { text: String(c) })); });
    table.appendChild(el("thead", {}, [htr]));
    var tbody = el("tbody", {});
    rows.forEach(function (row, r) {
      var tr = el("tr", {}, [el("td", { class: "rownum", text: String(nav.start + r + 1) })]);
      row.forEach(function (cell) { tr.appendChild(cellNode(cell)); });
      tbody.appendChild(tr);
    });
    table.appendChild(tbody);
    scroll.appendChild(table);
    if (!rows.length) scroll.appendChild(el("div", { class: "empty", text: "No rows" }));
    refs.results.appendChild(scroll);
    refs.results.appendChild(pager(rows.length, nav));
    lastGrid = { columns: columns, rows: rows };
  }

  function pager(count, nav) {
    var from = count ? nav.start + 1 : 0;
    var to = nav.start + count;
    var label = nav.total != null ? from + "–" + to + " of " + nav.total : from + "–" + to + (nav.hasNext ? "+" : "");
    var prev = button("‹ Prev", nav.onPrev, "ghost");
    var next = button("Next ›", nav.onNext, "ghost");
    prev.disabled = !nav.hasPrev;
    next.disabled = !nav.hasNext;
    return el("div", { class: "pager" }, [el("span", { class: "range", text: "Rows " + label }), el("span", { class: "spacer" }), prev, next]);
  }

  function cellNode(cell) {
    if (cell === null) return el("td", { class: "null", text: "NULL" });
    if (cell && cell.__blob !== undefined) return el("td", { class: "blob", text: "‹blob " + cell.__blob + " B›" });
    return el("td", { text: String(cell) });
  }

  function renderMessage(text) { refs.results.innerHTML = ""; refs.results.appendChild(el("div", { class: "notice", text: text })); lastGrid = null; }
  function renderError(text) {
    refs.results.innerHTML = "";
    refs.results.appendChild(el("div", { class: "error-box" }, [
      el("div", { class: "error-title", text: "Query error" }),
      el("pre", { class: "error-msg", text: text }),
    ]));
    lastGrid = null;
  }

  function welcome() {
    refs.results.innerHTML = "";
    refs.results.appendChild(el("div", { class: "welcome" }, [
      el("div", { class: "welcome-title", text: "DuckDB Explorer" }),
      el("div", { class: "welcome-sub", text: "Open a local Parquet / CSV / JSON file (or drag it in) and run SQL. Everything runs in your browser — files are never uploaded." }),
    ]));
  }

  function exportCSV() {
    if (!lastGrid || !lastGrid.rows.length) { setStatus("Nothing to export", "error"); return; }
    var esc = function (v) {
      if (v === null) return "";
      var s = v && v.__blob !== undefined ? "" : String(v);
      return /[",\n]/.test(s) ? '"' + s.replace(/"/g, '""') + '"' : s;
    };
    var lines = [lastGrid.columns.map(esc).join(",")];
    lastGrid.rows.forEach(function (row) { lines.push(row.map(esc).join(",")); });
    var url = URL.createObjectURL(new Blob([lines.join("\n")], { type: "text/csv" }));
    var a = el("a", { href: url, download: "export.csv" });
    document.body.appendChild(a); a.click();
    setTimeout(function () { URL.revokeObjectURL(url); a.remove(); }, 0);
  }

  async function closeCursor() {
    if (cursor && cursor.reader) { try { if (cursor.reader.cancel) await cursor.reader.cancel(); else if (cursor.reader.return) await cursor.reader.return(); } catch (e) {} }
    cursor = null;
  }

  function setStatus(text, kind) {
    refs.status.className = "status" + (kind ? " " + kind : "");
    refs.status.querySelector(".status-text").textContent = text;
  }
  function since(t0) { return (performance.now() - t0).toFixed(1) + " ms"; }
  function msg(e) { return (e && (e.message || e.toString())) || "unknown error"; }

  async function boot() {
    buildUI();
    if (typeof sc.hideStatus === "function") sc.hideStatus();
    applyTheme(sc.theme, sc.colors);
    if (typeof sc.onTheme === "function") sc.onTheme(applyTheme);
    try {
      await initEngine();
      setBusy(false);
      welcome();
      setStatus("Ready — open a file or a remote URL", "ok");
    } catch (e) {
      renderError("DuckDB engine failed to load: " + msg(e));
      setStatus("Engine failed to load", "error");
    }
  }

  function styleTag() {
    var css = [
      ":root{--accent:#fcd34d}",
      'body[data-theme="dark"]{--bg:#0b1220;--panel:#0f1a2e;--panel2:#16233b;--head:#152238;--border:#243449;--text:#e6edf6;--muted:#8598b3;--danger:#f87171;--ok:#34d399}',
      'body[data-theme="light"]{--bg:#f6f8fb;--panel:#ffffff;--panel2:#eef2f7;--head:#f1f5f9;--border:#dbe2ec;--text:#0f172a;--muted:#64748b;--danger:#dc2626;--ok:#059669}',
      "*{box-sizing:border-box}",
      "html,body{margin:0;height:100%;background:var(--bg);color:var(--text);font:13px/1.5 ui-sans-serif,system-ui,-apple-system,Segoe UI,Roboto,sans-serif}",
      ".app{position:relative;display:flex;flex-direction:column;height:100vh}",
      ".toolbar{display:flex;gap:8px;align-items:center;padding:9px 12px;border-bottom:1px solid var(--border);background:var(--panel)}",
      ".dbname{color:var(--muted);font-size:12px;max-width:52ch;overflow:hidden;text-overflow:ellipsis;white-space:nowrap}",
      ".spacer{flex:1}",
      ".btn{background:var(--panel2);color:var(--text);border:1px solid var(--border);border-radius:7px;padding:6px 13px;cursor:pointer;font:inherit;transition:border-color .12s,background .12s}",
      ".btn:hover:not(:disabled){border-color:var(--accent)}",
      ".btn:disabled{opacity:.4;cursor:default}",
      ".btn.primary{background:var(--accent);color:#20160a;border-color:transparent;font-weight:600}",
      ".btn.ghost{background:transparent}",
      ".select{background:var(--panel2);color:var(--text);border:1px solid var(--border);border-radius:7px;padding:6px 8px;font:inherit;cursor:pointer}",
      ".body{flex:1;display:flex;min-height:0}",
      ".sidebar{width:236px;flex-shrink:0;border-right:1px solid var(--border);background:var(--panel);display:flex;flex-direction:column;min-height:0}",
      ".side-head{padding:10px 12px;font-size:11px;letter-spacing:.06em;text-transform:uppercase;color:var(--muted);border-bottom:1px solid var(--border)}",
      ".schema{overflow:auto;padding:6px 4px;flex:1}",
      ".schema-group{margin-bottom:8px}",
      ".schema-group-head{padding:4px 10px;font-size:11px;color:var(--muted);text-transform:uppercase;letter-spacing:.04em}",
      ".table-row{display:flex;align-items:center;gap:4px;padding:4px 8px;border-radius:6px;cursor:pointer}",
      ".table-row:hover{background:var(--panel2)}",
      ".caret{width:14px;color:var(--muted);font-size:10px;text-align:center;cursor:pointer}",
      ".table-name{overflow:hidden;text-overflow:ellipsis;white-space:nowrap}",
      ".cols{padding:2px 0 4px 22px}",
      ".col{display:flex;justify-content:space-between;gap:8px;padding:2px 10px 2px 0;color:var(--muted);font-size:12px}",
      ".col-name{color:var(--text);overflow:hidden;text-overflow:ellipsis;white-space:nowrap}",
      ".col-type{font-size:11px;opacity:.8;flex-shrink:0}",
      ".side-empty{padding:12px;color:var(--muted);font-size:12px}",
      ".main{flex:1;display:flex;flex-direction:column;min-width:0;min-height:0}",
      ".editor-pane{display:flex;flex-direction:column;flex-shrink:0}",
      ".editor{height:150px;resize:vertical;background:var(--panel);color:var(--text);border:0;padding:12px 14px;font:13px/1.6 ui-monospace,SFMono-Regular,Menlo,Consolas,monospace;outline:none}",
      ".runbar{display:flex;gap:10px;align-items:center;padding:8px 12px;border-top:1px solid var(--border);border-bottom:1px solid var(--border);background:var(--panel)}",
      ".kbd{color:var(--muted);font-size:11px;border:1px solid var(--border);border-radius:5px;padding:1px 6px}",
      ".result-pane{flex:1;display:flex;flex-direction:column;min-height:0}",
      ".results{flex:1;display:flex;flex-direction:column;min-height:0;overflow:hidden}",
      ".grid-scroll{flex:1;overflow:auto}",
      ".grid{border-collapse:collapse;width:100%;font-size:12.5px}",
      ".grid th,.grid td{border-bottom:1px solid var(--border);border-right:1px solid var(--border);padding:5px 10px;text-align:left;white-space:nowrap;max-width:460px;overflow:hidden;text-overflow:ellipsis}",
      ".grid thead th{position:sticky;top:0;background:var(--head);z-index:1;font-weight:600}",
      ".grid .rownum{color:var(--muted);text-align:right;background:var(--panel);position:sticky;left:0}",
      ".grid tbody tr:hover td{background:var(--panel2)}",
      ".grid td.null{color:var(--muted);font-style:italic}",
      ".grid td.blob{color:var(--accent)}",
      ".pager{display:flex;align-items:center;gap:8px;padding:7px 12px;border-top:1px solid var(--border);background:var(--panel)}",
      ".range{color:var(--muted);font-size:12px}",
      ".empty,.notice,.welcome{padding:26px;text-align:center;color:var(--muted)}",
      ".welcome-title{font-size:15px;color:var(--text);margin-bottom:6px;font-weight:600}",
      ".welcome-sub{max-width:56ch;margin:0 auto}",
      ".loading{display:flex;flex-direction:column;align-items:center;gap:12px;padding:44px;color:var(--muted)}",
      ".spinner{width:26px;height:26px;border:3px solid var(--border);border-top-color:var(--accent);border-radius:50%;animation:spin .8s linear infinite}",
      "@keyframes spin{to{transform:rotate(360deg)}}",
      ".error-box{margin:14px;border:1px solid var(--danger);border-radius:8px;overflow:hidden}",
      ".error-title{padding:7px 12px;background:var(--panel2);color:var(--danger);font-weight:600;font-size:12px}",
      ".error-msg{margin:0;padding:12px;white-space:pre-wrap;font:12px/1.5 ui-monospace,monospace;color:var(--text)}",
      ".status{display:flex;align-items:center;gap:8px;padding:6px 12px;border-top:1px solid var(--border);background:var(--panel);color:var(--muted);font-size:12px}",
      ".status .dot{width:8px;height:8px;border-radius:50%;background:var(--muted);flex-shrink:0}",
      ".status.ok .dot{background:var(--ok)}",
      ".status.error{color:var(--danger)}",
      ".status.error .dot{background:var(--danger)}",
      ".status-text{overflow:hidden;text-overflow:ellipsis;white-space:nowrap}",
      ".drop-overlay{position:absolute;inset:0;display:none;align-items:center;justify-content:center;background:rgba(2,6,23,.72);color:#fff;font-size:16px;border:2px dashed var(--accent);z-index:10;pointer-events:none}",
      ".app.dragging .drop-overlay{display:flex}",
    ].join("\n");
    return el("style", { text: css });
  }

  if (document.readyState === "loading") document.addEventListener("DOMContentLoaded", boot);
  else boot();
})();
