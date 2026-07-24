(function () {
  "use strict";

  var sc = window.shellcn || {};
  var SQL = null;
  var db = null;
  var dbName = "";
  var cursor = null;
  var pageSize = 100;

  var refs = {};

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
        else if (k === "html") n.innerHTML = attrs[k];
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

    refs.file = el("input", {
      type: "file",
      accept: ".db,.sqlite,.sqlite3,.db3,.s3db,.sl3",
      style: "display:none",
    });
    refs.file.addEventListener("change", function () {
      if (refs.file.files && refs.file.files[0]) openFile(refs.file.files[0]);
      refs.file.value = "";
    });

    refs.dbLabel = el("span", { class: "dbname", text: "No database" });
    refs.download = button("Download", downloadDatabase, "ghost", "Download the current database file");
    refs.download.disabled = true;

    refs.open = button("Open database", function () { refs.file.click(); }, "primary");
    refs.new = button("New", newDatabase, "", "Create an empty in-memory database");
    var toolbar = el("div", { class: "toolbar" }, [
      refs.open,
      refs.new,
      refs.download,
      el("span", { class: "spacer" }),
      refs.dbLabel,
    ]);

    refs.schema = el("div", { class: "schema" });
    var sidebar = el("aside", { class: "sidebar" }, [
      el("div", { class: "side-head" }, ["Schema"]),
      refs.schema,
    ]);

    refs.editor = el("textarea", {
      class: "editor",
      spellcheck: "false",
      placeholder: "SELECT name FROM sqlite_master;",
    });
    refs.editor.addEventListener("keydown", function (e) {
      if ((e.ctrlKey || e.metaKey) && e.key === "Enter") {
        e.preventDefault();
        runQuery();
      }
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

    var runbar = el("div", { class: "runbar" }, [
      button("Run", runQuery, "primary", "Ctrl/⌘ + Enter"),
      el("span", { class: "kbd", text: "⌘↵" }),
      el("span", { class: "spacer" }),
      refs.pageSize,
      button("Export CSV", exportCSV, "ghost", "Export the current page to CSV"),
    ]);

    refs.results = el("div", { class: "results" });
    var editorPane = el("section", { class: "editor-pane" }, [refs.editor, runbar]);
    var resultPane = el("section", { class: "result-pane" }, [refs.results]);
    var main = el("div", { class: "main" }, [editorPane, resultPane]);

    refs.status = el("div", { class: "status" }, [el("span", { class: "dot" }), el("span", { class: "status-text", text: "Starting…" })]);

    refs.app = el("div", { class: "app" }, [
      toolbar,
      el("div", { class: "body" }, [sidebar, main]),
      refs.status,
    ]);
    document.body.appendChild(refs.file);
    document.body.appendChild(refs.app);

    setupDrop();
    refs.open.disabled = true;
    refs.new.disabled = true;
    renderLoading("Loading SQLite engine…");
  }

  function renderLoading(text) {
    refs.results.innerHTML = "";
    refs.results.appendChild(el("div", { class: "loading" }, [
      el("div", { class: "spinner" }),
      el("div", { class: "loading-text", text: text || "Loading…" }),
    ]));
  }

  function setupDrop() {
    var overlay = el("div", { class: "drop-overlay", text: "Drop a SQLite file to open" });
    refs.app.appendChild(overlay);
    var depth = 0;
    refs.app.addEventListener("dragenter", function (e) { e.preventDefault(); depth++; refs.app.classList.add("dragging"); });
    refs.app.addEventListener("dragover", function (e) { e.preventDefault(); });
    refs.app.addEventListener("dragleave", function () { if (--depth <= 0) { depth = 0; refs.app.classList.remove("dragging"); } });
    refs.app.addEventListener("drop", function (e) {
      e.preventDefault();
      depth = 0;
      refs.app.classList.remove("dragging");
      if (e.dataTransfer && e.dataTransfer.files && e.dataTransfer.files[0]) openFile(e.dataTransfer.files[0]);
    });
  }

  async function initEngine() {
    if (typeof initSqlJs !== "function") throw new Error("sql.js engine not loaded");
    var wasmBinary = await sc.asset("sql-wasm.wasm");
    SQL = await initSqlJs({ wasmBinary: wasmBinary });
  }

  async function openFile(file) {
    if (!SQL) return;
    try {
      var buf = new Uint8Array(await file.arrayBuffer());
      closeDB();
      db = new SQL.Database(buf);
      dbName = file.name;
      afterOpen();
      setStatus("Opened " + file.name + " · " + fmtBytes(file.size) + " · nothing uploaded", "ok");
    } catch (e) {
      setStatus("Could not open file: " + msg(e), "error");
    }
  }

  function newDatabase() {
    if (!SQL) return;
    closeDB();
    db = new SQL.Database();
    dbName = "untitled.db";
    afterOpen();
    setStatus("New empty database", "ok");
  }

  function afterOpen() {
    refs.download.disabled = false;
    refs.dbLabel.textContent = dbName;
    refs.editor.value = "";
    refreshSchema();
    welcome();
  }

  function closeDB() {
    closeCursor();
    if (db) { try { db.close(); } catch (e) {} db = null; }
  }

  function closeCursor() {
    if (cursor && cursor.stmt) { try { cursor.stmt.free(); } catch (e) {} }
    cursor = null;
  }

  function refreshSchema() {
    refs.schema.innerHTML = "";
    if (!db) return;
    var tables = listOf("SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%' ORDER BY name");
    var views = listOf("SELECT name FROM sqlite_master WHERE type='view' ORDER BY name");
    refs.schema.appendChild(schemaGroup("Tables", tables));
    if (views.length) refs.schema.appendChild(schemaGroup("Views", views));
    if (!tables.length && !views.length)
      refs.schema.appendChild(el("div", { class: "side-empty", text: "No tables yet" }));
  }

  function schemaGroup(title, names) {
    var group = el("div", { class: "schema-group" }, [
      el("div", { class: "schema-group-head", text: title + " (" + names.length + ")" }),
    ]);
    names.forEach(function (name) {
      var cols = el("div", { class: "cols" });
      var expanded = false;
      var caret = el("span", { class: "caret", text: "▸" });
      var head = el("div", { class: "table-row" }, [
        caret,
        el("span", { class: "table-name", text: name, title: name }),
      ]);
      caret.addEventListener("click", function (e) {
        e.stopPropagation();
        expanded = !expanded;
        caret.textContent = expanded ? "▾" : "▸";
        cols.style.display = expanded ? "block" : "none";
        if (expanded && !cols.dataset.loaded) {
          columnsOf(name).forEach(function (c) {
            cols.appendChild(el("div", { class: "col" }, [
              el("span", { class: "col-name", text: c.name }),
              el("span", { class: "col-type", text: c.type || "" }),
            ]));
          });
          cols.dataset.loaded = "1";
        }
      });
      head.addEventListener("click", function () {
        refs.editor.value = 'SELECT * FROM "' + name + '";';
        runQuery();
      });
      cols.style.display = "none";
      group.appendChild(el("div", {}, [head, cols]));
    });
    return group;
  }

  function listOf(sql) {
    var out = [];
    try {
      var r = db.exec(sql);
      if (r.length) out = r[0].values.map(function (v) { return v[0]; });
    } catch (e) {}
    return out;
  }

  function columnsOf(name) {
    var out = [];
    try {
      var r = db.exec('PRAGMA table_info("' + name.replace(/"/g, '""') + '")');
      if (r.length) r[0].values.forEach(function (v) { out.push({ name: v[1], type: v[2] }); });
    } catch (e) {}
    return out;
  }

  function isReadQuery(text) {
    var t = text.replace(/^\s*(--[^\n]*\n|\/\*[\s\S]*?\*\/)\s*/g, "").trim().toLowerCase();
    if (!/^(select|with|pragma|explain|values)\b/.test(t)) return false;
    return text.replace(/;+\s*$/, "").indexOf(";") === -1;
  }

  function runQuery() {
    if (!db) { setStatus("Open or create a database first", "error"); return; }
    var text = refs.editor.value.trim();
    if (!text) return;
    closeCursor();
    var t0 = performance.now();
    try {
      if (isReadQuery(text)) {
        var stmt = db.prepare(text.replace(/;+\s*$/, ""));
        cursor = { stmt: stmt, columns: stmt.getColumnNames() };
        renderPage(0);
        setStatus("Query ran in " + elapsed(t0), "ok");
      } else {
        var res = db.exec(text);
        var last = res.length ? res[res.length - 1] : null;
        if (last) renderStatic(last, t0);
        else {
          renderMessage(db.getRowsModified() + " row(s) affected");
          setStatus("Executed in " + elapsed(t0) + " · " + db.getRowsModified() + " row(s) affected", "ok");
        }
        refreshSchema();
      }
    } catch (e) {
      renderError(msg(e));
      setStatus("Error", "error");
    }
  }

  function renderPage(pageIndex) {
    var stmt = cursor.stmt;
    stmt.reset();
    var skip = pageIndex * pageSize;
    for (var i = 0; i < skip; i++) if (!stmt.step()) break;
    var rows = [];
    while (rows.length < pageSize && stmt.step()) rows.push(stmt.get());
    var hasMore = stmt.step();
    cursor.page = pageIndex;
    renderGrid(cursor.columns, rows, {
      start: skip,
      hasPrev: pageIndex > 0,
      hasNext: hasMore,
      onPrev: function () { renderPage(pageIndex - 1); },
      onNext: function () { renderPage(pageIndex + 1); },
    });
  }

  function renderStatic(result, t0) {
    var page = 0;
    function show(p) {
      page = p;
      var start = p * pageSize;
      var rows = result.values.slice(start, start + pageSize);
      renderGrid(result.columns, rows, {
        start: start,
        total: result.values.length,
        hasPrev: p > 0,
        hasNext: start + pageSize < result.values.length,
        onPrev: function () { show(p - 1); },
        onNext: function () { show(p + 1); },
      });
    }
    show(0);
    setStatus(result.values.length + " row(s) · " + elapsed(t0), "ok");
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
    var label = nav.total != null ? from + "–" + to + " of " + nav.total : from + "–" + to;
    var prev = button("‹ Prev", nav.onPrev, "ghost");
    var next = button("Next ›", nav.onNext, "ghost");
    prev.disabled = !nav.hasPrev;
    next.disabled = !nav.hasNext;
    return el("div", { class: "pager" }, [
      el("span", { class: "range", text: "Rows " + label }),
      el("span", { class: "spacer" }),
      prev,
      next,
    ]);
  }

  function cellNode(cell) {
    if (cell === null) return el("td", { class: "null", text: "NULL" });
    if (cell instanceof Uint8Array) return el("td", { class: "blob", text: "‹blob " + cell.length + " B›" });
    return el("td", { text: String(cell) });
  }

  function renderMessage(text) {
    refs.results.innerHTML = "";
    refs.results.appendChild(el("div", { class: "notice", text: text }));
    lastGrid = null;
  }

  function renderError(text) {
    refs.results.innerHTML = "";
    refs.results.appendChild(el("div", { class: "error-box" }, [
      el("div", { class: "error-title", text: "Query error" }),
      el("pre", { class: "error-msg", text: text }),
    ]));
    lastGrid = null;
  }

  var lastGrid = null;

  function welcome() {
    refs.results.innerHTML = "";
    refs.results.appendChild(el("div", { class: "welcome" }, [
      el("div", { class: "welcome-title", text: db ? "Ready" : "SQLite Explorer" }),
      el("div", { class: "welcome-sub", text: db ? "Write SQL above and press Run, or pick a table on the left." : "Open a .sqlite/.db file or create a new database. Everything runs in your browser — the file is never uploaded." }),
    ]));
  }

  function exportCSV() {
    if (!lastGrid || !lastGrid.rows.length) { setStatus("Nothing to export", "error"); return; }
    var esc = function (v) {
      if (v === null) return "";
      var s = v instanceof Uint8Array ? "" : String(v);
      return /[",\n]/.test(s) ? '"' + s.replace(/"/g, '""') + '"' : s;
    };
    var lines = [lastGrid.columns.map(esc).join(",")];
    lastGrid.rows.forEach(function (row) { lines.push(row.map(esc).join(",")); });
    saveBlob(new Blob([lines.join("\n")], { type: "text/csv" }), "export.csv");
  }

  function downloadDatabase() {
    if (!db) return;
    try {
      saveBlob(new Blob([db.export()], { type: "application/octet-stream" }), dbName || "database.db");
    } catch (e) {
      setStatus("Export failed: " + msg(e), "error");
    }
  }

  function saveBlob(blob, filename) {
    var url = URL.createObjectURL(blob);
    var a = el("a", { href: url, download: filename });
    document.body.appendChild(a);
    a.click();
    setTimeout(function () { URL.revokeObjectURL(url); a.remove(); }, 0);
  }

  function setStatus(text, kind) {
    refs.status.className = "status" + (kind ? " " + kind : "");
    refs.status.querySelector(".status-text").textContent = text;
  }
  function elapsed(t0) { return (performance.now() - t0).toFixed(1) + " ms"; }
  function msg(e) { return (e && (e.message || e.toString())) || "unknown error"; }
  function fmtBytes(n) {
    if (n < 1024) return n + " B";
    if (n < 1048576) return (n / 1024).toFixed(1) + " KB";
    return (n / 1048576).toFixed(1) + " MB";
  }

  async function boot() {
    buildUI();
    if (typeof sc.hideStatus === "function") sc.hideStatus();
    applyTheme(sc.theme, sc.colors);
    if (typeof sc.onTheme === "function") sc.onTheme(applyTheme);
    try {
      await initEngine();
      refs.open.disabled = false;
      refs.new.disabled = false;
      welcome();
      setStatus("Ready — open a database to begin", "ok");
    } catch (e) {
      renderError("Engine failed to load: " + msg(e));
      setStatus("Engine failed to load", "error");
    }
  }

  function styleTag() {
    var css = [
      ":root{--accent:#0ea5e9}",
      'body[data-theme="dark"]{--bg:#0b1220;--panel:#0f1a2e;--panel2:#16233b;--head:#152238;--border:#243449;--text:#e6edf6;--muted:#8598b3;--danger:#f87171;--ok:#34d399}',
      'body[data-theme="light"]{--bg:#f6f8fb;--panel:#ffffff;--panel2:#eef2f7;--head:#f1f5f9;--border:#dbe2ec;--text:#0f172a;--muted:#64748b;--danger:#dc2626;--ok:#059669}',
      "*{box-sizing:border-box}",
      "html,body{margin:0;height:100%;background:var(--bg);color:var(--text);font:13px/1.5 ui-sans-serif,system-ui,-apple-system,Segoe UI,Roboto,sans-serif}",
      ".app{position:relative;display:flex;flex-direction:column;height:100vh}",
      ".toolbar{display:flex;gap:8px;align-items:center;padding:9px 12px;border-bottom:1px solid var(--border);background:var(--panel)}",
      ".dbname{color:var(--muted);font-size:12px;max-width:40ch;overflow:hidden;text-overflow:ellipsis;white-space:nowrap}",
      ".spacer{flex:1}",
      ".btn{background:var(--panel2);color:var(--text);border:1px solid var(--border);border-radius:7px;padding:6px 13px;cursor:pointer;font:inherit;transition:border-color .12s,background .12s}",
      ".btn:hover:not(:disabled){border-color:var(--accent)}",
      ".btn:disabled{opacity:.4;cursor:default}",
      ".btn.primary{background:var(--accent);color:#04121f;border-color:transparent;font-weight:600}",
      ".btn.primary:hover:not(:disabled){filter:brightness(1.06)}",
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
      ".loading{display:flex;flex-direction:column;align-items:center;gap:12px;padding:44px;color:var(--muted)}",
      ".spinner{width:26px;height:26px;border:3px solid var(--border);border-top-color:var(--accent);border-radius:50%;animation:spin .8s linear infinite}",
      "@keyframes spin{to{transform:rotate(360deg)}}",
      ".welcome-title{font-size:15px;color:var(--text);margin-bottom:6px;font-weight:600}",
      ".welcome-sub{max-width:52ch;margin:0 auto}",
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
