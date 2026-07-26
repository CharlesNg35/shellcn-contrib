(function () {
  "use strict";

  var sc = window.shellcn || {};
  var DUCK = null, adb = null, conn = null;
  var pageSize = 50, mode = null, cursor = null, table = null, selection = {};
  var refs = {}, lastGrid = null, searchTimer = null;

  function applyTheme(theme, colors) {
    document.body.dataset.theme = theme === "light" ? "light" : "dark";
    var accent = (colors && colors.primary500) || (sc.colors && sc.colors.primary500);
    if (accent) document.documentElement.style.setProperty("--accent", accent);
  }
  function el(tag, attrs, children) {
    var n = document.createElement(tag);
    if (attrs) for (var k in attrs) { if (k === "class") n.className = attrs[k]; else if (k === "text") n.textContent = attrs[k]; else n.setAttribute(k, attrs[k]); }
    (children || []).forEach(function (c) { if (c == null) return; n.appendChild(typeof c === "string" ? document.createTextNode(c) : c); });
    return n;
  }
  function button(label, onClick, kind, title) {
    var b = el("button", { class: "btn " + (kind || ""), type: "button", title: title || label }, [label]);
    b.addEventListener("click", onClick);
    return b;
  }
  function q1(s) { return '"' + String(s).replace(/"/g, '""') + '"'; }
  function qname(cat, sch, name) { return q1(cat) + "." + q1(sch) + "." + q1(name); }
  function sqlStr(s) { return "'" + String(s).replace(/'/g, "''") + "'"; }
  function escapeLike(s) { return String(s).replace(/[\\%_]/g, "\\$&"); }
  function dropdown(label, items) {
    var wrap = el("div", { class: "dropdown" });
    var menu = el("div", { class: "menu" });
    var open = false;
    function onDoc(e) { if (!wrap.contains(e.target)) setOpen(false); }
    function setOpen(v) { open = v; menu.classList.toggle("show", v); if (v) document.addEventListener("click", onDoc); else document.removeEventListener("click", onDoc); }
    items.forEach(function (it) { var mi = el("button", { class: "menu-item", type: "button" }, [it.label]); mi.addEventListener("click", function () { setOpen(false); it.onClick(); }); menu.appendChild(mi); });
    var btn = el("button", { class: "btn ghost", type: "button" }, [label]);
    btn.addEventListener("click", function (e) { e.stopPropagation(); setOpen(!open); });
    wrap.appendChild(btn); wrap.appendChild(menu);
    return wrap;
  }

  var SQL_KW = {}, SQL_FN = {};
  "SELECT FROM WHERE INSERT INTO VALUES UPDATE SET DELETE CREATE TABLE VIEW SCHEMA DROP ALTER ADD COLUMN CONSTRAINT PRIMARY KEY FOREIGN REFERENCES NOT NULL DEFAULT UNIQUE CHECK AND OR IN IS LIKE BETWEEN LIMIT OFFSET ORDER BY GROUP HAVING JOIN LEFT RIGHT INNER OUTER FULL CROSS ON AS DISTINCT UNION ALL EXCEPT INTERSECT EXISTS CASE WHEN THEN ELSE END ASC DESC PRAGMA EXPLAIN DESCRIBE SHOW WITH RECURSIVE ATTACH DETACH USE COPY QUALIFY USING SAMPLE PIVOT UNPIVOT".split(/\s+/).forEach(function (k) { SQL_KW[k] = 1; });
  "COUNT SUM AVG MIN MAX ABS ROUND COALESCE NULLIF LENGTH LOWER UPPER SUBSTR TRIM REPLACE DATE_TRUNC DATE_PART STRFTIME EPOCH NOW LIST ARRAY STRUCT UNNEST REGEXP_MATCHES READ_CSV_AUTO READ_JSON_AUTO PARQUET_SCAN ROW_NUMBER RANK".split(/\s+/).forEach(function (k) { SQL_FN[k] = 1; });
  function escHtml(s) { return s.replace(/&/g, "&amp;").replace(/</g, "&lt;").replace(/>/g, "&gt;"); }
  function highlightSQL(src) {
    var re = /(--[^\n]*|\/\*[\s\S]*?\*\/)|('(?:[^']|'')*')|("(?:[^"]|"")*")|(\b\d+(?:\.\d+)?\b)|([A-Za-z_][A-Za-z0-9_]*)|(\s+)|([^\s\w])/g;
    var out = "", m;
    while ((m = re.exec(src))) {
      var t = m[0], cls = "";
      if (m[1]) cls = "hl-comment"; else if (m[2]) cls = "hl-string"; else if (m[3]) cls = "hl-ident";
      else if (m[4]) cls = "hl-num";
      else if (m[5]) { var u = t.toUpperCase(); cls = SQL_KW[u] ? "hl-kw" : (SQL_FN[u] ? "hl-fn" : ""); }
      else if (m[7]) cls = "hl-punc";
      out += cls ? '<span class="' + cls + '">' + escHtml(t) + "</span>" : escHtml(t);
      if (re.lastIndex === m.index) re.lastIndex++;
    }
    return out;
  }
  function syncHighlight() { refs.hl.innerHTML = highlightSQL(refs.editor.value) + "\n"; }
  function setEditorValue(v) { refs.editor.value = v; syncHighlight(); }

  function openModal(box) {
    var overlay = el("div", { class: "modal-overlay" });
    overlay.appendChild(box);
    overlay.addEventListener("click", function (e) { if (e.target === overlay) closeModal(overlay); });
    overlay._onKey = function (e) { if (e.key === "Escape") closeModal(overlay); };
    document.addEventListener("keydown", overlay._onKey);
    document.body.appendChild(overlay);
    requestAnimationFrame(function () { overlay.classList.add("show"); });
    return overlay;
  }
  function closeModal(overlay) {
    if (!overlay || overlay._closing) return;
    overlay._closing = true;
    overlay.classList.remove("show");
    document.removeEventListener("keydown", overlay._onKey);
    setTimeout(function () { overlay.remove(); }, 200);
  }
  function confirmDialog(message, onConfirm, danger) {
    var overlay;
    var ok = button(danger ? "Delete" : "Confirm", function () { closeModal(overlay); onConfirm(); }, danger ? "primary danger-solid" : "primary");
    var box = el("div", { class: "modal" }, [el("div", { class: "modal-msg", text: message }), el("div", { class: "modal-actions" }, [button("Cancel", function () { closeModal(overlay); }, "ghost"), ok])]);
    overlay = openModal(box);
    ok.focus();
  }

  function buildUI() {
    document.body.appendChild(styleTag());
    refs.file = el("input", { type: "file", accept: ".parquet,.csv,.tsv,.json,.ndjson,.arrow,.duckdb,.db", style: "display:none" });
    refs.file.addEventListener("change", function () { if (refs.file.files && refs.file.files[0]) openLocal(refs.file.files[0]); refs.file.value = ""; });
    refs.open = button("Open file", function () { refs.file.click(); }, "primary");
    refs.new = button("New", newDatabase, "", "Create an empty in-memory database");
    refs.dbLabel = el("span", { class: "dbname", text: "No database" });
    var toolbar = el("div", { class: "toolbar" }, [refs.open, refs.new, el("span", { class: "spacer" }), refs.dbLabel]);

    refs.schema = el("div", { class: "schema" });
    var sidebar = el("aside", { class: "sidebar" }, [el("div", { class: "side-head", text: "Schema" }), refs.schema]);

    refs.editor = el("textarea", { class: "editor", spellcheck: "false", placeholder: "SELECT * FROM 'data.parquet' LIMIT 100;" });
    refs.hl = el("pre", { class: "editor-hl" });
    refs.editorWrap = el("div", { class: "editor-wrap" }, [refs.hl, refs.editor]);
    refs.editor.addEventListener("input", syncHighlight);
    refs.editor.addEventListener("scroll", function () { refs.hl.scrollTop = refs.editor.scrollTop; refs.hl.scrollLeft = refs.editor.scrollLeft; });
    refs.editor.addEventListener("keydown", function (e) { if ((e.ctrlKey || e.metaKey) && e.key === "Enter") { e.preventDefault(); runQuery(); } });

    refs.pageSize = el("select", { class: "select", title: "Rows per page" });
    [25, 50, 100, 200, 500].forEach(function (n) { var o = el("option", { value: String(n), text: n + " rows" }); if (n === pageSize) o.selected = true; refs.pageSize.appendChild(o); });
    refs.pageSize.addEventListener("change", function () { pageSize = parseInt(refs.pageSize.value, 10); reload(); });

    refs.search = el("input", { type: "search", class: "input", placeholder: "Search rows…", title: "Filter rows of the browsed table" });
    refs.search.disabled = true;
    refs.search.addEventListener("input", function () { clearTimeout(searchTimer); searchTimer = setTimeout(applySearch, 250); });

    refs.run = button("Run", runQuery, "primary", "Ctrl/⌘ + Enter");
    var runbar = el("div", { class: "runbar" }, [refs.run, el("span", { class: "kbd", text: "⌘↵" }), el("span", { class: "spacer" }), refs.search, refs.pageSize, dropdown("Export ▾", [{ label: "Export CSV", onClick: exportCSV }, { label: "Export JSON", onClick: exportJSON }, { label: "Export Markdown", onClick: exportMarkdown }])]);

    refs.results = el("div", { class: "results" });
    var main = el("div", { class: "main" }, [el("section", { class: "editor-pane" }, [refs.editorWrap, runbar]), el("section", { class: "result-pane" }, [refs.results])]);
    refs.status = el("div", { class: "status" }, [el("span", { class: "dot" }), el("span", { class: "status-text", text: "Starting…" })]);
    refs.app = el("div", { class: "app" }, [toolbar, el("div", { class: "body" }, [sidebar, main]), refs.status]);
    document.body.appendChild(refs.file);
    document.body.appendChild(refs.app);
    refs.toasts = el("div", { class: "toasts" });
    document.body.appendChild(refs.toasts);
    setupDrop();
    setBusy(true);
    renderLoading("Loading DuckDB engine…");
  }
  function setBusy(b) { [refs.open, refs.new, refs.run].forEach(function (x) { if (x) x.disabled = b; }); }
  function renderLoading(text) { refs.results.innerHTML = ""; refs.results.appendChild(el("div", { class: "loading" }, [el("div", { class: "spinner" }), el("div", { text: text || "Loading…" })])); }
  function setupDrop() {
    var overlay = el("div", { class: "drop-overlay", text: "Drop a Parquet / CSV / JSON file to open" });
    refs.app.appendChild(overlay);
    var depth = 0;
    refs.app.addEventListener("dragenter", function (e) { e.preventDefault(); depth++; refs.app.classList.add("dragging"); });
    refs.app.addEventListener("dragover", function (e) { e.preventDefault(); });
    refs.app.addEventListener("dragleave", function () { if (--depth <= 0) { depth = 0; refs.app.classList.remove("dragging"); } });
    refs.app.addEventListener("drop", function (e) { e.preventDefault(); depth = 0; refs.app.classList.remove("dragging"); if (e.dataTransfer && e.dataTransfer.files && e.dataTransfer.files[0]) openLocal(e.dataTransfer.files[0]); });
  }

  async function initEngine() {
    var mjsUrl = await sc.assetURL("duckdb.mjs", "text/javascript");
    DUCK = await import(mjsUrl);
    var worker = new Worker(await sc.assetURL("duckdb-browser-eh.worker.js", "text/javascript"));
    adb = new DUCK.AsyncDuckDB(new DUCK.ConsoleLogger(), worker);
    await adb.instantiate(await sc.assetURL("duckdb-eh.wasm", "application/wasm"));
    conn = await adb.connect();
  }

  function baseName(name) { var b = String(name).split(/[\\/]/).pop().split("?")[0]; return b || "data"; }
  function viewName(name) { return baseName(name).replace(/\.[^.]+$/, "").replace(/[^A-Za-z0-9_]/g, "_") || "data"; }
  function scanExpr(name) {
    var ext = (baseName(name).match(/\.([^.]+)$/) || [, ""])[1].toLowerCase();
    var qq = sqlStr(name);
    if (ext === "csv" || ext === "tsv" || ext === "txt") return "read_csv_auto(" + qq + ")";
    if (ext === "json" || ext === "ndjson") return "read_json_auto(" + qq + ")";
    if (ext === "parquet") return "parquet_scan(" + qq + ")";
    return qq;
  }

  async function openLocal(file) {
    if (!conn) return;
    try {
      var ext = (baseName(file.name).match(/\.([^.]+)$/) || [, ""])[1].toLowerCase();
      await adb.registerFileHandle(file.name, file, DUCK.DuckDBDataProtocol.BROWSER_FILEREADER, true);
      if (ext === "duckdb" || ext === "db") {
        await conn.query("ATTACH " + sqlStr(file.name) + " AS attached (READ_ONLY)");
        setEditorValue("SHOW ALL TABLES;");
        await runQuery();
      } else {
        var v = viewName(file.name);
        await conn.query("CREATE OR REPLACE VIEW " + q1(v) + " AS SELECT * FROM " + scanExpr(file.name));
        await refreshSchema();
        await openObject("memory", "main", v, "VIEW");
      }
      refs.dbLabel.textContent = file.name + "  ·  local, in browser";
      await refreshSchema();
      notify("Opened " + file.name + " · nothing uploaded", "ok");
    } catch (e) { renderError(msg(e)); notify("Could not open file: " + msg(e), "error"); }
  }
  async function newDatabase() { if (!conn) return; setEditorValue(""); mode = null; cursor = null; table = null; setSearch(false); await refreshSchema(); welcome(); setStatus("In-memory database — create tables or open a file", "ok"); }
  function reload() { if (mode === "table") loadTablePage(table.page || 0); else if (mode === "query" && cursor) runQuery(); }

  async function listRows(sql) { try { var res = await conn.query(sql); return res.toArray().map(function (r) { return Object.values(r.toJSON ? r.toJSON() : r); }); } catch (e) { return []; } }

  async function refreshSchema() {
    refs.schema.innerHTML = "";
    var rows = await listRows("SELECT table_catalog, table_schema, table_name, table_type FROM information_schema.tables WHERE table_schema NOT IN ('information_schema','pg_catalog') ORDER BY table_catalog, table_schema, table_name");
    if (!rows.length) { refs.schema.appendChild(el("div", { class: "side-empty", text: "No tables or views yet" })); return; }
    var tables = rows.filter(function (r) { return String(r[3]).indexOf("VIEW") === -1; });
    var views = rows.filter(function (r) { return String(r[3]).indexOf("VIEW") !== -1; });
    if (tables.length) refs.schema.appendChild(schemaGroup("Tables", tables));
    if (views.length) refs.schema.appendChild(schemaGroup("Views", views));
  }
  function schemaGroup(title, rows) {
    var group = el("div", { class: "schema-group" }, [el("div", { class: "schema-group-head", text: title + " (" + rows.length + ")" })]);
    rows.forEach(function (row) {
      var cat = row[0], sch = row[1], name = row[2], type = row[3];
      var label = sch && sch !== "main" ? sch + "." + name : name;
      var cols = el("div", { class: "cols" }); cols.style.display = "none";
      var expanded = false;
      var caret = el("span", { class: "caret", text: "▸" });
      var head = el("div", { class: "table-row" }, [caret, el("span", { class: "table-name", text: label, title: cat + "." + sch + "." + name })]);
      caret.addEventListener("click", async function (e) {
        e.stopPropagation(); expanded = !expanded; caret.textContent = expanded ? "▾" : "▸"; cols.style.display = expanded ? "block" : "none";
        if (expanded && !cols.dataset.loaded) { (await describe(cat, sch, name)).forEach(function (c) { cols.appendChild(el("div", { class: "col" }, [el("span", { class: "col-name", text: c.name }), el("span", { class: "col-type", text: c.type || "" })])); }); cols.dataset.loaded = "1"; }
      });
      head.addEventListener("click", function () { openObject(cat, sch, name, type); });
      group.appendChild(el("div", {}, [head, cols]));
    });
    return group;
  }
  async function describe(cat, sch, name) {
    var out = [];
    try { var res = await conn.query("DESCRIBE " + qname(cat, sch, name)); res.toArray().forEach(function (r) { var o = r.toJSON ? r.toJSON() : r; out.push({ name: o.column_name, type: o.column_type, pk: String(o.key || "").toUpperCase() === "PRI", notnull: String(o["null"]).toUpperCase() === "NO" }); }); } catch (e) {}
    return out;
  }

  async function openObject(cat, sch, name, type) {
    var cols = await describe(cat, sch, name);
    var pk = cols.filter(function (c) { return c.pk; });
    var writable = cat !== "attached" && String(type).indexOf("VIEW") === -1 && pk.length > 0;
    table = { cat: cat, sch: sch, name: name, cols: cols, pk: pk, editable: writable, page: 0, search: "" };
    setEditorValue("SELECT * FROM " + qname(cat, sch, name));
    mode = "table"; selection = {};
    setSearch(true);
    await loadTablePage(0);
  }

  function setSearch(enabled) { clearTimeout(searchTimer); refs.search.value = ""; refs.search.disabled = !enabled; }
  function applySearch() {
    if (mode !== "table" || !table) return;
    var term = refs.search.value.trim();
    if (term === (table.search || "")) return;
    table.search = term;
    loadTablePage(0);
  }
  function searchWhere() {
    if (!table || !table.search) return "";
    var lit = sqlStr("%" + escapeLike(table.search) + "%");
    return " WHERE (" + table.cols.map(function (c) { return "CAST(" + q1(c.name) + " AS VARCHAR) ILIKE " + lit + " ESCAPE '\\'"; }).join(" OR ") + ")";
  }
  function emptyText() { return mode === "table" && table && table.search ? "No rows match “" + table.search + "”" : "No rows"; }

  async function loadTablePage(pageIndex) {
    mode = "table"; table.page = pageIndex; selection = {};
    var qn = qname(table.cat, table.sch, table.name);
    var where = searchWhere();
    var total = 0;
    try { var c = await conn.query("SELECT count(*) AS n FROM " + qn + where); total = Number(c.toArray()[0].n); } catch (e) {}
    var offset = pageIndex * pageSize;
    var names = table.cols.map(function (c) { return c.name; });
    var rows = [];
    try { var res = await conn.query("SELECT * FROM " + qn + where + " LIMIT " + pageSize + " OFFSET " + offset); rows = res.toArray().map(function (r) { var o = r.toJSON ? r.toJSON() : r; return names.map(function (n) { return normalize(o[n]); }); }); }
    catch (e) { renderError(msg(e)); return; }
    grid(names, rows, { start: offset, total: total, editable: table.editable, hasPrev: pageIndex > 0, hasNext: offset + rows.length < total, onPrev: function () { loadTablePage(pageIndex - 1); }, onNext: function () { loadTablePage(pageIndex + 1); } });
    setStatus(table.name + " · " + total + " row(s)" + (table.search ? " matching “" + table.search + "”" : "") + (table.editable ? " · editable" : ""), "ok");
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
    setSearch(false);
    setBusy(true); setStatus("Running…");
    var t0 = performance.now();
    try {
      if (isReadQuery(text)) {
        mode = "query"; table = null;
        cursor = { reader: await conn.send(text.replace(/;+\s*$/, "")), columns: null, rows: [], done: false };
        await renderCursorPage(0);
        setStatus("Query ran in " + since(t0), "ok");
      } else {
        mode = null; table = null;
        await conn.query(text);
        renderMessage("Statement executed.");
        await refreshSchema();
        setStatus("Executed in " + since(t0), "ok");
      }
    } catch (e) { renderError(msg(e)); setStatus("Error", "error"); }
    finally { setBusy(false); }
  }

  async function fill(upto) {
    while (cursor.rows.length < upto && !cursor.done) {
      var nx = await cursor.reader.next();
      if (nx.done) { cursor.done = true; break; }
      var batch = nx.value;
      if (!cursor.columns) cursor.columns = batch.schema.fields.map(function (f) { return f.name; });
      var arr = batch.toArray();
      for (var i = 0; i < arr.length; i++) { var row = arr[i]; cursor.rows.push(cursor.columns.map(function (c) { return normalize(row[c]); })); }
    }
    if (!cursor.columns && cursor.reader.schema) cursor.columns = cursor.reader.schema.fields.map(function (f) { return f.name; });
  }
  async function renderCursorPage(pageIndex) {
    await fill((pageIndex + 1) * pageSize + 1);
    var start = pageIndex * pageSize;
    var rows = cursor.rows.slice(start, start + pageSize);
    grid(cursor.columns || [], rows, { start: start, total: cursor.done ? cursor.rows.length : null, editable: false, hasPrev: pageIndex > 0, hasNext: cursor.rows.length > start + pageSize, onPrev: function () { renderCursorPage(pageIndex - 1); }, onNext: function () { renderCursorPage(pageIndex + 1); } });
  }

  function normalize(v) {
    if (v === null || v === undefined) return null;
    if (typeof v === "bigint") return v.toString();
    if (v instanceof Uint8Array) return { __blob: v.length };
    if (typeof v === "object") { try { return JSON.stringify(v); } catch (e) { return String(v); } }
    return v;
  }

  function grid(columns, rows, nav) {
    refs.results.innerHTML = "";
    var editable = !!nav.editable;
    var bar = el("div", { class: "gridbar" });
    if (editable) {
      refs.addBtn = button("＋ Add row", function () { openRowForm("add", null); }, "ghost");
      refs.editBtn = button("Edit", editSelected, "ghost"); refs.editBtn.disabled = true;
      refs.delBtn = button("Delete", deleteSelected, "ghost danger"); refs.delBtn.disabled = true;
      bar.appendChild(refs.addBtn); bar.appendChild(refs.editBtn); bar.appendChild(refs.delBtn);
    }
    bar.appendChild(el("span", { class: "spacer" }));
    bar.appendChild(el("span", { class: "muted", text: editable ? "Select a row, or double-click to edit" : "Read-only result" }));
    refs.results.appendChild(bar);

    var scroll = el("div", { class: "grid-scroll" });
    var tableEl = el("table", { class: "grid" });
    var htr = el("tr", {});
    if (editable) htr.appendChild(el("th", { class: "selcol" }, [""]));
    htr.appendChild(el("th", { class: "rownum", text: "#" }));
    columns.forEach(function (c) { htr.appendChild(el("th", { text: String(c) })); });
    tableEl.appendChild(el("thead", {}, [htr]));
    var tbody = el("tbody", {});
    rows.forEach(function (row, r) {
      var tr = el("tr", {});
      var key = editable ? rowKey(row, columns) : null;
      var values = null;
      if (editable) {
        values = {}; columns.forEach(function (c, i) { values[c] = row[i]; });
        var kk = JSON.stringify(key);
        var cb = el("input", { type: "checkbox" });
        cb.addEventListener("change", function () { if (cb.checked) selection[kk] = { key: key, values: values }; else delete selection[kk]; tr.classList.toggle("sel", cb.checked); refreshEditDel(); });
        tr.appendChild(el("td", { class: "selcol" }, [cb]));
      }
      tr.appendChild(el("td", { class: "rownum", text: String(nav.start + r + 1) }));
      row.forEach(function (cell) { tr.appendChild(cellNode(cell)); });
      if (editable) tr.addEventListener("dblclick", function () { openRowForm("edit", { key: key, values: values }); });
      tbody.appendChild(tr);
    });
    tableEl.appendChild(tbody);
    scroll.appendChild(tableEl);
    if (!rows.length) scroll.appendChild(el("div", { class: "empty", text: emptyText() }));
    refs.results.appendChild(scroll);
    refs.results.appendChild(pager(rows.length, nav));
    lastGrid = { columns: columns, rows: rows };
  }

  function rowKey(row, columns) { return table.pk.map(function (c) { return row[columns.indexOf(c.name)]; }); }
  function refreshEditDel() { var n = Object.keys(selection).length; if (refs.editBtn) refs.editBtn.disabled = n !== 1; if (refs.delBtn) refs.delBtn.disabled = n === 0; }
  function editSelected() { var keys = Object.keys(selection); if (keys.length === 1) openRowForm("edit", selection[keys[0]]); }
  function typeHint(c) { return (c.type || "") + (c.pk ? " · pk" : ""); }
  function pkWhere() { return table.pk.map(function (c) { return q1(c.name) + " = ?"; }).join(" AND "); }

  function openRowForm(kind, existing) {
    var inputs = {};
    var fields = table.cols.map(function (c) {
      var inp = el("input", { class: "cell-input", spellcheck: "false", placeholder: c.type || "" });
      var v = existing ? existing.values[c.name] : undefined;
      inp.value = v === null || v === undefined ? "" : (v && v.__blob !== undefined ? "" : String(v));
      inputs[c.name] = inp;
      return el("label", { class: "form-field" }, [el("span", { class: "form-label" }, [el("span", { text: c.name }), el("span", { class: "form-type", text: typeHint(c) })]), inp]);
    });
    var overlay;
    var saveBtn = button(kind === "edit" ? "Save changes" : "Insert row", function () { saveRow(kind, inputs, existing, overlay); }, "primary");
    var box = el("div", { class: "modal modal-form" }, [
      el("div", { class: "modal-title", text: (kind === "edit" ? "Edit row · " : "New row · ") + table.name }),
      el("div", { class: "form-grid" }, fields),
      el("div", { class: "modal-actions" }, [button("Cancel", function () { closeModal(overlay); }, "ghost"), saveBtn]),
    ]);
    overlay = openModal(box);
    var first = box.querySelector("input"); if (first) { first.focus(); first.select(); }
  }

  async function saveRow(kind, inputs, existing, overlay) {
    try {
      if (kind === "edit") {
        var vals = table.cols.map(function (c) { return coerce(inputs[c.name].value, c.type, c.notnull); });
        var stmt = await conn.prepare("UPDATE " + qname(table.cat, table.sch, table.name) + " SET " + table.cols.map(function (c) { return q1(c.name) + " = ?"; }).join(", ") + " WHERE " + pkWhere());
        await stmt.query.apply(stmt, vals.concat(existing.key));
        await stmt.close();
      } else {
        var cols = [], marks = [], v2 = [];
        table.cols.forEach(function (c) { var raw = inputs[c.name].value; if (raw === "" && !c.pk) return; cols.push(q1(c.name)); marks.push("?"); v2.push(coerce(raw, c.type, c.notnull)); });
        if (!cols.length) { notify("Enter at least the key columns", "error"); return; }
        var ins = await conn.prepare("INSERT INTO " + qname(table.cat, table.sch, table.name) + " (" + cols.join(", ") + ") VALUES (" + marks.join(", ") + ")");
        await ins.query.apply(ins, v2);
        await ins.close();
      }
      closeModal(overlay); loadTablePage(table.page || 0);
      toast(kind === "edit" ? "Row updated in " + table.name : "Row added to " + table.name, "ok");
    } catch (e) { notify("Save failed: " + msg(e), "error"); }
  }

  function deleteSelected() {
    var entries = Object.keys(selection).map(function (k) { return selection[k]; });
    if (!entries.length || !table) return;
    confirmDialog("Delete " + entries.length + " row(s) from " + table.name + "? This cannot be undone.", async function () {
      try {
        for (var i = 0; i < entries.length; i++) { var stmt = await conn.prepare("DELETE FROM " + qname(table.cat, table.sch, table.name) + " WHERE " + pkWhere()); await stmt.query.apply(stmt, entries[i].key); await stmt.close(); }
        loadTablePage(table.page || 0); toast(entries.length + " row(s) deleted from " + table.name, "ok");
      } catch (e) { notify("Delete failed: " + msg(e), "error"); }
    }, true);
  }

  function coerce(raw, type, notnull) {
    if (raw === "") return notnull ? "" : null;
    var t = (type || "").toUpperCase();
    if (/INT|DOUBLE|FLOAT|REAL|DECIMAL|NUMERIC|HUGEINT|BIGINT/.test(t)) { var n = Number(raw); return isNaN(n) ? raw : n; }
    if (/BOOL/.test(t)) return /^(true|1|t|yes)$/i.test(raw);
    return raw;
  }

  function pager(count, nav) {
    var from = count ? nav.start + 1 : 0, to = nav.start + count;
    var label = nav.total != null ? from + "–" + to + " of " + nav.total : "Rows " + from + "–" + to;
    var prev = button("‹ Prev", nav.onPrev, "ghost"), next = button("Next ›", nav.onNext, "ghost");
    prev.disabled = !nav.hasPrev; next.disabled = !nav.hasNext;
    return el("div", { class: "pager" }, [el("span", { class: "range", text: label }), el("span", { class: "spacer" }), prev, next]);
  }
  function cellNode(cell) {
    if (cell === null) return el("td", { class: "null", text: "NULL" });
    if (cell && cell.__blob !== undefined) return el("td", { class: "blob", text: "‹blob " + cell.__blob + " B›" });
    return el("td", { text: String(cell) });
  }
  function renderMessage(text) { refs.results.innerHTML = ""; refs.results.appendChild(el("div", { class: "notice", text: text })); lastGrid = null; }
  function renderError(text) { refs.results.innerHTML = ""; refs.results.appendChild(el("div", { class: "error-box" }, [el("div", { class: "error-title", text: "Query error" }), el("pre", { class: "error-msg", text: text })])); lastGrid = null; }
  function welcome() { refs.results.innerHTML = ""; refs.results.appendChild(el("div", { class: "welcome" }, [el("div", { class: "welcome-title", text: "DuckDB Explorer" }), el("div", { class: "welcome-sub", text: "Open a local Parquet / CSV / JSON file (or drag it in) and run SQL. Everything runs in your browser — files are never uploaded." })])); }

  function isBlob(v) { return v instanceof Uint8Array || (v && v.__blob !== undefined); }
  function saveBlob(blob, filename) { var url = URL.createObjectURL(blob); var a = el("a", { href: url, download: filename }); document.body.appendChild(a); a.click(); setTimeout(function () { URL.revokeObjectURL(url); a.remove(); }, 0); }
  function haveRows() { if (!lastGrid || !lastGrid.rows.length) { notify("Nothing to export", "error"); return false; } return true; }
  function exportCSV() {
    if (!haveRows()) return;
    var esc = function (v) { if (v === null || v === undefined) return ""; var s = isBlob(v) ? "" : String(v); return /[",\n]/.test(s) ? '"' + s.replace(/"/g, '""') + '"' : s; };
    var lines = [lastGrid.columns.map(esc).join(",")];
    lastGrid.rows.forEach(function (row) { lines.push(row.map(esc).join(",")); });
    saveBlob(new Blob([lines.join("\n")], { type: "text/csv" }), "export.csv");
    toast("Exported " + lastGrid.rows.length + " row(s) to CSV", "ok");
  }
  function exportJSON() {
    if (!haveRows()) return;
    var out = lastGrid.rows.map(function (row) { var o = {}; lastGrid.columns.forEach(function (c, i) { o[c] = isBlob(row[i]) ? null : row[i]; }); return o; });
    saveBlob(new Blob([JSON.stringify(out, null, 2)], { type: "application/json" }), "export.json");
    toast("Exported " + lastGrid.rows.length + " row(s) to JSON", "ok");
  }
  function exportMarkdown() {
    if (!haveRows()) return;
    var cell = function (v) { if (v === null || v === undefined) return ""; return (isBlob(v) ? "" : String(v)).replace(/\|/g, "\\|").replace(/\n/g, " "); };
    var out = ["| " + lastGrid.columns.map(cell).join(" | ") + " |", "| " + lastGrid.columns.map(function () { return "---"; }).join(" | ") + " |"];
    lastGrid.rows.forEach(function (row) { out.push("| " + row.map(cell).join(" | ") + " |"); });
    saveBlob(new Blob([out.join("\n")], { type: "text/markdown" }), "export.md");
    toast("Exported " + lastGrid.rows.length + " row(s) to Markdown", "ok");
  }
  async function closeCursor() { if (cursor && cursor.reader) { try { if (cursor.reader.cancel) await cursor.reader.cancel(); else if (cursor.reader.return) await cursor.reader.return(); } catch (e) {} } cursor = null; }

  function setStatus(text, kind) { refs.status.className = "status" + (kind ? " " + kind : ""); refs.status.querySelector(".status-text").textContent = text; }
  function toast(message, kind) {
    if (!refs.toasts) return;
    var t = el("div", { class: "toast " + (kind || "info"), role: "status" }, [el("span", { class: "toast-icon", text: kind === "error" ? "✕" : kind === "ok" ? "✓" : "•" }), el("span", { text: message })]);
    refs.toasts.appendChild(t);
    requestAnimationFrame(function () { t.classList.add("show"); });
    var hide = function () { t.classList.remove("show"); setTimeout(function () { t.remove(); }, 220); };
    var timer = setTimeout(hide, 2800);
    t.addEventListener("click", function () { clearTimeout(timer); hide(); });
  }
  function notify(message, kind) { setStatus(message, kind); toast(message, kind); }
  function since(t0) { return (performance.now() - t0).toFixed(1) + " ms"; }
  function msg(e) { return (e && (e.message || e.toString())) || "unknown error"; }

  async function boot() {
    buildUI();
    if (typeof sc.hideStatus === "function") sc.hideStatus();
    applyTheme(sc.theme, sc.colors);
    if (typeof sc.onTheme === "function") sc.onTheme(applyTheme);
    try { await initEngine(); setBusy(false); welcome(); setStatus("Ready — open a file to begin", "ok"); }
    catch (e) { renderError("DuckDB engine failed to load: " + msg(e)); setStatus("Engine failed to load", "error"); }
  }

  function styleTag() {
    var css = [
      ":root{--accent:#fcd34d}",
      'body[data-theme="dark"]{--bg:#0b1220;--panel:#0f1a2e;--panel2:#16233b;--head:#152238;--border:#243449;--text:#e6edf6;--muted:#8598b3;--danger:#f87171;--ok:#34d399;--sel:rgba(252,211,77,.16)}',
      'body[data-theme="light"]{--bg:#f6f8fb;--panel:#ffffff;--panel2:#eef2f7;--head:#f1f5f9;--border:#dbe2ec;--text:#0f172a;--muted:#64748b;--danger:#dc2626;--ok:#059669;--sel:rgba(202,138,4,.14)}',
      "*{box-sizing:border-box}",
      "html,body{margin:0;height:100%;background:var(--bg);color:var(--text);font:13px/1.5 ui-sans-serif,system-ui,-apple-system,Segoe UI,Roboto,sans-serif}",
      ".app{position:relative;display:flex;flex-direction:column;height:100vh}",
      ".toolbar{display:flex;gap:8px;align-items:center;padding:9px 12px;border-bottom:1px solid var(--border);background:var(--panel)}",
      ".dbname{color:var(--muted);font-size:12px;max-width:52ch;overflow:hidden;text-overflow:ellipsis;white-space:nowrap}",
      ".spacer{flex:1}.muted{color:var(--muted);font-size:12px}",
      ".btn{background:var(--panel2);color:var(--text);border:1px solid var(--border);border-radius:7px;padding:6px 13px;cursor:pointer;font:inherit;transition:border-color .12s}",
      ".btn:hover:not(:disabled){border-color:var(--accent)}",
      ".btn:disabled{opacity:.4;cursor:default}",
      ".btn.primary{background:var(--accent);color:#20160a;border-color:transparent;font-weight:600}",
      ".btn.ghost{background:transparent}",
      ".btn.danger{color:var(--danger)}.btn.danger:hover:not(:disabled){border-color:var(--danger)}",
      ".btn.danger-solid{background:var(--danger);color:#fff;border-color:transparent}",
      ".select{background:var(--panel2);color:var(--text);border:1px solid var(--border);border-radius:7px;padding:6px 8px;font:inherit;cursor:pointer}",
      ".input{width:230px;background:var(--panel2);color:var(--text);border:1px solid var(--border);border-radius:7px;padding:6px 10px;font:inherit;outline:none}",
      ".input:focus{border-color:var(--accent)}",
      ".input:disabled{opacity:.45}",
      ".dropdown{position:relative}",
      ".menu{position:absolute;right:0;top:calc(100% + 6px);min-width:170px;background:var(--panel);border:1px solid var(--border);border-radius:9px;box-shadow:0 12px 34px rgba(0,0,0,.4);display:none;overflow:hidden;z-index:20}",
      ".menu.show{display:block}",
      ".menu-item{display:block;width:100%;text-align:left;background:none;border:0;color:var(--text);padding:9px 14px;cursor:pointer;font:inherit}",
      ".menu-item:hover{background:var(--panel2)}",
      ".body{flex:1;display:flex;min-height:0}",
      ".sidebar{width:236px;flex-shrink:0;border-right:1px solid var(--border);background:var(--panel);display:flex;flex-direction:column;min-height:0}",
      ".side-head{padding:10px 12px;font-size:11px;letter-spacing:.06em;text-transform:uppercase;color:var(--muted);border-bottom:1px solid var(--border)}",
      ".schema{overflow:auto;padding:6px 4px;flex:1}",
      ".schema-group{margin-bottom:8px}.schema-group-head{padding:4px 10px;font-size:11px;color:var(--muted);text-transform:uppercase}",
      ".table-row{display:flex;align-items:center;gap:4px;padding:4px 8px;border-radius:6px;cursor:pointer}.table-row:hover{background:var(--panel2)}",
      ".caret{width:14px;color:var(--muted);font-size:10px;text-align:center}",
      ".table-name{overflow:hidden;text-overflow:ellipsis;white-space:nowrap}",
      ".cols{padding:2px 0 4px 22px}",
      ".col{display:flex;justify-content:space-between;gap:8px;padding:2px 10px 2px 0;color:var(--muted);font-size:12px}",
      ".col-name{color:var(--text);overflow:hidden;text-overflow:ellipsis;white-space:nowrap}.col-type{font-size:11px;opacity:.8;flex-shrink:0}",
      ".side-empty{padding:12px;color:var(--muted);font-size:12px}",
      ".main{flex:1;display:flex;flex-direction:column;min-width:0;min-height:0}",
      ".editor-pane{display:flex;flex-direction:column;flex-shrink:0}",
      ".editor-wrap{position:relative;background:var(--panel)}",
      ".editor,.editor-hl{margin:0;border:0;padding:9px 14px;font:13px/1.5 ui-monospace,SFMono-Regular,Menlo,Consolas,monospace;white-space:pre-wrap;word-break:break-word;tab-size:4}",
      ".editor-hl{position:absolute;inset:0;overflow:hidden;pointer-events:none;color:var(--text);background:transparent}",
      ".editor{position:relative;display:block;width:100%;height:76px;min-height:36px;resize:vertical;background:transparent;color:transparent;caret-color:var(--text);outline:none}",
      ".hl-kw{color:#7dd3fc;font-weight:600}.hl-fn{color:#c4b5fd}.hl-string{color:#86efac}.hl-ident{color:#fca5a5}.hl-num{color:#fdba74}.hl-comment{color:var(--muted);font-style:italic}.hl-punc{color:var(--muted)}",
      'body[data-theme="light"] .hl-kw{color:#0369a1}body[data-theme="light"] .hl-fn{color:#7c3aed}body[data-theme="light"] .hl-string{color:#15803d}body[data-theme="light"] .hl-ident{color:#b91c1c}body[data-theme="light"] .hl-num{color:#c2410c}',
      ".runbar{display:flex;gap:10px;align-items:center;padding:7px 12px;border-top:1px solid var(--border);border-bottom:1px solid var(--border);background:var(--panel)}",
      ".kbd{color:var(--muted);font-size:11px;border:1px solid var(--border);border-radius:5px;padding:1px 6px}",
      ".result-pane{flex:1;display:flex;flex-direction:column;min-height:0}",
      ".results{flex:1;display:flex;flex-direction:column;min-height:0;overflow:hidden}",
      ".gridbar{display:flex;gap:8px;align-items:center;padding:6px 12px;border-bottom:1px solid var(--border);background:var(--panel)}",
      ".grid-scroll{flex:1;overflow:auto}",
      ".grid{border-collapse:collapse;width:100%;font-size:12.5px}",
      ".grid th,.grid td{border-bottom:1px solid var(--border);border-right:1px solid var(--border);padding:5px 10px;text-align:left;white-space:nowrap;max-width:460px;overflow:hidden;text-overflow:ellipsis}",
      ".grid thead th{position:sticky;top:0;background:var(--head);z-index:1;font-weight:600}",
      ".grid .rownum{color:var(--muted);text-align:right;background:var(--panel)}.grid .selcol{width:34px;text-align:center}",
      ".grid tbody tr:hover td{background:var(--panel2)}.grid tbody tr.sel td{background:var(--sel)}",
      ".grid td.null{color:var(--muted);font-style:italic}.grid td.blob{color:var(--accent)}",
      ".cell-input{width:100%;min-width:80px;background:var(--panel);color:var(--text);border:1px solid var(--border);border-radius:6px;padding:6px 8px;font:inherit;outline:none}",
      ".cell-input:focus{border-color:var(--accent)}",
      ".pager{display:flex;align-items:center;gap:8px;padding:7px 12px;border-top:1px solid var(--border);background:var(--panel)}",
      ".range{color:var(--muted);font-size:12px}",
      ".empty,.notice,.welcome{padding:26px;text-align:center;color:var(--muted)}",
      ".welcome-title{font-size:15px;color:var(--text);margin-bottom:6px;font-weight:600}.welcome-sub{max-width:56ch;margin:0 auto}",
      ".loading{display:flex;flex-direction:column;align-items:center;gap:12px;padding:44px;color:var(--muted)}",
      ".spinner{width:26px;height:26px;border:3px solid var(--border);border-top-color:var(--accent);border-radius:50%;animation:spin .8s linear infinite}",
      "@keyframes spin{to{transform:rotate(360deg)}}",
      ".error-box{margin:14px;border:1px solid var(--danger);border-radius:8px;overflow:hidden}",
      ".error-title{padding:7px 12px;background:var(--panel2);color:var(--danger);font-weight:600;font-size:12px}",
      ".error-msg{margin:0;padding:12px;white-space:pre-wrap;font:12px/1.5 ui-monospace,monospace;color:var(--text)}",
      ".status{display:flex;align-items:center;gap:8px;padding:6px 12px;border-top:1px solid var(--border);background:var(--panel);color:var(--muted);font-size:12px}",
      ".status .dot{width:8px;height:8px;border-radius:50%;background:var(--muted);flex-shrink:0}",
      ".status.ok .dot{background:var(--ok)}.status.error{color:var(--danger)}.status.error .dot{background:var(--danger)}",
      ".status-text{overflow:hidden;text-overflow:ellipsis;white-space:nowrap}",
      ".drop-overlay{position:absolute;inset:0;display:none;align-items:center;justify-content:center;background:rgba(2,6,23,.72);color:#fff;font-size:16px;border:2px dashed var(--accent);z-index:10;pointer-events:none}",
      ".app.dragging .drop-overlay{display:flex}",
      ".modal-overlay{position:fixed;inset:0;display:flex;align-items:center;justify-content:center;background:rgba(2,6,23,.55);z-index:50;opacity:0;transition:opacity .18s ease}",
      ".modal-overlay.show{opacity:1}",
      ".modal{background:var(--panel);border:1px solid var(--border);border-radius:12px;padding:20px;max-width:420px;box-shadow:0 16px 50px rgba(0,0,0,.45);opacity:0;transform:translateY(10px) scale(.97);transition:opacity .18s ease,transform .18s ease}",
      ".modal-overlay.show .modal{opacity:1;transform:none}",
      ".modal-title{font-weight:600;font-size:14px;margin-bottom:16px}",
      ".modal-msg{margin-bottom:16px;line-height:1.5}",
      ".modal-actions{display:flex;gap:8px;justify-content:flex-end}",
      ".modal-form{width:min(560px,92vw);max-height:82vh;overflow:auto}",
      ".form-grid{display:grid;grid-template-columns:repeat(auto-fill,minmax(220px,1fr));gap:12px;margin-bottom:18px}",
      ".form-field{display:flex;flex-direction:column;gap:5px}",
      ".form-label{font-size:11px;color:var(--muted);display:flex;justify-content:space-between;gap:8px}.form-type{opacity:.7;font-size:10px}",
      ".toasts{position:fixed;top:14px;right:14px;display:flex;flex-direction:column;gap:8px;z-index:100;max-width:340px}",
      ".toast{display:flex;align-items:center;gap:9px;background:var(--panel);color:var(--text);border:1px solid var(--border);border-left:3px solid var(--muted);border-radius:9px;padding:10px 14px;font-size:12.5px;box-shadow:0 10px 30px rgba(0,0,0,.4);opacity:0;transform:translateX(16px);transition:opacity .2s ease,transform .2s ease;cursor:pointer}",
      ".toast.show{opacity:1;transform:none}",
      ".toast.ok{border-left-color:var(--ok)}.toast.error{border-left-color:var(--danger)}",
      ".toast-icon{width:16px;height:16px;flex-shrink:0;display:flex;align-items:center;justify-content:center;border-radius:50%;font-size:10px;color:#fff;background:var(--muted)}",
      ".toast.ok .toast-icon{background:var(--ok)}.toast.error .toast-icon{background:var(--danger)}",
    ].join("\n");
    return el("style", { text: css });
  }

  if (document.readyState === "loading") document.addEventListener("DOMContentLoaded", boot);
  else boot();
})();
