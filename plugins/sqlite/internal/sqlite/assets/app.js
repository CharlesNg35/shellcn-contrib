(function () {
  "use strict";

  var sc = window.shellcn || {};
  var SQL = null;
  var db = null;
  var dbName = "";
  var dirty = false;
  var pageSize = 50;
  var mode = null;
  var cursor = null;
  var table = null;
  var selection = null;
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

  function confirmDialog(message, onConfirm, danger) {
    var overlay = el("div", { class: "modal-overlay" });
    function close() { document.removeEventListener("keydown", onKey); overlay.remove(); }
    function onKey(e) { if (e.key === "Escape") close(); }
    var confirmBtn = button(danger ? "Delete" : "Confirm", function () { close(); onConfirm(); }, danger ? "primary danger-solid" : "primary");
    var box = el("div", { class: "modal" }, [
      el("div", { class: "modal-msg", text: message }),
      el("div", { class: "modal-actions" }, [button("Cancel", close, "ghost"), confirmBtn]),
    ]);
    overlay.appendChild(box);
    overlay.addEventListener("click", function (e) { if (e.target === overlay) close(); });
    document.addEventListener("keydown", onKey);
    document.body.appendChild(overlay);
    confirmBtn.focus();
  }

  function ident(name) {
    return '"' + String(name).replace(/"/g, '""') + '"';
  }

  var SQL_KW = {}, SQL_FN = {};
  "SELECT FROM WHERE INSERT INTO VALUES UPDATE SET DELETE CREATE TABLE VIEW INDEX DROP ALTER ADD COLUMN CONSTRAINT PRIMARY KEY FOREIGN REFERENCES NOT NULL DEFAULT UNIQUE CHECK AUTOINCREMENT AND OR IN IS LIKE GLOB BETWEEN LIMIT OFFSET ORDER BY GROUP HAVING JOIN LEFT RIGHT INNER OUTER FULL CROSS ON AS DISTINCT UNION ALL EXCEPT INTERSECT EXISTS CASE WHEN THEN ELSE END ASC DESC PRAGMA EXPLAIN WITH RECURSIVE BEGIN COMMIT ROLLBACK TRANSACTION IF REPLACE USING NATURAL COLLATE CAST".split(/\s+/).forEach(function (k) { SQL_KW[k] = 1; });
  "COUNT SUM AVG MIN MAX ABS ROUND COALESCE IFNULL NULLIF LENGTH LOWER UPPER SUBSTR TRIM LTRIM RTRIM REPLACE INSTR DATE TIME DATETIME STRFTIME JULIANDAY GROUP_CONCAT TYPEOF HEX QUOTE RANDOM ROW_NUMBER RANK".split(/\s+/).forEach(function (k) { SQL_FN[k] = 1; });

  function escHtml(s) { return s.replace(/&/g, "&amp;").replace(/</g, "&lt;").replace(/>/g, "&gt;"); }

  function highlightSQL(src) {
    var re = /(--[^\n]*|\/\*[\s\S]*?\*\/)|('(?:[^']|'')*')|("(?:[^"]|"")*"|`(?:[^`]|``)*`)|(\b\d+(?:\.\d+)?\b)|([A-Za-z_][A-Za-z0-9_]*)|(\s+)|([^\s\w])/g;
    var out = "", m;
    while ((m = re.exec(src))) {
      var t = m[0], cls = "";
      if (m[1]) cls = "hl-comment";
      else if (m[2]) cls = "hl-string";
      else if (m[3]) cls = "hl-ident";
      else if (m[4]) cls = "hl-num";
      else if (m[5]) { var u = t.toUpperCase(); cls = SQL_KW[u] ? "hl-kw" : (SQL_FN[u] ? "hl-fn" : ""); }
      else if (m[7]) cls = "hl-punc";
      var e = escHtml(t);
      out += cls ? '<span class="' + cls + '">' + e + "</span>" : e;
      if (re.lastIndex === m.index) re.lastIndex++;
    }
    return out;
  }

  function syncHighlight() { refs.hl.innerHTML = highlightSQL(refs.editor.value) + "\n"; }
  function setEditorValue(v) { refs.editor.value = v; syncHighlight(); }

  function buildUI() {
    document.body.appendChild(styleTag());

    refs.file = el("input", { type: "file", accept: ".db,.sqlite,.sqlite3,.db3,.s3db,.sl3", style: "display:none" });
    refs.file.addEventListener("change", function () {
      if (refs.file.files && refs.file.files[0]) openFile(refs.file.files[0]);
      refs.file.value = "";
    });

    refs.open = button("Open database", function () { refs.file.click(); }, "primary");
    refs.new = button("New", newDatabase, "", "Create an empty in-memory database");
    refs.download = button("Download", downloadDatabase, "ghost", "Download the current database file");
    refs.download.disabled = true;
    refs.dbLabel = el("span", { class: "dbname", text: "No database" });

    var toolbar = el("div", { class: "toolbar" }, [
      refs.open, refs.new, refs.download,
      el("span", { class: "spacer" }),
      refs.dbLabel,
    ]);

    refs.schema = el("div", { class: "schema" });
    var sidebar = el("aside", { class: "sidebar" }, [
      el("div", { class: "side-head", text: "Schema" }),
      refs.schema,
    ]);

    refs.editor = el("textarea", { class: "editor", spellcheck: "false", placeholder: "SELECT * FROM sqlite_master;" });
    refs.hl = el("pre", { class: "editor-hl" });
    refs.editorWrap = el("div", { class: "editor-wrap" }, [refs.hl, refs.editor]);
    refs.editor.addEventListener("input", syncHighlight);
    refs.editor.addEventListener("scroll", function () { refs.hl.scrollTop = refs.editor.scrollTop; refs.hl.scrollLeft = refs.editor.scrollLeft; });
    refs.editor.addEventListener("keydown", function (e) {
      if ((e.ctrlKey || e.metaKey) && e.key === "Enter") { e.preventDefault(); runQuery(); }
    });

    refs.pageSize = el("select", { class: "select", title: "Rows per page" });
    [25, 50, 100, 200, 500].forEach(function (n) {
      var o = el("option", { value: String(n), text: n + " rows" });
      if (n === pageSize) o.selected = true;
      refs.pageSize.appendChild(o);
    });
    refs.pageSize.addEventListener("change", function () { pageSize = parseInt(refs.pageSize.value, 10); reload(); });

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
      el("section", { class: "editor-pane" }, [refs.editorWrap, runbar]),
      el("section", { class: "result-pane" }, [refs.results]),
    ]);

    refs.status = el("div", { class: "status" }, [el("span", { class: "dot" }), el("span", { class: "status-text", text: "Starting…" })]);

    refs.app = el("div", { class: "app" }, [toolbar, el("div", { class: "body" }, [sidebar, main]), refs.status]);
    document.body.appendChild(refs.file);
    document.body.appendChild(refs.app);

    setupDrop();
    refs.open.disabled = true;
    refs.new.disabled = true;
    renderLoading("Loading SQLite engine…");
  }

  function renderLoading(text) {
    refs.results.innerHTML = "";
    refs.results.appendChild(el("div", { class: "loading" }, [el("div", { class: "spinner" }), el("div", { text: text || "Loading…" })]));
  }

  function setupDrop() {
    var overlay = el("div", { class: "drop-overlay", text: "Drop a SQLite file to open" });
    refs.app.appendChild(overlay);
    var depth = 0;
    refs.app.addEventListener("dragenter", function (e) { e.preventDefault(); depth++; refs.app.classList.add("dragging"); });
    refs.app.addEventListener("dragover", function (e) { e.preventDefault(); });
    refs.app.addEventListener("dragleave", function () { if (--depth <= 0) { depth = 0; refs.app.classList.remove("dragging"); } });
    refs.app.addEventListener("drop", function (e) {
      e.preventDefault(); depth = 0; refs.app.classList.remove("dragging");
      if (e.dataTransfer && e.dataTransfer.files && e.dataTransfer.files[0]) openFile(e.dataTransfer.files[0]);
    });
  }

  async function initEngine() {
    if (typeof initSqlJs !== "function") throw new Error("sql.js engine not loaded");
    SQL = await initSqlJs({ wasmBinary: await sc.asset("sql-wasm.wasm") });
  }

  async function openFile(file) {
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
    closeDB();
    db = new SQL.Database();
    dbName = "untitled.db";
    afterOpen();
    setStatus("New empty database", "ok");
  }

  function afterOpen() {
    dirty = false;
    refs.download.disabled = true;
    refs.dbLabel.textContent = dbName;
    setEditorValue("");
    mode = null; cursor = null; table = null;
    refreshSchema();
    welcome();
  }

  function closeDB() {
    if (cursor && cursor.stmt) { try { cursor.stmt.free(); } catch (e) {} }
    cursor = null; table = null; mode = null;
    if (db) { try { db.close(); } catch (e) {} db = null; }
  }

  function markDirty() { dirty = true; refs.download.disabled = false; }

  function reload() {
    if (mode === "table") loadTablePage(table.page || 0);
    else if (mode === "query") renderCursorPage(0);
  }

  function refreshSchema() {
    refs.schema.innerHTML = "";
    if (!db) return;
    var tables = listCol("SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%' ORDER BY name");
    var views = listCol("SELECT name FROM sqlite_master WHERE type='view' ORDER BY name");
    if (tables.length) refs.schema.appendChild(schemaGroup("Tables", tables, true));
    if (views.length) refs.schema.appendChild(schemaGroup("Views", views, false));
    if (!tables.length && !views.length) refs.schema.appendChild(el("div", { class: "side-empty", text: "No tables yet" }));
  }

  function schemaGroup(title, names, editable) {
    var group = el("div", { class: "schema-group" }, [el("div", { class: "schema-group-head", text: title + " (" + names.length + ")" })]);
    names.forEach(function (name) {
      var cols = el("div", { class: "cols" });
      cols.style.display = "none";
      var expanded = false;
      var caret = el("span", { class: "caret", text: "▸" });
      var head = el("div", { class: "table-row" }, [caret, el("span", { class: "table-name", text: name, title: name })]);
      caret.addEventListener("click", function (e) {
        e.stopPropagation();
        expanded = !expanded;
        caret.textContent = expanded ? "▾" : "▸";
        cols.style.display = expanded ? "block" : "none";
        if (expanded && !cols.dataset.loaded) {
          columnsOf(name).forEach(function (c) {
            cols.appendChild(el("div", { class: "col" }, [el("span", { class: "col-name", text: c.name }), el("span", { class: "col-type", text: c.type || "" })]));
          });
          cols.dataset.loaded = "1";
        }
      });
      head.addEventListener("click", function () { editable ? openTable(name) : openView(name); });
      group.appendChild(el("div", {}, [head, cols]));
    });
    return group;
  }

  function listCol(sql) {
    try { var r = db.exec(sql); return r.length ? r[0].values.map(function (v) { return v[0]; }) : []; } catch (e) { return []; }
  }

  function columnsOf(name) {
    var out = [];
    try {
      var r = db.exec('PRAGMA table_info(' + ident(name) + ")");
      if (r.length) r[0].values.forEach(function (v) { out.push({ name: v[1], type: v[2], notnull: v[3], dflt: v[4], pk: v[5] }); });
    } catch (e) {}
    return out;
  }

  function openView(name) {
    setEditorValue("SELECT * FROM " + ident(name));
    runQuery();
  }

  function openTable(name) {
    var cols = columnsOf(name);
    if (!cols.length) { openView(name); return; }
    table = { name: name, cols: cols, page: 0, hasRowid: true };
    setEditorValue("SELECT * FROM " + ident(name));
    mode = "table";
    selection = {};
    loadTablePage(0);
  }

  function loadTablePage(pageIndex) {
    mode = "table";
    table.page = pageIndex;
    selection = {};
    var total = 0;
    try { var c = db.exec("SELECT count(*) FROM " + ident(table.name)); total = c.length ? c[0].values[0][0] : 0; } catch (e) {}
    var offset = pageIndex * pageSize;
    var names = table.cols.map(function (c) { return c.name; });
    var rows = [];
    try {
      var idsel = table.hasRowid ? "rowid AS __rid, " : "";
      var res = db.exec("SELECT " + idsel + table.cols.map(function (c) { return ident(c.name); }).join(", ") + " FROM " + ident(table.name) + " LIMIT " + pageSize + " OFFSET " + offset);
      if (res.length) rows = res[0].values;
    } catch (e) {
      if (table.hasRowid) { table.hasRowid = false; return loadTablePage(pageIndex); }
      renderError(msg(e)); return;
    }
    renderTableGrid(names, rows, {
      start: offset, total: total,
      hasPrev: pageIndex > 0, hasNext: offset + rows.length < total,
      onPrev: function () { loadTablePage(pageIndex - 1); },
      onNext: function () { loadTablePage(pageIndex + 1); },
    });
    setStatus("Table " + table.name + " · " + total + " row(s)", "ok");
  }

  function runQuery() {
    if (!db) { setStatus("Open or create a database first", "error"); return; }
    var text = refs.editor.value.trim();
    if (!text) return;
    if (cursor && cursor.stmt) { try { cursor.stmt.free(); } catch (e) {} cursor = null; }
    var t0 = performance.now();
    try {
      if (isSingleTableSelect(text)) {
        var tn = singleTableName(text);
        if (tn && tableExists(tn)) { openTable(tn); return; }
      }
      if (isReadQuery(text)) {
        mode = "query";
        var stmt = db.prepare(text.replace(/;+\s*$/, ""));
        cursor = { stmt: stmt, columns: stmt.getColumnNames() };
        renderCursorPage(0);
        setStatus("Query ran in " + since(t0), "ok");
      } else {
        mode = null;
        db.run(text);
        markDirty();
        renderMessage(db.getRowsModified() + " row(s) affected");
        refreshSchema();
        setStatus("Executed · " + db.getRowsModified() + " row(s) affected · " + since(t0), "ok");
      }
    } catch (e) {
      renderError(msg(e));
      setStatus("Error", "error");
    }
  }

  function renderCursorPage(pageIndex) {
    var stmt = cursor.stmt;
    stmt.reset();
    var skip = pageIndex * pageSize;
    for (var i = 0; i < skip; i++) if (!stmt.step()) break;
    var rows = [];
    while (rows.length < pageSize && stmt.step()) rows.push(stmt.get());
    var hasNext = stmt.step();
    cursor.page = pageIndex;
    renderReadonlyGrid(cursor.columns, rows, {
      start: skip, total: null,
      hasPrev: pageIndex > 0, hasNext: hasNext,
      onPrev: function () { renderCursorPage(pageIndex - 1); },
      onNext: function () { renderCursorPage(pageIndex + 1); },
    });
  }

  function isReadQuery(text) {
    var t = text.replace(/^\s*(--[^\n]*\n|\/\*[\s\S]*?\*\/)\s*/g, "").trim().toLowerCase();
    if (!/^(select|with|pragma|explain|values)\b/.test(t)) return false;
    return text.replace(/;+\s*$/, "").indexOf(";") === -1;
  }
  function isSingleTableSelect(text) {
    return /^\s*select\s+\*\s+from\s+("?[A-Za-z_][A-Za-z0-9_]*"?)\s*;?\s*$/i.test(text);
  }
  function singleTableName(text) {
    var m = text.match(/from\s+"?([A-Za-z_][A-Za-z0-9_]*)"?/i);
    return m ? m[1] : null;
  }
  function tableExists(name) {
    try { var r = db.exec("SELECT 1 FROM sqlite_master WHERE type='table' AND name=" + "'" + name.replace(/'/g, "''") + "'"); return r.length > 0; } catch (e) { return false; }
  }

  function baseGrid(columns, rows, nav, editable, rowMeta) {
    refs.results.innerHTML = "";
    var bar = el("div", { class: "gridbar" });
    if (editable) {
      bar.appendChild(button("＋ Add row", startAddRow, "ghost"));
      var del = button("Delete", deleteSelected, "ghost danger");
      del.disabled = true;
      refs.delBtn = del;
      bar.appendChild(del);
    }
    bar.appendChild(el("span", { class: "spacer" }));
    bar.appendChild(el("span", { class: "muted", text: editable ? "Double-click a cell to edit" : "Read-only result" }));
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
      var rid = rowMeta ? rowMeta(row) : null;
      if (editable) {
        var cb = el("input", { type: "checkbox" });
        cb.addEventListener("change", function () { if (cb.checked) selection[rid] = true; else delete selection[rid]; refreshDelBtn(); });
        tr.appendChild(el("td", { class: "selcol" }, [cb]));
      }
      tr.appendChild(el("td", { class: "rownum", text: String(nav.start + r + 1) }));
      var offset = editable ? 1 : 0;
      row.slice(offset).forEach(function (cell, ci) {
        var td = cellNode(cell);
        if (editable) {
          var col = columns[ci];
          td.addEventListener("dblclick", function () { editCell(td, table.name, col, cell, rid); });
          td.title = "Double-click to edit";
        }
        tr.appendChild(td);
      });
      tbody.appendChild(tr);
    });
    tableEl.appendChild(tbody);
    scroll.appendChild(tableEl);
    if (!rows.length) scroll.appendChild(el("div", { class: "empty", text: "No rows" }));
    refs.results.appendChild(scroll);
    refs.results.appendChild(pager(rows.length, nav));
    lastGrid = { columns: columns, rows: rows.map(function (r) { return editable ? r.slice(1) : r; }) };
  }

  function renderTableGrid(columns, rows, nav) {
    baseGrid(columns, rows, nav, true, function (row) { return row[0]; });
  }
  function renderReadonlyGrid(columns, rows, nav) {
    baseGrid(columns, rows, nav, false, null);
  }

  function refreshDelBtn() {
    if (refs.delBtn) refs.delBtn.disabled = Object.keys(selection).length === 0;
  }

  function editCell(td, tableName, col, oldVal, rid) {
    if (td.querySelector("input")) return;
    var input = el("input", { class: "cell-input", type: "text" });
    input.value = oldVal === null ? "" : (oldVal instanceof Uint8Array ? "" : String(oldVal));
    td.textContent = "";
    td.appendChild(input);
    input.focus();
    input.select();
    var done = false;
    function commit(save) {
      if (done) return; done = true;
      var raw = input.value;
      if (!save) { td.replaceWith(cellNodeEditable(oldVal, tableName, col, rid)); return; }
      var val = coerce(raw, col.type, col.notnull);
      try {
        db.run("UPDATE " + ident(tableName) + " SET " + ident(col.name) + " = ? WHERE rowid = ?", [val, rid]);
        markDirty();
        td.replaceWith(cellNodeEditable(val, tableName, col, rid));
        setStatus("Updated " + tableName + "." + col.name, "ok");
      } catch (e) {
        setStatus("Update failed: " + msg(e), "error");
        td.replaceWith(cellNodeEditable(oldVal, tableName, col, rid));
      }
    }
    input.addEventListener("keydown", function (e) { if (e.key === "Enter") commit(true); else if (e.key === "Escape") commit(false); });
    input.addEventListener("blur", function () { commit(true); });
  }

  function cellNodeEditable(val, tableName, col, rid) {
    var td = cellNode(val);
    td.title = "Double-click to edit";
    td.addEventListener("dblclick", function () { editCell(td, tableName, col, val, rid); });
    return td;
  }

  function startAddRow() {
    if (!table) return;
    var form = el("div", { class: "addrow" });
    var inputs = {};
    table.cols.forEach(function (c) {
      var wrap = el("label", { class: "addfield" }, [el("span", { class: "addlabel", text: c.name + (c.notnull && !c.dflt ? " *" : "") })]);
      var inp = el("input", { class: "cell-input", type: "text", placeholder: c.type || "" });
      inputs[c.name] = inp;
      wrap.appendChild(inp);
      form.appendChild(wrap);
    });
    var actions = el("div", { class: "addactions" }, [
      button("Insert", function () { doInsert(inputs); }, "primary"),
      button("Cancel", function () { loadTablePage(table.page || 0); }, "ghost"),
    ]);
    refs.results.innerHTML = "";
    refs.results.appendChild(el("div", { class: "addwrap" }, [el("div", { class: "addhead", text: "New row in " + table.name }), form, actions]));
    var first = form.querySelector("input");
    if (first) first.focus();
  }

  function doInsert(inputs) {
    var cols = [], marks = [], vals = [];
    table.cols.forEach(function (c) {
      var raw = inputs[c.name].value;
      if (raw === "" && (c.dflt !== null || !c.notnull)) return;
      cols.push(ident(c.name)); marks.push("?"); vals.push(coerce(raw, c.type, c.notnull));
    });
    try {
      var sql = cols.length
        ? "INSERT INTO " + ident(table.name) + " (" + cols.join(", ") + ") VALUES (" + marks.join(", ") + ")"
        : "INSERT INTO " + ident(table.name) + " DEFAULT VALUES";
      db.run(sql, vals);
      markDirty();
      setStatus("Row inserted into " + table.name, "ok");
      loadTablePage(table.page || 0);
    } catch (e) {
      setStatus("Insert failed: " + msg(e), "error");
    }
  }

  function deleteSelected() {
    var ids = Object.keys(selection);
    if (!ids.length || !table) return;
    confirmDialog("Delete " + ids.length + " row(s) from " + table.name + "? This cannot be undone.", function () {
      try {
        db.run("DELETE FROM " + ident(table.name) + " WHERE rowid IN (" + ids.map(function () { return "?"; }).join(",") + ")", ids.map(Number));
        markDirty();
        setStatus(ids.length + " row(s) deleted from " + table.name, "ok");
        loadTablePage(table.page || 0);
      } catch (e) {
        setStatus("Delete failed: " + msg(e), "error");
      }
    }, true);
  }

  function coerce(raw, type, notnull) {
    if (raw === "" ) return notnull ? "" : null;
    var t = (type || "").toUpperCase();
    if (/INT|REAL|FLOA|DOUB|NUM|DEC/.test(t)) { var n = Number(raw); return isNaN(n) ? raw : n; }
    return raw;
  }

  function pager(count, nav) {
    var from = count ? nav.start + 1 : 0;
    var to = nav.start + count;
    var label = nav.total != null ? from + "–" + to + " of " + nav.total : "Rows " + from + "–" + to;
    var prev = button("‹ Prev", nav.onPrev, "ghost");
    var next = button("Next ›", nav.onNext, "ghost");
    prev.disabled = !nav.hasPrev;
    next.disabled = !nav.hasNext;
    return el("div", { class: "pager" }, [el("span", { class: "range", text: label }), el("span", { class: "spacer" }), prev, next]);
  }

  function cellNode(cell) {
    if (cell === null) return el("td", { class: "null", text: "NULL" });
    if (cell instanceof Uint8Array) return el("td", { class: "blob", text: "‹blob " + cell.length + " B›" });
    return el("td", { text: String(cell) });
  }

  function renderMessage(text) { refs.results.innerHTML = ""; refs.results.appendChild(el("div", { class: "notice", text: text })); lastGrid = null; }
  function renderError(text) {
    refs.results.innerHTML = "";
    refs.results.appendChild(el("div", { class: "error-box" }, [el("div", { class: "error-title", text: "Query error" }), el("pre", { class: "error-msg", text: text })]));
    lastGrid = null;
  }

  var lastGrid = null;

  function welcome() {
    refs.results.innerHTML = "";
    refs.results.appendChild(el("div", { class: "welcome" }, [
      el("div", { class: "welcome-title", text: db ? "Ready" : "SQLite Explorer" }),
      el("div", { class: "welcome-sub", text: db ? "Pick a table on the left to browse and edit it, or write SQL and press Run." : "Open a .sqlite/.db file or create a new database. Everything runs in your browser — the file is never uploaded." }),
    ]));
  }

  function exportCSV() {
    if (!lastGrid || !lastGrid.rows.length) { setStatus("Nothing to export", "error"); return; }
    var esc = function (v) {
      if (v === null || v === undefined) return "";
      var s = v instanceof Uint8Array ? "" : String(v);
      return /[",\n]/.test(s) ? '"' + s.replace(/"/g, '""') + '"' : s;
    };
    var lines = [lastGrid.columns.map(esc).join(",")];
    lastGrid.rows.forEach(function (row) { lines.push(row.map(esc).join(",")); });
    saveBlob(new Blob([lines.join("\n")], { type: "text/csv" }), "export.csv");
  }

  function downloadDatabase() {
    if (!db) return;
    try { saveBlob(new Blob([db.export()], { type: "application/octet-stream" }), dbName || "database.db"); }
    catch (e) { setStatus("Export failed: " + msg(e), "error"); }
  }

  function saveBlob(blob, filename) {
    var url = URL.createObjectURL(blob);
    var a = el("a", { href: url, download: filename });
    document.body.appendChild(a); a.click();
    setTimeout(function () { URL.revokeObjectURL(url); a.remove(); }, 0);
  }

  function setStatus(text, kind) {
    refs.status.className = "status" + (kind ? " " + kind : "");
    refs.status.querySelector(".status-text").textContent = text;
  }
  function since(t0) { return (performance.now() - t0).toFixed(1) + " ms"; }
  function msg(e) { return (e && (e.message || e.toString())) || "unknown error"; }
  function fmtBytes(n) { if (n < 1024) return n + " B"; if (n < 1048576) return (n / 1024).toFixed(1) + " KB"; return (n / 1048576).toFixed(1) + " MB"; }

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
      ".muted{color:var(--muted);font-size:12px}",
      ".btn{background:var(--panel2);color:var(--text);border:1px solid var(--border);border-radius:7px;padding:6px 13px;cursor:pointer;font:inherit;transition:border-color .12s}",
      ".btn:hover:not(:disabled){border-color:var(--accent)}",
      ".btn:disabled{opacity:.4;cursor:default}",
      ".btn.primary{background:var(--accent);color:#04121f;border-color:transparent;font-weight:600}",
      ".btn.ghost{background:transparent}",
      ".btn.danger{color:var(--danger)}",
      ".btn.danger:hover:not(:disabled){border-color:var(--danger)}",
      ".select{background:var(--panel2);color:var(--text);border:1px solid var(--border);border-radius:7px;padding:6px 8px;font:inherit;cursor:pointer}",
      ".body{flex:1;display:flex;min-height:0}",
      ".sidebar{width:220px;flex-shrink:0;border-right:1px solid var(--border);background:var(--panel);display:flex;flex-direction:column;min-height:0}",
      ".side-head{padding:10px 12px;font-size:11px;letter-spacing:.06em;text-transform:uppercase;color:var(--muted);border-bottom:1px solid var(--border)}",
      ".schema{overflow:auto;padding:6px 4px;flex:1}",
      ".schema-group{margin-bottom:8px}",
      ".schema-group-head{padding:4px 10px;font-size:11px;color:var(--muted);text-transform:uppercase}",
      ".table-row{display:flex;align-items:center;gap:4px;padding:4px 8px;border-radius:6px;cursor:pointer}",
      ".table-row:hover{background:var(--panel2)}",
      ".caret{width:14px;color:var(--muted);font-size:10px;text-align:center}",
      ".table-name{overflow:hidden;text-overflow:ellipsis;white-space:nowrap}",
      ".cols{padding:2px 0 4px 22px}",
      ".col{display:flex;justify-content:space-between;gap:8px;padding:2px 10px 2px 0;color:var(--muted);font-size:12px}",
      ".col-name{color:var(--text);overflow:hidden;text-overflow:ellipsis;white-space:nowrap}",
      ".col-type{font-size:11px;opacity:.8;flex-shrink:0}",
      ".side-empty{padding:12px;color:var(--muted);font-size:12px}",
      ".main{flex:1;display:flex;flex-direction:column;min-width:0;min-height:0}",
      ".editor-pane{display:flex;flex-direction:column;flex-shrink:0}",
      ".editor-wrap{position:relative;background:var(--panel)}",
      ".editor,.editor-hl{margin:0;border:0;padding:9px 14px;font:13px/1.5 ui-monospace,SFMono-Regular,Menlo,Consolas,monospace;white-space:pre-wrap;word-break:break-word;tab-size:4}",
      ".editor-hl{position:absolute;inset:0;overflow:hidden;pointer-events:none;color:var(--text);background:transparent}",
      ".editor{position:relative;display:block;width:100%;height:76px;min-height:36px;resize:vertical;background:transparent;color:transparent;caret-color:var(--text);outline:none}",
      ".hl-kw{color:#7dd3fc;font-weight:600}",
      ".hl-fn{color:#c4b5fd}",
      ".hl-string{color:#86efac}",
      ".hl-ident{color:#fca5a5}",
      ".hl-num{color:#fdba74}",
      ".hl-comment{color:var(--muted);font-style:italic}",
      ".hl-punc{color:var(--muted)}",
      'body[data-theme="light"] .hl-kw{color:#0369a1}',
      'body[data-theme="light"] .hl-fn{color:#7c3aed}',
      'body[data-theme="light"] .hl-string{color:#15803d}',
      'body[data-theme="light"] .hl-ident{color:#b91c1c}',
      'body[data-theme="light"] .hl-num{color:#c2410c}',
      ".runbar{display:flex;gap:10px;align-items:center;padding:7px 12px;border-top:1px solid var(--border);border-bottom:1px solid var(--border);background:var(--panel)}",
      ".kbd{color:var(--muted);font-size:11px;border:1px solid var(--border);border-radius:5px;padding:1px 6px}",
      ".result-pane{flex:1;display:flex;flex-direction:column;min-height:0}",
      ".results{flex:1;display:flex;flex-direction:column;min-height:0;overflow:hidden}",
      ".gridbar{display:flex;gap:8px;align-items:center;padding:6px 12px;border-bottom:1px solid var(--border);background:var(--panel)}",
      ".grid-scroll{flex:1;overflow:auto}",
      ".grid{border-collapse:collapse;width:100%;font-size:12.5px}",
      ".grid th,.grid td{border-bottom:1px solid var(--border);border-right:1px solid var(--border);padding:5px 10px;text-align:left;white-space:nowrap;max-width:460px;overflow:hidden;text-overflow:ellipsis}",
      ".grid thead th{position:sticky;top:0;background:var(--head);z-index:1;font-weight:600}",
      ".grid .rownum{color:var(--muted);text-align:right;background:var(--panel)}",
      ".grid .selcol{width:34px;text-align:center}",
      ".grid tbody tr:hover td{background:var(--panel2)}",
      ".grid td.null{color:var(--muted);font-style:italic}",
      ".grid td.blob{color:var(--accent)}",
      ".cell-input{width:100%;min-width:80px;background:var(--panel);color:var(--text);border:1px solid var(--accent);border-radius:4px;padding:3px 6px;font:inherit;outline:none}",
      ".addwrap{padding:16px;overflow:auto}",
      ".addhead{font-weight:600;margin-bottom:12px}",
      ".addrow{display:grid;grid-template-columns:repeat(auto-fill,minmax(220px,1fr));gap:10px}",
      ".addfield{display:flex;flex-direction:column;gap:4px}",
      ".addlabel{font-size:11px;color:var(--muted)}",
      ".addactions{display:flex;gap:8px;margin-top:14px}",
      ".pager{display:flex;align-items:center;gap:8px;padding:7px 12px;border-top:1px solid var(--border);background:var(--panel)}",
      ".range{color:var(--muted);font-size:12px}",
      ".empty,.notice,.welcome{padding:26px;text-align:center;color:var(--muted)}",
      ".welcome-title{font-size:15px;color:var(--text);margin-bottom:6px;font-weight:600}",
      ".welcome-sub{max-width:52ch;margin:0 auto}",
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
      ".modal-overlay{position:fixed;inset:0;display:flex;align-items:center;justify-content:center;background:rgba(2,6,23,.6);z-index:50}",
      ".modal{background:var(--panel);border:1px solid var(--border);border-radius:10px;padding:20px;max-width:400px;box-shadow:0 10px 40px rgba(0,0,0,.4)}",
      ".modal-msg{margin-bottom:16px;line-height:1.5}",
      ".modal-actions{display:flex;gap:8px;justify-content:flex-end}",
      ".btn.danger-solid{background:var(--danger);color:#fff;border-color:transparent}",
    ].join("\n");
    return el("style", { text: css });
  }

  if (document.readyState === "loading") document.addEventListener("DOMContentLoaded", boot);
  else boot();
})();
