# SQLite (browser)

A ShellCN plugin that opens and queries local **SQLite** database files entirely
in the browser. The database is loaded by the WASM engine inside the sandboxed
panel — **the file is never uploaded to the gateway**.

## How it works

SQLite databases are files, not network services, so there is nothing for the
gateway to dial. The plugin ships the [sql.js](https://sql.js.org) WebAssembly
engine as a static asset and runs the whole explorer client-side:

- The Go side is a stateless plugin with a **no-op session** and a single asset
  route serving the embedded engine and app. There is no bridge route and no
  network egress.
- The panel is a single `PanelWasm`. The user opens a local `.sqlite`/`.db` file
  (file picker or drag-and-drop) and every query executes in the browser.

## Features

- Open a database file or create a new one; download the working copy back to
  disk.
- Schema sidebar with tables and views; expand a table to see its columns.
- SQL editor (Ctrl/⌘ + Enter to run) with an inline error panel.
- **Paginated results** — read queries stream through a prepared-statement cursor
  a page at a time, so large tables never materialise fully or freeze the UI.
  Configurable page size; CSV export of the current page.
- Light/dark theming that follows the gateway theme live.

## Build

```sh
make build PLUGIN=sqlite      # → dist/shellcn-plugin-sqlite
```

Drop the binary into the gateway's plugins directory (e.g. `plugins.d/`) to load
it. The bundled `sql.js` engine (~700 KB) is embedded in the binary.
