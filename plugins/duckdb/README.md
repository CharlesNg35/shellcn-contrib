# DuckDB (browser)

A ShellCN plugin that queries **local** Parquet / CSV / JSON files with
[DuckDB-Wasm](https://duckdb.org/docs/clients/wasm/overview), entirely in the
browser. Files are read client-side and **never uploaded** to the gateway.

## How it works

DuckDB compiles to WebAssembly and runs inside the sandboxed panel. The Go side
is a stateless plugin with a **no-op session** and a single asset route serving
the embedded engine and app — there is no gateway session and no server-side
query execution.

Files are opened via the file picker or drag-and-drop, registered with
`registerFileHandle`, and queried in place. `.duckdb`/`.db` database files are
attached read-only.

## Features

- Query Parquet, CSV, and JSON; attach `.duckdb` database files.
- Schema sidebar (catalog/schema-qualified tables and views, expandable to
  columns), SQL editor (Ctrl/⌘ + Enter), inline error panel.
- **Memory-safe pagination** — read queries stream through `conn.send()` a page at
  a time, so a huge result set never materialises fully or freezes the UI.
- CSV export of the current page; light/dark theming that follows the gateway.

## Notes

- The engine is a self-contained bundle of the DuckDB `eh` (single-threaded,
  exception-handling) build (~35 MB), embedded in the binary.
- **Remote files are not supported**: the WASM panel sandbox restricts outbound
  network access, so DuckDB's `httpfs` (remote HTTP/S3 reads) cannot be used.
  Everything runs against local files.

## Build

```sh
make build PLUGIN=duckdb      # → dist/shellcn-plugin-duckdb
```
