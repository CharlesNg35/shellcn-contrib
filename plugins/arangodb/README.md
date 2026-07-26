# ShellCN ArangoDB plugin

External ShellCN plugin for ArangoDB, the multi-model database: documents,
named graphs, ArangoSearch views and Foxx microservices in one workspace.

## What it exposes

- **Navigation** — databases expand into a fixed set of categories (collections,
  graphs, search views, analyzers, Foxx services, AQL activity). Every category
  is a leaf that opens a paginated list, so the sidebar never grows with data.
- **Documents** — an editable grid with runtime columns discovered from a
  bounded sample, staged edits, server-side sort/search, and a JSON dialog for
  full-document replacement.
- **AQL console** — a `StreamQuery` editor with completion sourced from the live
  collections, graphs and views, per-execution audit, cancellation, and an
  `EXPLAIN <query>` prefix that returns the optimizer plan instead of running it.
- **Graphs** — an interactive explorer (`PanelGraph`) seeded from real edges with
  click-to-expand traversals, plus a live `PanelCanvas` topology map of the named
  graph's vertex collections and edge definitions.
- **Search** — ArangoSearch views with a link table and a JSON definition editor,
  and analyzer create/inspect/delete.
- **Operations** — collection/index/shard inspection, JSON-schema validation
  editing, live `PanelMetrics` for databases, collections and the cluster, a
  cluster/server health view that degrades cleanly on single servers, and the
  running/slow AQL activity feed with a kill action.
- **Foxx** — mounted services with their manifest, Swagger routes,
  configuration, dependencies, README, development-mode toggle and uninstall.
- **Saved AQL** — user-scoped saved queries backed by plugin storage.

## Transport

ArangoDB speaks HTTP, so the driver's connection is built on an
`http.Transport` whose `DialContext` is the gateway's `cfg.Net.DialContext`.
The plugin never opens a socket itself. Only the direct transport is declared.

## Build

```sh
CGO_ENABLED=0 go build -trimpath -ldflags "-s -w" -o shellcn-plugin-arangodb ./cmd/shellcn-plugin-arangodb
```

Copy the binary into the gateway plugin directory, restart ShellCN, then enable
it under **Settings -> Protocols**.

## Tests

```sh
go test -race ./...
SHELLCN_ARANGODB_INTEGRATION=1 go test -run Integration ./...
```

The integration test starts `arangodb/arangodb:3.12` with
`ARANGO_ROOT_PASSWORD`, drives every route through the shared contrib harness,
and asserts full route coverage. Set `SHELLCN_ARANGODB_ENDPOINT` (and optionally
`SHELLCN_ARANGODB_PASSWORD`) to run against an existing deployment instead.
