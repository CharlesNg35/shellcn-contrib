# shellcn-contrib/plugins/weaviate

ShellCN external plugin for [Weaviate](https://weaviate.io).

It exposes the whole cluster as an explorer: a collection tree, schema and
vectorizer configuration, an editable object grid, GraphQL and vector/hybrid
search consoles, cross-reference graphs, a 2D embedding projection, shard and
node telemetry, multi-tenancy, aliases, and backups.

## Capabilities

| Area | What you get |
| --- | --- |
| Schema | Collection list/detail, property table, JSON definition editor with server-side canonical refresh, create/delete |
| Objects | Server-side sorted, filtered, cursor-paged live grid with runtime columns, inline insert/update/delete, a JSON document editor, and batch delete by `where` filter |
| Search | `PanelQueryEditor` for raw GraphQL and a typed JSON console for `fetch`, `nearVector`, `nearText`, `bm25`, and `hybrid` with `where`, `sort`, `autocut`, and named target vectors |
| Relationships | Schema-level cross-reference graph and object-level reference explorer, both expandable; reference create/delete |
| Embeddings | `PanelCanvas` PCA projection of a collection's vectors with k-means colouring, hover/click inspection, keyboard navigation, and screen-reader announcements |
| Cluster | Live `PanelMetrics` for the cluster and per node, node/shard tables, shard READY/READONLY switching |
| Tenancy | Tenant list, create, activate/deactivate/offload, delete |
| Operations | Alias CRUD, backup create/list/status/restore/cancel across every enabled backend |
| Workspace | Connection-scoped saved queries and an activity timeline, both on `rc.Storage` |

All egress goes through `cfg.Net`: the Weaviate client is built on an
`http.Client` whose transport dials with `cfg.Net.DialContext`, so direct and
agent-backed connections share one path and the plugin never opens its own
socket.

Mutating routes are refused when the connection is in read-only mode.

## Driver

[`github.com/weaviate/weaviate-go-client/v5`](https://github.com/weaviate/weaviate-go-client)
provides the REST and GraphQL builders plus the `weaviate/entities/models`
types. The plugin constructs the API groups it needs from a
`connection.Connection` it owns rather than `weaviate.NewClient`, for two
reasons: it avoids pulling the RBAC/go-openapi runtime tree into the binary, and
it can clear the connection's finalizer. In v5 that finalizer sends on an
unbuffered channel only an OIDC refresh goroutine ever reads, so letting it run
after a session closes permanently blocks the process-wide finalizer goroutine.

## Build

```sh
CGO_ENABLED=0 go build -trimpath -ldflags "-s -w" \
  -o shellcn-plugin-weaviate ./cmd/shellcn-plugin-weaviate
```

## Update dependencies

```sh
go get github.com/charlesng35/shellcn/sdk@latest
go mod tidy
```

## Integration test

```sh
SHELLCN_WEAVIATE_INTEGRATION=1 go test -run Integration ./...
```

It starts `cr.weaviate.io/semitechnologies/weaviate:1.38.6` with anonymous
access, `DEFAULT_VECTORIZER_MODULE=none`, and `backup-filesystem` enabled, then
drives every route through the shared contrib harness. To reuse a running
server instead:

```sh
SHELLCN_WEAVIATE_INTEGRATION=1 \
SHELLCN_WEAVIATE_ENDPOINT=http://localhost:8080 \
go test -run Integration ./...
```
