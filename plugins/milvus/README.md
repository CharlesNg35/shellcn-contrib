# ShellCN Milvus plugin

A Milvus vector-database cockpit for ShellCN.

- Databases, collections, partitions, indexes, segments, aliases and RBAC in one
  sidebar tree.
- Schema/field designer, editable entity grid, and a vector-similarity query
  console (ANN search, full-text search, scalar filter query, id lookup).
- 2D embedding projection explorer: PCA or seeded random projection of sampled
  vectors, per-vector-field switching, hover inspection, lasso sampling,
  zoom/pan, keyboard shortcuts.
- Live index-build task progress, load-state/segment metrics, and a session
  activity timeline.

The plugin talks gRPC through the official `github.com/milvus-io/milvus/client/v2`
SDK. All egress is dialled through the gateway transport (`cfg.Net.DialContext`
wired via `grpc.WithContextDialer`), so direct and agent connections share one
code path.

## Build

```sh
go build ./cmd/shellcn-plugin-milvus
```

## Test

```sh
go test -race ./...
SHELLCN_MILVUS_INTEGRATION=1 go test -run Integration ./...
```

The integration test starts `milvusdb/milvus` standalone (embedded etcd, local
storage) through the `docker` CLI, or reuses `SHELLCN_MILVUS_ADDRESS`.
