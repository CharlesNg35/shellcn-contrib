# ShellCN ScyllaDB plugin

External ShellCN plugin for ScyllaDB: keyspaces and tables, an editable CQL row
grid, a CQL console, a live token-ring / shard-per-core topology canvas,
per-table compaction, cache and latency metrics, a nodetool-style cluster
status, and a CQL tracing viewer.

It uses the ScyllaDB shard-aware CQL driver (`github.com/scylladb/gocql`, a
drop-in fork of `github.com/gocql/gocql`) and routes every socket through the
gateway transport (`cfg.Net`).

It registers 63 routes: 56 request handlers and 7 streams.

## Build

```sh
CGO_ENABLED=0 go build -trimpath -ldflags "-s -w" -o shellcn-plugin-scylladb ./cmd/shellcn-plugin-scylladb
```

Copy the binary into the gateway plugin directory, restart ShellCN, then enable
it under **Settings -> Protocols**.

## Integration test

```sh
SHELLCN_SCYLLADB_INTEGRATION=1 go test -run Integration ./...
```

The test starts `scylladb/scylla` with `--smp 1 --memory 1G --developer-mode 1
--overprovisioned 1`, or targets `SHELLCN_SCYLLADB_ADDR` when it is set.
