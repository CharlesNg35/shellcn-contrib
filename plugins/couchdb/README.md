# ShellCN CouchDB plugin

A production CouchDB cockpit for ShellCN: databases, document CRUD with real
`_rev` conflict handling, Mango queries with index reporting, map/reduce design
documents, replication management, and the live `_changes` feed.

## Building

```sh
go build ./cmd/shellcn-plugin-couchdb
```

## Connecting

| Field | Notes |
| --- | --- |
| Host / Port | Direct connections only; an agent supplies the endpoint itself. |
| Authentication | `basic`, `session` (cookie), stored credential (basic or cookie), or `none` for admin party. |
| TLS mode | `disable`, `require`, `verify-ca`, `verify-full` with an optional CA certificate. |
| Read-only mode | Blocks every CouchDB mutation. Saved queries stay editable because they live in ShellCN storage. |
| Page limit | Upper bound for `_all_docs`, view and `_find` pages. |

The plugin never opens a socket. It prefers the gateway's L7 transport
(`cfg.Net.HTTP()`) and otherwise builds an `http.Transport` whose `DialContext`
is `cfg.Net.DialContext`, so direct and agent connections share one audited
egress path.

## What it exposes

- **Server** — welcome/`_up` status, cluster membership, aggregate content
  counts, `_node/_local/_config` with admin hashes and auth secrets redacted, and
  Fauxton embedded through the gateway proxy.
- **Metrics** — live `_stats`/`_system` frames with a derived request rate and an
  Erlang process-memory usage row.
- **Active tasks** — compaction, indexing and replication tasks as a live
  timeline.
- **Databases** — sizes, fragmentation, shard layout, compaction and view
  cleanup, plus an editable `_security` document.
- **Documents** — `_all_docs` in an editable grid with runtime-discovered
  columns. Inline edits merge into the stored document and are rejected with an
  actionable message when the revision is stale.
- **Attachments** — per-document stub listing (type, size, originating
  generation, digest) with per-attachment deletion.
- **Revisions & conflicts** — `_revs_info` as a timeline, a bounded
  database-wide conflict scan, and one-click resolution that keeps a chosen
  revision and deletes the rest through `_bulk_docs`.
- **Mango** — a `_find` console that clamps limits, audits every execution
  without recording the query text, and reports the index CouchDB actually chose
  via `_explain`. Index CRUD and saved queries included.
- **Design documents** — JSON editor with map/reduce validation, view listings,
  raw and grouped-reduce results, and rendered definitions.
- **Replication** — `_replicator` documents joined with `_scheduler/docs`,
  scheduler history as a timeline, and a topology graph of every configured link.
  Credentials embedded in endpoints are redacted everywhere they are displayed.
- **Changes feed** — the real continuous `_changes` feed as a log stream and as
  the grid's watch source.

## Tests

```sh
gofmt -l .
go vet ./...
go test -race ./...
SHELLCN_COUCHDB_INTEGRATION=1 go test -run Integration ./...
```

The integration test starts `couchdb:3.5.2` through the docker CLI, runs the
documented single-node `_cluster_setup`, and drives every declared route. Set
`SHELLCN_COUCHDB_ENDPOINT` to reuse an existing server instead.
