# ShellCN Couchbase plugin

A Couchbase Server cockpit for ShellCN: buckets, scopes and collections, JSON
documents, SQL++ (N1QL) with an index advisor, live cluster and DCP metrics, a
bucket data map, and XDCR activity.

## Why REST instead of the Couchbase Go SDK

ShellCN plugins must never open their own sockets: every byte leaves through the
transport the gateway hands to `Connect` (`cfg.Net`), so direct and agent
connections share one audited egress path. Couchbase's own Go SDK (`gocb` /
`gocbcore`) exposes no dialer, transport, or `http.RoundTripper` hook —
`gocb.ClusterOptions` has no such field — so it cannot honour that contract.

This plugin therefore speaks the documented Couchbase REST APIs:

| Surface | Endpoint | Default port |
| --- | --- | --- |
| Cluster management, buckets, scopes/collections, stats, logs, XDCR, RBAC | `/pools/default`, `/pools/default/buckets`, `/pools/default/stats/range`, `/logs`, `/indexStatus`, `/settings/rbac/users` | 8091 (18091 TLS) |
| SQL++ (N1QL) queries, documents, indexes, schema inference | `/query/service`, `/admin/active_requests` | 8093 (18093 TLS) |

Both clients are built on `cfg.Net.DialContext`, and the embedded web console is
proxied through the same dialer.

## Capabilities

**Navigation** — `Cluster`, `Buckets → scopes → collections`, `Indexes`, and
`XDCR` in the sidebar. The tree stops at the collection: documents live in a
paginated, searchable grid, never in the tree. A workspace **bucket** scope
filter feeds every bucket-scoped read.

**Cluster** — object detail with health, edition, services, rebalance state and
RAM/disk usage rows; a live metrics panel (ops/s, documents, disk fetches, RAM
quota and disk usage, CPU trend); the node table; RBAC users; and a timeline of
the cluster event log.

**Buckets** — typed list with health, ops/s, quota usage and disk footprint;
detail overview, live metrics (ops/s, gets, sets, DCP backlog, resident ratio,
cache miss rate), scopes, collections, the data map, per-consumer DCP streams,
indexes, and a scoped SQL++ editor. Create, edit (RAM quota, replicas, flush
flag), compact, flush, and delete are audited actions; the destructive ones ask
for confirmation first.

**Documents and KV** — an editable grid whose columns are inferred from a
document sample (typed cells, JSON editor for nested values, staged edits), plus
a per-document view with a JSON code editor that upserts through the KV fast
path (`USE KEYS`) and a metadata sheet showing CAS, expiry, flags, encoding, and
size. A `Schema` tab runs Couchbase's own `INFER` to report every observed
field, its types, coverage, and sample values.

**SQL++** — a `StreamQuery` editor with server-side cancellation, autocomplete
sourced from the cluster's real keyspaces and the active collection's fields,
per-statement auditing (hashed, never the raw text), a confirmation challenge
before any mutating statement, and saved statements in plugin storage.

**Index advisor** — a second query editor that returns index recommendations for
a statement. It asks the server's `ADVISE` first and, because `ADVISE` is an
Enterprise-only feature, falls back to analysing the `EXPLAIN` plan itself: it
flags primary scans and proposes a concrete `CREATE INDEX` built from the
predicate and sort fields the plan actually uses.

**Data map** — a `PanelCanvas` picture of the selected bucket: one card per
scope, one bar per collection sized by its share of documents. It is keyboard
navigable (arrows / Home / End), exposes a labelled hit region per collection,
announces the selection to screen readers, and follows the workspace light/dark
theme.

**XDCR** — replications with status, backlog and throughput, remote cluster
references, and a `PanelTimeline` of replication, rebalance and failover events
from the cluster log, live-updated by a `StreamResource` watch.

**Live lists** — one generic `StreamResource` watch diffs each resource list by
identity and pushes only real additions, changes, and removals.

## Safety

- **Read-only mode** (default on) blocks every bucket, scope, collection,
  document, and index mutation, and rejects mutating SQL++ before it is sent.
- **Write confirmation** (default on) makes the SQL++ editor return a
  confirmation challenge for any statement that is not a pure read.
- **Redaction** masks secret-looking document fields and result columns in the
  grid, the document view, inferred schema samples, and query results; redacted
  fields cannot be edited.
- **Identifiers** (bucket, scope, collection, index) are whitelisted against
  Couchbase's own naming rules and back-quoted; **values** (document ids, search
  terms, limits) are always bound as query arguments.
- `scan_consistency` defaults to `request_plus` so an operator always sees the
  writes they just made.

## Connection form

| Field | Notes |
| --- | --- |
| Host, management port, query port | Any cluster node; TLS uses 18091/18093. |
| Default bucket | Pre-selects the workspace bucket picker. |
| Authentication | Password, or a reusable `db_password` credential whose identity supplies the username. |
| TLS mode | `disable`, `require`, `verify-ca`, `verify-full` (+ CA bundle). |
| Safety | Read-only, write confirmation, scan consistency, timeout, page limit, redacted fields. |

## Tests

```sh
gofmt -l .
go vet ./...
go test -race ./...
SHELLCN_COUCHBASE_INTEGRATION=1 go test -run Integration ./...
```

The integration test starts `couchbase:community-7.6.2`, provisions the node
through the documented initialization sequence (`/node/controller/setupServices`,
`/pools/default`, `/settings/web`, `/settings/indexes`), and drives every route
against it, asserting full route coverage. Point it at an existing cluster with
`SHELLCN_COUCHBASE_ENDPOINT=host:8091` (optionally
`SHELLCN_COUCHBASE_QUERY_PORT`). It skips cleanly when neither Docker nor an
endpoint is available.
