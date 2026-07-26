# ShellCN Chroma plugin

A ShellCN contrib plugin for [Chroma](https://www.trychroma.com/), built directly
on the **REST v2 API** (`/api/v2/...`). All egress runs through the ShellCN
gateway transport (`cfg.Net.DialContext`); the plugin never opens its own socket.

## Layout

Sidebar tree → `Server` (overview, metrics, databases, collections, tenant,
saved queries) and `Tenants` → databases → the collection list for that database.

### Collection detail

| Tab              | Panel               | What it gives you                                                              |
| ---------------- | ------------------- | ------------------------------------------------------------------------------ |
| Overview         | `PanelObjectDetail` | Identity, dimensions, distance space, index tuning, payload size, metadata.    |
| Records          | `PanelTable`        | Editable grid (document + metadata), server-side document search, bulk delete. |
| Search           | `PanelQueryEditor`  | JSON console over `/query` and `/get`, with completions and saved history.     |
| Embedding map    | `PanelCanvas`       | Deterministic 2D PCA scatter, k-means colouring, nearest-neighbour links.      |
| Distance spaces  | `PanelCanvas`       | The same anchor ranked under `l2`, `cosine`, and `ip`, with rank-flow ribbons. |
| Schema           | `PanelTable`        | Per-key index configuration reported by the server.                            |
| Metrics          | `PanelMetrics`      | Live record count, payload bytes, log position, index catch-up.                |
| History          | `PanelTimeline`     | Every search executed from this connection, with row count and duration.       |

Records open their own detail view: a property sheet (vector, L2 norm, metadata
keys) plus a **Nearest neighbors** table computed with the collection's own
distance space.

## Search console

The editor speaks Chroma-flavoured JSON and picks the right endpoint:

```jsonc
{ "query_embeddings": [[0.1, 0.2, 0.3]], "n_results": 10 }   // → /query
{ "query_id": "doc-42", "n_results": 10 }                    // → /query, by example
{ "query_text": "invoice",  "limit": 50 }                    // → /get, document contains
{ "where": { "tag": { "$eq": "x" } }, "limit": 50 }          // → /get, metadata filter
```

An unfiltered, unlimited fetch on a large collection returns a confirmation
challenge instead of scanning it. Every execution is audited and appended to the
connection's history timeline.

## Connection form

- **Server** — endpoint, tenant, database.
- **Authentication** — none, `x-chroma-token`, bearer, basic, or a stored
  ShellCN credential (API token or basic auth) via `FieldCredentialRef`.
- **TLS** — disable / require / verify-ca / verify-full with a CA override.
- **Safety** — read-only mode (default on), request timeout, page limit, and the
  vector sample size used by the two visual panels.

## API notes

Chroma's v2 surface is not uniform about names versus ids; the plugin resolves
each collection once and then addresses the endpoint that actually works:

| Operation                              | Path segment |
| -------------------------------------- | ------------ |
| `GET /collections/{name}`              | name         |
| `GET /collections/by-id/{id}`          | id           |
| `PUT /collections/{id}`                | id           |
| `DELETE /collections/{name}`           | name         |
| `.../{id}/add,get,query,update,delete` | id           |

`indexing_status` only exists on distributed Chroma; the metrics panel degrades
gracefully when a single-node server does not implement it.

## Development

```sh
export GONOSUMDB=github.com/charlesng35/shellcn,github.com/charlesng35/shellcn/sdk
go build ./...
go test -race ./...
SHELLCN_CHROMA_INTEGRATION=1 go test -run Integration ./...
```

The integration test starts `chromadb/chroma:1.5.9` through the `docker` CLI and
drives every declared route; set `SHELLCN_CHROMA_ENDPOINT` to reuse a running
server instead.
