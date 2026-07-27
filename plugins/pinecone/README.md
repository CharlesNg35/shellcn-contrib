# ShellCN Pinecone plugin

A ShellCN contrib plugin for [Pinecone](https://www.pinecone.io/), built directly
on the documented **control-plane** (`https://api.pinecone.io`) and **data-plane**
(`https://{index_host}`) REST APIs, pinned with the `X-Pinecone-Api-Version`
header. All egress runs through the ShellCN gateway transport
(`cfg.Net.DialContext`); the plugin never opens its own socket.

## Layout

Sidebar tree → `Project` (overview, indexes, collections), `Indexes`, and
`Collections`.

### Index detail

| Tab        | Panel               | What it gives you                                                       |
| ---------- | ------------------- | ----------------------------------------------------------------------- |
| Overview   | `PanelObjectDetail` | Identity, host, state, dimensions, metric, placement, tags, live stats.  |
| Namespaces | `PanelTable`        | Server-paged namespace listing with record counts.                       |
| Vectors    | `PanelTable`        | Record IDs paged straight off `/vectors/list`, hydrated with metadata.   |
| Search     | `PanelQueryEditor`  | JSON console over `/query`, with metadata-key completions.               |
| Stats      | `PanelObjectDetail` | `describe_index_stats`: totals, fullness, per-namespace record counts.   |
| Metrics    | `PanelMetrics`      | Live record count, vector payload, index fullness, stats latency.        |

Namespaces and vectors open their own detail views; a vector's detail adds a
**Nearest neighbors** table computed with `/query` by record id.

## Search console

The editor speaks Pinecone's `/query` body:

```jsonc
{ "vector": [0.1, 0.2, 0.3], "topK": 10, "includeMetadata": true }
{ "id": "doc-42", "topK": 10 }                                  // search by example
{ "sparseVector": { "indices": [1], "values": [0.9] } }         // sparse index
{ "id": "doc-42", "filter": { "genre": { "$eq": "drama" } } }   // metadata filter
```

The namespace is filled in from the panel's scope when the body omits it, `topK`
defaults to 10 and is range-checked, and the editor's own control keys
(`confirm`, `cancel`) never reach Pinecone.

## Connection form

- **Project** — control plane URL, API version, default namespace, private
  endpoints toggle.
- **Authentication** — a stored ShellCN API-token credential (`FieldCredentialRef`,
  the default) or an inline secret API key. The key is only ever sent as the
  `Api-Key` request header; it is never logged or persisted by the plugin.
- **TLS** — verify-full / verify-ca / require / disable with a CA override.
- **Safety** — read-only mode (default on), request timeout, page limit.

## Paging

Pinecone splits its listings across two very different shapes, and the plugin
never blurs them:

| Listing                             | Source                           | Paging                                    |
| ----------------------------------- | -------------------------------- | ----------------------------------------- |
| Indexes, collections                | one whole control-plane response | filtered, sorted, then sliced; real total |
| Namespaces (pod), stats breakdowns  | one `describe_index_stats` call  | filtered, sorted, then sliced; real total |
| Namespaces (serverless), vector ids | `paginationToken` server paging  | opaque cursor that resumes mid-page       |

Every cursor is opaque (base64 JSON of the server token plus a row offset) and
also decodes the grid's bare numeric-offset fallback, carrying the remainder
across server pages. Filters and sorts always run before paging, so they span
the whole listing rather than the visible page; an in-memory scan is capped at
`plugin.MaxPageLimit` rows and, when it stops there, drops `Total` and reports
`truncated` while still returning a real continuation cursor so the rest of the
data stays reachable. A token-paged listing never claims a `Total`, because
Pinecone's paginated endpoints do not report one. The vector table's search box
matches record IDs, because that is the only column Pinecone's `/vectors/list`
returns before rows are hydrated; use `prefix` for a server-side narrowing.

Ordering a vector listing by `id`, `index`, or `namespace` sorts and slices
first and hydrates only the page; ordering by a column that only a fetch can
fill (`metadata`, `dimension`) hydrates the whole scan first.

Index vector counts cost one `describe_index_stats` call per index, so they are
filled in for the current page only — unless the grid searches or sorts on them,
in which case every index is described before paging.

## API notes

- The data-plane base URL is discovered from `describe_index` and cached per
  index. It is scheme-less on the wire, so the control plane's scheme is reused
  (a plaintext control plane is never silently upgraded to TLS). `private_host`
  is preferred when the connection asks for private endpoints.
- Pinecone reports the unnamed namespace as `__default__`; that is the plugin's
  default and it is sent verbatim.
- `GET /namespaces` and `GET /vectors/list` are serverless-only. The namespace
  listing and the namespace detail fall back to `describe_index_stats`, which
  reports every namespace for pod indexes too, so those tabs still work. There
  is no equivalent source for record IDs, so the vector table surfaces the
  upstream refusal on a pod index.
- Collections (backups) are a pod-index feature.
- Upserts and id deletes are split into requests of 1000 records, Pinecone's
  per-request ceiling; a fetch is split at 100 ids to bound the query string.
- `describe_index_stats` reports one `indexFullness` figure and no memory or
  storage breakdown, so neither is invented in the UI.
- Both Pinecone error shapes are mapped to SDK sentinels: the control plane's
  `{"error":{"code","message"}}` and the data plane's `{"code","message"}`.

## Tests

```sh
go test ./...                                    # unit tests against an httptest mock of the REST API
SHELLCN_PINECONE_INTEGRATION=1 \
PINECONE_API_KEY=... [PINECONE_INDEX=...] go test ./... -run Integration
```

Pinecone is a hosted service, so the integration test has no container to start:
it is skipped unless both the switch and a real API key are present. Without
`PINECONE_INDEX` it creates a small serverless index, waits for it to become
ready, and deletes it again.
