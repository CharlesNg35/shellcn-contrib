# ShellCN Trino plugin

External ShellCN plugin for the [Trino](https://trino.io) distributed SQL query
engine, built on [`trino-go-client`](https://github.com/trinodb/trino-go-client).

This plugin is maintained in the ShellCN contrib monorepo. It is still a normal
ShellCN plugin: one Go module, one protocol, one release binary.

## What it gives you

- **Explorer** — catalogs -> schemas -> tables/views, expanded lazily.
- **Data grid** — bounded pagination with server-side search and sort, column
  metadata from `information_schema`, and append-only row inserts.
- **SQL editor** — multi-statement execution with the coordinator's query id and
  live statistics (state, splits, rows, bytes, elapsed) attached to each result.
- **Object detail** — table columns, `SHOW CREATE TABLE` DDL, `SHOW STATS`.
- **Cluster** — coordinator overview, live metrics, `system.runtime.nodes`, and
  `system.runtime.queries` with a kill action.
- **Session** — the connection's effective `SHOW SESSION` properties.

## Behaviour worth knowing

- Every list route pages in SQL (`OFFSET`/`LIMIT`) so search and sort span the
  whole dataset. `Total` is deliberately unset for connector data, because a
  `count(*)` over a Trino table is a second distributed query.
- The data grid is append-only. Trino rows carry no identity, so an `UPDATE` or
  `DELETE` built from a displayed row could not be targeted safely.
- Read-only mode (default on) blocks writes, DDL, `CALL`, `SET SESSION`, and
  `EXPLAIN ANALYZE`; it also blocks the kill-query action, which is a
  coordinator-side mutation.
- Columns matched by the redaction patterns are masked in the grid and are also
  excluded from its free-text filter and sort, so the filter cannot be used to
  probe a value the grid refuses to show.
- A panel opened on a catalog or schema scopes its executions with the
  `X-Trino-Catalog` / `X-Trino-Schema` request headers rather than mutating
  shared session state.
- `system.metadata.catalogs.connector_name` is required, so the catalog list
  needs Trino 393 or newer.

## Authentication

Pick one mode on the connection form:

| Mode                | Material                                              |
| ------------------- | ----------------------------------------------------- |
| User only           | `X-Trino-User` (unauthenticated coordinators)         |
| Password            | inline secret, sent as HTTP basic auth                |
| Stored password     | reusable **Database password** credential             |
| Access token (JWT)  | reusable **Bearer token** credential                  |
| Client certificate  | reusable **TLS client certificate** credential (mTLS) |

Password, stored-password, and access-token modes require a TLS mode other than
`disable`: Trino accepts neither over a plaintext hop, and the plugin refuses
the connection rather than authenticating without the secret it was given.

Secrets are always read through the gateway's credential mechanism or an
encrypted inline field. Neither the credential nor the TLS material reaches the
driver DSN: the password and bearer token are attached to the gateway-provided
HTTP transport, which is also where the client certificate and CA bundle are
applied.

## Build

```sh
CGO_ENABLED=0 go build -trimpath -ldflags "-s -w" -o shellcn-plugin-trino ./cmd/shellcn-plugin-trino
```

Copy the binary into the gateway plugin directory, restart ShellCN, then enable
it under **Settings -> Protocols**.

## Tests

```sh
go test ./...                                   # unit tests, mock coordinator
SHELLCN_TRINO_INTEGRATION=1 go test ./...       # spins trinodb/trino in docker
```

The integration test uses the image's built-in `tpch` and `memory` catalogs, so
it needs no external data. Point it at an existing cluster with
`SHELLCN_TRINO_URL=http://user@host:8080/?catalog=tpch&schema=tiny`.
