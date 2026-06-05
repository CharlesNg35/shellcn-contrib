# shellcn-contrib/plugins/surrealdb

An external (out-of-tree) [ShellCN](https://github.com/CharlesNg35/shellcn)
protocol plugin for [SurrealDB](https://surrealdb.com), built on the plugin SDK
and the official [surrealdb.go](https://github.com/surrealdb/surrealdb.go) driver.
It is a standalone Go program: it imports only the SDK and the driver, and the
gateway runs the compiled binary as a gRPC subprocess.

It is also a worked example of the **full plugin surface** - it exercises every
capability the SDK offers that maps to a database.

## Features (and the SDK capability each exercises)

| Feature in the UI                | SDK capability                              |
| -------------------------------- | ------------------------------------------- |
| Tables list + define/remove      | Unary routes (`GET`/`POST`/`DELETE`)        |
| Records grid (create/edit/delete)| Unary CRUD + a global **scope** filter      |
| SurrealQL **Query editor**       | WebSocket stream route → result grid        |
| Interactive **REPL** terminal    | Bidi stream + `OpenChannel` + **recording** |
| **Live tail** panel              | Server-stream (logs)                        |
| **Open in browser**              | `HTTPProxy` reverse proxy                   |
| Reusable credential on the form  | `FieldCredentialRef` (`db_password`)        |
| Direct **and** agent transport   | `cfg.Net` egress, `AgentProfile`            |

Egress always goes through the gateway transport (`cfg.Net.DialContext`), never a
direct dial - so direct and agent connections share one code path and the gateway
stays the audited choke point.

## Honest limitations

- **No native live queries.** SurrealDB live queries require a *WebSocket*
  connection, and the driver's WebSocket engine does not expose a custom dialer,
  so it cannot be routed through `cfg.Net` without reimplementing the transport.
  This plugin therefore uses the driver's **HTTP** engine (which *does* accept a
  custom `http.Client`) and the "Live tail" panel **polls** the selected table
  every 2s rather than subscribing to a native change feed. Everything else -
  query, CRUD, REPL - is unaffected.
- **Open-in-browser** proxies the SurrealDB HTTP endpoint. SurrealDB does not ship
  a full web UI, so this mainly demonstrates the proxy capability (e.g. `/health`,
  `/version`).

## Build & install

```sh
GOWORK=off CGO_ENABLED=0 go build -trimpath -ldflags "-s -w" -o shellcn-plugin-surrealdb ./cmd/shellcn-plugin-surrealdb
cp surrealdb /path/to/shellcn/plugins.d/surrealdb   # default plugins.dir
# restart the gateway; enable it under Settings → Protocols
```

This plugin depends on released modules directly. To update dependencies, run:

```sh
go get github.com/charlesng35/shellcn/sdk@latest
go get github.com/surrealdb/surrealdb.go@latest
go mod tidy
```

## Try it against a local SurrealDB

```sh
surreal start --user root --pass root memory   # SurrealDB on :8000
```

Then add a SurrealDB connection in the gateway (host `127.0.0.1`, port `8000`,
namespace/database `test`, user/pass `root`).

## Where things live

| File          | What it is                                                   |
| ------------- | ----------------------------------------------------------- |
| `main.go`     | Entry point - `sdk.Serve(&Plugin{})`.                       |
| `manifest.go` | Manifest, routes, `Connect` - what the gateway sees.        |
| `config.go`   | Connection schema and option parsing/validation.           |
| `session.go`  | Per-connection runtime; egress wiring; lifecycle.          |
| `handlers.go` | Unary route handlers (tables, records, CRUD).              |
| `stream.go`   | Query/REPL/change streams, the REPL channel, HTTP proxy.   |
| `docs/`       | The SDK authoring guide (from the starter template).       |

## License

See the main [ShellCN](https://github.com/CharlesNg35/shellcn) repository.
