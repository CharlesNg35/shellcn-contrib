# shellcn-contrib/plugins/qdrant

ShellCN external plugin for [Qdrant](https://qdrant.tech).

It provides collections, collection details, aliases, point browsing, point
upserts/deletes, payload index creation, snapshots, and JSON vector queries. It
talks to Qdrant through the HTTP API and uses the gateway transport, so direct
and agent-backed networking stay under ShellCN control.

Mutating routes are blocked when the connection is in read-only mode.

## Build

```sh
CGO_ENABLED=0 go build -trimpath -ldflags "-s -w" \
  -o shellcn-plugin-qdrant ./cmd/shellcn-plugin-qdrant
```

## Update dependencies

```sh
go get github.com/charlesng35/shellcn/sdk@latest
go mod tidy
```

## Integration test

```sh
SHELLCN_QDRANT_INTEGRATION=1 go test ./...
```

By default the test starts `qdrant/qdrant` with Docker. To use an existing
server instead:

```sh
SHELLCN_QDRANT_INTEGRATION=1 \
SHELLCN_QDRANT_ENDPOINT=http://localhost:6333 \
go test ./...
```
