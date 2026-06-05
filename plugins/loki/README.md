# shellcn-contrib/plugins/loki

ShellCN external plugin for [Grafana Loki](https://grafana.com/oss/loki/).

It provides labels, label values, streams, LogQL range queries, index stats,
volume, rule listing, LogQL formatting, and delete-request management through
Loki's HTTP API.

Delete scheduling and cancellation are blocked when the connection is in
read-only mode. Some Loki endpoints, such as volume and delete requests, depend
on server-side Loki configuration.

## Build

```sh
CGO_ENABLED=0 go build -trimpath -ldflags "-s -w" \
  -o shellcn-plugin-loki ./cmd/shellcn-plugin-loki
```

## Update dependencies

```sh
go get github.com/charlesng35/shellcn/sdk@latest
go mod tidy
```

## Integration test

```sh
SHELLCN_LOKI_INTEGRATION=1 go test ./...
```

By default the test starts `grafana/loki` with Docker. To use an existing server
instead:

```sh
SHELLCN_LOKI_INTEGRATION=1 \
SHELLCN_LOKI_ENDPOINT=http://localhost:3100 \
go test ./...
```
