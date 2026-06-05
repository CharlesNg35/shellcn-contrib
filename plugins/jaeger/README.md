# shellcn-contrib/plugins/jaeger

ShellCN external plugin for [Jaeger](https://www.jaegertracing.io/).

It provides services, operations, trace search, trace detail, and span inspection
through Jaeger Query's stable `/api/v3/*` HTTP API.

## Build

```sh
CGO_ENABLED=0 go build -trimpath -ldflags "-s -w" \
  -o shellcn-plugin-jaeger ./cmd/shellcn-plugin-jaeger
```

## Update dependencies

```sh
go get github.com/charlesng35/shellcn/sdk@latest
go mod tidy
```

## Integration test

```sh
SHELLCN_JAEGER_INTEGRATION=1 go test ./...
```

By default the test starts `jaegertracing/all-in-one` with Docker and seeds a
trace through Zipkin v2. To use an existing Jaeger instance instead:

```sh
SHELLCN_JAEGER_INTEGRATION=1 \
SHELLCN_JAEGER_ENDPOINT=http://localhost:16686 \
SHELLCN_JAEGER_ZIPKIN_ENDPOINT=http://localhost:9411 \
go test ./...
```
