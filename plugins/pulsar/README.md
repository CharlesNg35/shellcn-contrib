# ShellCN Pulsar plugin

External ShellCN plugin for Apache Pulsar.

This plugin is maintained in the ShellCN contrib monorepo. It is still a normal
ShellCN plugin: one Go module, one protocol, one release binary.

Topology, stats, policies, schemas, cursor operations, and message browsing use
the Pulsar admin REST API (`/admin/v2`) through the gateway transport. Producing
a message and the live tail use the binary protocol through
`github.com/apache/pulsar-client-go`, which dials the broker itself and therefore
only supports the direct transport.

## Build

```sh
CGO_ENABLED=0 go build -trimpath -ldflags "-s -w" -o shellcn-plugin-pulsar ./cmd/shellcn-plugin-pulsar
```

Copy the binary into the gateway plugin directory, restart ShellCN, then enable
it under **Settings -> Protocols**.

## Integration test

```sh
SHELLCN_PULSAR_INTEGRATION=1 go test ./internal/pulsar -run Integration -v
```

It starts `apachepulsar/pulsar` standalone with Docker (ports 8080 and 6650), or
targets an existing broker through `SHELLCN_PULSAR_ADMIN_URL` and
`SHELLCN_PULSAR_SERVICE_URL`. Without Docker and without those variables the test
skips.
