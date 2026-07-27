# ShellCN Consul plugin

External ShellCN plugin for [HashiCorp Consul](https://developer.hashicorp.com/consul).
It turns a Consul datacenter into a browsable, editable workspace inside ShellCN.

This plugin is maintained in the ShellCN contrib monorepo. It is still a normal
ShellCN plugin: one Go module, one protocol, one release binary.

## Features

- **Key/value store** — a lazy folder tree over the `/` separator, a key browser
  with JSON and binary detection, an entry grid with size, flags, lock session,
  and index metadata, plus create, update, check-and-set, delete, and recursive
  folder delete.
- **Catalog** — services with aggregated health, tag groups in the sidebar,
  per-service instances, nodes with their registered services, and full node and
  instance property sheets. Instances can be deregistered from the catalog.
- **Health** — every check by state, by service, or by node, with the check
  definition (HTTP/TCP/gRPC endpoint, interval, timeout, deregister window) and
  its last output on the detail view.
- **Service mesh** — intentions with source, destination, action, precedence, and
  layer-7 rule counts; allow/deny can be toggled and an intention deleted.
- **Coordination** — sessions with their node, behavior, TTL, lock delay, and
  attached checks (destroyable), plus prepared queries with their definition and
  live execution results.
- **Access control** — ACL tokens, policies (with their HCL rules), and roles.
  Token secrets are never returned to the browser. Clusters without ACLs, or
  tokens without the privilege for one area, degrade to an empty panel instead of
  an error.
- **Cluster** — agent self report, the agent's effective runtime configuration
  (Consul redacts its secrets), gossip members, Raft peers, and the federated
  datacenter list.
- **Live changes** — a streaming tail of key additions, removals, and value
  updates under the selected prefix, driven by Consul blocking queries.
- **Connectivity** — direct or agent (TCP) transport, ACL token or stored-token
  credential auth, TLS from `require` to full verification with optional client
  certificates, an optional reverse-proxy path prefix, and Enterprise namespace
  and admin partition scoping.

Writes, deletes, deregistration, intention changes, and session destruction are
blocked while a connection is in read-only mode (the default). Every request is
scoped by the workspace datacenter picker and by the connection's namespace,
partition, and KV root; keys outside the configured KV root are refused.

## Build

```sh
CGO_ENABLED=0 go build -o shellcn-plugin-consul ./cmd/shellcn-plugin-consul
```

## Test

```sh
go test ./...

# Integration test against a throwaway Consul agent (requires docker):
SHELLCN_CONSUL_INTEGRATION=1 go test ./... -run Integration

# Or point it at an existing agent:
SHELLCN_CONSUL_INTEGRATION=1 SHELLCN_CONSUL_ADDR=127.0.0.1:8500 go test ./... -run Integration
```
