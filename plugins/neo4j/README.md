# ShellCN Neo4j plugin

External ShellCN plugin for Neo4j.

This plugin is maintained in the ShellCN contrib monorepo. It is still a normal
ShellCN plugin: one Go module, one protocol, one release binary.

## Build

```sh
CGO_ENABLED=0 go build -trimpath -ldflags "-s -w" -o shellcn-plugin-neo4j ./cmd/shellcn-plugin-neo4j
```

Copy the binary into the gateway plugin directory, restart ShellCN, then enable
it under **Settings -> Protocols**.

## Transport

Neo4j is direct-transport only for now. The current Neo4j Go driver exposes
address resolution but no public socket dialer hook; its connection supplier is
internal. A local TCP shim would break verified TLS because the driver derives
the TLS server name from the dial address. Keep this plugin on direct transport
until the driver exposes a public dial hook or ShellCN adopts a maintained driver
adapter.
