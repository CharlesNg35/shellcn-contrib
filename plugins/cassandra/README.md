# ShellCN Cassandra plugin

External ShellCN plugin for Cassandra.

This plugin is maintained in the ShellCN contrib monorepo. It is still a normal
ShellCN plugin: one Go module, one protocol, one release binary.

## Build

```sh
CGO_ENABLED=0 go build -trimpath -ldflags "-s -w" -o shellcn-plugin-cassandra ./cmd/shellcn-plugin-cassandra
```

Copy the binary into the gateway plugin directory, restart ShellCN, then enable
it under **Settings -> Protocols**.
