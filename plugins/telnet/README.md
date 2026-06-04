# shellcn-contrib/plugins/telnet

ShellCN-maintained external plugin for legacy Telnet terminal access.

Telnet is useful for older network devices and appliances, but it is not a core
gateway protocol for most installations. This plugin keeps it available through
the Marketplace without growing the built-in gateway binary.

## Build

```sh
make build PLUGIN=telnet
```

The binary is written to `dist/shellcn-plugin-telnet` from the repository root.
