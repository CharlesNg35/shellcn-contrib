# Contributing

This repo contains ShellCN-maintained plugins that live outside the gateway
binary. It is a source-code repo, not the Marketplace registry.

External contributors can send PRs here for plugins the ShellCN team maintains.
If you are publishing your own plugin, keep it in your own GitHub repo and submit
its manifest to
[shellcn-plugin-registry](https://github.com/CharlesNg35/shellcn-plugin-registry) so it appears
in the Marketplace.

## Add a plugin

1. Start from [shellcn-plugin-starter](https://github.com/CharlesNg35/shellcn-plugin-starter).
2. Put the plugin in `plugins/<name>/`.
3. Keep it self-contained: its own `go.mod`, `cmd/shellcn-plugin-<name>/`,
   implementation package, `README.md`, tests, and release binary.
4. Use the published SDK module:
   ```sh
   go get github.com/charlesng35/shellcn/sdk@latest
   ```
5. Run:
   ```sh
   make fmt
   make test
   ```

## Plugin directory rules

- One protocol per directory.
- One binary per protocol.
- The binary should be named `shellcn-plugin-<name>`.
- The entry point should live at `cmd/shellcn-plugin-<name>/main.go`.
- Do not write to stdout. The plugin handshake uses stdout.
- Send logs to stderr.
- Keep plugins CGO-free unless there is no maintained pure-Go option.
- Use shared helpers only when they reduce real maintenance work across several
  plugins.

## Release

Tag one plugin at a time:

```sh
git tag <plugin-name>-v<version>
git push origin <plugin-name>-v<version>
```

Examples:

```sh
git tag surrealdb-v0.1.0
git tag mssql-v0.1.0
```

The release workflow detects the plugin name from the tag, builds only that
plugin, and uploads release assets.

## Marketplace

After releasing, submit the manifest to
[shellcn-plugin-registry](https://github.com/CharlesNg35/shellcn-plugin-registry). That registry
verifies checksums, executes the binaries through the real plugin handshake, and
builds the Marketplace index consumed by ShellCN.
