# ShellCN Cloudflare Plugin

Cloudflare operations cockpit for ShellCN.

## Features

- Accounts and zones
- DNS record list, create, update, and delete
- Cache purge by everything, files, tags, hosts, or prefixes
- Rulesets and WAF-focused ruleset views
- Legacy firewall rules and page rules, gated by connection config
- Custom certificates
- Workers routes
- Zone settings with guarded updates
- Cloudflare Tunnel inventory and tunnel configuration
- Audited API explorer for Cloudflare endpoints not yet modeled as resources
- Sandboxed cockpit panel for an at-a-glance overview

## Authentication

Use a stored ShellCN `api_token` credential for production connections. Inline
tokens are supported for one-off connections and are stored as connection-local
secrets.

Recommended read-only token permissions depend on which views are enabled, but a
useful starting point is:

- Account: Cloudflare Tunnel Read
- Account: Account Settings Read
- Zone: Zone Read
- Zone: DNS Read
- Zone: Rulesets Read
- Zone: Workers Routes Read
- Zone: SSL and Certificates Read

Enable write permissions only for the operations you intend to use, such as DNS
edit, cache purge, zone settings edit, or page rule delete. Keep `read_only`
enabled unless the connection is meant to mutate Cloudflare resources.

## Transport

The plugin uses ShellCN direct transport and routes Cloudflare API egress through
the gateway-provided network transport. It does not open a separate process-wide
HTTP client outside the ShellCN connection path.

## Build

```sh
go test ./...
CGO_ENABLED=0 go build -trimpath -ldflags "-s -w" -o shellcn-plugin-cloudflare ./cmd/shellcn-plugin-cloudflare
```
