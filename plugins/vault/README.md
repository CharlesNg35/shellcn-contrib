# ShellCN Vault plugin

External ShellCN plugin for [HashiCorp Vault](https://developer.hashicorp.com/vault).
It turns a Vault server into a browsable, editable workspace inside ShellCN.

This plugin is maintained in the ShellCN contrib monorepo. It is still a normal
ShellCN plugin: one Go module, one protocol, one release binary. It is built on
the official Go client, `github.com/hashicorp/vault/api`.

## Features

- **Server status** — health (version, cluster, replication mode, standby) and
  seal status (seal type, threshold, key shares, unseal progress, storage).
- **Secret engines** — every `sys/mounts` entry with its type, accessor, running
  plugin version, and the full tune configuration (lease TTLs, audit HMAC
  exclusions, passthrough headers, listing visibility).
- **KV v1 and KV v2 browsing** — a lazy folder tree per KV mount plus a paged
  secret list, with read/write/delete from the key-value panel. KV v2 adds the
  version history, per-version state (active / deleted / destroyed), secret
  metadata (current and oldest version, max versions, CAS, custom metadata),
  check-and-set writes, version-scoped soft delete, undelete, destroy, and full
  metadata purge.
- **Auth methods** — every `sys/auth` mount with its accessor, description, and
  tune configuration.
- **Policies** — list ACL policies and open the HCL in a code editor that saves
  back to Vault; create and delete policies. `root` and `default` are protected.
- **Tokens** — walk the accessor index, look accessors up (display name,
  policies, TTL, uses, orphan flag, entity, issue/expire time), create tokens,
  and revoke by accessor.
- **Leases** — a bounded breadth-first walk of the `sys/leases/lookup` tree with
  per-lease TTL and expiry, and single-lease revocation.
- **Namespaces** — an Enterprise namespace picker wired into every read as a
  scope filter; community editions simply report an empty list.
- **Audit devices** — enabled devices with their type, options, and local flag.
- **Connectivity** — direct or agent (TCP) transport; token, stored-token, or
  AppRole authentication; TLS from `require` to full verification with an
  optional CA bundle.

Every write, revoke, and destroy is blocked while a connection is in read-only
mode (the default).

## Credentials

Secrets never live in the connection config in plaintext:

- **Stored token** — a reusable `api_token` credential.
- **Token** — an inline `Secret` field, encrypted at rest by the gateway.
- **AppRole** — a plugin-declared `vault_approle` credential kind holding a
  public `role_id` and a secret `secret_id`. The plugin logs in at connect time
  against the configured AppRole mount and keeps the issued client token in
  memory only.

`VAULT_TOKEN` and `VAULT_NAMESPACE` from the gateway host's environment are
explicitly cleared so a connection can only ever use its own credentials.

## Pagination

Vault does not paginate `LIST`, so every listing filters and sorts the whole
result set before slicing the requested window, and reports an authoritative
total. Cursors are plain offsets, which is also what the grid falls back to.

Hierarchies without a natural end (KV keyspaces, the lease index) are walked
breadth-first under a fixed cap. That walk is snapshotted per scope for a few
seconds and every page, sort, and filter is served from the same snapshot, so
paging never re-walks the server and an offset always addresses the row it did
on the previous page. When the walk stops at the cap the response sets
`truncated` and `scanLimit`: the total then counts what was loaded, and the
listing is narrowed by opening a folder in the tree rather than by paging past
the cap. Any write, delete, or revoke drops the snapshots.

## Build

```sh
CGO_ENABLED=0 go build -trimpath -ldflags "-s -w" -o shellcn-plugin-vault ./cmd/shellcn-plugin-vault
```

Copy the binary into the gateway plugin directory, restart ShellCN, then enable
it under **Settings -> Protocols**.

## Tests

```sh
go test ./...
# End-to-end against a throwaway Vault dev container (requires docker):
SHELLCN_VAULT_INTEGRATION=1 go test ./internal/vault/
# Or point at an existing server:
SHELLCN_VAULT_ADDR=127.0.0.1:8200 SHELLCN_VAULT_TOKEN=root \
  SHELLCN_VAULT_INTEGRATION=1 go test ./internal/vault/
```
