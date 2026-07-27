# ShellCN Snowflake plugin

External ShellCN plugin for [Snowflake](https://www.snowflake.com/), built on the
official Go driver (`github.com/snowflakedb/gosnowflake/v2`).

This plugin is maintained in the ShellCN contrib monorepo. It is still a normal
ShellCN plugin: one Go module, one protocol, one release binary.

## What it exposes

- **Explorer** — databases → schemas → tables/views, with column, DDL and
  row-count detail.
- **Data grid** — bounded, server-paged table rows with free-text search and
  column sort pushed into Snowflake. Tables that declare a `PRIMARY KEY` get an
  editable grid (insert/update/delete); everything else stays read-only.
- **SQL editor** — multi-statement console with per-statement results, the
  server-assigned query id, cancellation and schema-aware completion.
- **Warehouses** — state, size, cluster and queue counts, resume / suspend /
  resize, plus a live load-and-credits metrics panel.
- **Access control** — roles, their grants, users, and the roles granted to a
  user, with grant/revoke actions.
- **Data pipeline objects** — stages (and their staged files), file formats,
  pipes, tasks (with run history) and streams.
- **Query history** — recent queries with status, elapsed time, bytes scanned,
  rows produced and cloud-services credits, plus an account-level live credit
  and throughput panel.

## Authentication

| Mode | Config |
| --- | --- |
| Password | inline `password` secret field |
| Stored password | a reusable `database_password` credential |
| Key pair | a reusable `snowflake_key_pair` credential (user + unencrypted PKCS#8 or PKCS#1 RSA private key) |

Register the public half of a key pair on the Snowflake user with
`ALTER USER <user> SET RSA_PUBLIC_KEY='<base64 public key>'`. Encrypted private
keys are rejected: decrypt the key before storing it as a credential.

## Connection notes

- **Account hostname is required.** The gateway only dials hosts declared in the
  connection config, so the account hostname (usually
  `<account>.snowflakecomputing.com`, or your PrivateLink hostname) must be set
  explicitly. All HTTP traffic, including login, rides the gateway transport.
- **Database is required.** Its `INFORMATION_SCHEMA` backs the catalog listings
  and the account-level query-history, warehouse-load and metering views.
- **Result sets stay small on purpose.** `row_limit` bounds every grid page and
  query result so Snowflake answers inline instead of handing back cloud-storage
  chunk URLs, which the gateway's target allow-list would not cover.
- Read-only mode and destructive-statement confirmation are on by default.

## Build

```sh
CGO_ENABLED=0 go build -trimpath -ldflags "-s -w" -o shellcn-plugin-snowflake ./cmd/shellcn-plugin-snowflake
```

Copy the binary into the gateway plugin directory, restart ShellCN, then enable
it under **Settings -> Protocols**.

## Tests

```sh
go test ./...
```

Unit tests drive every declared route against a scripted `database/sql` driver,
so the SQL each handler emits — including paging, search and sort push-down — is
asserted without a live account.

The integration test talks to a real Snowflake account and skips unless it is
explicitly enabled:

```sh
SHELLCN_SNOWFLAKE_INTEGRATION=1 \
SNOWFLAKE_ACCOUNT=myorg-myaccount \
SNOWFLAKE_USER=svc \
SNOWFLAKE_PASSWORD=... \
SNOWFLAKE_DATABASE=SHELLCN_TEST \
SNOWFLAKE_WAREHOUSE=COMPUTE_WH \
go test ./... -run Integration -v
```

Set `SNOWFLAKE_PRIVATE_KEY` (PEM) instead of `SNOWFLAKE_PASSWORD` to exercise
key-pair authentication; `SNOWFLAKE_HOST`, `SNOWFLAKE_SCHEMA` and
`SNOWFLAKE_ROLE` are optional. The test creates one uniquely named transient
database and drops it again.
