# ShellCN contrib plugins

ShellCN-maintained external plugins for
[ShellCN](https://github.com/CharlesNg35/shellcn).

This repo collects plugins maintained by the ShellCN team. They are useful, but
not core enough to ship inside the gateway binary. Outside contributors can send
PRs here, but independent plugin authors should usually keep their plugin in
their own repo and submit only the Marketplace manifest to
[shellcn-plugins](https://github.com/CharlesNg35/shellcn-plugins).

Each plugin here is still a normal ShellCN plugin: one Go module, one protocol,
one release binary.

## How this repo fits

| Repo | Purpose |
| ---- | ------- |
| [shellcn](https://github.com/CharlesNg35/shellcn) | The gateway, SDK, and small built-in plugin set. |
| [shellcn-contrib](https://github.com/CharlesNg35/shellcn-contrib) | Source code for first-party external plugins. |
| [shellcn-plugins](https://github.com/CharlesNg35/shellcn-plugins) | Marketplace registry consumed by the gateway. |
| [shellcn-plugin-starter](https://github.com/CharlesNg35/shellcn-plugin-starter) | Template for writing a new plugin. |

## Repository layout

```text
plugins/
  surrealdb/
    go.mod
    cmd/shellcn-plugin-surrealdb/main.go
    internal/surrealdb/
    README.md
  mssql/
    go.mod
    cmd/shellcn-plugin-mssql/main.go
    internal/mssql/
    README.md

shared/
  helper module used by maintained plugins

scripts/
  maintenance scripts used by CI and local development
```

Each directory under `plugins/` is a complete plugin project. Build it from that
directory:

```sh
cd plugins/surrealdb
CGO_ENABLED=0 go build -trimpath -ldflags "-s -w" \
  -o shellcn-plugin-surrealdb ./cmd/shellcn-plugin-surrealdb
```

Copy the binary into the gateway plugin directory, restart ShellCN, then enable
it under **Settings -> Protocols**.

## Releasing a plugin

Plugins are versioned independently. Tag releases as:

```text
<plugin-name>-v<version>
```

Example:

```sh
git tag surrealdb-v0.1.0
git push origin surrealdb-v0.1.0
```

The release workflow builds that plugin for the supported gateway platforms and
uploads binaries plus `checksums.txt` to the GitHub Release.

After the release exists, add or update the plugin manifest in
[shellcn-plugins](https://github.com/CharlesNg35/shellcn-plugins). Once merged,
the plugin becomes installable from the in-app Marketplace.

## Local maintenance

Run checks across every plugin:

```sh
make fmt
make test
```

Build one plugin:

```sh
make build PLUGIN=surrealdb
```

## Plugins planned for this repo

These protocols are better as first-party external plugins than built-ins:

| Area | Plugins |
| ---- | ------- |
| Databases | MSSQL, Oracle, CockroachDB, ClickHouse, Cassandra, DynamoDB, Neo4j, SurrealDB |
| Search | Elasticsearch, OpenSearch, Meilisearch, Typesense, Solr |
| Messaging | Kafka, RabbitMQ, NATS |
| Files and storage | NFS, MinIO |
| Observability | InfluxDB |

## License

See each plugin directory. Unless a plugin states otherwise, it follows the main
[ShellCN](https://github.com/CharlesNg35/shellcn) project license.
