# ShellCN VS Code Plugin

This is a ShellCN-maintained external plugin for serving a browser IDE through
`PanelWebProxy`.

The plugin supports agent transport only. The enrolled ShellCN agent must run on
a Docker sandbox host and expose the host Docker socket at
`/var/docker/docker.sock`.

The plugin creates a hardened `codercom/code-server:4.127.0` container for the current
`ActorScope + ConnectionID`, publishes its editor port on target loopback, and
removes the container when the ShellCN session closes.

## Runtime Model

- One live `code-server` container is created per ShellCN session scope.
- The container name is derived from `ActorScope + ConnectionID`.
- Docker named volumes for workspace, home, user data, and extensions are scoped
  by `ActorScope + ConnectionID`.
- The container root filesystem is read-only.
- The container drops all capabilities and uses `no-new-privileges`.
- The plugin never falls back to an unsandboxed editor.
- The plugin does not shell out to the Docker CLI.

`--auth none` is intentional because ShellCN owns authentication,
authorization, session lifecycle, and audit before requests reach the editor.
The published editor port is bound to loopback.

## Agent Install

Agent mode declares two enrollment artifacts:

- `docker-run`: starts only `shellcn-agent`, using host networking and mounting
  `/var/run/docker.sock` to `/var/docker/docker.sock`.
- `docker-compose`: the same agent-only setup as a Compose file.

The install artifacts do not start `code-server`. The plugin creates and removes
the editor container through the remote Docker daemon for each ShellCN session.

## Configure

| Field             | Default      | Notes                                                                                       |
| ----------------- | ------------ | ------------------------------------------------------------------------------------------- |
| `sandbox`         | `docker`     | Explicit sandbox selector. Docker is currently the only supported value.                    |
| `workspace_path`  | `/workspace` | Folder path sent to the editor on the initial load.                                         |
| `repository_url`  | empty        | Optional repository to clone into the scoped Docker workspace volume before VS Code starts. |
| `repository_ref`  | empty        | Optional branch, tag, or commit to checkout after clone/fetch.                              |
| `repository_auth` | `none`       | Use no token, a stored API token, or an inline token for private repositories.              |

## Build

```sh
GOWORK=off go test ./...
GOWORK=off CGO_ENABLED=0 go build -trimpath -ldflags "-s -w" \
  -o shellcn-plugin-vscode ./cmd/shellcn-plugin-vscode
```

Drop the resulting binary into the gateway's external plugin directory.
