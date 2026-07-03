# ShellCN Notebook Plugin

This is a ShellCN-maintained external plugin for serving JupyterLab through
ShellCN with `PanelWebProxy`.

The plugin supports agent transport only. The enrolled ShellCN agent must run on
a Docker sandbox host and expose the host Docker socket at
`/var/docker/docker.sock`.

The plugin creates a hardened JupyterLab container for the current
`ActorScope + ConnectionID`, publishes its notebook port on target loopback, and
removes the container when the ShellCN session closes.

## Runtime Model

- One live JupyterLab container is created per ShellCN session scope.
- The container name is derived from `ActorScope + ConnectionID`.
- Docker named volumes for home and workspace are scoped by
  `ActorScope + ConnectionID`.
- The container root filesystem is read-only.
- The container drops all capabilities and uses `no-new-privileges`.
- The plugin never falls back to an unsandboxed notebook.
- The plugin does not shell out to the Docker CLI.
- Jupyter runs under the internal base URL `/shellcn-notebook/`; the plugin maps
  that base URL to the connection proxy mount.

Jupyter authentication is disabled because ShellCN owns authentication,
authorization, session lifecycle, and audit before requests reach the notebook.
The published notebook port is bound to loopback.

## Agent Install

Agent mode declares two enrollment artifacts:

- `docker-run`: starts only `shellcn-agent`, using host networking and mounting
  `/var/run/docker.sock` to `/var/docker/docker.sock`.
- `docker-compose`: the same agent-only setup as a Compose file.

The install artifacts do not start JupyterLab. The plugin creates and removes the
notebook container through the remote Docker daemon for each ShellCN session.

## Configure

| Field | Default | Notes |
| --- | --- | --- |
| `sandbox` | `docker` | Explicit sandbox selector. Docker is currently the only supported value. |
| `image` | `quay.io/jupyter/minimal-notebook:python-3.13.5` | Docker image used for JupyterLab sessions. |

## Build

```sh
GOWORK=off go test ./...
GOWORK=off CGO_ENABLED=0 go build -trimpath -ldflags "-s -w" \
  -o shellcn-plugin-notebook ./cmd/shellcn-plugin-notebook
```

Drop the resulting binary into the gateway's external plugin directory.
