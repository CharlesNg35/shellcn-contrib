#!/usr/bin/env bash
set -euo pipefail

plugin=${1:?plugin name required}
dir="plugins/${plugin}"

if [ ! -f "${dir}/go.mod" ]; then
  echo "plugin not found: ${plugin}" >&2
  exit 1
fi

mkdir -p dist
export GONOSUMDB=github.com/charlesng35/shellcn,github.com/charlesng35/shellcn/sdk
CGO_ENABLED=0 go -C "$dir" build -trimpath -ldflags "-s -w" -o "../../dist/shellcn-plugin-${plugin}" "./cmd/shellcn-plugin-${plugin}"
