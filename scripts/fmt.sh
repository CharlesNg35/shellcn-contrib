#!/usr/bin/env bash
set -euo pipefail

find plugins shared -type f -name '*.go' -print0 2>/dev/null | xargs -0 -r gofmt -w
