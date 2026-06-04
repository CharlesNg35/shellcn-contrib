#!/usr/bin/env bash
set -euo pipefail

mapfile -t modules < <(find plugins shared -mindepth 1 -maxdepth 2 -name go.mod -print 2>/dev/null | sort)

if [ "${#modules[@]}" -eq 0 ]; then
  echo "no plugin modules found"
  exit 0
fi

for mod in "${modules[@]}"; do
  dir=$(dirname "$mod")
  echo "::group::${dir}"
  export GONOSUMDB=github.com/charlesng35/shellcn,github.com/charlesng35/shellcn/sdk
  files=$(find "$dir" -type f -name '*.go' -print)
  if [ -n "$files" ]; then
    unformatted=$(gofmt -l $files)
    if [ -n "$unformatted" ]; then
      echo "$unformatted"
      exit 1
    fi
  fi
  go -C "$dir" mod tidy
  go -C "$dir" vet ./...
  go -C "$dir" test ./...
  go -C "$dir" build ./...
  echo "::endgroup::"
done
