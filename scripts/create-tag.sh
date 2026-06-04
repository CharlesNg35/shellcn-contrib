#!/usr/bin/env bash
set -euo pipefail

remote=${REMOTE:-origin}

fail() {
  echo "$*" >&2
  exit 1
}

is_semver() {
  [[ "$1" =~ ^v?[0-9]+\.[0-9]+\.[0-9]+$ ]]
}

normalize_version() {
  local version=$1
  version=${version#v}
  is_semver "$version" || fail "invalid version: ${version}. Expected x.y.z, for example 0.1.0"
  echo "$version"
}

tag_exists_remote() {
  git ls-remote --exit-code --tags "$remote" "refs/tags/$1" >/dev/null 2>&1
}

tag_exists_local() {
  git rev-parse -q --verify "refs/tags/$1" >/dev/null
}

next_major() {
  local version=$1 major minor patch
  IFS=. read -r major minor patch <<< "$version"
  echo "$((major + 1)).0.0"
}

next_minor() {
  local version=$1 major minor patch
  IFS=. read -r major minor patch <<< "$version"
  echo "${major}.$((minor + 1)).0"
}

next_patch() {
  local version=$1 major minor patch
  IFS=. read -r major minor patch <<< "$version"
  echo "${major}.${minor}.$((patch + 1))"
}

select_plugin() {
  local plugins=("$@")
  local choice plugin

  echo "Available plugins:"
  for i in "${!plugins[@]}"; do
    printf "  %2d) %s\n" "$((i + 1))" "${plugins[$i]}"
  done

  while true; do
    read -r -p "Plugin name or number: " choice
    if [[ "$choice" =~ ^[0-9]+$ ]] && ((choice >= 1 && choice <= ${#plugins[@]})); then
      echo "${plugins[$((choice - 1))]}"
      return
    fi
    for plugin in "${plugins[@]}"; do
      if [ "$choice" = "$plugin" ]; then
        echo "$plugin"
        return
      fi
    done
    echo "Unknown plugin: ${choice}" >&2
  done
}

latest_version_for_plugin() {
  local plugin=$1
  git tag --list "${plugin}-v*" |
    sed -E "s/^${plugin}-v//" |
    grep -E '^[0-9]+\.[0-9]+\.[0-9]+$' |
    sort -V |
    tail -n 1
}

select_version() {
  local plugin=$1
  local latest version major minor patch choice custom

  latest=$(latest_version_for_plugin "$plugin" || true)
  if [ -z "$latest" ]; then
    read -r -p "Version [0.1.0]: " version
    version=${version:-0.1.0}
    normalize_version "$version"
    return
  fi

  major=$(next_major "$latest")
  minor=$(next_minor "$latest")
  patch=$(next_patch "$latest")

  echo "Latest ${plugin} version: v${latest}"
  echo "  1) patch v${patch}"
  echo "  2) minor v${minor}"
  echo "  3) major v${major}"
  echo "  4) custom"

  while true; do
    read -r -p "Select version [1]: " choice
    choice=${choice:-1}
    case "$choice" in
      1|patch)
        echo "$patch"
        return
        ;;
      2|minor)
        echo "$minor"
        return
        ;;
      3|major)
        echo "$major"
        return
        ;;
      4|custom)
        read -r -p "Version: " custom
        normalize_version "$custom"
        return
        ;;
      *)
        echo "Choose 1, 2, 3, or 4." >&2
        ;;
    esac
  done
}

git rev-parse --show-toplevel >/dev/null
git diff --quiet || fail "working tree has unstaged changes"
git diff --cached --quiet || fail "index has staged changes"

echo "Fetching remote tags from ${remote}..."
git fetch "$remote" --tags --prune --prune-tags

mapfile -t plugins < <(find plugins -maxdepth 1 -mindepth 1 -type d -printf '%f\n' | sort)
[ "${#plugins[@]}" -gt 0 ] || fail "no plugins found under plugins/"

plugin=$(select_plugin "${plugins[@]}")
version=$(select_version "$plugin")
tag="${plugin}-v${version}"

if tag_exists_local "$tag"; then
  fail "tag already exists locally: ${tag}"
fi
if tag_exists_remote "$tag"; then
  fail "tag already exists on ${remote}: ${tag}"
fi

echo "Creating tag ${tag} at $(git rev-parse --short HEAD)"
git tag "$tag"

read -r -p "Push ${tag} to ${remote}? [y/N] " push
case "$push" in
  y|Y|yes|YES)
    git push "$remote" "refs/tags/${tag}:refs/tags/${tag}"
    ;;
  *)
    echo "Tag created locally only. Push later with:"
    echo "  git push ${remote} refs/tags/${tag}:refs/tags/${tag}"
    ;;
esac
