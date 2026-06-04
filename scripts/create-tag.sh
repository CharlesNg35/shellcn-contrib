#!/usr/bin/env bash
set -euo pipefail

remote=${REMOTE:-origin}

if [ -t 2 ] && [ -n "${TERM:-}" ] && command -v tput >/dev/null 2>&1; then
  bold=$(tput bold || true)
  dim=$(tput dim || true)
  reset=$(tput sgr0 || true)
  red=$(tput setaf 1 || true)
  green=$(tput setaf 2 || true)
  yellow=$(tput setaf 3 || true)
  cyan=$(tput setaf 6 || true)
else
  bold=
  dim=
  reset=
  red=
  green=
  yellow=
  cyan=
fi

fail() {
  echo "${red}$*${reset}" >&2
  exit 1
}

info() {
  echo "${cyan}$*${reset}" >&2
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

latest_version_for_plugin() {
  local plugin=$1
  git tag --list "${plugin}-v*" |
    sed -E "s/^${plugin}-v//" |
    grep -E '^[0-9]+\.[0-9]+\.[0-9]+$' |
    sort -V |
    tail -n 1
}

can_use_fzf() {
  command -v fzf >/dev/null 2>&1 && [ -r /dev/tty ] && [ -w /dev/tty ]
}

select_with_fzf() {
  local prompt=$1
  shift
  printf '%s\n' "$@" |
    fzf --ansi --height=70% --reverse --border --prompt="${prompt} "
}

status_for_plugin() {
  local plugin=$1 latest
  latest=$(latest_version_for_plugin "$plugin" || true)
  if [ -n "$latest" ]; then
    echo "${green}latest v${latest}${reset}"
  else
    echo "${yellow}no release tag${reset}"
  fi
}

select_plugin() {
  local plugins=("$@")
  local choice selected line status
  local display=()

  for plugin in "${plugins[@]}"; do
    status=$(status_for_plugin "$plugin")
    display+=("${plugin}"$'\t'"${status}")
  done

  if can_use_fzf; then
    selected=$(select_with_fzf "plugin>" "${display[@]}") || fail "no plugin selected"
    echo "${selected%%$'\t'*}"
    return
  fi

  echo "${bold}Plugins${reset}" >&2
  echo "${dim}Install fzf for arrow-key selection. Falling back to Bash select.${reset}" >&2
  PS3="${bold}Select plugin [1-${#plugins[@]}]: ${reset}"
  select line in "${display[@]}"; do
    if [[ "${REPLY:-}" =~ ^[0-9]+$ ]] && ((REPLY >= 1 && REPLY <= ${#display[@]})); then
      echo "${plugins[$((REPLY - 1))]}"
      return
    fi
    echo "Choose a number from 1 to ${#plugins[@]}." >&2
  done
}

select_custom_version() {
  local custom
  while true; do
    read -r -p "Custom version: " custom
    normalize_version "$custom"
    return
  done
}

select_version_from_options() {
  local prompt=$1
  shift
  local selected line
  local options=("$@")

  if can_use_fzf; then
    selected=$(select_with_fzf "${prompt}>" "${options[@]}") || fail "no version selected"
    echo "${selected%%$'\t'*}"
    return
  fi

  PS3="${bold}${prompt} [1-${#options[@]}]: ${reset}"
  select line in "${options[@]}"; do
    if [[ "${REPLY:-}" =~ ^[0-9]+$ ]] && ((REPLY >= 1 && REPLY <= ${#options[@]})); then
      echo "${line%%$'\t'*}"
      return
    fi
    echo "Choose a number from 1 to ${#options[@]}." >&2
  done
}

select_version() {
  local plugin=$1
  local latest major minor patch version
  local options=()

  latest=$(latest_version_for_plugin "$plugin" || true)
  if [ -z "$latest" ]; then
    info "No existing release tag for ${bold}${plugin}${reset}."
    options+=("0.1.0"$'\t'"${green}initial release${reset}")
    options+=("custom"$'\t'"custom version")
    version=$(select_version_from_options "version" "${options[@]}")
    if [ "$version" = "custom" ]; then
      select_custom_version
    else
      echo "$version"
    fi
    return
  fi

  major=$(next_major "$latest")
  minor=$(next_minor "$latest")
  patch=$(next_patch "$latest")

  info "Current ${bold}${plugin}${reset} version: ${green}v${latest}${reset}"
  options+=("${patch}"$'\t'"patch")
  options+=("${minor}"$'\t'"minor")
  options+=("${major}"$'\t'"major")
  options+=("custom"$'\t'"custom version")

  version=$(select_version_from_options "version" "${options[@]}")
  if [ "$version" = "custom" ]; then
    select_custom_version
  else
    echo "$version"
  fi
}

confirm_push() {
  local tag=$1 push
  read -r -p "Push ${green}${tag}${reset} to ${remote}? [y/N] " push
  case "$push" in
    y | Y | yes | YES)
      return 0
      ;;
    *)
      return 1
      ;;
  esac
}

git rev-parse --show-toplevel >/dev/null

info "Fetching remote tags from ${remote}..."
git fetch "$remote" --tags --prune --prune-tags

git diff --quiet || fail "working tree has unstaged changes"
git diff --cached --quiet || fail "index has staged changes"

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

echo "Creating tag ${green}${tag}${reset} at ${bold}$(git rev-parse --short HEAD)${reset}"
git tag "$tag"

if confirm_push "$tag"; then
  git push "$remote" "refs/tags/${tag}:refs/tags/${tag}"
else
  echo "Tag created locally only. Push later with:"
  echo "  git push ${remote} refs/tags/${tag}:refs/tags/${tag}"
fi
