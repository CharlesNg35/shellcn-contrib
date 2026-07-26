# ShellCN memcached plugin

External ShellCN plugin for [memcached](https://memcached.org). It turns a
memcached instance into a browsable, measurable, operable workspace instead of a
black box you poke with `telnet`.

This plugin is maintained in the ShellCN contrib monorepo. It is still a normal
ShellCN plugin: one Go module, one protocol, one release binary.

## Features

- **Key browser** — the LRU crawler (`lru_crawler metadump`) enumerates live
  keys with their slab class, size, client flags, CAS id, TTL, and last-access
  time. Read, create, edit, and delete values with text / JSON / binary
  (base64) modes and a `:` namespace tree. Editing a value **preserves the
  item's existing TTL and client flags** unless you change them explicitly.
- **Item operations** — `set` / `add` / `replace` / `append` / `prepend` /
  `cas` with explicit TTL and flags, plus `touch`, `incr`, and `decr`.
- **Live metrics** — hit ratio, gets/sec, sets/sec, evictions/sec, item and
  connection counts, and used/capacity rows for cache memory and connection
  slots, streamed over a metrics WebSocket. A poll that fails is streamed as an
  unavailable frame, and the watched server property sheet flips its status to
  **unreachable** with the failure reason, so a server that stops answering is
  never mistaken for a server whose numbers stopped changing.
- **Slab allocation map** — a canvas visualizer that draws one bar per slab
  class: allocated capacity, used chunks, and the amber band of memory lost to
  chunk rounding (`used_chunks * chunk_size − mem_requested`). This is the
  number that explains "why is memcached full when my data is small", and no
  other memcached client shows it. It is keyboard navigable (arrows, PageUp /
  PageDown, Home / End), announces the selected class to screen readers, and
  follows the ShellCN light/dark theme. It is paired with the exact numbers in a
  `stats slabs` + `stats items` table beneath it.
- **Segmented LRU table** — per-class HOT / WARM / COLD / TEMP counts, ages,
  evictions, out-of-memory events, reclaims, and LRU movements.
- **Connections and settings** — live `stats conns` and `stats settings`.
- **Activity timeline** — a dedicated watcher connection (`watch mutations
  evictions deletions connevents`) turns memcached's internal log into a live
  event timeline with severities and per-key resources.
- **Command console** — an audited memcached console with an allow-list, a
  completion source, per-command risk classification, and a server-side
  confirmation challenge for anything that changes server-wide state. Storage
  commands accept their data block on the following lines and the declared byte
  count is validated before anything is sent. Replies without server-side paging
  (`lru_crawler metadump`) stop at a row cap and are reported as truncated.
- **Saved commands** — connection-scoped plugin storage, validated against the
  same allow-list before being saved.
- **Server operations** — `flush_all` (with delay), `stats reset`,
  `cache_memlimit`, `verbosity`, and `lru_crawler crawl`, each with an honest
  risk level and a consequence-focused confirmation.
- **Connectivity** — direct or agent (TCP) transport, optional ASCII token
  authentication (`-Y authfile`) from an inline password or a reusable stored
  credential, and TLS from `require` to full verification with optional client
  certificates.

Writes, deletes, stores, flushes, and every runtime tuning command are blocked
while a connection is in read-only mode (the default).

## Key scanner caveats

memcached has no keyspace index. The key browser uses `lru_crawler metadump`,
which has real limits you should know about:

- It is a **point-in-time crawl of the LRU**, not a snapshot. Keys stored while
  the crawl runs may be missed, and expired-but-not-yet-reclaimed items can
  still appear.
- It requires the LRU crawler to be enabled (`stats settings` →
  `lru_crawler yes`, the default on modern builds).
- Only one crawler request runs at a time; a concurrent crawl is reported as a
  conflict rather than silently returning partial data.
- The dump has no server-side paging, so the connection's **Key scan cap**
  bounds it. When the cap trips, the plugin reports a truncated scan and drops
  that connection instead of draining an unbounded dump. Narrow the scan with
  the slab-class scope filter or the search box, or raise the cap.

## Egress

The plugin never opens its own socket. Both the
[gomemcache](https://github.com/bradfitz/gomemcache) data-plane client and the
plugin's raw ASCII connections (statistics, slab data, LRU crawler, meta debug,
watcher, console) dial through `cfg.Net.DialContext`, so direct and agent
connections share one audited path. The configured address is pinned and handed
to that dialer verbatim, so the plugin host never resolves the target itself and
an agent connection works against names that only resolve on the far side. TLS
and the ASCII auth handshake are applied
inside that dialer, so every connection the plugin owns is authenticated the
same way.

## Build

```sh
CGO_ENABLED=0 go build -trimpath -ldflags "-s -w" -o shellcn-plugin-memcached ./cmd/shellcn-plugin-memcached
```

Copy the binary into the gateway plugin directory, restart ShellCN, then enable
it under **Settings -> Protocols**.

## Tests

```sh
go test -race ./...
# End-to-end against a throwaway memcached container (requires docker):
SHELLCN_MEMCACHED_INTEGRATION=1 go test -run Integration ./internal/memcached/
# Or point at an existing instance:
SHELLCN_MEMCACHED_ADDR=127.0.0.1:11211 SHELLCN_MEMCACHED_INTEGRATION=1 go test -run Integration ./internal/memcached/
```
