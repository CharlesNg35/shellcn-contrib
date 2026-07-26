package memcached

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/charlesng35/shellcn/sdk/plugin"
)

type replyKind int

const (
	replyLine replyKind = iota
	replyStats
	replyRetrieval
	replyMeta
)

type commandClass int

const (
	classRead commandClass = iota
	classWrite
	classAdmin
)

type commandSpec struct {
	Name    string
	Reply   replyKind
	Data    bool
	Class   commandClass
	Summary string
}

// commandCatalog is the console allow-list. Anything absent is refused, which
// keeps "watch" (it would hijack a pooled connection), "quit", and "shutdown"
// out of the console.
var commandCatalog = map[string]commandSpec{
	"get":            {Name: "get", Reply: replyRetrieval, Class: classRead, Summary: "get <key>* — fetch one or more values"},
	"gets":           {Name: "gets", Reply: replyRetrieval, Class: classRead, Summary: "gets <key>* — fetch values with CAS ids"},
	"gat":            {Name: "gat", Reply: replyRetrieval, Class: classWrite, Summary: "gat <exptime> <key>* — fetch and update TTL"},
	"gats":           {Name: "gats", Reply: replyRetrieval, Class: classWrite, Summary: "gats <exptime> <key>* — fetch with CAS and update TTL"},
	"set":            {Name: "set", Reply: replyLine, Data: true, Class: classWrite, Summary: "set <key> <flags> <exptime> <bytes> — store unconditionally"},
	"add":            {Name: "add", Reply: replyLine, Data: true, Class: classWrite, Summary: "add <key> <flags> <exptime> <bytes> — store only if absent"},
	"replace":        {Name: "replace", Reply: replyLine, Data: true, Class: classWrite, Summary: "replace <key> <flags> <exptime> <bytes> — store only if present"},
	"append":         {Name: "append", Reply: replyLine, Data: true, Class: classWrite, Summary: "append <key> <flags> <exptime> <bytes> — append to an existing value"},
	"prepend":        {Name: "prepend", Reply: replyLine, Data: true, Class: classWrite, Summary: "prepend <key> <flags> <exptime> <bytes> — prepend to an existing value"},
	"cas":            {Name: "cas", Reply: replyLine, Data: true, Class: classWrite, Summary: "cas <key> <flags> <exptime> <bytes> <cas> — store if unchanged"},
	"delete":         {Name: "delete", Reply: replyLine, Class: classWrite, Summary: "delete <key> — remove one key"},
	"incr":           {Name: "incr", Reply: replyLine, Class: classWrite, Summary: "incr <key> <delta> — atomic increment"},
	"decr":           {Name: "decr", Reply: replyLine, Class: classWrite, Summary: "decr <key> <delta> — atomic decrement"},
	"touch":          {Name: "touch", Reply: replyLine, Class: classWrite, Summary: "touch <key> <exptime> — update TTL without reading"},
	"mg":             {Name: "mg", Reply: replyMeta, Class: classRead, Summary: "mg <key> <flags>* — meta get (v value, t TTL, c CAS, f flags, s size, h hit, l last access)"},
	"ms":             {Name: "ms", Reply: replyMeta, Data: true, Class: classWrite, Summary: "ms <key> <datalen> <flags>* — meta set (T TTL, F flags, C CAS, M mode)"},
	"md":             {Name: "md", Reply: replyMeta, Class: classWrite, Summary: "md <key> <flags>* — meta delete (I invalidate, T TTL)"},
	"ma":             {Name: "ma", Reply: replyMeta, Class: classWrite, Summary: "ma <key> <flags>* — meta arithmetic (D delta, M I/D mode, N autovivify)"},
	"mn":             {Name: "mn", Reply: replyMeta, Class: classRead, Summary: "mn — meta no-op"},
	"me":             {Name: "me", Reply: replyLine, Class: classRead, Summary: "me <key> — dump an item's internal metadata"},
	"stats":          {Name: "stats", Reply: replyStats, Class: classRead, Summary: "stats [items|slabs|settings|conns|sizes|reset]"},
	"version":        {Name: "version", Reply: replyLine, Class: classRead, Summary: "version — server version"},
	"verbosity":      {Name: "verbosity", Reply: replyLine, Class: classAdmin, Summary: "verbosity <level> — set server log verbosity"},
	"flush_all":      {Name: "flush_all", Reply: replyLine, Class: classAdmin, Summary: "flush_all [delay] — invalidate every item"},
	"cache_memlimit": {Name: "cache_memlimit", Reply: replyLine, Class: classAdmin, Summary: "cache_memlimit <megabytes> — retune the cache memory limit"},
	"lru_crawler":    {Name: "lru_crawler", Reply: replyStats, Class: classAdmin, Summary: "lru_crawler <metadump|crawl|enable|disable|sleep|tocrawl> <arg>"},
	"slabs":          {Name: "slabs", Reply: replyLine, Class: classAdmin, Summary: "slabs <reassign|automove> <args> — move pages between slab classes"},
	"lru":            {Name: "lru", Reply: replyLine, Class: classAdmin, Summary: "lru <tune|mode|temp_ttl> <args> — retune the segmented LRU"},
}

// resolveCommand validates one console submission and returns the spec plus the
// exact bytes to put on the wire.
func resolveCommand(input string) (commandSpec, string, error) {
	text := strings.ReplaceAll(input, "\r\n", "\n")
	head, rest, _ := strings.Cut(text, "\n")
	head = strings.TrimSpace(head)
	if head == "" {
		return commandSpec{}, "", fmt.Errorf("%w: a command is required", plugin.ErrInvalidInput)
	}
	tokens := strings.Fields(head)
	name := strings.ToLower(tokens[0])
	spec, ok := commandCatalog[name]
	if !ok {
		return commandSpec{}, "", fmt.Errorf("%w: %q is not an allowed memcached command", plugin.ErrInvalidInput, tokens[0])
	}
	if strings.EqualFold(lastToken(tokens), "noreply") {
		return commandSpec{}, "", fmt.Errorf("%w: noreply cannot be used from the console because the reply is what you see", plugin.ErrInvalidInput)
	}
	spec = refineCommand(spec, tokens)

	if !spec.Data {
		if strings.TrimSpace(rest) != "" {
			return commandSpec{}, "", fmt.Errorf("%w: %s does not take a data block", plugin.ErrInvalidInput, spec.Name)
		}
		return spec, head + "\r\n", nil
	}

	declared, err := declaredBytes(spec.Name, tokens)
	if err != nil {
		return commandSpec{}, "", err
	}
	data := strings.TrimSuffix(rest, "\n")
	if len(data) != declared {
		return commandSpec{}, "", fmt.Errorf("%w: %s declared %d bytes but the data block is %d bytes", plugin.ErrInvalidInput, spec.Name, declared, len(data))
	}
	return spec, head + "\r\n" + data + "\r\n", nil
}

// refineCommand upgrades the risk class and reply shape from a subcommand, so
// "stats" stays a read while "stats reset" is an administrative operation.
func refineCommand(spec commandSpec, tokens []string) commandSpec {
	if len(tokens) < 2 {
		return spec
	}
	sub := strings.ToLower(tokens[1])
	switch spec.Name {
	case "stats":
		if sub == "reset" {
			// "stats reset" answers with a bare RESET line, not a STAT block.
			spec.Class, spec.Reply = classAdmin, replyLine
		}
	case "lru_crawler":
		switch sub {
		case "metadump", "mgdump":
			spec.Class = classRead
		default:
			spec.Reply = replyLine
		}
	}
	return spec
}

func declaredBytes(name string, tokens []string) (int, error) {
	index := 4
	if name == "ms" {
		index = 2
	}
	if len(tokens) <= index {
		return 0, fmt.Errorf("%w: %s is missing its byte count", plugin.ErrInvalidInput, name)
	}
	size, err := strconv.Atoi(tokens[index])
	if err != nil || size < 0 {
		return 0, fmt.Errorf("%w: %s byte count %q is not a non-negative integer", plugin.ErrInvalidInput, name, tokens[index])
	}
	return size, nil
}

func lastToken(tokens []string) string {
	if len(tokens) == 0 {
		return ""
	}
	return tokens[len(tokens)-1]
}

type completionEntry struct {
	Label  string `json:"label"`
	Detail string `json:"detail"`
	Kind   string `json:"kind"`
}

func completionEntries() []completionEntry {
	out := make([]completionEntry, 0, len(commandCatalog)+8)
	for _, spec := range commandCatalog {
		kind := "read"
		switch spec.Class {
		case classWrite:
			kind = "write"
		case classAdmin:
			kind = "admin"
		}
		out = append(out, completionEntry{Label: spec.Name, Detail: spec.Summary, Kind: kind})
	}
	for _, sub := range []string{"stats items", "stats slabs", "stats settings", "stats conns", "stats sizes", "stats reset", "lru_crawler metadump all", "lru_crawler crawl all"} {
		out = append(out, completionEntry{Label: sub, Detail: "subcommand", Kind: "read"})
	}
	return out
}
