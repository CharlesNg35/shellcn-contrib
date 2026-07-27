package prometheus

import (
	"regexp"
	"strconv"
	"testing"

	"github.com/charlesng35/shellcn/sdk/plugin"
	"github.com/charlesng35/shellcn/sdk/plugintest"
)

func TestManifestValidates(t *testing.T) {
	plugintest.ValidatePlugin(t, New())
}

func TestContainsMatcherSurvivesPromQLUnquoting(t *testing.T) {
	matcher := containsMatcher("http.")
	// PromQL unquotes a double-quoted string before the regex engine sees it.
	pattern, err := strconv.Unquote(matcher)
	if err != nil {
		t.Fatalf("PromQL cannot unquote %s: %v", matcher, err)
	}
	re, err := regexp.Compile(pattern)
	if err != nil {
		t.Fatalf("compile %q: %v", pattern, err)
	}
	if !re.MatchString("go_HTTP.duration") {
		t.Fatalf("%q should match case-insensitively", pattern)
	}
	if re.MatchString("httpx_total") {
		t.Fatalf("%q should keep the dot literal", pattern)
	}
}

func TestKeepContainingIsCaseInsensitive(t *testing.T) {
	got := keepContaining([]string{"node-exporter", "kube-state", "NODE_cpu"}, "NODE")
	if len(got) != 2 || got[0] != "node-exporter" || got[1] != "NODE_cpu" {
		t.Fatalf("unexpected filter result: %#v", got)
	}
}

func TestOrderNamesOnlyReversesItsOwnColumn(t *testing.T) {
	names := []string{"a", "b", "c"}
	orderNames(names, "name", []plugin.SortKey{{Field: "type", Desc: true}})
	if names[0] != "a" {
		t.Fatalf("another column's sort reordered the names: %#v", names)
	}
	orderNames(names, "name", []plugin.SortKey{{Field: "name", Desc: true}})
	if names[0] != "c" {
		t.Fatalf("descending name sort not applied: %#v", names)
	}
}
