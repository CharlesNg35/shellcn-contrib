package nfs

import (
	"testing"

	"github.com/charlesng35/shellcn/sdk/plugintest"
)

func TestManifestValidates(t *testing.T) {
	plugintest.ValidatePlugin(t, New())
}

func TestClientResolveConfinesPathsToRoot(t *testing.T) {
	client := &Client{root: "/exports/team"}
	tests := map[string]string{
		"/":                   "/exports/team",
		"/etc/passwd":         "/exports/team/etc/passwd",
		"/exports/team":       "/exports/team",
		"/exports/team/app":   "/exports/team/app",
		"relative/file.txt":   "/exports/team/relative/file.txt",
		"/exports/team/../db": "/exports/team/exports/db",
	}
	for input, want := range tests {
		got, err := client.resolve(input)
		if err != nil {
			t.Fatalf("resolve(%q): %v", input, err)
		}
		if got != want {
			t.Fatalf("resolve(%q) = %q, want %q", input, got, want)
		}
	}
}

func TestClientResolveAllowsExportRootWhenRootIsSlash(t *testing.T) {
	client := &Client{root: "/"}
	got, err := client.resolve("/etc/passwd")
	if err != nil {
		t.Fatalf("resolve: %v", err)
	}
	if got != "/etc/passwd" {
		t.Fatalf("resolve = %q, want /etc/passwd", got)
	}
}
