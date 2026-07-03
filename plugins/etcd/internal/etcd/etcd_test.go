package etcd

import (
	"testing"

	"github.com/charlesng35/shellcn/sdk/plugin"
	"github.com/charlesng35/shellcn/sdk/plugintest"
)

func TestManifestValidatesWithDirectAndAgent(t *testing.T) {
	p := New()
	plugintest.ValidatePlugin(t, p)
	plugintest.ValidateProjectionPanelConfigs(t, plugintest.Projection(t, p))

	m := p.Manifest()
	if m.Agent == nil {
		t.Fatal("etcd should declare an agent profile")
	}
	if len(m.SupportedTransports) != 2 {
		t.Fatalf("expected direct and agent transports, got %+v", m.SupportedTransports)
	}
	if !plugintest.CredentialKindSupported(m.Config, plugin.CredentialKindDBPassword) {
		t.Fatal("database password credential should support etcd")
	}
	if !plugintest.CredentialKindSupported(m.Config, plugin.CredentialKindTLSClientCert) {
		t.Fatal("TLS client certificate credential should support etcd")
	}
}

func TestKeysPanelDeclaresNamespaceDelimiter(t *testing.T) {
	m := New().Manifest()
	for _, tab := range m.Tabs {
		if tab.Key != "keys" {
			continue
		}
		cfg, ok := tab.Config.(plugin.KVConfig)
		if !ok {
			t.Fatalf("keys tab config is not a KVConfig: %T", tab.Config)
		}
		if cfg.Delimiter != "/" {
			t.Fatalf("keys tab must group by the etcd namespace delimiter, got %q", cfg.Delimiter)
		}
		return
	}
	t.Fatal("keys tab not found")
}

func TestParseOptionsDefaultsToNoAuthReadOnly(t *testing.T) {
	opts, err := parseOptions(plugin.ConnectConfig{Config: map[string]any{"host": "127.0.0.1"}})
	if err != nil {
		t.Fatalf("parse options: %v", err)
	}
	if opts.Username != "" || opts.Password != "" || opts.Port != defaultPort {
		t.Fatalf("unexpected auth defaults: %+v", opts)
	}
	if !opts.ReadOnly {
		t.Fatal("read-only should default on")
	}
	if opts.KeyPrefix != defaultKeyPrefix {
		t.Fatalf("unexpected key prefix: %q", opts.KeyPrefix)
	}
}

func TestParseOptionsUsesPasswordCredentialAndTLS(t *testing.T) {
	opts, err := parseOptions(plugin.ConnectConfig{
		Config: map[string]any{
			"host":     "etcd.internal",
			"auth":     authCredential,
			"tls_mode": "require",
		},
		Credentials: plugin.NewResolvedCredentials(
			plugin.CredentialBinding{
				Field:      plugin.CredentialRefField,
				Credential: plugin.ResolvedCredential{Values: map[string]string{"username": "root", "password": "secret"}},
			},
			plugin.CredentialBinding{
				Field:      clientCertField,
				Credential: plugin.ResolvedCredential{Values: map[string]string{"certificate": "pem-material"}},
			},
		),
	})
	if err != nil {
		t.Fatalf("parse options: %v", err)
	}
	if opts.Username != "root" || opts.Password != "secret" {
		t.Fatalf("credential auth not applied: %+v", opts)
	}
	if opts.TLSMode != "require" || opts.ClientCertificate != "pem-material" {
		t.Fatalf("TLS material not applied: %+v", opts)
	}
}

func TestParseOptionsRejectsMissingHost(t *testing.T) {
	if _, err := parseOptions(plugin.ConnectConfig{Config: map[string]any{}}); err == nil {
		t.Fatal("expected error for missing host")
	}
}

func TestEnsureWritableBlocksInReadOnly(t *testing.T) {
	if err := ensureWritable(&Session{opts: options{ReadOnly: true}}); err == nil {
		t.Fatal("read-only session should block writes")
	}
	if err := ensureWritable(&Session{opts: options{ReadOnly: false}}); err != nil {
		t.Fatalf("writable session should allow writes: %v", err)
	}
}
