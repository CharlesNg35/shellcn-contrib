package dbcred

import (
	"testing"

	"github.com/charlesng35/shellcn/sdk/plugin"
)

func TestApplyPasswordCredentialIgnoresCredentialKindRouting(t *testing.T) {
	got := ApplyPasswordCredential(plugin.ConnectConfig{Config: map[string]any{
		plugin.CredentialValuesKey(plugin.CredentialIDField):       map[string]string{"username": "default", "password": "redis-password"},
		plugin.CredentialResolvedKindKey(plugin.CredentialIDField): string(plugin.CredentialTLSClientCert),
	}}, "", "")
	if got.Username != "default" || got.Password != "redis-password" || got.ClientCertificate != "" || got.TLSMode != "" {
		t.Fatalf("unexpected password-only material: %+v", got)
	}
}

func TestApplyClientCertificateCredentialUsesFieldSpecificSecret(t *testing.T) {
	got := ApplyClientCertificateCredential(plugin.ConnectConfig{Config: map[string]any{
		plugin.CredentialValuesKey("auth_client_cert_id"): map[string]string{
			"subject":     "cert-user",
			"certificate": "cert-material",
			"private_key": "key-material",
		},
	}}, "auth_client_cert_id", "", "disable", "")
	if got.Username != "cert-user" || got.Password != "" || got.ClientCertificate != "cert-material\nkey-material" || !got.UsedTLSClientCredential {
		t.Fatalf("unexpected client certificate material: %+v", got)
	}
	if got.TLSMode != "require" {
		t.Fatalf("client certificate should enable TLS, got %q", got.TLSMode)
	}
}
