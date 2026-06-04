package dbcred

import (
	"testing"

	"github.com/charlesng35/shellcn/sdk/plugin"
)

func TestApplyPasswordCredentialIgnoresCredentialKindRouting(t *testing.T) {
	got := ApplyPasswordCredential(plugin.ConnectConfig{Config: map[string]any{
		plugin.CredentialIdentityKey(plugin.CredentialField):     "default",
		plugin.CredentialSecretKey(plugin.CredentialField):       "redis-password",
		plugin.CredentialResolvedKindKey(plugin.CredentialField): string(plugin.CredentialTLSClientCert),
	}}, "", "")
	if got.Username != "default" || got.Password != "redis-password" || got.ClientCertificate != "" || got.TLSMode != "" {
		t.Fatalf("unexpected password-only material: %+v", got)
	}
}

func TestApplyClientCertificateCredentialUsesFieldSpecificSecret(t *testing.T) {
	got := ApplyClientCertificateCredential(plugin.ConnectConfig{Config: map[string]any{
		plugin.CredentialIdentityKey("auth_client_cert_id"): "cert-user",
		plugin.CredentialSecretKey("auth_client_cert_id"):   "pem-material",
	}}, "auth_client_cert_id", "", "disable", "")
	if got.Username != "cert-user" || got.Password != "" || got.ClientCertificate != "pem-material" || !got.UsedTLSClientCredential {
		t.Fatalf("unexpected client certificate material: %+v", got)
	}
	if got.TLSMode != "require" {
		t.Fatalf("client certificate should enable TLS, got %q", got.TLSMode)
	}
}
