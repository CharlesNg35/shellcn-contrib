package dbcred

import (
	"testing"

	"github.com/charlesng35/shellcn/sdk/plugin"
)

func TestApplyPasswordCredentialIgnoresCredentialKindRouting(t *testing.T) {
	got := ApplyPasswordCredential(plugin.ConnectConfig{
		Credentials: plugin.NewResolvedCredentials(plugin.CredentialBinding{
			Field: plugin.CredentialRefField,
			Credential: plugin.ResolvedCredential{
				Kind:   plugin.CredentialKindTLSClientCert,
				Values: map[string]string{"username": "default", "password": "redis-password"},
			},
		}),
	}, "", "")
	if got.Username != "default" || got.Password != "redis-password" || got.ClientCertificate != "" || got.TLSMode != "" {
		t.Fatalf("unexpected password-only material: %+v", got)
	}
}

func TestApplyClientCertificateCredentialUsesFieldSpecificSecret(t *testing.T) {
	got := ApplyClientCertificateCredential(plugin.ConnectConfig{
		Credentials: plugin.NewResolvedCredentials(plugin.CredentialBinding{
			Field: "auth_client_cert_id",
			Credential: plugin.ResolvedCredential{Values: map[string]string{
				"subject":     "cert-user",
				"certificate": "cert-material",
				"private_key": "key-material",
			}},
		}),
	}, "auth_client_cert_id", "", "disable", "")
	if got.Username != "cert-user" || got.Password != "" || got.ClientCertificate != "cert-material\nkey-material" || !got.UsedTLSClientCredential {
		t.Fatalf("unexpected client certificate material: %+v", got)
	}
	if got.TLSMode != "require" {
		t.Fatalf("client certificate should enable TLS, got %q", got.TLSMode)
	}
}
