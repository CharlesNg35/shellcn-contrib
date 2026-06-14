// Package dbcred contains reusable credential handling for database plugins.
package dbcred

import (
	"strings"

	"github.com/charlesng35/shellcn/sdk/plugin"
)

type AuthMaterial struct {
	Username                string
	Password                string
	TLSMode                 string
	ClientCertificate       string
	UsedTLSClientCredential bool
}

func ResolvedSecret(cfg plugin.ConnectConfig, field string) string {
	values := cfg.CredentialValuesFor(field)
	for _, key := range []string{"password", "token", "api_key", "secret_access_key", "private_key", "certificate"} {
		if value := strings.TrimSpace(values[key]); value != "" {
			return value
		}
	}
	return ""
}

func ResolvedIdentity(cfg plugin.ConnectConfig, field string) string {
	values := cfg.CredentialValuesFor(field)
	for _, key := range []string{"username", "subject", "access_key_id", "token_id"} {
		if value := strings.TrimSpace(values[key]); value != "" {
			return value
		}
	}
	return ""
}

func ResolvedKind(cfg plugin.ConnectConfig, field string) plugin.CredentialKind {
	return cfg.CredentialKindFor(field)
}

func ResolvedClientCertificate(cfg plugin.ConnectConfig, field string) string {
	cert := cfg.CredentialValueFor(field, "certificate")
	key := cfg.CredentialValueFor(field, "private_key")
	if cert != "" || key != "" {
		return strings.TrimSpace(cert + "\n" + key)
	}
	return ""
}

func ApplyPasswordCredential(cfg plugin.ConnectConfig, username, password string) AuthMaterial {
	username = strings.TrimSpace(username)
	if identity := cfg.CredentialValueFor(plugin.CredentialIDField, "username"); identity != "" {
		username = identity
	}
	if secret := cfg.CredentialValueFor(plugin.CredentialIDField, "password"); secret != "" {
		password = secret
	}
	return AuthMaterial{Username: username, Password: password}
}

func ApplyClientCertificateCredential(cfg plugin.ConnectConfig, field, username, tlsMode, clientCertificate string) AuthMaterial {
	username = strings.TrimSpace(username)
	tlsMode = strings.TrimSpace(tlsMode)
	if identity := ResolvedIdentity(cfg, field); identity != "" {
		username = identity
	}
	if cert := ResolvedClientCertificate(cfg, field); cert != "" {
		clientCertificate = cert
	}
	if clientCertificate != "" && (tlsMode == "" || tlsMode == "disable") {
		tlsMode = "require"
	}
	return AuthMaterial{
		Username:                username,
		TLSMode:                 tlsMode,
		ClientCertificate:       clientCertificate,
		UsedTLSClientCredential: clientCertificate != "",
	}
}
