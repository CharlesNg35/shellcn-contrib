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
	return cfg.CredentialSecretFor(field)
}

func ResolvedIdentity(cfg plugin.ConnectConfig, field string) string {
	return cfg.CredentialIdentityFor(field)
}

func ResolvedKind(cfg plugin.ConnectConfig, field string) plugin.CredentialKind {
	return cfg.CredentialKindFor(field)
}

func ApplyPasswordCredential(cfg plugin.ConnectConfig, username, password string) AuthMaterial {
	username = strings.TrimSpace(username)
	if identity := cfg.CredentialIdentityFor(plugin.CredentialField); identity != "" {
		username = identity
	}
	if secret := cfg.CredentialSecretFor(plugin.CredentialField); secret != "" {
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
	if secret := ResolvedSecret(cfg, field); secret != "" {
		clientCertificate = secret
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
