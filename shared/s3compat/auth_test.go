package s3compat

import (
	"testing"

	"github.com/charlesng35/shellcn/sdk/plugin"
)

func TestNormalizeOptionsValidatesAuthFields(t *testing.T) {
	for name, cfg := range map[string]map[string]any{
		"access key missing secret": {"bucket": "b", "auth": "access_key", "access_key_id": "ak"},
		"credential missing secret": {
			"bucket": "b", "auth": "credential",
			plugin.CredentialValuesKey(plugin.CredentialIDField): map[string]string{"access_key_id": "ak"},
		},
		"unsupported auth": {"bucket": "b", "auth": "basic", "access_key_id": "ak", "secret_access_key": "sk"},
	} {
		t.Run(name, func(t *testing.T) {
			var opts Options
			if err := normalizeOptions(plugin.ConnectConfig{Config: cfg}, &opts); err == nil {
				t.Fatal("expected validation error")
			}
		})
	}

	t.Run("inline access key", func(t *testing.T) {
		var opts Options
		err := normalizeOptions(plugin.ConnectConfig{Config: map[string]any{
			"bucket": "b", "auth": "access_key", "access_key_id": "ak", "secret_access_key": "sk", "session_token": "tok",
		}}, &opts)
		if err != nil {
			t.Fatalf("inline access key should validate: %v", err)
		}
		if opts.AccessKeyID != "ak" || opts.SecretKey != "sk" || opts.SessionToken != "tok" {
			t.Fatalf("inline material not applied: %+v", opts)
		}
	})

	t.Run("stored cloud access key", func(t *testing.T) {
		var opts Options
		err := normalizeOptions(plugin.ConnectConfig{Config: map[string]any{
			"bucket": "b", "auth": "credential",
			plugin.CredentialValuesKey(plugin.CredentialIDField): map[string]string{
				"access_key_id":     "ak",
				"secret_access_key": "sk",
			},
		}}, &opts)
		if err != nil {
			t.Fatalf("stored access key should validate: %v", err)
		}
		if opts.AccessKeyID != "ak" || opts.SecretKey != "sk" {
			t.Fatalf("credential material not applied: %+v", opts)
		}
	})
}
