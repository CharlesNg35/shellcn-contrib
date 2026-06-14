package surrealdb

import (
	"fmt"
	"net/url"
	"strings"
	"time"

	"github.com/charlesng35/shellcn/sdk/plugin"
)

const (
	defaultHost         = "127.0.0.1"
	defaultPort         = 8000
	defaultQueryTimeout = 30 * time.Second
	defaultRowLimit     = 100
)

// options is the validated, runtime view of a connection's config. Schema
// defaults are UI hints only; every fallback is applied here in code.
type options struct {
	scheme    string // "http" or "https"
	host      string
	port      int
	namespace string
	database  string
	username  string
	password  string
	readOnly  bool
	timeout   time.Duration
	rowLimit  int
}

// addr is the upstream host:port the gateway dials on the plugin's behalf.
func (o options) addr() string { return fmt.Sprintf("%s:%d", o.host, o.port) }

// baseURL is the SurrealDB HTTP endpoint (scheme + authority); the driver appends
// /rpc and /health itself.
func (o options) baseURL() *url.URL {
	return &url.URL{Scheme: o.scheme, Host: o.addr()}
}

// parseOptions reads the decrypted connection config, applies fallbacks, and
// resolves the credential (a reusable credential_ref wins over inline fields).
func parseOptions(cfg plugin.ConnectConfig) (options, error) {
	o := options{
		host:      strings.TrimSpace(cfg.String("host")),
		namespace: strings.TrimSpace(cfg.String("namespace")),
		database:  strings.TrimSpace(cfg.String("database")),
		username:  strings.TrimSpace(cfg.String("username")),
		password:  cfg.String("password"),
		readOnly:  boolValue(cfg.Config["read_only"], true),
		timeout:   durationValue(cfg.Config["query_timeout"], defaultQueryTimeout),
		rowLimit:  intValue(cfg.Config["row_limit"], defaultRowLimit, 1, plugin.MaxPageLimit),
	}
	if o.host == "" {
		o.host = defaultHost
	}
	if port, ok := cfg.Int("port"); ok && port > 0 {
		o.port = port
	} else {
		o.port = defaultPort
	}
	o.scheme = "http"
	if tls, _ := cfg.Config["tls"].(bool); tls {
		o.scheme = "https"
	}

	if id := cfg.CredentialValueFor(plugin.CredentialIDField, "username"); id != "" {
		o.username = id
	}
	if secret := cfg.CredentialValueFor(plugin.CredentialIDField, "password"); secret != "" {
		o.password = secret
	}

	if o.namespace == "" || o.database == "" {
		return o, fmt.Errorf("%w: namespace and database are required", plugin.ErrInvalidInput)
	}
	return o, nil
}

// configSchema is the connection form and the saved-config validation contract.
func configSchema() plugin.Schema {
	return plugin.Schema{Groups: []plugin.Group{
		{
			Name: "Connection",
			Fields: []plugin.Field{
				{Key: "host", Label: "Host", Type: plugin.FieldText, Required: true, Default: defaultHost},
				{
					Key: "port", Label: "Port", Type: plugin.FieldNumber, Default: defaultPort,
					Validators: []plugin.Validator{
						{Type: plugin.ValidatorMin, Value: 1},
						{Type: plugin.ValidatorMax, Value: 65535},
					},
				},
				{Key: "tls", Label: "Use TLS (https)", Type: plugin.FieldToggle},
				{Key: "namespace", Label: "Namespace", Type: plugin.FieldText, Required: true, Default: "test"},
				{Key: "database", Label: "Database", Type: plugin.FieldText, Required: true, Default: "test"},
			},
		},
		{
			Name: "Safety",
			Fields: []plugin.Field{
				{
					Key: "read_only", Label: "Read-only mode", Type: plugin.FieldToggle, Default: true,
					Help: "Blocks write, delete, DEFINE, REMOVE, UPDATE, CREATE, RELATE, and other mutating SurrealQL.",
				},
				{
					Key: "query_timeout", Label: "Query timeout", Type: plugin.FieldDuration, Default: defaultQueryTimeout.String(),
				},
				{
					Key: "row_limit", Label: "Query row limit", Type: plugin.FieldNumber, Default: defaultRowLimit,
					Validators: []plugin.Validator{
						{Type: plugin.ValidatorMin, Value: 1},
						{Type: plugin.ValidatorMax, Value: plugin.MaxPageLimit},
					},
				},
			},
		},
		{
			Name: "Authentication",
			Fields: []plugin.Field{
				{Key: "username", Label: "Username", Type: plugin.FieldText, Default: "root"},
				{
					Key: "password", Label: "Password", Type: plugin.FieldPassword, Secret: true,
					Help: "Inline password. Prefer a reusable credential below.",
				},
				{
					Key: "credential", Label: "Credential", Type: plugin.FieldCredentialRef,
					Help: "A reusable DB password credential (overrides the inline password).",
					Credential: &plugin.CredentialSelector{
						Kind: plugin.CredentialDBPassword,
					},
				},
			},
		},
	}}
}

func boolValue(v any, fallback bool) bool {
	if b, ok := v.(bool); ok {
		return b
	}
	return fallback
}

func durationValue(v any, fallback time.Duration) time.Duration {
	switch x := v.(type) {
	case time.Duration:
		if x > 0 {
			return x
		}
	case string:
		if d, err := time.ParseDuration(strings.TrimSpace(x)); err == nil && d > 0 {
			return d
		}
	}
	return fallback
}

func intValue(v any, fallback, min, max int) int {
	n := 0
	switch x := v.(type) {
	case int:
		n = x
	case int64:
		n = int(x)
	case float64:
		n = int(x)
	}
	if n < min {
		n = fallback
	}
	if n > max {
		n = max
	}
	return n
}

// createRecordSchema validates the "New record" form (and the create route).
func createRecordSchema() *plugin.Schema {
	return &plugin.Schema{Groups: []plugin.Group{{
		Name: "Record",
		Fields: []plugin.Field{
			{
				Key: "data", Label: "Record (JSON)", Type: plugin.FieldJSON, Required: true,
				Help: "The record content as a JSON object, e.g. {\"name\": \"alice\"}.",
			},
		},
	}}}
}

// defineTableSchema validates the "Define table" form.
func defineTableSchema() *plugin.Schema {
	return &plugin.Schema{Groups: []plugin.Group{{
		Name: "Table",
		Fields: []plugin.Field{
			{
				Key: "name", Label: "Table name", Type: plugin.FieldText, Required: true,
				Validators: []plugin.Validator{
					{Type: plugin.ValidatorRegex, Value: `^[A-Za-z_][A-Za-z0-9_]*$`, Message: "letters, digits, underscore; not starting with a digit"},
				},
			},
		},
	}}}
}

// defineFieldSchema validates the "Define field" form.
func defineFieldSchema() *plugin.Schema {
	return &plugin.Schema{Groups: []plugin.Group{{
		Name: "Field",
		Fields: []plugin.Field{
			{
				Key: "name", Label: "Field name", Type: plugin.FieldText, Required: true,
				Validators: []plugin.Validator{
					{Type: plugin.ValidatorRegex, Value: `^[A-Za-z_][A-Za-z0-9_.]*$`, Message: "letters, digits, underscore, dots"},
				},
			},
			{
				Key: "type", Label: "Type", Type: plugin.FieldSelect,
				Help: "Optional SurrealQL type constraint.",
				Options: []plugin.Option{
					{Label: "any", Value: ""},
					{Label: "string", Value: "string"},
					{Label: "int", Value: "int"},
					{Label: "float", Value: "float"},
					{Label: "number", Value: "number"},
					{Label: "bool", Value: "bool"},
					{Label: "datetime", Value: "datetime"},
					{Label: "duration", Value: "duration"},
					{Label: "object", Value: "object"},
					{Label: "array", Value: "array"},
					{Label: "record", Value: "record"},
				},
			},
		},
	}}}
}

// defineIndexSchema validates the "Define index" form.
func defineIndexSchema() *plugin.Schema {
	return &plugin.Schema{Groups: []plugin.Group{{
		Name: "Index",
		Fields: []plugin.Field{
			{
				Key: "name", Label: "Index name", Type: plugin.FieldText, Required: true,
				Validators: []plugin.Validator{
					{Type: plugin.ValidatorRegex, Value: `^[A-Za-z_][A-Za-z0-9_]*$`, Message: "letters, digits, underscore"},
				},
			},
			{
				Key: "fields", Label: "Fields", Type: plugin.FieldText, Required: true,
				Placeholder: "name, email", Help: "Comma-separated field names.",
			},
			{Key: "unique", Label: "Unique", Type: plugin.FieldToggle},
		},
	}}}
}

// editRecordSchema validates the record edit form (full-content replace).
func editRecordSchema() *plugin.Schema {
	return &plugin.Schema{Groups: []plugin.Group{{
		Name: "Record",
		Fields: []plugin.Field{
			{
				Key: "data", Label: "Record (JSON)", Type: plugin.FieldJSON, Required: true,
				Help: "The full record content as a JSON object.",
			},
		},
	}}}
}
