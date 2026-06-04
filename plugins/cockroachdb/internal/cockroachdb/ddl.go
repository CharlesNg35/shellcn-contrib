package cockroachdb

import (
	"fmt"
	"regexp"
	"strings"

	"github.com/charlesng35/shellcn-contrib/shared/sqldb"
	"github.com/charlesng35/shellcn/sdk/plugin"
)

// liveOpsTokenRE matches a CockroachDB session/query identifier as returned by
// SHOW SESSIONS / SHOW QUERIES (a hexadecimal token). CANCEL takes the id as a
// string literal, so the token is validated against this strict allow-list and
// then embedded as a single-quoted literal — never raw interpolation.
var liveOpsTokenRE = regexp.MustCompile(`^[0-9a-fA-F]{1,64}$`)

// stringLiteral renders s as a safe single-quoted SQL string literal, doubling
// embedded quotes. Callers must still validate any value that is not already
// constrained to a safe alphabet.
func stringLiteral(s string) string {
	return "'" + strings.ReplaceAll(s, "'", "''") + "'"
}

func liveOpsToken(raw string) (string, error) {
	raw = strings.TrimSpace(raw)
	if !liveOpsTokenRE.MatchString(raw) {
		return "", fmt.Errorf("%w: invalid identifier", plugin.ErrInvalidInput)
	}
	return raw, nil
}

// cancelSessionSQL builds CANCEL SESSION '<id>' from a validated hex token.
func cancelSessionSQL(id string) (string, error) {
	token, err := liveOpsToken(id)
	if err != nil {
		return "", err
	}
	return "CANCEL SESSION " + stringLiteral(token), nil
}

// cancelQuerySQL builds CANCEL QUERY '<id>' from a validated hex token.
func cancelQuerySQL(id string) (string, error) {
	token, err := liveOpsToken(id)
	if err != nil {
		return "", err
	}
	return "CANCEL QUERY " + stringLiteral(token), nil
}

type userCreateRequest struct {
	Name     string `json:"name"`
	Password string `json:"password"`
	Login    bool   `json:"login"`
}

// createUserSQL builds CREATE USER <name> [WITH PASSWORD '<pw>'] [LOGIN|NOLOGIN].
// The name is a validated, quoted identifier; the password is a safe string
// literal (never an identifier, never interpolated raw).
func createUserSQL(req userCreateRequest) (string, error) {
	name, err := sqldb.SafeIdentifier(req.Name)
	if err != nil {
		return "", err
	}
	stmt := "CREATE USER " + sqldb.QuoteIdent(name)
	password := req.Password
	if password != "" {
		stmt += " WITH PASSWORD " + stringLiteral(password)
	}
	return stmt, nil
}

func dropUserSQL(name string) (string, error) {
	user, err := sqldb.SafeIdentifier(name)
	if err != nil {
		return "", err
	}
	return "DROP USER " + sqldb.QuoteIdent(user), nil
}

const (
	grantTargetRole     = "role"
	grantTargetDatabase = "database"
	grantTargetSchema   = "schema"
	grantTargetTable    = "table"
)

var validPrivileges = map[string]bool{
	"ALL":        true,
	"SELECT":     true,
	"INSERT":     true,
	"UPDATE":     true,
	"DELETE":     true,
	"CREATE":     true,
	"DROP":       true,
	"GRANT":      true,
	"ZONECONFIG": true,
	"CONNECT":    true,
	"USAGE":      true,
}

type grantRequest struct {
	User      string `json:"user"`
	Target    string `json:"target"`
	Role      string `json:"role"`
	Privilege string `json:"privilege"`
	Object    string `json:"object"`
}

// grantSQL builds either a role grant (GRANT <role> TO <user>) or an
// object-privilege grant (GRANT <priv> ON <kind> <object> TO <user>). All
// identifiers are validated/quoted and the privilege is checked against a fixed
// allow-list so nothing untrusted reaches the statement.
func grantSQL(req grantRequest) (string, error) {
	user, err := sqldb.SafeIdentifier(req.User)
	if err != nil {
		return "", err
	}
	target := req.Target
	if target == "" {
		target = grantTargetRole
	}
	if target == grantTargetRole {
		role, err := sqldb.SafeIdentifier(req.Role)
		if err != nil {
			return "", err
		}
		return "GRANT " + sqldb.QuoteIdent(role) + " TO " + sqldb.QuoteIdent(user), nil
	}
	priv := strings.ToUpper(strings.TrimSpace(req.Privilege))
	if !validPrivileges[priv] {
		return "", fmt.Errorf("%w: unsupported privilege", plugin.ErrInvalidInput)
	}
	object, kind, err := grantObject(target, req.Object)
	if err != nil {
		return "", err
	}
	return "GRANT " + priv + " ON " + kind + " " + object + " TO " + sqldb.QuoteIdent(user), nil
}

// grantObject validates and quotes the grant target object, returning the quoted
// reference and the ON-clause keyword for the object kind.
func grantObject(target, raw string) (string, string, error) {
	switch target {
	case grantTargetDatabase:
		name, err := sqldb.SafeIdentifier(raw)
		if err != nil {
			return "", "", err
		}
		return sqldb.QuoteIdent(name), "DATABASE", nil
	case grantTargetSchema:
		name, err := sqldb.SafeIdentifier(raw)
		if err != nil {
			return "", "", err
		}
		return sqldb.QuoteIdent(name), "SCHEMA", nil
	case grantTargetTable:
		ref, err := qualifiedRef(raw)
		if err != nil {
			return "", "", err
		}
		return ref, "TABLE", nil
	default:
		return "", "", fmt.Errorf("%w: unsupported grant target", plugin.ErrInvalidInput)
	}
}

const (
	constraintPrimaryKey = "primary_key"
	constraintUnique     = "unique"
	constraintCheck      = "check"
	constraintForeignKey = "foreign_key"
)

var validOnDelete = map[string]bool{
	"NO ACTION":   true,
	"RESTRICT":    true,
	"CASCADE":     true,
	"SET NULL":    true,
	"SET DEFAULT": true,
}

// renameTableSQL builds ALTER TABLE ... RENAME TO ... with both identifiers
// validated and quoted (the new name is bare — RENAME TO cannot move the table
// to another schema).
func renameTableSQL(schema, table, newName string) (string, error) {
	to, err := sqldb.SafeIdentifier(newName)
	if err != nil {
		return "", err
	}
	return "ALTER TABLE " + sqldb.Qualified(schema, table) + " RENAME TO " + sqldb.QuoteIdent(to), nil
}

func renameColumnSQL(schema, table, column, newName string) (string, error) {
	col, err := sqldb.SafeIdentifier(column)
	if err != nil {
		return "", err
	}
	to, err := sqldb.SafeIdentifier(newName)
	if err != nil {
		return "", err
	}
	return "ALTER TABLE " + sqldb.Qualified(schema, table) + " RENAME COLUMN " + sqldb.QuoteIdent(col) + " TO " + sqldb.QuoteIdent(to), nil
}

func alterColumnTypeSQL(schema, table, column, newType, using string) (string, error) {
	col, err := sqldb.SafeIdentifier(column)
	if err != nil {
		return "", err
	}
	dataType := strings.TrimSpace(newType)
	if !sqldb.SafeType(dataType) {
		return "", fmt.Errorf("%w: unsafe column type", plugin.ErrInvalidInput)
	}
	stmt := "ALTER TABLE " + sqldb.Qualified(schema, table) + " ALTER COLUMN " + sqldb.QuoteIdent(col) + " TYPE " + dataType
	if u := strings.TrimSpace(using); u != "" {
		if !sqldb.SafeDefault(u) {
			return "", fmt.Errorf("%w: unsafe USING expression", plugin.ErrInvalidInput)
		}
		stmt += " USING " + u
	}
	return stmt, nil
}

func dropConstraintSQL(schema, table, name string) (string, error) {
	con, err := sqldb.SafeIdentifier(name)
	if err != nil {
		return "", err
	}
	return "ALTER TABLE " + sqldb.Qualified(schema, table) + " DROP CONSTRAINT " + sqldb.QuoteIdent(con), nil
}

type constraintRequest struct {
	Name       string `json:"name"`
	Type       string `json:"type"`
	Columns    any    `json:"columns"`
	Check      string `json:"check"`
	RefTable   string `json:"refTable"`
	RefColumns string `json:"refColumns"`
	OnDelete   string `json:"onDelete"`
}

// addConstraintSQL builds ALTER TABLE ... ADD CONSTRAINT for PK/UNIQUE/CHECK/FK.
// Identifiers are validated and quoted; the CHECK expression is value-free and
// passes the same conservative safety gate as a column default.
func addConstraintSQL(schema, table string, req constraintRequest) (string, error) {
	name, err := sqldb.SafeIdentifier(req.Name)
	if err != nil {
		return "", err
	}
	prefix := "ALTER TABLE " + sqldb.Qualified(schema, table) + " ADD CONSTRAINT " + sqldb.QuoteIdent(name) + " "
	switch req.Type {
	case constraintPrimaryKey, constraintUnique:
		cols, err := sqldb.IdentifierListValue(req.Columns, sqldb.QuoteIdent)
		if err != nil {
			return "", err
		}
		keyword := "PRIMARY KEY"
		if req.Type == constraintUnique {
			keyword = "UNIQUE"
		}
		return prefix + keyword + " (" + strings.Join(cols, ", ") + ")", nil
	case constraintCheck:
		expr := strings.TrimSpace(req.Check)
		if expr == "" {
			return "", fmt.Errorf("%w: check expression is required", plugin.ErrInvalidInput)
		}
		if !sqldb.SafeDefault(expr) {
			return "", fmt.Errorf("%w: unsafe check expression", plugin.ErrInvalidInput)
		}
		return prefix + "CHECK (" + expr + ")", nil
	case constraintForeignKey:
		cols, err := sqldb.IdentifierListValue(req.Columns, sqldb.QuoteIdent)
		if err != nil {
			return "", err
		}
		refTable, err := qualifiedRef(req.RefTable)
		if err != nil {
			return "", err
		}
		refCols, err := sqldb.IdentifierList(req.RefColumns, sqldb.QuoteIdent)
		if err != nil {
			return "", err
		}
		stmt := prefix + "FOREIGN KEY (" + strings.Join(cols, ", ") + ") REFERENCES " + refTable + " (" + strings.Join(refCols, ", ") + ")"
		if onDelete := strings.ToUpper(strings.TrimSpace(req.OnDelete)); onDelete != "" {
			if !validOnDelete[onDelete] {
				return "", fmt.Errorf("%w: unsupported ON DELETE action", plugin.ErrInvalidInput)
			}
			stmt += " ON DELETE " + onDelete
		}
		return stmt, nil
	default:
		return "", fmt.Errorf("%w: unsupported constraint type", plugin.ErrInvalidInput)
	}
}

// qualifiedRef parses a foreign-key target table given either bare ("orders")
// or schema-qualified ("public.orders"), validating and quoting each part.
func qualifiedRef(raw string) (string, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return "", fmt.Errorf("%w: referenced table is required", plugin.ErrInvalidInput)
	}
	if schema, table, ok := strings.Cut(raw, "."); ok {
		s, err := sqldb.SafeIdentifier(schema)
		if err != nil {
			return "", err
		}
		t, err := sqldb.SafeIdentifier(table)
		if err != nil {
			return "", err
		}
		return sqldb.Qualified(s, t), nil
	}
	t, err := sqldb.SafeIdentifier(raw)
	if err != nil {
		return "", err
	}
	return sqldb.QuoteIdent(t), nil
}
