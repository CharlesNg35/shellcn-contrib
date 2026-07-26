package arangodb

import (
	"strconv"
	"strings"

	driver "github.com/arangodb/go-driver/v2/arangodb"

	"github.com/charlesng35/shellcn-contrib/shared/broker"
	"github.com/charlesng35/shellcn/sdk/plugin"
)

const (
	kindDatabase   = "database"
	kindCollection = "collection"
	kindGraph      = "graph"
	kindView       = "view"
	kindAnalyzer   = "analyzer"
	kindFoxx       = "foxx_service"
	kindCluster    = "cluster"
	kindAQLQuery   = "aql_query"
	kindSavedQuery = "saved_query"
)

// paramOrQuery reads a renderer-supplied param, falling back to the `p.<name>`
// query form the generic grid uses for list panels.
func paramOrQuery(rc *plugin.RequestContext, key string) string {
	if value := strings.TrimSpace(rc.Param(key)); value != "" {
		return value
	}
	return strings.TrimSpace(rc.Query().Get("p." + key))
}

// databaseParam resolves the target database for a request, defaulting to the
// database configured on the connection.
func databaseParam(rc *plugin.RequestContext, s *Session) string {
	if value := paramOrQuery(rc, "database"); value != "" {
		return value
	}
	return s.opts.Database
}

func collectionScope(rc *plugin.RequestContext) (*Session, string, string, error) {
	s, err := session(rc)
	if err != nil {
		return nil, "", "", err
	}
	database, err := safeDatabaseName(databaseParam(rc, s))
	if err != nil {
		return nil, "", "", err
	}
	collection, err := safeName(paramOrQuery(rc, "collection"), "collection")
	if err != nil {
		return nil, "", "", err
	}
	return s, database, collection, nil
}

func documentScope(rc *plugin.RequestContext) (*Session, string, string, string, error) {
	s, database, collection, err := collectionScope(rc)
	if err != nil {
		return nil, "", "", "", err
	}
	key, err := safeKey(paramOrQuery(rc, "key"))
	if err != nil {
		return nil, "", "", "", err
	}
	return s, database, collection, key, nil
}

func graphScope(rc *plugin.RequestContext) (*Session, string, string, error) {
	s, err := session(rc)
	if err != nil {
		return nil, "", "", err
	}
	database, err := safeDatabaseName(databaseParam(rc, s))
	if err != nil {
		return nil, "", "", err
	}
	name, err := safeName(paramOrQuery(rc, "graph"), "graph")
	if err != nil {
		return nil, "", "", err
	}
	return s, database, name, nil
}

func viewScope(rc *plugin.RequestContext) (*Session, string, string, error) {
	s, err := session(rc)
	if err != nil {
		return nil, "", "", err
	}
	database, err := safeDatabaseName(databaseParam(rc, s))
	if err != nil {
		return nil, "", "", err
	}
	name, err := safeName(paramOrQuery(rc, "view"), "view")
	if err != nil {
		return nil, "", "", err
	}
	return s, database, name, nil
}

// pageRows applies the grid's search, sort and cursor to an in-memory slice.
func pageRows(rc *plugin.RequestContext, rows []row) (plugin.Page[row], error) {
	return broker.PageRows(rc, rows)
}

func replicationFactorText(factor driver.ReplicationFactor) string {
	if factor == driver.ReplicationFactorSatellite {
		return "satellite"
	}
	return strconv.Itoa(int(factor))
}
