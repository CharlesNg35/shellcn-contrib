package milvus

import (
	"context"
	"fmt"
	"strings"

	"github.com/milvus-io/milvus/client/v2/milvusclient"

	"github.com/charlesng35/shellcn/sdk/plugin"
)

func Routes() []plugin.Route {
	return []plugin.Route{
		{ID: rid("overview"), Method: plugin.MethodGet, Path: "/overview", Permission: perm("cluster.read"), Risk: plugin.RiskSafe, AuditEvent: rid("overview"), Handle: overview},
		{ID: rid("overview.metrics"), Method: plugin.MethodWS, Path: "/overview/metrics", Permission: perm("cluster.read"), Risk: plugin.RiskSafe, AuditEvent: rid("overview.metrics"), Stream: overviewMetrics},
		{ID: rid("activity.list"), Method: plugin.MethodGet, Path: "/activity", Permission: perm("cluster.read"), Risk: plugin.RiskSafe, AuditEvent: rid("activity.list"), Handle: listActivity},

		{ID: rid("databases.list"), Method: plugin.MethodGet, Path: "/databases", Permission: perm("databases.read"), Risk: plugin.RiskSafe, AuditEvent: rid("databases.list"), Handle: listDatabases},
		{ID: rid("database.read"), Method: plugin.MethodGet, Path: "/databases/{name}", Permission: perm("databases.read"), Risk: plugin.RiskSafe, AuditEvent: rid("database.read"), Handle: readDatabase},
		{ID: rid("database.create"), Method: plugin.MethodPost, Path: "/databases", Permission: perm("databases.write"), Risk: plugin.RiskWrite, AuditEvent: rid("database.create"), Input: databaseSchema(), Handle: createDatabase},
		{ID: rid("database.drop"), Method: plugin.MethodDelete, Path: "/databases/{name}", Permission: perm("databases.delete"), Risk: plugin.RiskDestructive, AuditEvent: rid("database.drop"), Handle: dropDatabase},

		{ID: rid("tree.collections"), Method: plugin.MethodGet, Path: "/tree/collections", Permission: perm("collections.read"), Risk: plugin.RiskSafe, AuditEvent: rid("tree.collections"), Handle: treeCollections},
		{ID: rid("tree.partitions"), Method: plugin.MethodGet, Path: "/tree/collections/{collection}/partitions", Permission: perm("collections.read"), Risk: plugin.RiskSafe, AuditEvent: rid("tree.partitions"), Handle: treePartitions},

		{ID: rid("collections.list"), Method: plugin.MethodGet, Path: "/collections", Permission: perm("collections.read"), Risk: plugin.RiskSafe, AuditEvent: rid("collections.list"), Handle: listCollections},
		{ID: rid("collection.read"), Method: plugin.MethodGet, Path: "/collections/{collection}", Permission: perm("collections.read"), Risk: plugin.RiskSafe, AuditEvent: rid("collection.read"), Handle: readCollection},
		{ID: rid("collection.watch"), Method: plugin.MethodWS, Path: "/collections/{collection}/watch", Permission: perm("collections.read"), Risk: plugin.RiskSafe, AuditEvent: rid("collection.watch"), Stream: watchCollection},
		{ID: rid("collection.schema"), Method: plugin.MethodGet, Path: "/collections/{collection}/schema", Permission: perm("collections.read"), Risk: plugin.RiskSafe, AuditEvent: rid("collection.schema"), Handle: listSchemaFields},
		{ID: rid("collection.graph"), Method: plugin.MethodGet, Path: "/collections/{collection}/graph", Permission: perm("collections.read"), Risk: plugin.RiskSafe, AuditEvent: rid("collection.graph"), Handle: collectionGraph},
		{ID: rid("collection.metrics"), Method: plugin.MethodWS, Path: "/collections/{collection}/metrics", Permission: perm("collections.read"), Risk: plugin.RiskSafe, AuditEvent: rid("collection.metrics"), Stream: collectionMetrics},
		{ID: rid("collection.projection"), Method: plugin.MethodWS, Path: "/collections/{collection}/projection", Permission: perm("entities.read"), Risk: plugin.RiskSafe, AuditEvent: rid("collection.projection"), Stream: projectionStream},
		{ID: rid("collection.create"), Method: plugin.MethodPost, Path: "/collections", Permission: perm("collections.write"), Risk: plugin.RiskWrite, AuditEvent: rid("collection.create"), Input: collectionSchema(), Handle: createCollection},
		{ID: rid("collection.field.add"), Method: plugin.MethodPost, Path: "/collections/{collection}/fields", Permission: perm("collections.write"), Risk: plugin.RiskWrite, AuditEvent: rid("collection.field.add"), Input: addFieldSchema(), Handle: addCollectionField},
		{ID: rid("collection.rename"), Method: plugin.MethodPost, Path: "/collections/{collection}/rename", Permission: perm("collections.write"), Risk: plugin.RiskWrite, AuditEvent: rid("collection.rename"), Input: renameSchema(), Handle: renameCollection},
		{ID: rid("collection.load"), Method: plugin.MethodPost, Path: "/collections/{collection}/load", Permission: perm("collections.write"), Risk: plugin.RiskWrite, AuditEvent: rid("collection.load"), Handle: loadCollection},
		{ID: rid("collection.release"), Method: plugin.MethodPost, Path: "/collections/{collection}/release", Permission: perm("collections.write"), Risk: plugin.RiskWrite, AuditEvent: rid("collection.release"), Handle: releaseCollection},
		{ID: rid("collection.flush"), Method: plugin.MethodPost, Path: "/collections/{collection}/flush", Permission: perm("collections.write"), Risk: plugin.RiskWrite, AuditEvent: rid("collection.flush"), Handle: flushCollection},
		{ID: rid("collection.compact"), Method: plugin.MethodPost, Path: "/collections/{collection}/compact", Permission: perm("collections.write"), Risk: plugin.RiskWrite, AuditEvent: rid("collection.compact"), Handle: compactCollection},
		{ID: rid("collection.drop"), Method: plugin.MethodDelete, Path: "/collections/{collection}", Permission: perm("collections.delete"), Risk: plugin.RiskDestructive, AuditEvent: rid("collection.drop"), Handle: dropCollection},

		{ID: rid("partitions.list"), Method: plugin.MethodGet, Path: "/collections/{collection}/partitions", Permission: perm("partitions.read"), Risk: plugin.RiskSafe, AuditEvent: rid("partitions.list"), Handle: listPartitions},
		{ID: rid("partition.read"), Method: plugin.MethodGet, Path: "/collections/{collection}/partitions/{partition}", Permission: perm("partitions.read"), Risk: plugin.RiskSafe, AuditEvent: rid("partition.read"), Handle: readPartition},
		{ID: rid("partition.create"), Method: plugin.MethodPost, Path: "/collections/{collection}/partitions", Permission: perm("partitions.write"), Risk: plugin.RiskWrite, AuditEvent: rid("partition.create"), Input: partitionSchema(), Handle: createPartition},
		{ID: rid("partition.load"), Method: plugin.MethodPost, Path: "/collections/{collection}/partitions/{partition}/load", Permission: perm("partitions.write"), Risk: plugin.RiskWrite, AuditEvent: rid("partition.load"), Handle: loadPartition},
		{ID: rid("partition.release"), Method: plugin.MethodPost, Path: "/collections/{collection}/partitions/{partition}/release", Permission: perm("partitions.write"), Risk: plugin.RiskWrite, AuditEvent: rid("partition.release"), Handle: releasePartition},
		{ID: rid("partition.drop"), Method: plugin.MethodDelete, Path: "/collections/{collection}/partitions/{partition}", Permission: perm("partitions.delete"), Risk: plugin.RiskDestructive, AuditEvent: rid("partition.drop"), Handle: dropPartition},

		{ID: rid("indexes.list"), Method: plugin.MethodGet, Path: "/collections/{collection}/indexes", Permission: perm("indexes.read"), Risk: plugin.RiskSafe, AuditEvent: rid("indexes.list"), Handle: listIndexes},
		{ID: rid("index.read"), Method: plugin.MethodGet, Path: "/collections/{collection}/indexes/{index}", Permission: perm("indexes.read"), Risk: plugin.RiskSafe, AuditEvent: rid("index.read"), Handle: readIndex},
		{ID: rid("index.progress"), Method: plugin.MethodWS, Path: "/collections/{collection}/indexes/{index}/progress", Permission: perm("indexes.read"), Risk: plugin.RiskSafe, AuditEvent: rid("index.progress"), Stream: indexProgress},
		{ID: rid("index.create"), Method: plugin.MethodPost, Path: "/collections/{collection}/indexes", Permission: perm("indexes.write"), Risk: plugin.RiskWrite, AuditEvent: rid("index.create"), Input: indexSchema(), Handle: createIndex},
		{ID: rid("index.drop"), Method: plugin.MethodDelete, Path: "/collections/{collection}/indexes/{index}", Permission: perm("indexes.delete"), Risk: plugin.RiskDestructive, AuditEvent: rid("index.drop"), Handle: dropIndex},

		{ID: rid("segments.list"), Method: plugin.MethodGet, Path: "/collections/{collection}/segments", Permission: perm("segments.read"), Risk: plugin.RiskSafe, AuditEvent: rid("segments.list"), Handle: listSegments},

		{ID: rid("aliases.list"), Method: plugin.MethodGet, Path: "/collections/{collection}/aliases", Permission: perm("aliases.read"), Risk: plugin.RiskSafe, AuditEvent: rid("aliases.list"), Handle: listAliases},
		{ID: rid("alias.create"), Method: plugin.MethodPost, Path: "/collections/{collection}/aliases", Permission: perm("aliases.write"), Risk: plugin.RiskWrite, AuditEvent: rid("alias.create"), Input: aliasSchema(), Handle: createAlias},
		{ID: rid("alias.drop"), Method: plugin.MethodDelete, Path: "/aliases/{alias}", Permission: perm("aliases.delete"), Risk: plugin.RiskDestructive, AuditEvent: rid("alias.drop"), Handle: dropAlias},

		{ID: rid("entities.columns"), Method: plugin.MethodGet, Path: "/collections/{collection}/entities/columns", Permission: perm("entities.read"), Risk: plugin.RiskSafe, AuditEvent: rid("entities.columns"), Handle: entityColumns},
		{ID: rid("entities.list"), Method: plugin.MethodGet, Path: "/collections/{collection}/entities", Permission: perm("entities.read"), Risk: plugin.RiskSafe, AuditEvent: rid("entities.list"), Handle: listEntities},
		{ID: rid("entity.insert"), Method: plugin.MethodPost, Path: "/collections/{collection}/entities", Permission: perm("entities.write"), Risk: plugin.RiskWrite, AuditEvent: rid("entity.insert"), Handle: insertEntity},
		{ID: rid("entity.update"), Method: plugin.MethodPatch, Path: "/collections/{collection}/entities", Permission: perm("entities.write"), Risk: plugin.RiskWrite, AuditEvent: rid("entity.update"), Handle: updateEntity},
		{ID: rid("entity.delete"), Method: plugin.MethodDelete, Path: "/collections/{collection}/entities", Permission: perm("entities.delete"), Risk: plugin.RiskDestructive, AuditEvent: rid("entity.delete"), Handle: deleteEntity},

		{ID: rid("query"), Method: plugin.MethodWS, Path: "/collections/{collection}/query", Permission: perm("query.execute"), Risk: plugin.RiskSafe, AuditEvent: rid("query"), Stream: queryStream},
		{ID: rid("query.complete"), Method: plugin.MethodGet, Path: "/collections/{collection}/query/complete", Permission: perm("query.execute"), Risk: plugin.RiskSafe, AuditEvent: rid("query.complete"), Handle: queryCompletions},
		{ID: rid("queries.list"), Method: plugin.MethodGet, Path: "/queries", Permission: perm("query.read"), Risk: plugin.RiskSafe, AuditEvent: rid("queries.list"), Handle: listSavedQueries},
		{ID: rid("query.save"), Method: plugin.MethodPost, Path: "/queries", Permission: perm("query.write"), Risk: plugin.RiskWrite, AuditEvent: rid("query.save"), Input: savedQuerySchema(), Handle: saveQuery},
		{ID: rid("query.delete"), Method: plugin.MethodDelete, Path: "/queries/{id}", Permission: perm("query.delete"), Risk: plugin.RiskDestructive, AuditEvent: rid("query.delete"), Handle: deleteSavedQuery},

		{ID: rid("users.list"), Method: plugin.MethodGet, Path: "/users", Permission: perm("users.read"), Risk: plugin.RiskSafe, AuditEvent: rid("users.list"), Handle: listUsers},
		{ID: rid("user.read"), Method: plugin.MethodGet, Path: "/users/{user}", Permission: perm("users.read"), Risk: plugin.RiskSafe, AuditEvent: rid("user.read"), Handle: readUser},
		{ID: rid("user.roles"), Method: plugin.MethodGet, Path: "/users/{user}/roles", Permission: perm("users.read"), Risk: plugin.RiskSafe, AuditEvent: rid("user.roles"), Handle: listUserRoles},
		{ID: rid("user.create"), Method: plugin.MethodPost, Path: "/users", Permission: perm("users.write"), Risk: plugin.RiskWrite, AuditEvent: rid("user.create"), Input: userSchema(), Handle: createUser},
		{ID: rid("user.password"), Method: plugin.MethodPost, Path: "/users/{user}/password", Permission: perm("users.write"), Risk: plugin.RiskWrite, AuditEvent: rid("user.password"), Input: passwordSchema(), Handle: updateUserPassword},
		{ID: rid("user.role.grant"), Method: plugin.MethodPost, Path: "/users/{user}/roles", Permission: perm("users.write"), Risk: plugin.RiskPrivileged, AuditEvent: rid("user.role.grant"), Input: roleRefSchema(), Handle: grantUserRole},
		{ID: rid("user.role.revoke"), Method: plugin.MethodDelete, Path: "/users/{user}/roles/{role}", Permission: perm("users.write"), Risk: plugin.RiskDestructive, AuditEvent: rid("user.role.revoke"), Handle: revokeUserRole},
		{ID: rid("user.delete"), Method: plugin.MethodDelete, Path: "/users/{user}", Permission: perm("users.delete"), Risk: plugin.RiskDestructive, AuditEvent: rid("user.delete"), Handle: deleteUser},

		{ID: rid("roles.list"), Method: plugin.MethodGet, Path: "/roles", Permission: perm("roles.read"), Risk: plugin.RiskSafe, AuditEvent: rid("roles.list"), Handle: listRoles},
		{ID: rid("role.read"), Method: plugin.MethodGet, Path: "/roles/{role}", Permission: perm("roles.read"), Risk: plugin.RiskSafe, AuditEvent: rid("role.read"), Handle: readRole},
		{ID: rid("role.privileges"), Method: plugin.MethodGet, Path: "/roles/{role}/privileges", Permission: perm("roles.read"), Risk: plugin.RiskSafe, AuditEvent: rid("role.privileges"), Handle: listRolePrivileges},
		{ID: rid("role.create"), Method: plugin.MethodPost, Path: "/roles", Permission: perm("roles.write"), Risk: plugin.RiskWrite, AuditEvent: rid("role.create"), Input: roleSchema(), Handle: createRole},
		{ID: rid("role.privilege.grant"), Method: plugin.MethodPost, Path: "/roles/{role}/privileges", Permission: perm("roles.write"), Risk: plugin.RiskPrivileged, AuditEvent: rid("role.privilege.grant"), Input: privilegeSchema(), Handle: grantRolePrivilege},
		{ID: rid("role.privilege.revoke"), Method: plugin.MethodPost, Path: "/roles/{role}/privileges/revoke", Permission: perm("roles.write"), Risk: plugin.RiskDestructive, AuditEvent: rid("role.privilege.revoke"), Input: privilegeSchema(), Handle: revokeRolePrivilege},
		{ID: rid("role.delete"), Method: plugin.MethodDelete, Path: "/roles/{role}", Permission: perm("roles.delete"), Risk: plugin.RiskDestructive, AuditEvent: rid("role.delete"), Handle: deleteRole},

		{ID: rid("resourcegroups.list"), Method: plugin.MethodGet, Path: "/resource-groups", Permission: perm("cluster.read"), Risk: plugin.RiskSafe, AuditEvent: rid("resourcegroups.list"), Handle: listResourceGroups},
	}
}

func rid(suffix string) string  { return protocolName + "." + suffix }
func perm(suffix string) string { return protocolName + "." + suffix }

func session(rc *plugin.RequestContext) (*Session, error) { return unwrap(rc.Session) }

func clientOf(rc *plugin.RequestContext) (*Session, *milvusclient.Client, error) {
	s, err := session(rc)
	if err != nil {
		return nil, nil, err
	}
	c, err := s.clientFor(rc.Ctx, rc.Param("database"))
	if err != nil {
		return nil, nil, err
	}
	return s, c, nil
}

func writableClient(rc *plugin.RequestContext) (*Session, *milvusclient.Client, error) {
	s, err := session(rc)
	if err != nil {
		return nil, nil, err
	}
	if s.opts.ReadOnly {
		return nil, nil, fmt.Errorf("%w: connection is read-only", plugin.ErrForbidden)
	}
	c, err := s.clientFor(rc.Ctx, rc.Param("database"))
	if err != nil {
		return nil, nil, err
	}
	return s, c, nil
}

func identifier(kind, value string) (string, error) {
	value = strings.TrimSpace(value)
	if value == "" {
		return "", fmt.Errorf("%w: %s is required", plugin.ErrInvalidInput, kind)
	}
	if !identifierPattern.MatchString(value) {
		return "", fmt.Errorf("%w: invalid %s name %q", plugin.ErrInvalidInput, kind, value)
	}
	return value, nil
}

func collectionParam(rc *plugin.RequestContext) (string, error) {
	return identifier("collection", rc.Param("collection"))
}

func databaseName(rc *plugin.RequestContext) string {
	if name := strings.TrimSpace(rc.Param("database")); name != "" {
		return name
	}
	if s, err := session(rc); err == nil {
		return s.opts.Database
	}
	return defaultDatabase
}

func withTimeout(rc *plugin.RequestContext, s *Session) (context.Context, context.CancelFunc) {
	return context.WithTimeout(rc.Ctx, s.opts.Timeout)
}

func ok(result any, err error) (any, error) {
	if err != nil {
		return nil, mapError(err)
	}
	if result == nil {
		return row{"ok": true}, nil
	}
	return result, nil
}
