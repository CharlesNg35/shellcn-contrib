package couchbase

import (
	"fmt"

	"github.com/charlesng35/shellcn/sdk/plugin"
)

func routes() []plugin.Route {
	out := []plugin.Route{
		// --- navigation ------------------------------------------------------
		{ID: rid("tree.buckets"), Method: plugin.MethodGet, Path: "/tree/buckets",
			Permission: "couchbase.buckets.read", Risk: plugin.RiskSafe, AuditEvent: rid("tree.buckets"), Handle: treeBuckets},
		{ID: rid("tree.scopes"), Method: plugin.MethodGet, Path: "/tree/buckets/{bucket}/scopes",
			Permission: "couchbase.buckets.read", Risk: plugin.RiskSafe, AuditEvent: rid("tree.scopes"), Handle: treeScopes},
		{ID: rid("tree.collections"), Method: plugin.MethodGet, Path: "/tree/buckets/{bucket}/scopes/{scope}/collections",
			Permission: "couchbase.buckets.read", Risk: plugin.RiskSafe, AuditEvent: rid("tree.collections"), Handle: treeCollections},

		// --- cluster ---------------------------------------------------------
		{ID: rid("cluster.list"), Method: plugin.MethodGet, Path: "/cluster",
			Permission: "couchbase.cluster.read", Risk: plugin.RiskSafe, AuditEvent: rid("cluster.list"), Handle: clusterList},
		{ID: rid("cluster.overview"), Method: plugin.MethodGet, Path: "/cluster/overview",
			Permission: "couchbase.cluster.read", Risk: plugin.RiskSafe, AuditEvent: rid("cluster.overview"), Handle: clusterOverview},
		{ID: rid("cluster.watch"), Method: plugin.MethodWS, Path: "/cluster/watch",
			Permission: "couchbase.cluster.read", Risk: plugin.RiskSafe, AuditEvent: rid("cluster.watch"), Stream: clusterWatch},
		{ID: rid("cluster.metrics"), Method: plugin.MethodWS, Path: "/cluster/metrics",
			Permission: "couchbase.cluster.read", Risk: plugin.RiskSafe, AuditEvent: rid("cluster.metrics"), Stream: clusterMetrics},
		{ID: rid("nodes.list"), Method: plugin.MethodGet, Path: "/nodes",
			Permission: "couchbase.cluster.read", Risk: plugin.RiskSafe, AuditEvent: rid("nodes.list"), Handle: nodesList},
		{ID: rid("events.list"), Method: plugin.MethodGet, Path: "/events",
			Permission: "couchbase.cluster.read", Risk: plugin.RiskSafe, AuditEvent: rid("events.list"), Handle: eventsList},
		{ID: rid("events.watch"), Method: plugin.MethodWS, Path: "/events/watch",
			Permission: "couchbase.cluster.read", Risk: plugin.RiskSafe, AuditEvent: rid("events.watch"), Stream: eventsWatch},
		{ID: rid("resource.watch"), Method: plugin.MethodWS, Path: "/watch/{kind}",
			Permission: "couchbase.resources.watch", Risk: plugin.RiskSafe, AuditEvent: rid("resource.watch"), Stream: resourceWatch},
		{ID: rid("users.list"), Method: plugin.MethodGet, Path: "/users",
			Permission: "couchbase.security.read", Risk: plugin.RiskSafe, AuditEvent: rid("users.list"), Handle: usersList},

		// --- buckets ---------------------------------------------------------
		{ID: rid("buckets.list"), Method: plugin.MethodGet, Path: "/buckets",
			Permission: "couchbase.buckets.read", Risk: plugin.RiskSafe, AuditEvent: rid("buckets.list"), Handle: bucketsList},
		{ID: rid("bucket.read"), Method: plugin.MethodGet, Path: "/buckets/{bucket}",
			Permission: "couchbase.buckets.read", Risk: plugin.RiskSafe, AuditEvent: rid("bucket.read"), Handle: bucketRead},
		{ID: rid("bucket.dcp"), Method: plugin.MethodGet, Path: "/buckets/{bucket}/dcp",
			Permission: "couchbase.buckets.read", Risk: plugin.RiskSafe, AuditEvent: rid("bucket.dcp"), Handle: bucketDCP},
		{ID: rid("bucket.metrics"), Method: plugin.MethodWS, Path: "/buckets/{bucket}/metrics",
			Permission: "couchbase.buckets.read", Risk: plugin.RiskSafe, AuditEvent: rid("bucket.metrics"), Stream: bucketMetrics},
		{ID: rid("bucket.create"), Method: plugin.MethodPost, Path: "/buckets",
			Permission: "couchbase.buckets.write", Risk: plugin.RiskWrite, AuditEvent: rid("bucket.create"),
			Input: createBucketSchema(), Handle: bucketCreate},
		{ID: rid("bucket.update"), Method: plugin.MethodPut, Path: "/buckets/{bucket}",
			Permission: "couchbase.buckets.write", Risk: plugin.RiskWrite, AuditEvent: rid("bucket.update"),
			Input: updateBucketSchema(), Handle: bucketUpdate},
		{ID: rid("bucket.compact"), Method: plugin.MethodPost, Path: "/buckets/{bucket}/compact",
			Permission: "couchbase.buckets.write", Risk: plugin.RiskWrite, AuditEvent: rid("bucket.compact"), Handle: bucketCompact},
		{ID: rid("bucket.flush"), Method: plugin.MethodPost, Path: "/buckets/{bucket}/flush",
			Permission: "couchbase.buckets.delete", Risk: plugin.RiskDestructive, AuditEvent: rid("bucket.flush"), Handle: bucketFlush},
		{ID: rid("bucket.delete"), Method: plugin.MethodDelete, Path: "/buckets/{bucket}",
			Permission: "couchbase.buckets.delete", Risk: plugin.RiskDestructive, AuditEvent: rid("bucket.delete"), Handle: bucketDelete},

		// --- scopes and collections ------------------------------------------
		{ID: rid("scopes.list"), Method: plugin.MethodGet, Path: "/buckets/{bucket}/scopes",
			Permission: "couchbase.buckets.read", Risk: plugin.RiskSafe, AuditEvent: rid("scopes.list"), Handle: scopesList},
		{ID: rid("scope.read"), Method: plugin.MethodGet, Path: "/scopes/{keyspace}",
			Permission: "couchbase.buckets.read", Risk: plugin.RiskSafe, AuditEvent: rid("scope.read"), Handle: scopeRead},
		{ID: rid("scope.create"), Method: plugin.MethodPost, Path: "/buckets/{bucket}/scopes",
			Permission: "couchbase.buckets.write", Risk: plugin.RiskWrite, AuditEvent: rid("scope.create"),
			Input: createScopeSchema(), Handle: scopeCreate},
		{ID: rid("scope.delete"), Method: plugin.MethodDelete, Path: "/scopes/{keyspace}",
			Permission: "couchbase.buckets.delete", Risk: plugin.RiskDestructive, AuditEvent: rid("scope.delete"), Handle: scopeDelete},
		{ID: rid("collections.list"), Method: plugin.MethodGet, Path: "/collections",
			Permission: "couchbase.buckets.read", Risk: plugin.RiskSafe, AuditEvent: rid("collections.list"), Handle: collectionsList},
		{ID: rid("collection.read"), Method: plugin.MethodGet, Path: "/collections/{keyspace}",
			Permission: "couchbase.buckets.read", Risk: plugin.RiskSafe, AuditEvent: rid("collection.read"), Handle: collectionRead},
		{ID: rid("collection.schema"), Method: plugin.MethodGet, Path: "/collections/{keyspace}/schema",
			Permission: "couchbase.documents.read", Risk: plugin.RiskSafe, AuditEvent: rid("collection.schema"), Handle: collectionSchema},
		{ID: rid("collection.create"), Method: plugin.MethodPost, Path: "/scopes/{keyspace}/collections",
			Permission: "couchbase.buckets.write", Risk: plugin.RiskWrite, AuditEvent: rid("collection.create"),
			Input: createCollectionSchema(), Handle: collectionCreate},
		{ID: rid("collection.delete"), Method: plugin.MethodDelete, Path: "/collections/{keyspace}",
			Permission: "couchbase.buckets.delete", Risk: plugin.RiskDestructive, AuditEvent: rid("collection.delete"), Handle: collectionDelete},

		// --- documents and KV -------------------------------------------------
		{ID: rid("documents.list"), Method: plugin.MethodGet, Path: "/documents",
			Permission: "couchbase.documents.read", Risk: plugin.RiskSafe, AuditEvent: rid("documents.list"), Handle: documentsList},
		{ID: rid("documents.columns"), Method: plugin.MethodGet, Path: "/documents/columns",
			Permission: "couchbase.documents.read", Risk: plugin.RiskSafe, AuditEvent: rid("documents.columns"), Handle: documentColumns},
		{ID: rid("document.read"), Method: plugin.MethodGet, Path: "/documents/{id}",
			Permission: "couchbase.documents.read", Risk: plugin.RiskSafe, AuditEvent: rid("document.read"), Handle: documentRead},
		{ID: rid("document.content"), Method: plugin.MethodGet, Path: "/documents/{id}/content",
			Permission: "couchbase.documents.read", Risk: plugin.RiskSafe, AuditEvent: rid("document.content"), Handle: documentContent},
		{ID: rid("document.insert"), Method: plugin.MethodPost, Path: "/documents",
			Permission: "couchbase.documents.write", Risk: plugin.RiskWrite, AuditEvent: rid("document.insert"),
			Input: createDocumentSchema(), Handle: documentInsert},
		{ID: rid("document.update"), Method: plugin.MethodPut, Path: "/documents",
			Permission: "couchbase.documents.write", Risk: plugin.RiskWrite, AuditEvent: rid("document.update"), Handle: documentUpdate},
		{ID: rid("document.save"), Method: plugin.MethodPut, Path: "/documents/{id}",
			Permission: "couchbase.documents.write", Risk: plugin.RiskWrite, AuditEvent: rid("document.save"), Handle: documentSave},
		{ID: rid("document.remove"), Method: plugin.MethodDelete, Path: "/documents/{id}",
			Permission: "couchbase.documents.delete", Risk: plugin.RiskDestructive, AuditEvent: rid("document.remove"), Handle: documentRemove},

		// --- indexes ----------------------------------------------------------
		{ID: rid("indexes.list"), Method: plugin.MethodGet, Path: "/indexes",
			Permission: "couchbase.indexes.read", Risk: plugin.RiskSafe, AuditEvent: rid("indexes.list"), Handle: indexesList},
		{ID: rid("index.read"), Method: plugin.MethodGet, Path: "/indexes/{id}",
			Permission: "couchbase.indexes.read", Risk: plugin.RiskSafe, AuditEvent: rid("index.read"), Handle: indexRead},
		{ID: rid("index.create"), Method: plugin.MethodPost, Path: "/indexes",
			Permission: "couchbase.indexes.write", Risk: plugin.RiskWrite, AuditEvent: rid("index.create"),
			Input: createIndexSchema(), Handle: indexCreate},
		{ID: rid("index.build"), Method: plugin.MethodPost, Path: "/indexes/{id}/build",
			Permission: "couchbase.indexes.write", Risk: plugin.RiskWrite, AuditEvent: rid("index.build"), Handle: indexBuild},
		{ID: rid("index.drop"), Method: plugin.MethodDelete, Path: "/indexes/{id}",
			Permission: "couchbase.indexes.delete", Risk: plugin.RiskDestructive, AuditEvent: rid("index.drop"), Handle: indexDrop},

		// --- XDCR --------------------------------------------------------------
		{ID: rid("replications.list"), Method: plugin.MethodGet, Path: "/xdcr/replications",
			Permission: "couchbase.xdcr.read", Risk: plugin.RiskSafe, AuditEvent: rid("replications.list"), Handle: replicationsList},
		{ID: rid("replication.read"), Method: plugin.MethodGet, Path: "/xdcr/replications/{id}",
			Permission: "couchbase.xdcr.read", Risk: plugin.RiskSafe, AuditEvent: rid("replication.read"), Handle: replicationRead},
		{ID: rid("remotes.list"), Method: plugin.MethodGet, Path: "/xdcr/remotes",
			Permission: "couchbase.xdcr.read", Risk: plugin.RiskSafe, AuditEvent: rid("remotes.list"), Handle: remotesList},

		// --- SQL++ -------------------------------------------------------------
		{ID: rid("query"), Method: plugin.MethodWS, Path: "/query",
			Permission: "couchbase.query.exec", Risk: plugin.RiskPrivileged, AuditEvent: rid("query"), Stream: queryStream},
		{ID: rid("advisor"), Method: plugin.MethodWS, Path: "/advisor",
			Permission: "couchbase.indexes.read", Risk: plugin.RiskSafe, AuditEvent: rid("advisor"), Stream: advisorStream},
		{ID: rid("query.complete"), Method: plugin.MethodGet, Path: "/query/complete",
			Permission: "couchbase.query.exec", Risk: plugin.RiskSafe, AuditEvent: rid("query.complete"), Handle: queryComplete},
		{ID: rid("query.cancel"), Method: plugin.MethodPost, Path: "/query/cancel",
			Permission: "couchbase.query.exec", Risk: plugin.RiskWrite, AuditEvent: rid("query.cancel"), Handle: queryCancel},
		{ID: rid("queries.list"), Method: plugin.MethodGet, Path: "/saved-queries",
			Permission: "couchbase.query.exec", Risk: plugin.RiskSafe, AuditEvent: rid("queries.list"), Handle: savedQueriesList},
		{ID: rid("query.save"), Method: plugin.MethodPost, Path: "/saved-queries",
			Permission: "couchbase.query.exec", Risk: plugin.RiskWrite, AuditEvent: rid("query.save"),
			Input: saveQuerySchema(), Handle: savedQuerySave},
		{ID: rid("query.delete"), Method: plugin.MethodDelete, Path: "/saved-queries/{id}",
			Permission: "couchbase.query.exec", Risk: plugin.RiskDestructive, AuditEvent: rid("query.delete"), Handle: savedQueryDelete},

		// --- data map and console ---------------------------------------------
		{ID: rid("datamap"), Method: plugin.MethodWS, Path: "/datamap",
			Permission: "couchbase.buckets.read", Risk: plugin.RiskSafe, AuditEvent: rid("datamap"), Stream: dataMapStream},
		{ID: rid("console.url"), Method: plugin.MethodGet, Path: "/console-url",
			Permission: "couchbase.console.open", Risk: plugin.RiskSafe, AuditEvent: rid("console.url"), Handle: consoleURL},
	}

	for i := range out {
		if out[i].Handle == nil {
			continue
		}
		switch out[i].Risk {
		case plugin.RiskWrite, plugin.RiskDestructive:
			out[i].Handle = readOnlyGuard(out[i].ID, out[i].Handle)
		}
	}
	return out
}

// readOnlyGuard refuses target mutations while the connection is in read-only
// mode. Plugin-owned storage records are the operator's own data, so they stay
// writable.
func readOnlyGuard(id string, next plugin.Handler) plugin.Handler {
	switch id {
	case rid("query.save"), rid("query.delete"), rid("query.cancel"):
		return next
	}
	return func(rc *plugin.RequestContext) (any, error) {
		s, err := session(rc)
		if err != nil {
			return nil, err
		}
		if s.opts.ReadOnly {
			return nil, fmt.Errorf("%w: read-only mode blocks %s", plugin.ErrForbidden, id)
		}
		return next(rc)
	}
}

// --- input schemas ----------------------------------------------------------

func createBucketSchema() *plugin.Schema {
	return &plugin.Schema{Groups: []plugin.Group{{Name: "Bucket", Fields: []plugin.Field{
		{Key: "name", Label: "Bucket name", Type: plugin.FieldText, Required: true,
			Validators: []plugin.Validator{{Type: plugin.ValidatorRegex, Value: bucketNameRE.String(), Message: "letters, digits, and . _ % -"}}},
		{Key: "bucket_type", Label: "Bucket type", Type: plugin.FieldSelect, Required: true, Default: "couchbase", Options: []plugin.Option{
			{Label: "Couchbase (persistent)", Value: "couchbase"},
			{Label: "Ephemeral (memory only)", Value: "ephemeral"},
		}},
		{Key: "ram_quota_mb", Label: "RAM quota (MB)", Type: plugin.FieldNumber, Required: true, Default: 128,
			Validators: []plugin.Validator{{Type: plugin.ValidatorMin, Value: 100}, {Type: plugin.ValidatorMax, Value: 1048576}}},
		{Key: "replica_number", Label: "Replicas", Type: plugin.FieldStepper, Default: 0,
			Validators: []plugin.Validator{{Type: plugin.ValidatorMin, Value: 0}, {Type: plugin.ValidatorMax, Value: 3}}},
		{Key: "eviction_policy", Label: "Eviction policy", Type: plugin.FieldSelect, Default: "valueOnly", Options: []plugin.Option{
			{Label: "Value only", Value: "valueOnly"},
			{Label: "Full ejection", Value: "fullEviction"},
			{Label: "No eviction (ephemeral)", Value: "noEviction"},
			{Label: "NRU eviction (ephemeral)", Value: "nruEviction"},
		}},
		{Key: "flush_enabled", Label: "Allow flush", Type: plugin.FieldToggle, Help: "Permits destroying every document in the bucket in one operation."},
	}}}}
}

// updateBucketSchema prefills from the bucket row the browser already holds.
// ${resource.*} carries only the resource identity, so the current settings have
// to come from the record, which keeps their number and boolean types.
func updateBucketSchema() *plugin.Schema {
	return &plugin.Schema{Groups: []plugin.Group{{Name: "Bucket", Fields: []plugin.Field{
		{Key: "ram_quota_mb", Label: "RAM quota (MB)", Type: plugin.FieldNumber, Required: true, Default: "${record.ram_quota_mb}",
			Help:       "Per-node memory quota; shrinking it below the resident working set forces ejections.",
			Validators: []plugin.Validator{{Type: plugin.ValidatorMin, Value: 100}, {Type: plugin.ValidatorMax, Value: 1048576}}},
		{Key: "replica_number", Label: "Replicas", Type: plugin.FieldStepper, Default: "${record.replicas}",
			Help:       "Changing the replica count triggers a rebalance before it takes effect.",
			Validators: []plugin.Validator{{Type: plugin.ValidatorMin, Value: 0}, {Type: plugin.ValidatorMax, Value: 3}}},
		{Key: "flush_enabled", Label: "Allow flush", Type: plugin.FieldToggle, Default: "${record.flush_enabled}",
			Help: "Permits destroying every document in the bucket in one operation."},
	}}}}
}

func createScopeSchema() *plugin.Schema {
	return &plugin.Schema{Groups: []plugin.Group{{Name: "Scope", Fields: []plugin.Field{
		{Key: "name", Label: "Scope name", Type: plugin.FieldText, Required: true,
			Validators: []plugin.Validator{{Type: plugin.ValidatorRegex, Value: scopeNameRE.String(), Message: "letters, digits, and _ % -"}}},
	}}}}
}

func createCollectionSchema() *plugin.Schema {
	return &plugin.Schema{Groups: []plugin.Group{{Name: "Collection", Fields: []plugin.Field{
		{Key: "name", Label: "Collection name", Type: plugin.FieldText, Required: true,
			Validators: []plugin.Validator{{Type: plugin.ValidatorRegex, Value: scopeNameRE.String(), Message: "letters, digits, and _ % -"}}},
		{Key: "max_ttl", Label: "Document expiry (seconds)", Type: plugin.FieldNumber, Default: 0,
			Help:       "0 uses the bucket default; -1 disables expiry for this collection.",
			Validators: []plugin.Validator{{Type: plugin.ValidatorMin, Value: -1}}},
	}}}}
}

func createDocumentSchema() *plugin.Schema {
	return &plugin.Schema{Groups: []plugin.Group{{Name: "Document", Fields: []plugin.Field{
		{Key: "key", Label: "Document ID", Type: plugin.FieldText, Required: true, Placeholder: "order::1001"},
		{Key: "value", Label: "Document", Type: plugin.FieldJSON, Required: true,
			Help: "A JSON object; Couchbase stores it verbatim under the document ID."},
	}}}}
}

func createIndexSchema() *plugin.Schema {
	secondary := &plugin.Condition{AllOf: []plugin.Rule{{Field: "primary", Op: plugin.OpNeq, Value: true}}}
	return &plugin.Schema{Groups: []plugin.Group{{Name: "Index", Fields: []plugin.Field{
		{Key: "name", Label: "Index name", Type: plugin.FieldText, Required: true,
			Validators: []plugin.Validator{{Type: plugin.ValidatorRegex, Value: `^[A-Za-z][A-Za-z0-9_\-]{0,199}$`, Message: "letters, digits, underscore, dash"}}},
		{Key: "primary", Label: "Primary index", Type: plugin.FieldToggle, Help: "Indexes every document key in the collection."},
		{Key: "fields", Label: "Indexed fields", Type: plugin.FieldMultiSelect, VisibleWhen: secondary,
			OptionsSource: &plugin.DataSource{RouteID: rid("documents.columns"), Params: map[string]string{"keyspace": "${resource.uid}"}},
			Help:          "Fields are indexed in the order chosen; the leading field drives the index scan."},
		{Key: "deferred", Label: "Defer build", Type: plugin.FieldToggle, Help: "Creates the index definition without building it, so several indexes can be built together."},
		{Key: "num_replica", Label: "Index replicas", Type: plugin.FieldStepper, Default: 0,
			Validators: []plugin.Validator{{Type: plugin.ValidatorMin, Value: 0}, {Type: plugin.ValidatorMax, Value: 3}}},
	}}}}
}

func saveQuerySchema() *plugin.Schema {
	return &plugin.Schema{Groups: []plugin.Group{{Name: "Saved query", Fields: []plugin.Field{
		{Key: "name", Label: "Name", Type: plugin.FieldText, Required: true},
		{Key: "statement", Label: "SQL++", Type: plugin.FieldTextarea, Required: true},
		{Key: "description", Label: "Description", Type: plugin.FieldText},
		{Key: "keyspace", Label: "Keyspace", Type: plugin.FieldText, Placeholder: "bucket.scope.collection"},
	}}}}
}
