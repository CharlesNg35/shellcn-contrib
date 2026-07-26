package couchdb

import (
	"fmt"

	"github.com/charlesng35/shellcn/sdk/plugin"
)

func routes() []plugin.Route {
	out := []plugin.Route{
		// --- server -----------------------------------------------------------
		{ID: rid("server.list"), Method: plugin.MethodGet, Path: "/server", Permission: "couchdb.server.read", Risk: plugin.RiskSafe, AuditEvent: rid("server.list"), Handle: listServer},
		{ID: rid("server.overview"), Method: plugin.MethodGet, Path: "/server/overview", Permission: "couchdb.server.read", Risk: plugin.RiskSafe, AuditEvent: rid("server.overview"), Handle: serverOverview},
		{ID: rid("server.config"), Method: plugin.MethodGet, Path: "/server/config", Permission: "couchdb.server.read", Risk: plugin.RiskSafe, AuditEvent: rid("server.config"), Handle: serverConfig},
		{ID: rid("server.metrics"), Method: plugin.MethodWS, Path: "/server/metrics", Permission: "couchdb.server.read", Risk: plugin.RiskSafe, AuditEvent: rid("server.metrics"), Stream: metricsStream},
		{ID: rid("fauxton.url"), Method: plugin.MethodGet, Path: "/server/fauxton-url", Permission: "couchdb.server.proxy", Risk: plugin.RiskSafe, AuditEvent: rid("fauxton.url"), Handle: fauxtonURL},
		{ID: rid("tasks.list"), Method: plugin.MethodGet, Path: "/tasks", Permission: "couchdb.tasks.read", Risk: plugin.RiskSafe, AuditEvent: rid("tasks.list"), Handle: listTasks},
		{ID: rid("tasks.watch"), Method: plugin.MethodWS, Path: "/tasks/watch", Permission: "couchdb.tasks.read", Risk: plugin.RiskSafe, AuditEvent: rid("tasks.watch"), Stream: watchTasks},
		{ID: rid("nodes.list"), Method: plugin.MethodGet, Path: "/nodes", Permission: "couchdb.nodes.read", Risk: plugin.RiskSafe, AuditEvent: rid("nodes.list"), Handle: listNodes},
		{ID: rid("nodes.tree"), Method: plugin.MethodGet, Path: "/tree/nodes", Permission: "couchdb.nodes.read", Risk: plugin.RiskSafe, AuditEvent: rid("nodes.tree"), Handle: treeNodes},
		{ID: rid("node.read"), Method: plugin.MethodGet, Path: "/nodes/{node}", Permission: "couchdb.nodes.read", Risk: plugin.RiskSafe, AuditEvent: rid("node.read"), Handle: readNode},
		{ID: rid("node.stats"), Method: plugin.MethodGet, Path: "/nodes/{node}/stats", Permission: "couchdb.nodes.read", Risk: plugin.RiskSafe, AuditEvent: rid("node.stats"), Handle: nodeStats},

		// --- databases --------------------------------------------------------
		{ID: rid("databases.tree"), Method: plugin.MethodGet, Path: "/tree/databases", Permission: "couchdb.databases.read", Risk: plugin.RiskSafe, AuditEvent: rid("databases.tree"), Handle: treeDatabases},
		{ID: rid("databases.list"), Method: plugin.MethodGet, Path: "/databases", Permission: "couchdb.databases.read", Risk: plugin.RiskSafe, AuditEvent: rid("databases.list"), Handle: listDatabases},
		{ID: rid("databases.watch"), Method: plugin.MethodWS, Path: "/databases/watch", Permission: "couchdb.databases.read", Risk: plugin.RiskSafe, AuditEvent: rid("databases.watch"), Stream: watchDatabases},
		{ID: rid("database.read"), Method: plugin.MethodGet, Path: "/databases/{db}", Permission: "couchdb.databases.read", Risk: plugin.RiskSafe, AuditEvent: rid("database.read"), Handle: readDatabase},
		{ID: rid("database.watch"), Method: plugin.MethodWS, Path: "/databases/{db}/watch", Permission: "couchdb.databases.read", Risk: plugin.RiskSafe, AuditEvent: rid("database.watch"), Stream: watchDatabase},
		{ID: rid("database.create"), Method: plugin.MethodPut, Path: "/databases", Permission: "couchdb.databases.write", Risk: plugin.RiskWrite, AuditEvent: rid("database.create"), Input: createDatabaseSchema(), Handle: createDatabase},
		{ID: rid("database.delete"), Method: plugin.MethodDelete, Path: "/databases/{db}", Permission: "couchdb.databases.delete", Risk: plugin.RiskDestructive, AuditEvent: rid("database.delete"), Handle: deleteDatabase},
		{ID: rid("database.compact"), Method: plugin.MethodPost, Path: "/databases/{db}/compact", Permission: "couchdb.databases.write", Risk: plugin.RiskWrite, AuditEvent: rid("database.compact"), Handle: compactDatabase},
		{ID: rid("database.viewcleanup"), Method: plugin.MethodPost, Path: "/databases/{db}/view-cleanup", Permission: "couchdb.databases.write", Risk: plugin.RiskWrite, AuditEvent: rid("database.viewcleanup"), Handle: cleanupViews},
		{ID: rid("database.security.read"), Method: plugin.MethodGet, Path: "/databases/{db}/security", Permission: "couchdb.security.read", Risk: plugin.RiskSafe, AuditEvent: rid("database.security.read"), Handle: readSecurity},
		{ID: rid("database.security.update"), Method: plugin.MethodPut, Path: "/databases/{db}/security", Permission: "couchdb.security.write", Risk: plugin.RiskPrivileged, AuditEvent: rid("database.security.update"), Input: contentSchema("Security document"), Handle: updateSecurity},

		// --- documents --------------------------------------------------------
		{ID: rid("documents.list"), Method: plugin.MethodGet, Path: "/databases/{db}/documents", Permission: "couchdb.documents.read", Risk: plugin.RiskSafe, AuditEvent: rid("documents.list"), Handle: listDocuments},
		{ID: rid("documents.columns"), Method: plugin.MethodGet, Path: "/databases/{db}/documents/columns", Permission: "couchdb.documents.read", Risk: plugin.RiskSafe, AuditEvent: rid("documents.columns"), Handle: documentColumns},
		{ID: rid("documents.watch"), Method: plugin.MethodWS, Path: "/databases/{db}/documents/watch", Permission: "couchdb.documents.read", Risk: plugin.RiskSafe, AuditEvent: rid("documents.watch"), Stream: watchDocuments},
		{ID: rid("documents.insert"), Method: plugin.MethodPost, Path: "/databases/{db}/documents/rows", Permission: "couchdb.documents.write", Risk: plugin.RiskWrite, AuditEvent: rid("documents.insert"), Handle: insertDocumentRow},
		{ID: rid("documents.update"), Method: plugin.MethodPut, Path: "/databases/{db}/documents/rows", Permission: "couchdb.documents.write", Risk: plugin.RiskWrite, AuditEvent: rid("documents.update"), Handle: updateDocumentRow},
		{ID: rid("documents.remove"), Method: plugin.MethodDelete, Path: "/databases/{db}/documents/rows", Permission: "couchdb.documents.delete", Risk: plugin.RiskDestructive, AuditEvent: rid("documents.remove"), Handle: deleteDocumentRow},
		{ID: rid("documents.bulk"), Method: plugin.MethodPost, Path: "/databases/{db}/documents/bulk", Permission: "couchdb.documents.write", Risk: plugin.RiskWrite, AuditEvent: rid("documents.bulk"), Input: contentSchema("Bulk document payload"), Handle: bulkDocuments},
		{ID: rid("document.read"), Method: plugin.MethodGet, Path: "/databases/{db}/documents/{docid}", Permission: "couchdb.documents.read", Risk: plugin.RiskSafe, AuditEvent: rid("document.read"), Handle: readDocument},
		{ID: rid("document.watch"), Method: plugin.MethodWS, Path: "/databases/{db}/documents/{docid}/watch", Permission: "couchdb.documents.read", Risk: plugin.RiskSafe, AuditEvent: rid("document.watch"), Stream: watchDocument},
		{ID: rid("document.create"), Method: plugin.MethodPost, Path: "/databases/{db}/documents", Permission: "couchdb.documents.write", Risk: plugin.RiskWrite, AuditEvent: rid("document.create"), Input: contentSchema("Document"), Handle: createDocument},
		{ID: rid("document.update"), Method: plugin.MethodPut, Path: "/databases/{db}/documents/{docid}", Permission: "couchdb.documents.write", Risk: plugin.RiskWrite, AuditEvent: rid("document.update"), Input: contentSchema("Document"), Handle: updateDocument},
		{ID: rid("document.delete"), Method: plugin.MethodDelete, Path: "/databases/{db}/documents/{docid}", Permission: "couchdb.documents.delete", Risk: plugin.RiskDestructive, AuditEvent: rid("document.delete"), Handle: deleteDocument},
		{ID: rid("document.purge"), Method: plugin.MethodPost, Path: "/databases/{db}/documents/{docid}/purge", Permission: "couchdb.documents.purge", Risk: plugin.RiskDestructive, AuditEvent: rid("document.purge"), Handle: purgeDocument},
		{ID: rid("document.revisions"), Method: plugin.MethodGet, Path: "/databases/{db}/documents/{docid}/revisions", Permission: "couchdb.documents.read", Risk: plugin.RiskSafe, AuditEvent: rid("document.revisions"), Handle: documentRevisions},
		{ID: rid("document.conflicts"), Method: plugin.MethodGet, Path: "/databases/{db}/documents/{docid}/conflicts", Permission: "couchdb.documents.read", Risk: plugin.RiskSafe, AuditEvent: rid("document.conflicts"), Handle: documentConflicts},
		{ID: rid("document.resolve"), Method: plugin.MethodPost, Path: "/databases/{db}/documents/{docid}/resolve", Permission: "couchdb.documents.delete", Risk: plugin.RiskDestructive, AuditEvent: rid("document.resolve"), Input: resolveSchema(), Handle: resolveDocument},
		{ID: rid("conflicts.list"), Method: plugin.MethodGet, Path: "/databases/{db}/conflicts", Permission: "couchdb.documents.read", Risk: plugin.RiskSafe, AuditEvent: rid("conflicts.list"), Handle: listConflicts},
		{ID: rid("attachments.list"), Method: plugin.MethodGet, Path: "/databases/{db}/documents/{docid}/attachments", Permission: "couchdb.documents.read", Risk: plugin.RiskSafe, AuditEvent: rid("attachments.list"), Handle: listAttachments},
		{ID: rid("attachment.delete"), Method: plugin.MethodDelete, Path: "/databases/{db}/documents/{docid}/attachments/{name}", Permission: "couchdb.documents.delete", Risk: plugin.RiskDestructive, AuditEvent: rid("attachment.delete"), Handle: deleteAttachment},

		// --- mango ------------------------------------------------------------
		{ID: rid("mango.query"), Method: plugin.MethodWS, Path: "/databases/{db}/find", Permission: "couchdb.query.execute", Risk: plugin.RiskSafe, AuditEvent: rid("mango.query"), Stream: mangoStream},
		{ID: rid("mango.explain"), Method: plugin.MethodPost, Path: "/databases/{db}/explain", Permission: "couchdb.query.execute", Risk: plugin.RiskSafe, AuditEvent: rid("mango.explain"), Input: contentSchema("Mango query"), Handle: explainMango},
		{ID: rid("mango.complete"), Method: plugin.MethodGet, Path: "/databases/{db}/complete", Permission: "couchdb.query.execute", Risk: plugin.RiskSafe, AuditEvent: rid("mango.complete"), Handle: completeMango},
		{ID: rid("indexes.list"), Method: plugin.MethodGet, Path: "/databases/{db}/indexes", Permission: "couchdb.indexes.read", Risk: plugin.RiskSafe, AuditEvent: rid("indexes.list"), Handle: listIndexes},
		{ID: rid("index.create"), Method: plugin.MethodPost, Path: "/databases/{db}/indexes", Permission: "couchdb.indexes.write", Risk: plugin.RiskWrite, AuditEvent: rid("index.create"), Input: createIndexSchema(), Handle: createIndex},
		{ID: rid("index.delete"), Method: plugin.MethodDelete, Path: "/databases/{db}/indexes/{ddoc}/{name}", Permission: "couchdb.indexes.delete", Risk: plugin.RiskDestructive, AuditEvent: rid("index.delete"), Handle: deleteIndex},

		// --- design documents & views -----------------------------------------
		{ID: rid("designdocs.list"), Method: plugin.MethodGet, Path: "/databases/{db}/design-docs", Permission: "couchdb.designdocs.read", Risk: plugin.RiskSafe, AuditEvent: rid("designdocs.list"), Handle: listDesignDocs},
		{ID: rid("designdoc.read"), Method: plugin.MethodGet, Path: "/databases/{db}/design-docs/{ddoc}", Permission: "couchdb.designdocs.read", Risk: plugin.RiskSafe, AuditEvent: rid("designdoc.read"), Handle: readDesignDoc},
		{ID: rid("designdoc.create"), Method: plugin.MethodPost, Path: "/databases/{db}/design-docs", Permission: "couchdb.designdocs.write", Risk: plugin.RiskWrite, AuditEvent: rid("designdoc.create"), Input: contentSchema("Design document"), Handle: createDesignDoc},
		{ID: rid("designdoc.save"), Method: plugin.MethodPut, Path: "/databases/{db}/design-docs/{ddoc}", Permission: "couchdb.designdocs.write", Risk: plugin.RiskWrite, AuditEvent: rid("designdoc.save"), Input: contentSchema("Design document"), Handle: saveDesignDoc},
		{ID: rid("designdoc.delete"), Method: plugin.MethodDelete, Path: "/databases/{db}/design-docs/{ddoc}", Permission: "couchdb.designdocs.delete", Risk: plugin.RiskDestructive, AuditEvent: rid("designdoc.delete"), Handle: deleteDesignDoc},
		{ID: rid("views.list"), Method: plugin.MethodGet, Path: "/databases/{db}/design-docs/{ddoc}/views", Permission: "couchdb.designdocs.read", Risk: plugin.RiskSafe, AuditEvent: rid("views.list"), Handle: listViews},
		{ID: rid("view.rows"), Method: plugin.MethodGet, Path: "/databases/{db}/design-docs/{ddoc}/views/{view}/rows", Permission: "couchdb.views.read", Risk: plugin.RiskSafe, AuditEvent: rid("view.rows"), Handle: viewRows},
		{ID: rid("view.reduce"), Method: plugin.MethodGet, Path: "/databases/{db}/design-docs/{ddoc}/views/{view}/reduce", Permission: "couchdb.views.read", Risk: plugin.RiskSafe, AuditEvent: rid("view.reduce"), Handle: viewReduce},
		{ID: rid("view.definition"), Method: plugin.MethodGet, Path: "/databases/{db}/design-docs/{ddoc}/views/{view}", Permission: "couchdb.views.read", Risk: plugin.RiskSafe, AuditEvent: rid("view.definition"), Handle: viewDefinition},

		// --- replication ------------------------------------------------------
		{ID: rid("replications.tree"), Method: plugin.MethodGet, Path: "/tree/replications", Permission: "couchdb.replications.read", Risk: plugin.RiskSafe, AuditEvent: rid("replications.tree"), Handle: treeReplications},
		{ID: rid("replications.list"), Method: plugin.MethodGet, Path: "/replications", Permission: "couchdb.replications.read", Risk: plugin.RiskSafe, AuditEvent: rid("replications.list"), Handle: listReplications},
		{ID: rid("replications.watch"), Method: plugin.MethodWS, Path: "/replications/watch", Permission: "couchdb.replications.read", Risk: plugin.RiskSafe, AuditEvent: rid("replications.watch"), Stream: watchReplications},
		{ID: rid("replication.graph"), Method: plugin.MethodGet, Path: "/replications/graph", Permission: "couchdb.replications.read", Risk: plugin.RiskSafe, AuditEvent: rid("replication.graph"), Handle: replicationGraph},
		{ID: rid("scheduler.jobs"), Method: plugin.MethodGet, Path: "/scheduler/jobs", Permission: "couchdb.replications.read", Risk: plugin.RiskSafe, AuditEvent: rid("scheduler.jobs"), Handle: schedulerJobs},
		{ID: rid("replication.read"), Method: plugin.MethodGet, Path: "/replications/{id}", Permission: "couchdb.replications.read", Risk: plugin.RiskSafe, AuditEvent: rid("replication.read"), Handle: readReplication},
		{ID: rid("replication.events"), Method: plugin.MethodGet, Path: "/replications/{id}/events", Permission: "couchdb.replications.read", Risk: plugin.RiskSafe, AuditEvent: rid("replication.events"), Handle: replicationEvents},
		{ID: rid("replication.create"), Method: plugin.MethodPost, Path: "/replications", Permission: "couchdb.replications.write", Risk: plugin.RiskWrite, AuditEvent: rid("replication.create"), Input: createReplicationSchema(), Handle: createReplication},
		{ID: rid("replication.delete"), Method: plugin.MethodDelete, Path: "/replications/{id}", Permission: "couchdb.replications.delete", Risk: plugin.RiskDestructive, AuditEvent: rid("replication.delete"), Handle: deleteReplication},

		// --- changes feed -----------------------------------------------------
		{ID: rid("changes.stream"), Method: plugin.MethodWS, Path: "/databases/{db}/changes", Permission: "couchdb.changes.read", Risk: plugin.RiskSafe, AuditEvent: rid("changes.stream"), Stream: changesStream},
		{ID: rid("changes.origins"), Method: plugin.MethodGet, Path: "/databases/{db}/changes/origins", Permission: "couchdb.changes.read", Risk: plugin.RiskSafe, AuditEvent: rid("changes.origins"), Handle: changeOrigins},

		// --- saved Mango queries (plugin storage) -----------------------------
		{ID: rid("queries.list"), Method: plugin.MethodGet, Path: "/queries", Permission: "couchdb.query.read", Risk: plugin.RiskSafe, AuditEvent: rid("queries.list"), Handle: listSavedQueries},
		{ID: rid("query.save"), Method: plugin.MethodPost, Path: "/queries", Permission: "couchdb.query.write", Risk: plugin.RiskWrite, AuditEvent: rid("query.save"), Input: saveQuerySchema(), Handle: saveQuery},
		{ID: rid("query.delete"), Method: plugin.MethodDelete, Path: "/queries", Permission: "couchdb.query.delete", Risk: plugin.RiskDestructive, AuditEvent: rid("query.delete"), Handle: deleteSavedQuery},
	}

	for i := range out {
		if out[i].Handle == nil || pluginOwnedRoutes[out[i].ID] {
			continue
		}
		switch out[i].Risk {
		case plugin.RiskWrite, plugin.RiskDestructive, plugin.RiskPrivileged:
			out[i].Handle = readOnlyGuard(out[i].Handle)
		}
	}
	return out
}

// pluginOwnedRoutes write to ShellCN's own scoped storage rather than to
// CouchDB, so a read-only connection may still manage them.
var pluginOwnedRoutes = map[string]bool{
	rid("query.save"):   true,
	rid("query.delete"): true,
}

// readOnlyGuard refuses every mutating handler when the connection was saved in
// read-only mode. Saved queries are plugin-owned storage, not CouchDB state, so
// they stay writable.
func readOnlyGuard(next plugin.Handler) plugin.Handler {
	return func(rc *plugin.RequestContext) (any, error) {
		s, err := session(rc)
		if err == nil && s.opts.readOnly {
			return nil, fmt.Errorf("%w: this connection is read-only", plugin.ErrForbidden)
		}
		return next(rc)
	}
}

// --- input schemas -----------------------------------------------------------

// contentSchema is the JSON-body contract shared by every code-editor save and
// raw-JSON action: the editor posts {"content": "<text>"}.
func contentSchema(label string) *plugin.Schema {
	return &plugin.Schema{Groups: []plugin.Group{{Name: label, Fields: []plugin.Field{
		{Key: "content", Label: label, Type: plugin.FieldJSON, Required: true},
	}}}}
}

func createDatabaseSchema() *plugin.Schema {
	return &plugin.Schema{Groups: []plugin.Group{{Name: "Database", Fields: []plugin.Field{
		{
			Key: "name", Label: "Name", Type: plugin.FieldText, Required: true,
			Placeholder: "orders",
			Help:        "Lowercase letters, digits and _$()+-/ only; must start with a letter.",
			Validators: []plugin.Validator{
				{Type: plugin.ValidatorRegex, Value: `^[a-z][a-z0-9_$()+/-]*$`, Message: "must start with a lowercase letter and use a-z 0-9 _ $ ( ) + - /"},
			},
		},
		{
			Key: "shards", Label: "Shards (q)", Type: plugin.FieldNumber, Default: 2,
			Validators: []plugin.Validator{{Type: plugin.ValidatorMin, Value: 1}, {Type: plugin.ValidatorMax, Value: 64}},
		},
		{
			Key: "replicas", Label: "Replicas (n)", Type: plugin.FieldNumber, Default: 1,
			Validators: []plugin.Validator{{Type: plugin.ValidatorMin, Value: 1}, {Type: plugin.ValidatorMax, Value: 9}},
		},
		{Key: "partitioned", Label: "Partitioned", Type: plugin.FieldToggle, Help: "Partitioned databases require every document id to be \"partition:key\"."},
	}}}}
}

func createIndexSchema() *plugin.Schema {
	return &plugin.Schema{Groups: []plugin.Group{{Name: "Mango index", Fields: []plugin.Field{
		{Key: "name", Label: "Index name", Type: plugin.FieldText, Placeholder: "by-type-created"},
		{Key: "ddoc", Label: "Design document", Type: plugin.FieldText, Placeholder: "mango-indexes", Help: "Leave empty to let CouchDB pick one."},
		{
			Key: "fields", Label: "Fields", Type: plugin.FieldArray, Required: true, MinItems: 1,
			AddLabel: "Add field", ItemLabel: "Field",
			Item: &plugin.Field{Key: "field", Label: "Field", Type: plugin.FieldText, Required: true},
			Help: "Ordered index keys, e.g. type then created_at.",
		},
		{
			Key: "type", Label: "Index type", Type: plugin.FieldSelect, Default: "json",
			Options: []plugin.Option{{Label: "JSON", Value: "json"}, {Label: "Text (requires the search build)", Value: "text"}},
		},
		{Key: "partial_selector", Label: "Partial filter selector", Type: plugin.FieldJSON, Help: "Optional selector limiting which documents enter the index."},
	}}}}
}

func createReplicationSchema() *plugin.Schema {
	return &plugin.Schema{Groups: []plugin.Group{{Name: "Replication", Fields: []plugin.Field{
		{Key: "id", Label: "Replication id", Type: plugin.FieldText, Placeholder: "orders-to-backup", Help: "Document id inside _replicator. Generated when empty."},
		{
			Key: "source", Label: "Source", Type: plugin.FieldText, Required: true, Placeholder: "orders or https://remote/orders",
			Help: "A local database name is expanded to this connection's own URL; CouchDB 3 rejects bare names inside _replicator.",
		},
		{Key: "target", Label: "Target", Type: plugin.FieldText, Required: true, Placeholder: "orders-backup or https://remote/orders"},
		{Key: "continuous", Label: "Continuous", Type: plugin.FieldToggle, Help: "Keep replicating until the document is deleted."},
		{Key: "create_target", Label: "Create target", Type: plugin.FieldToggle},
		{
			Key: "use_connection_credentials", Label: "Use this connection's credentials", Type: plugin.FieldToggle,
			Help: "Attaches this connection's username and password to local endpoints. CouchDB stores them in clear text inside the _replicator document.",
		},
		{Key: "selector", Label: "Selector", Type: plugin.FieldJSON, Help: "Optional Mango selector restricting which documents replicate."},
		{Key: "doc_ids", Label: "Document ids", Type: plugin.FieldArray, AddLabel: "Add document id", ItemLabel: "Document id",
			Item: &plugin.Field{Key: "doc_id", Label: "Document id", Type: plugin.FieldText}},
	}}}}
}

func resolveSchema() *plugin.Schema {
	return &plugin.Schema{Groups: []plugin.Group{{Name: "Conflict resolution", Fields: []plugin.Field{
		{
			Key: "keep", Label: "Revision to keep", Type: plugin.FieldSelect, Default: "winner",
			Options: []plugin.Option{
				{Label: "CouchDB's winning revision", Value: "winner"},
				{Label: "Highest generation", Value: "highest"},
				{Label: "An explicit revision", Value: "explicit"},
			},
		},
		{
			Key: "rev", Label: "Revision", Type: plugin.FieldText, Placeholder: "3-abc...",
			VisibleWhen: &plugin.Condition{AllOf: []plugin.Rule{{Field: "keep", Op: plugin.OpEq, Value: "explicit"}}},
		},
	}}}}
}

func saveQuerySchema() *plugin.Schema {
	return &plugin.Schema{Groups: []plugin.Group{{Name: "Saved query", Fields: []plugin.Field{
		{Key: "name", Label: "Name", Type: plugin.FieldText, Required: true},
		{Key: "query", Label: "Mango query", Type: plugin.FieldJSON, Required: true, Default: "{\n  \"selector\": {}\n}"},
	}}}}
}
