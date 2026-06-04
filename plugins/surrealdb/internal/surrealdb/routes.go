package surrealdb

import "github.com/charlesng35/shellcn/sdk/plugin"

func routes() []plugin.Route {
	return []plugin.Route{
		// --- tree ------------------------------------------------------------
		{
			ID: "surrealdb.tree.tables", Method: plugin.MethodGet, Path: "/tree/tables",
			Permission: "surrealdb.tables.read", Risk: plugin.RiskSafe, AuditEvent: "surrealdb.tree.tables",
			Handle: treeTables,
		},

		// --- database overview ----------------------------------------------
		{
			ID: "surrealdb.db.overview", Method: plugin.MethodGet, Path: "/overview",
			Permission: "surrealdb.tables.read", Risk: plugin.RiskSafe, AuditEvent: "surrealdb.db.overview",
			Handle: dbOverview,
		},

		// --- tables ----------------------------------------------------------
		{
			ID: "surrealdb.tables.list", Method: plugin.MethodGet, Path: "/tables",
			Permission: "surrealdb.tables.read", Risk: plugin.RiskSafe, AuditEvent: "surrealdb.tables.list",
			Handle: listTables,
		},
		{
			ID: "surrealdb.table.define", Method: plugin.MethodPost, Path: "/tables",
			Permission: "surrealdb.tables.write", Risk: plugin.RiskWrite, AuditEvent: "surrealdb.table.define",
			Input: defineTableSchema(), Handle: defineTable,
		},
		{
			ID: "surrealdb.table.remove", Method: plugin.MethodDelete, Path: "/tables/{table}",
			Permission: "surrealdb.tables.delete", Risk: plugin.RiskDestructive, AuditEvent: "surrealdb.table.remove",
			Handle: removeTable,
		},
		{
			ID: "surrealdb.table.definition", Method: plugin.MethodGet, Path: "/tables/{table}/definition",
			Permission: "surrealdb.tables.read", Risk: plugin.RiskSafe, AuditEvent: "surrealdb.table.definition",
			Handle: tableDefinition,
		},
		{
			ID: "surrealdb.table.columns", Method: plugin.MethodGet, Path: "/tables/{table}/columns",
			Permission: "surrealdb.tables.read", Risk: plugin.RiskSafe, AuditEvent: "surrealdb.table.columns",
			Handle: tableColumnsRoute,
		},
		{
			ID: "surrealdb.table.fields", Method: plugin.MethodGet, Path: "/tables/{table}/fields",
			Permission: "surrealdb.tables.read", Risk: plugin.RiskSafe, AuditEvent: "surrealdb.table.fields",
			Handle: tableFields,
		},
		{
			ID: "surrealdb.table.indexes", Method: plugin.MethodGet, Path: "/tables/{table}/indexes",
			Permission: "surrealdb.tables.read", Risk: plugin.RiskSafe, AuditEvent: "surrealdb.table.indexes",
			Handle: tableIndexes,
		},
		{
			ID: "surrealdb.table.events", Method: plugin.MethodGet, Path: "/tables/{table}/events",
			Permission: "surrealdb.tables.read", Risk: plugin.RiskSafe, AuditEvent: "surrealdb.table.events",
			Handle: tableEvents,
		},

		// --- fields / indexes ------------------------------------------------
		{
			ID: "surrealdb.field.define", Method: plugin.MethodPost, Path: "/tables/{table}/fields",
			Permission: "surrealdb.tables.write", Risk: plugin.RiskWrite, AuditEvent: "surrealdb.field.define",
			Input: defineFieldSchema(), Handle: defineField,
		},
		{
			ID: "surrealdb.field.remove", Method: plugin.MethodDelete, Path: "/tables/{table}/fields/{name}",
			Permission: "surrealdb.tables.write", Risk: plugin.RiskDestructive, AuditEvent: "surrealdb.field.remove",
			Handle: removeField,
		},
		{
			ID: "surrealdb.index.define", Method: plugin.MethodPost, Path: "/tables/{table}/indexes",
			Permission: "surrealdb.tables.write", Risk: plugin.RiskWrite, AuditEvent: "surrealdb.index.define",
			Input: defineIndexSchema(), Handle: defineIndex,
		},
		{
			ID: "surrealdb.index.remove", Method: plugin.MethodDelete, Path: "/tables/{table}/indexes/{name}",
			Permission: "surrealdb.tables.write", Risk: plugin.RiskDestructive, AuditEvent: "surrealdb.index.remove",
			Handle: removeIndex,
		},

		// --- records ---------------------------------------------------------
		{
			ID: "surrealdb.records.list", Method: plugin.MethodGet, Path: "/records",
			Permission: "surrealdb.records.read", Risk: plugin.RiskSafe, AuditEvent: "surrealdb.records.list",
			Handle: listRecords,
		},
		{
			ID: "surrealdb.record.get", Method: plugin.MethodGet, Path: "/records/{id}",
			Permission: "surrealdb.records.read", Risk: plugin.RiskSafe, AuditEvent: "surrealdb.record.get",
			Handle: getRecord,
		},
		{
			ID: "surrealdb.record.create", Method: plugin.MethodPost, Path: "/records/create",
			Permission: "surrealdb.records.write", Risk: plugin.RiskWrite, AuditEvent: "surrealdb.record.create",
			Input: createRecordSchema(), Handle: createRecord,
		},
		{
			ID: "surrealdb.record.insert", Method: plugin.MethodPost, Path: "/records",
			Permission: "surrealdb.records.write", Risk: plugin.RiskWrite, AuditEvent: "surrealdb.record.insert",
			Handle: insertRecord,
		},
		{
			ID: "surrealdb.record.update", Method: plugin.MethodPatch, Path: "/records",
			Permission: "surrealdb.records.write", Risk: plugin.RiskWrite, AuditEvent: "surrealdb.record.update",
			Handle: updateRecord,
		},
		{
			ID: "surrealdb.record.edit", Method: plugin.MethodPatch, Path: "/records/{id}",
			Permission: "surrealdb.records.write", Risk: plugin.RiskWrite, AuditEvent: "surrealdb.record.edit",
			Input: editRecordSchema(), Handle: editRecord,
		},
		{
			ID: "surrealdb.record.delete", Method: plugin.MethodDelete, Path: "/records",
			Permission: "surrealdb.records.delete", Risk: plugin.RiskDestructive, AuditEvent: "surrealdb.record.delete",
			Handle: deleteRecord,
		},
		{
			ID: "surrealdb.record.remove", Method: plugin.MethodDelete, Path: "/records/{id}",
			Permission: "surrealdb.records.delete", Risk: plugin.RiskDestructive, AuditEvent: "surrealdb.record.remove",
			Handle: removeRecord,
		},

		// --- database objects (functions, params, analyzers, users) ----------
		{
			ID: "surrealdb.functions.list", Method: plugin.MethodGet, Path: "/functions",
			Permission: "surrealdb.objects.read", Risk: plugin.RiskSafe, AuditEvent: "surrealdb.functions.list",
			Handle: objectLister("functions"),
		},
		{
			ID: "surrealdb.params.list", Method: plugin.MethodGet, Path: "/params",
			Permission: "surrealdb.objects.read", Risk: plugin.RiskSafe, AuditEvent: "surrealdb.params.list",
			Handle: objectLister("params"),
		},
		{
			ID: "surrealdb.analyzers.list", Method: plugin.MethodGet, Path: "/analyzers",
			Permission: "surrealdb.objects.read", Risk: plugin.RiskSafe, AuditEvent: "surrealdb.analyzers.list",
			Handle: objectLister("analyzers"),
		},
		{
			ID: "surrealdb.users.list", Method: plugin.MethodGet, Path: "/users",
			Permission: "surrealdb.objects.read", Risk: plugin.RiskSafe, AuditEvent: "surrealdb.users.list",
			Handle: objectLister("users"),
		},
		{
			ID: "surrealdb.object.definition", Method: plugin.MethodGet, Path: "/objects/{kind}/{name}/definition",
			Permission: "surrealdb.objects.read", Risk: plugin.RiskSafe, AuditEvent: "surrealdb.object.definition",
			Handle: objectDefinition,
		},
		{
			ID: "surrealdb.object.remove", Method: plugin.MethodDelete, Path: "/objects/{kind}/{name}",
			Permission: "surrealdb.objects.delete", Risk: plugin.RiskDestructive, AuditEvent: "surrealdb.object.remove",
			Handle: removeObject,
		},

		// --- streams ---------------------------------------------------------
		{
			ID: "surrealdb.query", Method: plugin.MethodWS, Path: "/query",
			Permission: "surrealdb.query.exec", Risk: plugin.RiskPrivileged, AuditEvent: "surrealdb.query",
			Stream: queryStream,
		},
		{
			ID: "surrealdb.repl", Method: plugin.MethodWS, Path: "/repl",
			Permission: "surrealdb.query.exec", Risk: plugin.RiskPrivileged, AuditEvent: "surrealdb.repl",
			Stream: replStream,
		},
		{
			ID: "surrealdb.table.tail", Method: plugin.MethodWS, Path: "/tables/{table}/tail",
			Permission: "surrealdb.records.read", Risk: plugin.RiskSafe, AuditEvent: "surrealdb.table.tail",
			Stream: tailStream,
		},

		// --- open in browser -------------------------------------------------
		{
			ID: "surrealdb.proxy.url", Method: plugin.MethodGet, Path: "/proxy-url",
			Permission: "surrealdb.proxy.open", Risk: plugin.RiskSafe, AuditEvent: "surrealdb.proxy.url",
			Handle: proxyURL,
		},
	}
}
