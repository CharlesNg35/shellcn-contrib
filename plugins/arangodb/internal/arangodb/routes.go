package arangodb

import "github.com/charlesng35/shellcn/sdk/plugin"

const (
	permDatabasesRead   = "arangodb.databases.read"
	permDatabasesWrite  = "arangodb.databases.write"
	permDatabasesDelete = "arangodb.databases.delete"

	permCollectionsRead   = "arangodb.collections.read"
	permCollectionsWrite  = "arangodb.collections.write"
	permCollectionsDelete = "arangodb.collections.delete"

	permDocumentsRead   = "arangodb.documents.read"
	permDocumentsWrite  = "arangodb.documents.write"
	permDocumentsDelete = "arangodb.documents.delete"

	permGraphsRead   = "arangodb.graphs.read"
	permGraphsWrite  = "arangodb.graphs.write"
	permGraphsDelete = "arangodb.graphs.delete"

	permViewsRead   = "arangodb.views.read"
	permViewsWrite  = "arangodb.views.write"
	permViewsDelete = "arangodb.views.delete"

	permAnalyzersRead   = "arangodb.analyzers.read"
	permAnalyzersWrite  = "arangodb.analyzers.write"
	permAnalyzersDelete = "arangodb.analyzers.delete"

	permFoxxRead   = "arangodb.foxx.read"
	permFoxxWrite  = "arangodb.foxx.write"
	permFoxxDelete = "arangodb.foxx.delete"

	permClusterRead = "arangodb.cluster.read"

	permAQLRead    = "arangodb.aql.read"
	permAQLExecute = "arangodb.aql.execute"
	permAQLKill    = "arangodb.aql.kill"

	permSavedRead   = "arangodb.saved_queries.read"
	permSavedWrite  = "arangodb.saved_queries.write"
	permSavedDelete = "arangodb.saved_queries.delete"
)

func routes() []plugin.Route {
	return []plugin.Route{
		{ID: rid("tree.databases"), Method: plugin.MethodGet, Path: "/tree/databases", Permission: permDatabasesRead, Risk: plugin.RiskSafe, AuditEvent: rid("tree.databases"), Handle: treeDatabases},
		{ID: rid("tree.database"), Method: plugin.MethodGet, Path: "/tree/databases/{database}", Permission: permDatabasesRead, Risk: plugin.RiskSafe, AuditEvent: rid("tree.database"), Handle: treeDatabase},

		{ID: rid("databases.list"), Method: plugin.MethodGet, Path: "/databases", Permission: permDatabasesRead, Risk: plugin.RiskSafe, AuditEvent: rid("databases.list"), Handle: databasesList},
		{ID: rid("database.read"), Method: plugin.MethodGet, Path: "/databases/{database}", Permission: permDatabasesRead, Risk: plugin.RiskSafe, AuditEvent: rid("database.read"), Handle: databaseRead},
		{ID: rid("database.create"), Method: plugin.MethodPost, Path: "/databases", Permission: permDatabasesWrite, Risk: plugin.RiskWrite, AuditEvent: rid("database.create"), Input: databaseCreateSchema(), Handle: databaseCreate},
		{ID: rid("database.drop"), Method: plugin.MethodDelete, Path: "/databases/{database}", Permission: permDatabasesDelete, Risk: plugin.RiskDestructive, AuditEvent: rid("database.drop"), Handle: databaseDrop},
		{ID: rid("database.metrics"), Method: plugin.MethodWS, Path: "/databases/{database}/metrics", Permission: permDatabasesRead, Risk: plugin.RiskSafe, AuditEvent: rid("database.metrics"), Stream: databaseMetricsStream},
		{ID: rid("database.schema"), Method: plugin.MethodGet, Path: "/databases/{database}/schema-graph", Permission: permCollectionsRead, Risk: plugin.RiskSafe, AuditEvent: rid("database.schema"), Handle: schemaGraph},

		{ID: rid("collections.list"), Method: plugin.MethodGet, Path: "/databases/{database}/collections", Permission: permCollectionsRead, Risk: plugin.RiskSafe, AuditEvent: rid("collections.list"), Handle: collectionsList},
		{ID: rid("collection.read"), Method: plugin.MethodGet, Path: "/databases/{database}/collections/{collection}", Permission: permCollectionsRead, Risk: plugin.RiskSafe, AuditEvent: rid("collection.read"), Handle: collectionRead},
		{ID: rid("collection.create"), Method: plugin.MethodPost, Path: "/databases/{database}/collections", Permission: permCollectionsWrite, Risk: plugin.RiskWrite, AuditEvent: rid("collection.create"), Input: collectionCreateSchema(), Handle: collectionCreate},
		{ID: rid("collection.drop"), Method: plugin.MethodDelete, Path: "/databases/{database}/collections/{collection}", Permission: permCollectionsDelete, Risk: plugin.RiskDestructive, AuditEvent: rid("collection.drop"), Handle: collectionDrop},
		{ID: rid("collection.truncate"), Method: plugin.MethodPost, Path: "/databases/{database}/collections/{collection}/truncate", Permission: permCollectionsDelete, Risk: plugin.RiskDestructive, AuditEvent: rid("collection.truncate"), Handle: collectionTruncate},
		{ID: rid("collection.shards"), Method: plugin.MethodGet, Path: "/databases/{database}/collections/{collection}/shards", Permission: permCollectionsRead, Risk: plugin.RiskSafe, AuditEvent: rid("collection.shards"), Handle: collectionShards},
		{ID: rid("collection.schema.read"), Method: plugin.MethodGet, Path: "/databases/{database}/collections/{collection}/schema", Permission: permCollectionsRead, Risk: plugin.RiskSafe, AuditEvent: rid("collection.schema.read"), Handle: collectionSchemaRead},
		{ID: rid("collection.schema.update"), Method: plugin.MethodPut, Path: "/databases/{database}/collections/{collection}/schema", Permission: permCollectionsWrite, Risk: plugin.RiskWrite, AuditEvent: rid("collection.schema.update"), Handle: collectionSchemaUpdate},
		{ID: rid("collection.metrics"), Method: plugin.MethodWS, Path: "/databases/{database}/collections/{collection}/metrics", Permission: permCollectionsRead, Risk: plugin.RiskSafe, AuditEvent: rid("collection.metrics"), Stream: collectionMetricsStream},

		{ID: rid("documents.list"), Method: plugin.MethodGet, Path: "/databases/{database}/collections/{collection}/documents", Permission: permDocumentsRead, Risk: plugin.RiskSafe, AuditEvent: rid("documents.list"), Handle: documentsList},
		{ID: rid("documents.columns"), Method: plugin.MethodGet, Path: "/databases/{database}/collections/{collection}/documents/columns", Permission: permDocumentsRead, Risk: plugin.RiskSafe, AuditEvent: rid("documents.columns"), Handle: documentColumns},
		{ID: rid("document.insert"), Method: plugin.MethodPost, Path: "/databases/{database}/collections/{collection}/documents", Permission: permDocumentsWrite, Risk: plugin.RiskWrite, AuditEvent: rid("document.insert"), Handle: documentInsert},
		{ID: rid("document.update"), Method: plugin.MethodPatch, Path: "/databases/{database}/collections/{collection}/documents", Permission: permDocumentsWrite, Risk: plugin.RiskWrite, AuditEvent: rid("document.update"), Handle: documentUpdate},
		{ID: rid("document.delete"), Method: plugin.MethodDelete, Path: "/databases/{database}/collections/{collection}/documents", Permission: permDocumentsDelete, Risk: plugin.RiskDestructive, AuditEvent: rid("document.delete"), Handle: documentDelete},
		{ID: rid("document.read"), Method: plugin.MethodGet, Path: "/databases/{database}/collections/{collection}/documents/{key}", Permission: permDocumentsRead, Risk: plugin.RiskSafe, AuditEvent: rid("document.read"), Handle: documentRead},
		{ID: rid("document.replace"), Method: plugin.MethodPut, Path: "/databases/{database}/collections/{collection}/documents/{key}", Permission: permDocumentsWrite, Risk: plugin.RiskWrite, AuditEvent: rid("document.replace"), Handle: documentReplace},

		{ID: rid("indexes.list"), Method: plugin.MethodGet, Path: "/databases/{database}/collections/{collection}/indexes", Permission: permCollectionsRead, Risk: plugin.RiskSafe, AuditEvent: rid("indexes.list"), Handle: indexesList},
		{ID: rid("index.create"), Method: plugin.MethodPost, Path: "/databases/{database}/collections/{collection}/indexes", Permission: permCollectionsWrite, Risk: plugin.RiskWrite, AuditEvent: rid("index.create"), Input: indexCreateSchema(), Handle: indexCreate},
		{ID: rid("index.delete"), Method: plugin.MethodDelete, Path: "/databases/{database}/collections/{collection}/indexes/{index}", Permission: permCollectionsDelete, Risk: plugin.RiskDestructive, AuditEvent: rid("index.delete"), Handle: indexDelete},

		{ID: rid("graphs.list"), Method: plugin.MethodGet, Path: "/databases/{database}/graphs", Permission: permGraphsRead, Risk: plugin.RiskSafe, AuditEvent: rid("graphs.list"), Handle: graphsList},
		{ID: rid("graph.read"), Method: plugin.MethodGet, Path: "/databases/{database}/graphs/{graph}", Permission: permGraphsRead, Risk: plugin.RiskSafe, AuditEvent: rid("graph.read"), Handle: graphRead},
		{ID: rid("graph.create"), Method: plugin.MethodPost, Path: "/databases/{database}/graphs", Permission: permGraphsWrite, Risk: plugin.RiskWrite, AuditEvent: rid("graph.create"), Input: graphCreateSchema(), Handle: graphCreate},
		{ID: rid("graph.drop"), Method: plugin.MethodDelete, Path: "/databases/{database}/graphs/{graph}", Permission: permGraphsDelete, Risk: plugin.RiskDestructive, AuditEvent: rid("graph.drop"), Handle: graphDrop},
		{ID: rid("graph.edges"), Method: plugin.MethodGet, Path: "/databases/{database}/graphs/{graph}/edge-definitions", Permission: permGraphsRead, Risk: plugin.RiskSafe, AuditEvent: rid("graph.edges"), Handle: graphEdgeDefinitions},
		{ID: rid("graph.vertices"), Method: plugin.MethodGet, Path: "/databases/{database}/graphs/{graph}/vertex-collections", Permission: permGraphsRead, Risk: plugin.RiskSafe, AuditEvent: rid("graph.vertices"), Handle: graphVertexCollections},
		{ID: rid("graph.explore"), Method: plugin.MethodGet, Path: "/databases/{database}/graphs/{graph}/explore", Permission: permGraphsRead, Risk: plugin.RiskSafe, AuditEvent: rid("graph.explore"), Handle: graphExplore},
		{ID: rid("graph.expand"), Method: plugin.MethodGet, Path: "/databases/{database}/graphs/{graph}/expand", Permission: permGraphsRead, Risk: plugin.RiskSafe, AuditEvent: rid("graph.expand"), Handle: graphExpand},
		{ID: rid("graph.topology"), Method: plugin.MethodWS, Path: "/databases/{database}/graphs/{graph}/topology", Permission: permGraphsRead, Risk: plugin.RiskSafe, AuditEvent: rid("graph.topology"), Stream: graphCanvasStream},

		{ID: rid("views.list"), Method: plugin.MethodGet, Path: "/databases/{database}/views", Permission: permViewsRead, Risk: plugin.RiskSafe, AuditEvent: rid("views.list"), Handle: viewsList},
		{ID: rid("view.read"), Method: plugin.MethodGet, Path: "/databases/{database}/views/{view}", Permission: permViewsRead, Risk: plugin.RiskSafe, AuditEvent: rid("view.read"), Handle: viewRead},
		{ID: rid("view.definition"), Method: plugin.MethodGet, Path: "/databases/{database}/views/{view}/definition", Permission: permViewsRead, Risk: plugin.RiskSafe, AuditEvent: rid("view.definition"), Handle: viewDefinition},
		{ID: rid("view.update"), Method: plugin.MethodPut, Path: "/databases/{database}/views/{view}/definition", Permission: permViewsWrite, Risk: plugin.RiskWrite, AuditEvent: rid("view.update"), Handle: viewUpdate},
		{ID: rid("view.create"), Method: plugin.MethodPost, Path: "/databases/{database}/views", Permission: permViewsWrite, Risk: plugin.RiskWrite, AuditEvent: rid("view.create"), Input: viewCreateSchema(), Handle: viewCreate},
		{ID: rid("view.drop"), Method: plugin.MethodDelete, Path: "/databases/{database}/views/{view}", Permission: permViewsDelete, Risk: plugin.RiskDestructive, AuditEvent: rid("view.drop"), Handle: viewDrop},
		{ID: rid("view.links"), Method: plugin.MethodGet, Path: "/databases/{database}/views/{view}/links", Permission: permViewsRead, Risk: plugin.RiskSafe, AuditEvent: rid("view.links"), Handle: viewLinks},

		{ID: rid("analyzers.list"), Method: plugin.MethodGet, Path: "/databases/{database}/analyzers", Permission: permAnalyzersRead, Risk: plugin.RiskSafe, AuditEvent: rid("analyzers.list"), Handle: analyzersList},
		{ID: rid("analyzer.read"), Method: plugin.MethodGet, Path: "/databases/{database}/analyzers/{analyzer}", Permission: permAnalyzersRead, Risk: plugin.RiskSafe, AuditEvent: rid("analyzer.read"), Handle: analyzerRead},
		{ID: rid("analyzer.create"), Method: plugin.MethodPost, Path: "/databases/{database}/analyzers", Permission: permAnalyzersWrite, Risk: plugin.RiskWrite, AuditEvent: rid("analyzer.create"), Input: analyzerCreateSchema(), Handle: analyzerCreate},
		{ID: rid("analyzer.delete"), Method: plugin.MethodDelete, Path: "/databases/{database}/analyzers/{analyzer}", Permission: permAnalyzersDelete, Risk: plugin.RiskDestructive, AuditEvent: rid("analyzer.delete"), Handle: analyzerDelete},

		{ID: rid("foxx.list"), Method: plugin.MethodGet, Path: "/databases/{database}/foxx", Permission: permFoxxRead, Risk: plugin.RiskSafe, AuditEvent: rid("foxx.list"), Handle: foxxList},
		{ID: rid("foxx.read"), Method: plugin.MethodGet, Path: "/foxx/{service}", Permission: permFoxxRead, Risk: plugin.RiskSafe, AuditEvent: rid("foxx.read"), Handle: foxxRead},
		{ID: rid("foxx.routes"), Method: plugin.MethodGet, Path: "/foxx/{service}/routes", Permission: permFoxxRead, Risk: plugin.RiskSafe, AuditEvent: rid("foxx.routes"), Handle: foxxRoutes},
		{ID: rid("foxx.readme"), Method: plugin.MethodGet, Path: "/foxx/{service}/readme", Permission: permFoxxRead, Risk: plugin.RiskSafe, AuditEvent: rid("foxx.readme"), Handle: foxxReadme},
		{ID: rid("foxx.configuration"), Method: plugin.MethodGet, Path: "/foxx/{service}/configuration", Permission: permFoxxRead, Risk: plugin.RiskSafe, AuditEvent: rid("foxx.configuration"), Handle: foxxConfiguration},
		{ID: rid("foxx.dependencies"), Method: plugin.MethodGet, Path: "/foxx/{service}/dependencies", Permission: permFoxxRead, Risk: plugin.RiskSafe, AuditEvent: rid("foxx.dependencies"), Handle: foxxDependencies},
		{ID: rid("foxx.development"), Method: plugin.MethodPut, Path: "/foxx/{service}/development", Permission: permFoxxWrite, Risk: plugin.RiskWrite, AuditEvent: rid("foxx.development"), Input: foxxDevelopmentSchema(), Handle: foxxDevelopment},
		{ID: rid("foxx.uninstall"), Method: plugin.MethodDelete, Path: "/foxx/{service}", Permission: permFoxxDelete, Risk: plugin.RiskDestructive, AuditEvent: rid("foxx.uninstall"), Handle: foxxUninstall},

		{ID: rid("cluster.list"), Method: plugin.MethodGet, Path: "/cluster", Permission: permClusterRead, Risk: plugin.RiskSafe, AuditEvent: rid("cluster.list"), Handle: clusterList},
		{ID: rid("cluster.read"), Method: plugin.MethodGet, Path: "/cluster/overview", Permission: permClusterRead, Risk: plugin.RiskSafe, AuditEvent: rid("cluster.read"), Handle: clusterRead},
		{ID: rid("cluster.servers"), Method: plugin.MethodGet, Path: "/cluster/servers", Permission: permClusterRead, Risk: plugin.RiskSafe, AuditEvent: rid("cluster.servers"), Handle: clusterServers},
		{ID: rid("cluster.metrics"), Method: plugin.MethodWS, Path: "/cluster/metrics", Permission: permClusterRead, Risk: plugin.RiskSafe, AuditEvent: rid("cluster.metrics"), Stream: clusterMetricsStream},

		{ID: rid("query"), Method: plugin.MethodWS, Path: "/query", Permission: permAQLExecute, Risk: plugin.RiskPrivileged, AuditEvent: rid("query"), Stream: queryStream},
		{ID: rid("query.cancel"), Method: plugin.MethodPost, Path: "/query/cancel", Permission: permAQLExecute, Risk: plugin.RiskWrite, AuditEvent: rid("query.cancel"), Handle: queryCancel},
		{ID: rid("query.complete"), Method: plugin.MethodGet, Path: "/query/completion", Permission: permAQLRead, Risk: plugin.RiskSafe, AuditEvent: rid("query.complete"), Handle: queryCompletion},
		{ID: rid("queries.running"), Method: plugin.MethodGet, Path: "/databases/{database}/queries/running", Permission: permAQLRead, Risk: plugin.RiskSafe, AuditEvent: rid("queries.running"), Handle: runningQueries},
		{ID: rid("queries.slow"), Method: plugin.MethodGet, Path: "/databases/{database}/queries/slow", Permission: permAQLRead, Risk: plugin.RiskSafe, AuditEvent: rid("queries.slow"), Handle: slowQueries},
		{ID: rid("query.read"), Method: plugin.MethodGet, Path: "/queries/{id}", Permission: permAQLRead, Risk: plugin.RiskSafe, AuditEvent: rid("query.read"), Handle: queryRead},
		{ID: rid("query.kill"), Method: plugin.MethodDelete, Path: "/queries/{id}", Permission: permAQLKill, Risk: plugin.RiskDestructive, AuditEvent: rid("query.kill"), Handle: killQuery},
		{ID: rid("queries.slow.clear"), Method: plugin.MethodDelete, Path: "/databases/{database}/queries/slow", Permission: permAQLKill, Risk: plugin.RiskDestructive, AuditEvent: rid("queries.slow.clear"), Handle: clearSlowQueries},

		// Saved AQL, stored per user through plugin storage.
		{ID: rid("saved.list"), Method: plugin.MethodGet, Path: "/saved-queries", Permission: permSavedRead, Risk: plugin.RiskSafe, AuditEvent: rid("saved.list"), Handle: listSavedQueries},
		{ID: rid("saved.read"), Method: plugin.MethodGet, Path: "/saved-queries/{id}", Permission: permSavedRead, Risk: plugin.RiskSafe, AuditEvent: rid("saved.read"), Handle: readSavedQuery},
		{ID: rid("saved.create"), Method: plugin.MethodPost, Path: "/saved-queries", Permission: permSavedWrite, Risk: plugin.RiskWrite, AuditEvent: rid("saved.create"), Input: savedQuerySchema(), Handle: createSavedQuery},
		{ID: rid("saved.delete"), Method: plugin.MethodDelete, Path: "/saved-queries/{id}", Permission: permSavedDelete, Risk: plugin.RiskDestructive, AuditEvent: rid("saved.delete"), Handle: deleteSavedQuery},
	}
}

func streams() []plugin.Stream {
	return []plugin.Stream{
		{ID: rid("query"), Kind: plugin.StreamQuery, RouteID: rid("query")},
		{ID: rid("database.metrics"), Kind: plugin.StreamMetrics, RouteID: rid("database.metrics")},
		{ID: rid("collection.metrics"), Kind: plugin.StreamMetrics, RouteID: rid("collection.metrics")},
		{ID: rid("cluster.metrics"), Kind: plugin.StreamMetrics, RouteID: rid("cluster.metrics")},
		{ID: rid("graph.topology"), Kind: plugin.StreamCanvas, RouteID: rid("graph.topology")},
	}
}
