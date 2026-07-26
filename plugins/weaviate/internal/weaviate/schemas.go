package weaviate

import "github.com/charlesng35/shellcn/sdk/plugin"

func collectionCreateSchema() *plugin.Schema {
	multiTenant := &plugin.Condition{AllOf: []plugin.Rule{{Field: "multi_tenancy", Op: plugin.OpEq, Value: true}}}
	return &plugin.Schema{Groups: []plugin.Group{
		{Name: "Collection", Fields: []plugin.Field{
			{Key: "name", Label: "Name", Type: plugin.FieldText, Required: true, Placeholder: "Article", Help: "CamelCase, starting with an uppercase letter.", Validators: []plugin.Validator{
				{Type: plugin.ValidatorRegex, Value: `^[A-Z][A-Za-z0-9_]*$`, Message: "start with an uppercase letter; letters, digits, and underscores only"},
			}},
			{Key: "description", Label: "Description", Type: plugin.FieldTextarea},
		}},
		{Name: "Vectors", Fields: []plugin.Field{
			{Key: "vectorizer", Label: "Vectorizer module", Type: plugin.FieldAutocomplete, Default: "none", Help: "Use none to supply vectors yourself.", Options: []plugin.Option{
				{Label: "none (bring your own vectors)", Value: "none"},
				{Label: "text2vec-weaviate", Value: "text2vec-weaviate"},
				{Label: "text2vec-openai", Value: "text2vec-openai"},
				{Label: "text2vec-cohere", Value: "text2vec-cohere"},
				{Label: "text2vec-huggingface", Value: "text2vec-huggingface"},
				{Label: "text2vec-ollama", Value: "text2vec-ollama"},
				{Label: "text2vec-transformers", Value: "text2vec-transformers"},
				{Label: "multi2vec-clip", Value: "multi2vec-clip"},
			}},
			{Key: "vector_index_type", Label: "Vector index", Type: plugin.FieldSelect, Default: "hnsw", Options: []plugin.Option{
				{Label: "HNSW", Value: "hnsw"},
				{Label: "Flat", Value: "flat"},
				{Label: "Dynamic", Value: "dynamic"},
			}},
			{Key: "distance", Label: "Distance metric", Type: plugin.FieldSelect, Default: "cosine", Options: []plugin.Option{
				{Label: "Cosine", Value: "cosine"},
				{Label: "Dot", Value: "dot"},
				{Label: "L2 squared", Value: "l2-squared"},
				{Label: "Hamming", Value: "hamming"},
				{Label: "Manhattan", Value: "manhattan"},
			}},
		}},
		{Name: "Distribution", Fields: []plugin.Field{
			{Key: "replication_factor", Label: "Replication factor", Type: plugin.FieldNumber, Default: 1, Validators: []plugin.Validator{{Type: plugin.ValidatorMin, Value: 1}, {Type: plugin.ValidatorMax, Value: 32}}},
			{Key: "async_replication", Label: "Async replication", Type: plugin.FieldToggle},
			{Key: "multi_tenancy", Label: "Multi-tenancy", Type: plugin.FieldToggle},
			{Key: "auto_tenant_creation", Label: "Auto-create tenants", Type: plugin.FieldToggle, VisibleWhen: multiTenant},
		}},
		{Name: "Keyword index", Fields: []plugin.Field{
			{Key: "bm25_k1", Label: "BM25 k1", Type: plugin.FieldNumber, Step: 0.1, Validators: []plugin.Validator{{Type: plugin.ValidatorMin, Value: 0}, {Type: plugin.ValidatorMax, Value: 10}}},
			{Key: "bm25_b", Label: "BM25 b", Type: plugin.FieldNumber, Step: 0.05, Validators: []plugin.Validator{{Type: plugin.ValidatorMin, Value: 0}, {Type: plugin.ValidatorMax, Value: 1}}},
		}},
	}}
}

func propertyCreateSchema() *plugin.Schema {
	return &plugin.Schema{Groups: []plugin.Group{{Name: "Property", Fields: []plugin.Field{
		{Key: "name", Label: "Name", Type: plugin.FieldText, Required: true, Validators: []plugin.Validator{
			{Type: plugin.ValidatorRegex, Value: `^[A-Za-z_][A-Za-z0-9_]*$`, Message: "letters, digits, and underscores only"},
		}},
		{Key: "data_type", Label: "Data type", Type: plugin.FieldAutocomplete, Required: true, Default: "text", Help: "Use a collection name to create a cross-reference.", Options: []plugin.Option{
			{Label: "text", Value: "text"},
			{Label: "text[]", Value: "text[]"},
			{Label: "int", Value: "int"},
			{Label: "int[]", Value: "int[]"},
			{Label: "number", Value: "number"},
			{Label: "number[]", Value: "number[]"},
			{Label: "boolean", Value: "boolean"},
			{Label: "boolean[]", Value: "boolean[]"},
			{Label: "date", Value: "date"},
			{Label: "date[]", Value: "date[]"},
			{Label: "uuid", Value: "uuid"},
			{Label: "uuid[]", Value: "uuid[]"},
			{Label: "object", Value: "object"},
			{Label: "object[]", Value: "object[]"},
			{Label: "blob", Value: "blob"},
			{Label: "geoCoordinates", Value: "geoCoordinates"},
			{Label: "phoneNumber", Value: "phoneNumber"},
		}},
		{Key: "description", Label: "Description", Type: plugin.FieldTextarea},
		{Key: "tokenization", Label: "Tokenization", Type: plugin.FieldSelect, Options: []plugin.Option{
			{Label: "word", Value: "word"},
			{Label: "lowercase", Value: "lowercase"},
			{Label: "whitespace", Value: "whitespace"},
			{Label: "field", Value: "field"},
			{Label: "trigram", Value: "trigram"},
		}},
		{Key: "index_filterable", Label: "Filterable index", Type: plugin.FieldToggle, Default: true},
		{Key: "index_searchable", Label: "Searchable index", Type: plugin.FieldToggle, Default: true},
	}}}}
}

func referenceSchema() *plugin.Schema {
	return &plugin.Schema{Groups: []plugin.Group{{Name: "Cross-reference", Fields: []plugin.Field{
		{Key: "property", Label: "Reference property", Type: plugin.FieldText, Required: true},
		{Key: "target_id", Label: "Target object ID", Type: plugin.FieldText, Required: true, Placeholder: "00000000-0000-0000-0000-000000000000"},
		{Key: "target_collection", Label: "Target collection", Type: plugin.FieldText, Help: "Defaults to the property's declared target."},
	}}}}
}

func batchDeleteSchema() *plugin.Schema {
	return &plugin.Schema{Groups: []plugin.Group{{Name: "Delete by filter", Fields: []plugin.Field{
		{Key: "where", Label: "Where filter", Type: plugin.FieldTextarea, Required: true,
			Placeholder: `{"operator":"Equal","path":["title"],"valueText":"draft"}`,
			Help:        "Weaviate where-filter JSON. Every matching object and its vectors are deleted."},
		{Key: "dry_run", Label: "Dry run", Type: plugin.FieldToggle, Help: "Count the matches without deleting anything."},
		{Key: "tenant", Label: "Tenant", Type: plugin.FieldText, Help: "Required for multi-tenant collections."},
	}}}}
}

func shardUpdateSchema() *plugin.Schema {
	return &plugin.Schema{Groups: []plugin.Group{{Name: "Shard", Fields: []plugin.Field{
		{Key: "status", Label: "Status", Type: plugin.FieldSelect, Required: true, Default: "READY", Options: []plugin.Option{
			{Label: "Ready (accept writes)", Value: "READY"},
			{Label: "Read-only", Value: "READONLY"},
		}},
	}}}}
}

func tenantCreateSchema() *plugin.Schema {
	return &plugin.Schema{Groups: []plugin.Group{{Name: "Tenant", Fields: []plugin.Field{
		{Key: "name", Label: "Name", Type: plugin.FieldText, Required: true},
		{Key: "activity_status", Label: "Activity status", Type: plugin.FieldSelect, Default: "ACTIVE", Options: tenantStatusOptions()},
	}}}}
}

func tenantUpdateSchema() *plugin.Schema {
	return &plugin.Schema{Groups: []plugin.Group{{Name: "Tenant", Fields: []plugin.Field{
		{Key: "activity_status", Label: "Activity status", Type: plugin.FieldSelect, Required: true, Default: "ACTIVE", Options: tenantStatusOptions()},
	}}}}
}

func tenantStatusOptions() []plugin.Option {
	return []plugin.Option{
		{Label: "Active", Value: "ACTIVE"},
		{Label: "Inactive", Value: "INACTIVE"},
		{Label: "Offloaded", Value: "OFFLOADED"},
	}
}

func aliasCreateSchema() *plugin.Schema {
	return &plugin.Schema{Groups: []plugin.Group{{Name: "Alias", Fields: []plugin.Field{
		{Key: "name", Label: "Alias", Type: plugin.FieldText, Required: true, Help: "Aliases stand in for collection names, so they follow the same CamelCase rule.", Validators: []plugin.Validator{
			{Type: plugin.ValidatorRegex, Value: `^[A-Z][A-Za-z0-9_]*$`, Message: "start with an uppercase letter; letters, digits, and underscores only"},
		}},
		{Key: "collection", Label: "Target collection", Type: plugin.FieldAutocomplete, Required: true, OptionsSource: &plugin.DataSource{RouteID: rid("collections.list")}},
	}}}}
}

func aliasUpdateSchema() *plugin.Schema {
	return &plugin.Schema{Groups: []plugin.Group{{Name: "Alias", Fields: []plugin.Field{
		{Key: "collection", Label: "Target collection", Type: plugin.FieldAutocomplete, Required: true, OptionsSource: &plugin.DataSource{RouteID: rid("collections.list")}},
	}}}}
}

func backupCreateSchema() *plugin.Schema {
	return &plugin.Schema{Groups: []plugin.Group{{Name: "Backup", Fields: []plugin.Field{
		{Key: "id", Label: "Backup ID", Type: plugin.FieldText, Required: true, Placeholder: "nightly-2026-07-26", Validators: []plugin.Validator{
			{Type: plugin.ValidatorRegex, Value: `^[A-Za-z0-9][A-Za-z0-9._-]*$`, Message: "letters, digits, dots, dashes, and underscores only"},
		}},
		{Key: "backend", Label: "Backend", Type: plugin.FieldSelect, Required: true, Default: "filesystem", Options: []plugin.Option{
			{Label: "Filesystem", Value: "filesystem"},
			{Label: "S3", Value: "s3"},
			{Label: "GCS", Value: "gcs"},
			{Label: "Azure", Value: "azure"},
		}},
		{Key: "collections", Label: "Collections", Type: plugin.FieldMultiSelect, Help: "Leave empty to back up every collection.", OptionsSource: &plugin.DataSource{RouteID: rid("collections.list")}},
		{Key: "wait", Label: "Wait for completion", Type: plugin.FieldToggle, Default: true},
	}}}}
}

func savedQuerySchema() *plugin.Schema {
	return &plugin.Schema{Groups: []plugin.Group{{Name: "Saved query", Fields: []plugin.Field{
		{Key: "name", Label: "Name", Type: plugin.FieldText, Required: true},
		{Key: "kind", Label: "Kind", Type: plugin.FieldSelect, Required: true, Default: "graphql", Options: savedQueryKinds()},
		{Key: "collection", Label: "Collection", Type: plugin.FieldText, VisibleWhen: &plugin.Condition{AllOf: []plugin.Rule{{Field: "kind", Op: plugin.OpEq, Value: "search"}}}},
		{Key: "query", Label: "Query", Type: plugin.FieldTextarea, Required: true},
	}}}}
}
