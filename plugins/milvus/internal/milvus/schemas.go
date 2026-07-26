package milvus

import "github.com/charlesng35/shellcn/sdk/plugin"

func nameField(label, help string) plugin.Field {
	return plugin.Field{
		Key: "name", Label: label, Type: plugin.FieldText, Required: true, Help: help,
		Validators: []plugin.Validator{{Type: plugin.ValidatorRegex, Value: identifierPattern.String(), Message: "letters, digits and underscores, starting with a letter or underscore"}},
	}
}

func fieldTypeOptions() []plugin.Option {
	return []plugin.Option{
		{Label: "Int64", Value: "Int64"},
		{Label: "Int32", Value: "Int32"},
		{Label: "Int16", Value: "Int16"},
		{Label: "Int8", Value: "Int8"},
		{Label: "Bool", Value: "Bool"},
		{Label: "Float", Value: "Float"},
		{Label: "Double", Value: "Double"},
		{Label: "VarChar", Value: "VarChar"},
		{Label: "JSON", Value: "JSON"},
		{Label: "Array", Value: "Array"},
		{Label: "Float vector", Value: "FloatVector"},
		{Label: "Binary vector", Value: "BinaryVector"},
		{Label: "Float16 vector", Value: "Float16Vector"},
		{Label: "BFloat16 vector", Value: "BFloat16Vector"},
		{Label: "Int8 vector", Value: "Int8Vector"},
		{Label: "Sparse float vector", Value: "SparseFloatVector"},
	}
}

func metricTypeOptions() []plugin.Option {
	return []plugin.Option{
		{Label: "Cosine", Value: "COSINE"},
		{Label: "Inner product", Value: "IP"},
		{Label: "Euclidean (L2)", Value: "L2"},
		{Label: "Hamming", Value: "HAMMING"},
		{Label: "Jaccard", Value: "JACCARD"},
		{Label: "BM25", Value: "BM25"},
	}
}

// fieldDesignerField is the repeatable schema-designer row used by collection
// creation; the same shape backs the single "add field" form.
func fieldDesignerField() plugin.Field {
	dimVisible := &plugin.Condition{AllOf: []plugin.Rule{{Field: "type", Op: plugin.OpIn, Value: []any{"FloatVector", "BinaryVector", "Float16Vector", "BFloat16Vector", "Int8Vector"}}}}
	varcharVisible := &plugin.Condition{AllOf: []plugin.Rule{{Field: "type", Op: plugin.OpEq, Value: "VarChar"}}}
	arrayVisible := &plugin.Condition{AllOf: []plugin.Rule{{Field: "type", Op: plugin.OpEq, Value: "Array"}}}
	return plugin.Field{
		Key: "fields", Label: "Fields", Type: plugin.FieldArray, MinItems: 1, AddLabel: "Add field", ItemLabel: "Field",
		Item: &plugin.Field{Key: "field", Label: "Field", Type: plugin.FieldObject, Fields: fieldDesignerFields(dimVisible, varcharVisible, arrayVisible)},
	}
}

func fieldDesignerFields(dimVisible, varcharVisible, arrayVisible *plugin.Condition) []plugin.Field {
	return []plugin.Field{
		nameField("Field name", ""),
		{Key: "type", Label: "Data type", Type: plugin.FieldSelect, Required: true, Default: "Int64", Options: fieldTypeOptions()},
		{Key: "dim", Label: "Dimension", Type: plugin.FieldNumber, VisibleWhen: dimVisible, Required: true, Default: 128, Validators: []plugin.Validator{
			{Type: plugin.ValidatorMin, Value: 1}, {Type: plugin.ValidatorMax, Value: 32768},
		}},
		{Key: "max_length", Label: "Max length", Type: plugin.FieldNumber, VisibleWhen: varcharVisible, Default: 512, Validators: []plugin.Validator{
			{Type: plugin.ValidatorMin, Value: 1}, {Type: plugin.ValidatorMax, Value: 65535},
		}},
		{Key: "element_type", Label: "Element type", Type: plugin.FieldSelect, VisibleWhen: arrayVisible, Default: "VarChar", Options: []plugin.Option{
			{Label: "Int64", Value: "Int64"}, {Label: "Int32", Value: "Int32"}, {Label: "Bool", Value: "Bool"},
			{Label: "Float", Value: "Float"}, {Label: "Double", Value: "Double"}, {Label: "VarChar", Value: "VarChar"},
		}},
		{Key: "max_capacity", Label: "Max capacity", Type: plugin.FieldNumber, VisibleWhen: arrayVisible, Default: 64, Validators: []plugin.Validator{
			{Type: plugin.ValidatorMin, Value: 1}, {Type: plugin.ValidatorMax, Value: 4096},
		}},
		{Key: "primary", Label: "Primary key", Type: plugin.FieldToggle},
		{Key: "auto_id", Label: "Auto id", Type: plugin.FieldToggle, Help: "Only valid on an Int64 primary key."},
		{Key: "nullable", Label: "Nullable", Type: plugin.FieldToggle},
		{Key: "partition_key", Label: "Partition key", Type: plugin.FieldToggle},
		{Key: "clustering_key", Label: "Clustering key", Type: plugin.FieldToggle},
		{Key: "description", Label: "Description", Type: plugin.FieldText},
	}
}

func collectionSchema() *plugin.Schema {
	return &plugin.Schema{Groups: []plugin.Group{
		{Name: "Collection", Fields: []plugin.Field{
			nameField("Collection name", ""),
			{Key: "description", Label: "Description", Type: plugin.FieldText},
			{Key: "consistency_level", Label: "Consistency level", Type: plugin.FieldSelect, Default: "Bounded", Options: []plugin.Option{
				{Label: "Strong", Value: "Strong"},
				{Label: "Bounded", Value: "Bounded"},
				{Label: "Session", Value: "Session"},
				{Label: "Eventually", Value: "Eventually"},
			}},
			{Key: "shard_num", Label: "Shards", Type: plugin.FieldNumber, Default: 1, Validators: []plugin.Validator{
				{Type: plugin.ValidatorMin, Value: 1}, {Type: plugin.ValidatorMax, Value: 64},
			}},
			{Key: "enable_dynamic_field", Label: "Enable dynamic field", Type: plugin.FieldToggle, Help: "Stores undeclared keys in the $meta JSON field."},
		}},
		{Name: "Schema", Fields: []plugin.Field{fieldDesignerField()}},
		{Name: "Index", Fields: []plugin.Field{
			{Key: "auto_index", Label: "Build AUTOINDEX on vector fields", Type: plugin.FieldToggle, Default: true},
			{Key: "metric_type", Label: "Metric type", Type: plugin.FieldSelect, Default: "COSINE", Options: metricTypeOptions(), VisibleWhen: &plugin.Condition{AllOf: []plugin.Rule{
				{Field: "auto_index", Op: plugin.OpEq, Value: true},
			}}},
			{Key: "load_after_create", Label: "Load collection after creation", Type: plugin.FieldToggle, Default: true},
		}},
	}}
}

func addFieldSchema() *plugin.Schema {
	dimVisible := &plugin.Condition{AllOf: []plugin.Rule{{Field: "type", Op: plugin.OpIn, Value: []any{"FloatVector", "BinaryVector", "Float16Vector", "BFloat16Vector", "Int8Vector"}}}}
	varcharVisible := &plugin.Condition{AllOf: []plugin.Rule{{Field: "type", Op: plugin.OpEq, Value: "VarChar"}}}
	arrayVisible := &plugin.Condition{AllOf: []plugin.Rule{{Field: "type", Op: plugin.OpEq, Value: "Array"}}}
	fields := fieldDesignerFields(dimVisible, varcharVisible, arrayVisible)
	// A field added to an existing collection can never become a key.
	kept := fields[:0]
	for _, f := range fields {
		switch f.Key {
		case "primary", "auto_id", "partition_key", "clustering_key", "nullable":
			continue
		}
		kept = append(kept, f)
	}
	kept = append(kept, plugin.Field{Key: "nullable", Label: "Nullable", Type: plugin.FieldToggle, Default: true, Help: "Added fields must be nullable or carry a default."})
	return &plugin.Schema{Groups: []plugin.Group{{Name: "Field", Fields: kept}}}
}

func databaseSchema() *plugin.Schema {
	return &plugin.Schema{Groups: []plugin.Group{{Name: "Database", Fields: []plugin.Field{
		nameField("Database name", ""),
		{Key: "properties", Label: "Properties", Type: plugin.FieldMap, KeyLabel: "Property", Item: &plugin.Field{Key: "value", Label: "Value", Type: plugin.FieldText}},
	}}}}
}

func partitionSchema() *plugin.Schema {
	return &plugin.Schema{Groups: []plugin.Group{{Name: "Partition", Fields: []plugin.Field{
		nameField("Partition name", ""),
	}}}}
}

func renameSchema() *plugin.Schema {
	return &plugin.Schema{Groups: []plugin.Group{{Name: "Rename", Fields: []plugin.Field{
		nameField("New collection name", ""),
	}}}}
}

func aliasSchema() *plugin.Schema {
	return &plugin.Schema{Groups: []plugin.Group{{Name: "Alias", Fields: []plugin.Field{
		nameField("Alias", ""),
	}}}}
}

func indexSchema() *plugin.Schema {
	ivfLike := &plugin.Condition{AllOf: []plugin.Rule{{Field: "index_type", Op: plugin.OpIn, Value: []any{"IVF_FLAT", "IVF_SQ8", "SCANN"}}}}
	hnswLike := &plugin.Condition{AllOf: []plugin.Rule{{Field: "index_type", Op: plugin.OpEq, Value: "HNSW"}}}
	vectorLike := &plugin.Condition{AllOf: []plugin.Rule{{Field: "index_type", Op: plugin.OpNin, Value: []any{"INVERTED", "BITMAP", "STL_SORT", "Trie"}}}}
	return &plugin.Schema{Groups: []plugin.Group{{Name: "Index", Fields: []plugin.Field{
		{Key: "field_name", Label: "Field", Type: plugin.FieldSelect, Required: true, OptionsSource: &plugin.DataSource{
			RouteID: rid("collection.schema"), Params: collectionParams(),
		}},
		{Key: "index_name", Label: "Index name", Type: plugin.FieldText, Help: "Defaults to the field name.", Validators: []plugin.Validator{
			{Type: plugin.ValidatorRegex, Value: `^$|` + identifierPattern.String(), Message: "letters, digits and underscores"},
		}},
		{Key: "index_type", Label: "Index type", Type: plugin.FieldSelect, Required: true, Default: "AUTOINDEX", Options: []plugin.Option{
			{Label: "AUTOINDEX", Value: "AUTOINDEX"},
			{Label: "FLAT", Value: "FLAT"},
			{Label: "IVF_FLAT", Value: "IVF_FLAT"},
			{Label: "IVF_SQ8", Value: "IVF_SQ8"},
			{Label: "HNSW", Value: "HNSW"},
			{Label: "SCANN", Value: "SCANN"},
			{Label: "DISKANN", Value: "DISKANN"},
			{Label: "SPARSE_INVERTED_INDEX", Value: "SPARSE_INVERTED_INDEX"},
			{Label: "INVERTED (scalar)", Value: "INVERTED"},
			{Label: "BITMAP (scalar)", Value: "BITMAP"},
			{Label: "STL_SORT (scalar)", Value: "STL_SORT"},
			{Label: "Trie (scalar)", Value: "Trie"},
		}},
		{Key: "metric_type", Label: "Metric type", Type: plugin.FieldSelect, Default: "COSINE", Options: metricTypeOptions(), VisibleWhen: vectorLike},
		{Key: "nlist", Label: "nlist", Type: plugin.FieldNumber, Default: 128, VisibleWhen: ivfLike, Validators: []plugin.Validator{
			{Type: plugin.ValidatorMin, Value: 1}, {Type: plugin.ValidatorMax, Value: 65536},
		}},
		{Key: "m", Label: "M", Type: plugin.FieldNumber, Default: 16, VisibleWhen: hnswLike, Validators: []plugin.Validator{
			{Type: plugin.ValidatorMin, Value: 2}, {Type: plugin.ValidatorMax, Value: 2048},
		}},
		{Key: "ef_construction", Label: "efConstruction", Type: plugin.FieldNumber, Default: 200, VisibleWhen: hnswLike, Validators: []plugin.Validator{
			{Type: plugin.ValidatorMin, Value: 8}, {Type: plugin.ValidatorMax, Value: 4096},
		}},
	}}}}
}

func userSchema() *plugin.Schema {
	return &plugin.Schema{Groups: []plugin.Group{{Name: "User", Fields: []plugin.Field{
		nameField("Username", ""),
		{Key: "password", Label: "Password", Type: plugin.FieldPassword, Required: true, Secret: true},
	}}}}
}

func passwordSchema() *plugin.Schema {
	return &plugin.Schema{Groups: []plugin.Group{{Name: "Password", Fields: []plugin.Field{
		{Key: "old_password", Label: "Current password", Type: plugin.FieldPassword, Required: true, Secret: true},
		{Key: "new_password", Label: "New password", Type: plugin.FieldPassword, Required: true, Secret: true},
	}}}}
}

func roleSchema() *plugin.Schema {
	return &plugin.Schema{Groups: []plugin.Group{{Name: "Role", Fields: []plugin.Field{
		nameField("Role name", ""),
	}}}}
}

func roleRefSchema() *plugin.Schema {
	return &plugin.Schema{Groups: []plugin.Group{{Name: "Role", Fields: []plugin.Field{
		{Key: "role", Label: "Role", Type: plugin.FieldSelect, Required: true, OptionsSource: &plugin.DataSource{RouteID: rid("roles.list")}},
	}}}}
}

func privilegeSchema() *plugin.Schema {
	return &plugin.Schema{Groups: []plugin.Group{{Name: "Privilege", Fields: []plugin.Field{
		{Key: "privilege", Label: "Privilege", Type: plugin.FieldAutocomplete, Required: true, Help: "Privilege or privilege-group name.", Options: []plugin.Option{
			{Label: "Search", Value: "Search"},
			{Label: "Query", Value: "Query"},
			{Label: "Insert", Value: "Insert"},
			{Label: "Delete", Value: "Delete"},
			{Label: "Load", Value: "Load"},
			{Label: "Release", Value: "Release"},
			{Label: "CreateIndex", Value: "CreateIndex"},
			{Label: "DropIndex", Value: "DropIndex"},
			{Label: "CreateCollection", Value: "CreateCollection"},
			{Label: "DropCollection", Value: "DropCollection"},
			{Label: "DescribeCollection", Value: "DescribeCollection"},
			{Label: "ShowCollections", Value: "ShowCollections"},
			{Label: "Flush", Value: "Flush"},
			{Label: "Compaction", Value: "Compaction"},
		}},
		{Key: "collection", Label: "Collection", Type: plugin.FieldText, Default: "*", Help: "Use * for every collection in the database."},
		{Key: "db", Label: "Database", Type: plugin.FieldText, Help: "Defaults to the selected database."},
	}}}}
}

func savedQuerySchema() *plugin.Schema {
	return &plugin.Schema{Groups: []plugin.Group{{Name: "Saved query", Fields: []plugin.Field{
		{Key: "name", Label: "Name", Type: plugin.FieldText, Required: true},
		{Key: "collection", Label: "Collection", Type: plugin.FieldText},
		{Key: "query", Label: "Query", Type: plugin.FieldTextarea, Required: true},
	}}}}
}
