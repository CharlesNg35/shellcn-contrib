package arangodb

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"

	driver "github.com/arangodb/go-driver/v2/arangodb"
	"github.com/arangodb/go-driver/v2/arangodb/shared"

	"github.com/charlesng35/shellcn/sdk/plugin"
)

func viewsList(rc *plugin.RequestContext) (any, error) {
	s, err := session(rc)
	if err != nil {
		return nil, err
	}
	database := databaseParam(rc, s)
	db, err := s.database(rc.Ctx, database)
	if err != nil {
		return nil, err
	}
	ctx, cancel := context.WithTimeout(rc.Ctx, s.opts.Timeout)
	defer cancel()
	views, err := db.ViewsAll(ctx)
	if err != nil {
		return nil, arangoErr(err)
	}
	rows := make([]row, 0, len(views))
	for _, view := range views {
		item := row{
			"name":     view.Name(),
			"database": database,
			"type":     string(view.Type()),
			"links":    0,
			"ref":      plugin.ResourceIdentity{Kind: kindView, Namespace: database, Name: view.Name(), UID: encodeID(kindView, database, view.Name())},
		}
		if search, err := view.ArangoSearchView(); err == nil {
			if props, err := search.Properties(ctx); err == nil {
				item["links"] = len(props.Links)
				item["linked_collections"] = joinStrings(sortedKeys(props.Links))
				item["id"] = props.ID
			}
		}
		if alias, err := view.ArangoSearchViewAlias(); err == nil {
			if props, err := alias.Properties(ctx); err == nil {
				names := make([]string, 0, len(props.Indexes))
				for _, index := range props.Indexes {
					names = append(names, index.Collection)
				}
				item["links"] = len(props.Indexes)
				item["linked_collections"] = joinStrings(names)
			}
		}
		rows = append(rows, item)
	}
	return pageRows(rc, rows)
}

func viewRead(rc *plugin.RequestContext) (any, error) {
	s, database, name, err := viewScope(rc)
	if err != nil {
		return nil, err
	}
	view, props, err := s.viewProperties(rc.Ctx, database, name)
	if err != nil {
		return nil, err
	}
	out := structRow(props)
	out["name"] = view.Name()
	out["database"] = database
	out["type"] = string(view.Type())
	if search, ok := props.(driver.ArangoSearchViewProperties); ok {
		out["links"] = len(search.Links)
		out["linked_collections"] = joinStrings(sortedKeys(search.Links))
		out["analyzers"] = joinStrings(viewAnalyzers(search.Links))
	}
	return out, nil
}

func viewDefinition(rc *plugin.RequestContext) (any, error) {
	s, database, name, err := viewScope(rc)
	if err != nil {
		return nil, err
	}
	_, props, err := s.viewProperties(rc.Ctx, database, name)
	if err != nil {
		return nil, err
	}
	return editorContent(props)
}

func viewUpdate(rc *plugin.RequestContext) (any, error) {
	s, database, name, err := viewScope(rc)
	if err != nil {
		return nil, err
	}
	if err := ensureWritable(s); err != nil {
		return nil, err
	}
	body, err := parseEditorBody(rc, "properties")
	if err != nil {
		return nil, err
	}
	data, err := json.Marshal(body)
	if err != nil {
		return nil, fmt.Errorf("%w: %v", plugin.ErrInvalidInput, err)
	}
	db, err := s.database(rc.Ctx, database)
	if err != nil {
		return nil, err
	}
	ctx, cancel := context.WithTimeout(rc.Ctx, s.opts.Timeout)
	defer cancel()
	view, err := db.View(ctx, name)
	if err != nil {
		return nil, arangoErr(err)
	}
	if search, err := view.ArangoSearchView(); err == nil {
		var props driver.ArangoSearchViewProperties
		if err := json.Unmarshal(data, &props); err != nil {
			return nil, fmt.Errorf("%w: invalid ArangoSearch view properties: %v", plugin.ErrInvalidInput, err)
		}
		if err := search.UpdateProperties(ctx, props); err != nil {
			return nil, arangoErr(err)
		}
	} else {
		alias, aliasErr := view.ArangoSearchViewAlias()
		if aliasErr != nil {
			return nil, fmt.Errorf("%w: view type %q cannot be edited", plugin.ErrNotSupported, view.Type())
		}
		var props driver.ArangoSearchAliasUpdateOpts
		if err := json.Unmarshal(data, &props); err != nil {
			return nil, fmt.Errorf("%w: invalid search-alias view properties: %v", plugin.ErrInvalidInput, err)
		}
		if err := alias.UpdateProperties(ctx, props); err != nil {
			return nil, arangoErr(err)
		}
	}
	_, props, err := s.viewProperties(rc.Ctx, database, name)
	if err != nil {
		return nil, err
	}
	return editorContent(props)
}

func viewCreateSchema() *plugin.Schema {
	return &plugin.Schema{Groups: []plugin.Group{{Name: "ArangoSearch view", Fields: []plugin.Field{
		{Key: "name", Label: "Name", Type: plugin.FieldText, Required: true,
			Validators: []plugin.Validator{{Type: plugin.ValidatorRegex, Value: namePattern, Message: "most characters are allowed; not / : % ? # + or control characters"}}},
		{Key: "collection", Label: "Linked collection", Type: plugin.FieldText,
			Help: "Optional. Creates a link that indexes every attribute of this collection."},
		{Key: "analyzer", Label: "Analyzer", Type: plugin.FieldAutocomplete, Default: "identity", Options: []plugin.Option{
			{Label: "identity", Value: "identity"},
			{Label: "text_en", Value: "text_en"},
			{Label: "text_de", Value: "text_de"},
			{Label: "text_fr", Value: "text_fr"},
			{Label: "text_es", Value: "text_es"},
		}, VisibleWhen: &plugin.Condition{AllOf: []plugin.Rule{{Field: "collection", Op: plugin.OpNotEmpty}}}},
	}}}}
}

func viewCreate(rc *plugin.RequestContext) (any, error) {
	s, err := session(rc)
	if err != nil {
		return nil, err
	}
	if err := ensureWritable(s); err != nil {
		return nil, err
	}
	var req struct {
		Name       string `json:"name" validate:"required"`
		Collection string `json:"collection"`
		Analyzer   string `json:"analyzer"`
	}
	if err := rc.Bind(&req); err != nil {
		return nil, err
	}
	name, err := safeName(req.Name, "view")
	if err != nil {
		return nil, err
	}
	database := databaseParam(rc, s)
	db, err := s.database(rc.Ctx, database)
	if err != nil {
		return nil, err
	}
	props := &driver.ArangoSearchViewProperties{}
	if collection := strings.TrimSpace(req.Collection); collection != "" {
		if _, err := safeName(collection, "collection"); err != nil {
			return nil, err
		}
		analyzer, err := safeAnalyzerName(orDefault(req.Analyzer, "identity"))
		if err != nil {
			return nil, err
		}
		props.Links = driver.ArangoSearchLinks{collection: driver.ArangoSearchElementProperties{
			Analyzers: []string{analyzer}, IncludeAllFields: boolPtr(true),
		}}
	}
	ctx, cancel := context.WithTimeout(rc.Ctx, s.opts.Timeout)
	defer cancel()
	if _, err := db.CreateArangoSearchView(ctx, name, props); err != nil {
		return nil, arangoErr(err)
	}
	return row{"ok": true, "name": name, "database": database}, nil
}

func viewDrop(rc *plugin.RequestContext) (any, error) {
	s, database, name, err := viewScope(rc)
	if err != nil {
		return nil, err
	}
	if err := ensureWritable(s); err != nil {
		return nil, err
	}
	db, err := s.database(rc.Ctx, database)
	if err != nil {
		return nil, err
	}
	ctx, cancel := context.WithTimeout(rc.Ctx, s.opts.Timeout)
	defer cancel()
	view, err := db.View(ctx, name)
	if err != nil {
		return nil, arangoErr(err)
	}
	if err := view.Remove(ctx); err != nil {
		return nil, arangoErr(err)
	}
	return actionResult{OK: true}, nil
}

// viewLinks flattens an ArangoSearch view's link definitions into one row per
// indexed collection so an operator can see analyzers and field coverage.
func viewLinks(rc *plugin.RequestContext) (any, error) {
	s, database, name, err := viewScope(rc)
	if err != nil {
		return nil, err
	}
	_, props, err := s.viewProperties(rc.Ctx, database, name)
	if err != nil {
		return nil, err
	}
	rows := make([]row, 0, 4)
	switch typed := props.(type) {
	case driver.ArangoSearchViewProperties:
		for _, collection := range sortedKeys(typed.Links) {
			link := typed.Links[collection]
			rows = append(rows, row{
				"collection":      collection,
				"analyzers":       joinStrings(link.Analyzers),
				"include_all":     boolOrFalse(link.IncludeAllFields),
				"track_positions": boolOrFalse(link.TrackListPositions),
				"store_values":    string(link.StoreValues),
				"fields":          joinStrings(sortedKeys(link.Fields)),
				"documents":       s.queryInt(rc.Ctx, database, "RETURN LENGTH(@@col)", map[string]any{"@col": collection}),
			})
		}
	case driver.ArangoSearchAliasViewProperties:
		for _, index := range typed.Indexes {
			rows = append(rows, row{
				"collection": index.Collection,
				"analyzers":  index.Index,
				"documents":  s.queryInt(rc.Ctx, database, "RETURN LENGTH(@@col)", map[string]any{"@col": index.Collection}),
			})
		}
	}
	return pageRows(rc, rows)
}

func (s *Session) viewProperties(ctx context.Context, database, name string) (driver.View, any, error) {
	db, err := s.database(ctx, database)
	if err != nil {
		return nil, nil, err
	}
	tctx, cancel := context.WithTimeout(ctx, s.opts.Timeout)
	defer cancel()
	view, err := db.View(tctx, name)
	if err != nil {
		return nil, nil, arangoErr(err)
	}
	if search, err := view.ArangoSearchView(); err == nil {
		props, err := search.Properties(tctx)
		if err != nil {
			return nil, nil, arangoErr(err)
		}
		return view, props, nil
	}
	alias, err := view.ArangoSearchViewAlias()
	if err != nil {
		return nil, nil, fmt.Errorf("%w: unsupported view type %q", plugin.ErrNotSupported, view.Type())
	}
	props, err := alias.Properties(tctx)
	if err != nil {
		return nil, nil, arangoErr(err)
	}
	return view, props, nil
}

func viewAnalyzers(links driver.ArangoSearchLinks) []string {
	seen := map[string]bool{}
	names := make([]string, 0, 4)
	for _, collection := range sortedKeys(links) {
		for _, analyzer := range links[collection].Analyzers {
			if !seen[analyzer] {
				seen[analyzer] = true
				names = append(names, analyzer)
			}
		}
	}
	return names
}

func analyzersList(rc *plugin.RequestContext) (any, error) {
	s, err := session(rc)
	if err != nil {
		return nil, err
	}
	database := databaseParam(rc, s)
	db, err := s.database(rc.Ctx, database)
	if err != nil {
		return nil, err
	}
	ctx, cancel := context.WithTimeout(rc.Ctx, s.opts.Timeout)
	defer cancel()
	reader, err := db.Analyzers(ctx)
	if err != nil {
		return nil, arangoErr(err)
	}
	rows := make([]row, 0, 16)
	for {
		analyzer, err := reader.Read()
		if err != nil {
			if shared.IsNoMoreDocuments(err) {
				break
			}
			return nil, arangoErr(err)
		}
		definition := analyzer.Definition()
		rows = append(rows, row{
			"name":        analyzer.Name(),
			"unique_name": analyzer.UniqueName(),
			"database":    database,
			"type":        string(definition.Type),
			"features":    analyzerFeatures(definition.Features),
			"builtin":     strings.HasPrefix(analyzer.UniqueName(), "_system::") && database != "_system",
			"properties":  structRow(definition.Properties),
			"ref":         plugin.ResourceIdentity{Kind: kindAnalyzer, Namespace: database, Name: analyzer.Name(), UID: encodeID(kindAnalyzer, database, analyzer.Name())},
		})
	}
	return pageRows(rc, rows)
}

func analyzerRead(rc *plugin.RequestContext) (any, error) {
	s, err := session(rc)
	if err != nil {
		return nil, err
	}
	database := databaseParam(rc, s)
	name, err := safeAnalyzerName(paramOrQuery(rc, "analyzer"))
	if err != nil {
		return nil, err
	}
	db, err := s.database(rc.Ctx, database)
	if err != nil {
		return nil, err
	}
	ctx, cancel := context.WithTimeout(rc.Ctx, s.opts.Timeout)
	defer cancel()
	analyzer, err := db.Analyzer(ctx, name)
	if err != nil {
		return nil, arangoErr(err)
	}
	definition := analyzer.Definition()
	return row{
		"name":        analyzer.Name(),
		"unique_name": analyzer.UniqueName(),
		"database":    database,
		"type":        string(definition.Type),
		"features":    analyzerFeatures(definition.Features),
		"properties":  structRow(definition.Properties),
	}, nil
}

func analyzerCreateSchema() *plugin.Schema {
	return &plugin.Schema{Groups: []plugin.Group{{Name: "Analyzer", Fields: []plugin.Field{
		{Key: "name", Label: "Name", Type: plugin.FieldText, Required: true,
			Validators: []plugin.Validator{{Type: plugin.ValidatorRegex, Value: namePattern, Message: "most characters are allowed; not / : % ? # + or control characters"}}},
		{Key: "type", Label: "Type", Type: plugin.FieldSelect, Required: true, Default: "text", Options: []plugin.Option{
			{Label: "Text", Value: "text"},
			{Label: "Identity", Value: "identity"},
			{Label: "Delimiter", Value: "delimiter"},
			{Label: "Stem", Value: "stem"},
			{Label: "Norm", Value: "norm"},
		}},
		{Key: "locale", Label: "Locale", Type: plugin.FieldAutocomplete, Default: "en", Options: []plugin.Option{
			{Label: "en", Value: "en"}, {Label: "de", Value: "de"}, {Label: "fr", Value: "fr"},
			{Label: "es", Value: "es"}, {Label: "ru", Value: "ru"}, {Label: "zh", Value: "zh"},
		}, VisibleWhen: &plugin.Condition{AllOf: []plugin.Rule{{Field: "type", Op: plugin.OpIn, Value: []any{"text", "stem", "norm"}}}}},
		{Key: "delimiter", Label: "Delimiter", Type: plugin.FieldText, Default: ",",
			VisibleWhen: &plugin.Condition{AllOf: []plugin.Rule{{Field: "type", Op: plugin.OpEq, Value: "delimiter"}}}},
		{Key: "features", Label: "Features", Type: plugin.FieldMultiSelect, Options: []plugin.Option{
			{Label: "frequency", Value: "frequency"},
			{Label: "norm", Value: "norm"},
			{Label: "position", Value: "position"},
		}, Default: []any{"frequency", "norm", "position"}},
	}}}}
}

func analyzerCreate(rc *plugin.RequestContext) (any, error) {
	s, err := session(rc)
	if err != nil {
		return nil, err
	}
	if err := ensureWritable(s); err != nil {
		return nil, err
	}
	var req struct {
		Name      string   `json:"name" validate:"required"`
		Type      string   `json:"type"`
		Locale    string   `json:"locale"`
		Delimiter string   `json:"delimiter"`
		Features  []string `json:"features"`
	}
	if err := rc.Bind(&req); err != nil {
		return nil, err
	}
	name, err := safeName(req.Name, "analyzer")
	if err != nil {
		return nil, err
	}
	definition := &driver.AnalyzerDefinition{Name: name, Type: driver.ArangoSearchAnalyzerType(orDefault(req.Type, "text"))}
	for _, feature := range req.Features {
		switch feature {
		case "frequency", "norm", "position":
			definition.Features = append(definition.Features, driver.ArangoSearchFeature(feature))
		default:
			return nil, fmt.Errorf("%w: unsupported analyzer feature %q", plugin.ErrInvalidInput, feature)
		}
	}
	switch definition.Type {
	case "text":
		// The driver always serialises `stopwords`, and the server rejects a null
		// list, so an explicit empty list is required for the text analyzer.
		definition.Properties = driver.ArangoSearchAnalyzerProperties{
			Locale: orDefault(req.Locale, "en"), Stopwords: []string{},
			Accent: boolPtr(false), Case: driver.ArangoSearchCaseLower, Stemming: boolPtr(true),
		}
	case "stem":
		definition.Properties = driver.ArangoSearchAnalyzerProperties{Locale: orDefault(req.Locale, "en")}
	case "norm":
		definition.Properties = driver.ArangoSearchAnalyzerProperties{
			Locale: orDefault(req.Locale, "en"), Accent: boolPtr(false), Case: driver.ArangoSearchCaseLower,
		}
	case "delimiter":
		definition.Properties = driver.ArangoSearchAnalyzerProperties{Delimiter: orDefault(req.Delimiter, ",")}
	case "identity":
	default:
		return nil, fmt.Errorf("%w: unsupported analyzer type %q", plugin.ErrInvalidInput, req.Type)
	}
	db, err := s.database(rc.Ctx, databaseParam(rc, s))
	if err != nil {
		return nil, err
	}
	ctx, cancel := context.WithTimeout(rc.Ctx, s.opts.Timeout)
	defer cancel()
	analyzer, created, err := db.EnsureCreatedAnalyzer(ctx, definition)
	if err != nil {
		return nil, arangoErr(err)
	}
	return row{"ok": true, "name": analyzer.Name(), "created": created}, nil
}

func analyzerDelete(rc *plugin.RequestContext) (any, error) {
	s, err := session(rc)
	if err != nil {
		return nil, err
	}
	if err := ensureWritable(s); err != nil {
		return nil, err
	}
	name, err := safeAnalyzerName(paramOrQuery(rc, "analyzer"))
	if err != nil {
		return nil, err
	}
	db, err := s.database(rc.Ctx, databaseParam(rc, s))
	if err != nil {
		return nil, err
	}
	ctx, cancel := context.WithTimeout(rc.Ctx, s.opts.Timeout)
	defer cancel()
	analyzer, err := db.Analyzer(ctx, name)
	if err != nil {
		return nil, arangoErr(err)
	}
	if err := analyzer.Remove(ctx, false); err != nil {
		return nil, arangoErr(err)
	}
	return actionResult{OK: true}, nil
}

func analyzerFeatures(features []driver.ArangoSearchFeature) string {
	names := make([]string, 0, len(features))
	for _, feature := range features {
		names = append(names, string(feature))
	}
	return joinStrings(names)
}

func orDefault(value, fallback string) string {
	if strings.TrimSpace(value) == "" {
		return fallback
	}
	return strings.TrimSpace(value)
}
