package arangodb

import (
	"context"
	"sort"
	"strings"

	driver "github.com/arangodb/go-driver/v2/arangodb"

	"github.com/charlesng35/shellcn/sdk/plugin"
)

func foxxList(rc *plugin.RequestContext) (any, error) {
	s, err := session(rc)
	if err != nil {
		return nil, err
	}
	database := databaseParam(rc, s)
	if _, err := safeDatabaseName(database); err != nil {
		return nil, err
	}
	ctx, cancel := context.WithTimeout(rc.Ctx, s.opts.Timeout)
	defer cancel()
	services, err := s.client.ListInstalledFoxxServices(ctx, database, boolPtr(true))
	if err != nil {
		return nil, arangoErr(err)
	}
	rows := make([]row, 0, len(services))
	for _, service := range services {
		mount := stringOrEmpty(service.Mount)
		rows = append(rows, row{
			"mount":       mount,
			"name":        orDefault(stringOrEmpty(service.Name), mount),
			"database":    database,
			"version":     stringOrEmpty(service.Version),
			"development": boolOrFalse(service.Development),
			"legacy":      boolOrFalse(service.Legacy),
			"provides":    joinStrings(sortedKeys(service.Provides)),
			"ref":         plugin.ResourceIdentity{Kind: kindFoxx, Namespace: database, Name: mount, UID: encodeID(kindFoxx, database, mount)},
		})
	}
	return pageRows(rc, rows)
}

func foxxRead(rc *plugin.RequestContext) (any, error) {
	s, database, mount, err := foxxScope(rc)
	if err != nil {
		return nil, err
	}
	ctx, cancel := context.WithTimeout(rc.Ctx, s.opts.Timeout)
	defer cancel()
	service, err := s.client.GetInstalledFoxxService(ctx, database, &mount)
	if err != nil {
		return nil, arangoErr(err)
	}
	out := row{
		"mount":       mount,
		"database":    database,
		"name":        orDefault(stringOrEmpty(service.Name), mount),
		"version":     stringOrEmpty(service.Version),
		"development": boolOrFalse(service.Development),
		"legacy":      boolOrFalse(service.Legacy),
		"path":        stringOrEmpty(service.Path),
	}
	if service.Manifest != nil {
		out["description"] = stringOrEmpty(service.Manifest.Description)
		out["author"] = stringOrEmpty(service.Manifest.Author)
		out["license"] = stringOrEmpty(service.Manifest.License)
		out["main"] = stringOrEmpty(service.Manifest.Main)
		out["engines"] = compact(service.Manifest.Engines)
		out["scripts"] = joinStrings(sortedKeys(service.Manifest.Scripts))
	}
	if swagger, err := s.client.GetFoxxServiceSwagger(ctx, database, &mount); err == nil {
		out["base_path"] = stringOrEmpty(swagger.BasePath)
		out["routes"] = len(swagger.Paths)
	}
	return out, nil
}

// foxxRoutes flattens the service's Swagger document into one row per HTTP
// operation, which is the view an operator actually needs when debugging a
// mounted service.
func foxxRoutes(rc *plugin.RequestContext) (any, error) {
	s, database, mount, err := foxxScope(rc)
	if err != nil {
		return nil, err
	}
	ctx, cancel := context.WithTimeout(rc.Ctx, s.opts.Timeout)
	defer cancel()
	swagger, err := s.client.GetFoxxServiceSwagger(ctx, database, &mount)
	if err != nil {
		return nil, arangoErr(err)
	}
	base := stringOrEmpty(swagger.BasePath)
	rows := make([]row, 0, len(swagger.Paths))
	for _, path := range sortedKeys(swagger.Paths) {
		operations := asMap(swagger.Paths[path])
		for _, method := range sortedKeys(operations) {
			operation := asMap(operations[method])
			rows = append(rows, row{
				"method":      strings.ToUpper(method),
				"path":        path,
				"full_path":   base + path,
				"summary":     compact(operation["summary"]),
				"description": compact(operation["description"]),
				"tags":        compact(operation["tags"]),
			})
		}
	}
	return pageRows(rc, rows)
}

func foxxReadme(rc *plugin.RequestContext) (any, error) {
	s, database, mount, err := foxxScope(rc)
	if err != nil {
		return nil, err
	}
	ctx, cancel := context.WithTimeout(rc.Ctx, s.opts.Timeout)
	defer cancel()
	readme, err := s.client.GetFoxxServiceReadme(ctx, database, &mount)
	if err != nil {
		// A disabled Foxx API, a permission denial or an unmounted service must
		// reach the operator rather than look like a missing README.
		return nil, arangoErr(err)
	}
	// The server answers 204 when the service ships no README, which the driver
	// reports as an empty body.
	if len(readme) == 0 {
		return "This Foxx service does not ship a README.", nil
	}
	return string(readme), nil
}

func foxxConfiguration(rc *plugin.RequestContext) (any, error) {
	s, database, mount, err := foxxScope(rc)
	if err != nil {
		return nil, err
	}
	ctx, cancel := context.WithTimeout(rc.Ctx, s.opts.Timeout)
	defer cancel()
	config, err := s.client.GetFoxxServiceConfiguration(ctx, database, &mount)
	if err != nil {
		return nil, arangoErr(err)
	}
	return pageRows(rc, settingRows(config, s.opts.RedactPatterns))
}

func foxxDependencies(rc *plugin.RequestContext) (any, error) {
	s, database, mount, err := foxxScope(rc)
	if err != nil {
		return nil, err
	}
	ctx, cancel := context.WithTimeout(rc.Ctx, s.opts.Timeout)
	defer cancel()
	deps, err := s.client.GetFoxxServiceDependencies(ctx, database, &mount)
	if err != nil {
		return nil, arangoErr(err)
	}
	return pageRows(rc, settingRows(deps, s.opts.RedactPatterns))
}

// settingRows renders Foxx configuration/dependency descriptors as a table.
// Secret-looking values are masked by the connection's redaction patterns.
func settingRows(settings map[string]any, patterns []string) []row {
	names := make([]string, 0, len(settings))
	for name := range settings {
		names = append(names, name)
	}
	sort.Strings(names)
	rows := make([]row, 0, len(names))
	for _, name := range names {
		descriptor := asMap(settings[name])
		value := normalize(map[string]any{name: descriptor["current"]}, patterns)
		rows = append(rows, row{
			"name":     name,
			"title":    compact(descriptor["title"]),
			"type":     compact(descriptor["type"]),
			"required": descriptor["required"],
			"default":  compact(descriptor["default"]),
			"current":  compact(asMap(value)[name]),
		})
	}
	return rows
}

func foxxDevelopment(rc *plugin.RequestContext) (any, error) {
	s, database, mount, err := foxxScope(rc)
	if err != nil {
		return nil, err
	}
	if err := ensureWritable(s); err != nil {
		return nil, err
	}
	var req struct {
		Enabled bool `json:"enabled"`
	}
	if err := rc.Bind(&req); err != nil {
		return nil, err
	}
	ctx, cancel := context.WithTimeout(rc.Ctx, s.opts.Timeout)
	defer cancel()
	if req.Enabled {
		if _, err := s.client.EnableDevelopmentMode(ctx, database, &mount); err != nil {
			return nil, arangoErr(err)
		}
	} else if _, err := s.client.DisableDevelopmentMode(ctx, database, &mount); err != nil {
		return nil, arangoErr(err)
	}
	return row{"ok": true, "mount": mount, "development": req.Enabled}, nil
}

func foxxDevelopmentSchema() *plugin.Schema {
	return &plugin.Schema{Groups: []plugin.Group{{Name: "Development mode", Fields: []plugin.Field{
		{Key: "enabled", Label: "Enable development mode", Type: plugin.FieldToggle,
			Help: "Development mode reloads service files on every request and must not stay on in production."},
	}}}}
}

func foxxUninstall(rc *plugin.RequestContext) (any, error) {
	s, database, mount, err := foxxScope(rc)
	if err != nil {
		return nil, err
	}
	if err := ensureWritable(s); err != nil {
		return nil, err
	}
	ctx, cancel := context.WithTimeout(rc.Ctx, s.opts.Timeout)
	defer cancel()
	if err := s.client.UninstallFoxxService(ctx, database, &driver.FoxxDeleteOptions{Mount: &mount, Teardown: boolPtr(true)}); err != nil {
		return nil, arangoErr(err)
	}
	return actionResult{OK: true}, nil
}

func foxxScope(rc *plugin.RequestContext) (*Session, string, string, error) {
	s, err := session(rc)
	if err != nil {
		return nil, "", "", err
	}
	if id := strings.TrimSpace(paramOrQuery(rc, "service")); id != "" {
		parts, err := decodeID(id, 3)
		if err != nil {
			return nil, "", "", err
		}
		database, err := safeDatabaseName(parts[1])
		if err != nil {
			return nil, "", "", err
		}
		mount, err := safeMount(parts[2])
		if err != nil {
			return nil, "", "", err
		}
		return s, database, mount, nil
	}
	database, err := safeDatabaseName(databaseParam(rc, s))
	if err != nil {
		return nil, "", "", err
	}
	mount, err := safeMount(paramOrQuery(rc, "mount"))
	if err != nil {
		return nil, "", "", err
	}
	return s, database, mount, nil
}
