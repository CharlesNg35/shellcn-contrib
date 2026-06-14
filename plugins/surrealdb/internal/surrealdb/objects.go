package surrealdb

import (
	"fmt"
	"strings"

	"github.com/charlesng35/shellcn/sdk/plugin"
)

// objectKind maps a resource kind / REMOVE keyword to its INFO FOR DB section.
type objectKind struct {
	section string // INFO FOR DB key
	keyword string // REMOVE <keyword> ...
	prefix  string // statement prefix when naming (e.g. "fn::" for functions)
}

var objectKinds = map[string]objectKind{
	"function": {section: "functions", keyword: "FUNCTION", prefix: "fn::"},
	"param":    {section: "params", keyword: "PARAM", prefix: "$"},
	"analyzer": {section: "analyzers", keyword: "ANALYZER"},
	"user":     {section: "users", keyword: "USER"},
}

// objectLister returns a list handler for one INFO FOR DB section.
func objectLister(sectionKey string) plugin.Handler {
	kind := kindForSection(sectionKey)
	return func(rc *plugin.RequestContext) (any, error) {
		db, err := sess(rc).client(rc.Ctx)
		if err != nil {
			return nil, err
		}
		info, err := dbInfo(rc, db)
		if err != nil {
			return nil, err
		}
		page, err := rc.Page()
		if err != nil {
			return nil, err
		}
		term := strings.ToLower(page.Search())
		defs := section(info, sectionKey)
		rows := make([]objectRow, 0, len(defs))
		for _, name := range sortedKeys(defs) {
			if term != "" && !strings.Contains(strings.ToLower(name), term) {
				continue
			}
			def, _ := defs[name].(string)
			rows = append(rows, objectRow{
				Name: name, Definition: def,
				Ref: plugin.ResourceIdentity{Kind: kind, Name: name, UID: name},
			})
		}
		return plugin.Page[objectRow]{Items: rows}, nil
	}
}

func kindForSection(sectionKey string) string {
	for k, v := range objectKinds {
		if v.section == sectionKey {
			return k
		}
	}
	return sectionKey
}

// objectDefinition returns one DB object's DEFINE statement as a document.
func objectDefinition(rc *plugin.RequestContext) (any, error) {
	kind, ok := objectKinds[rc.Param("kind")]
	if !ok {
		return nil, fmt.Errorf("%w: unknown object kind", plugin.ErrInvalidInput)
	}
	name, err := requireIdent(rc.Param("name"))
	if err != nil {
		return nil, err
	}
	db, err := sess(rc).client(rc.Ctx)
	if err != nil {
		return nil, err
	}
	info, err := dbInfo(rc, db)
	if err != nil {
		return nil, err
	}
	def, _ := section(info, kind.section)[name].(string)
	if def == "" {
		return nil, fmt.Errorf("%w: %s %s", plugin.ErrNotFound, rc.Param("kind"), name)
	}
	return map[string]any{"content": "```surql\n" + def + "\n```", "format": "markdown"}, nil
}

// removeObject drops a DB object. The {name} is validated; the REMOVE keyword and
// any name prefix (fn::, $) come from the kind catalog.
func removeObject(rc *plugin.RequestContext) (any, error) {
	kind, ok := objectKinds[rc.Param("kind")]
	if !ok {
		return nil, fmt.Errorf("%w: unknown object kind", plugin.ErrInvalidInput)
	}
	name, err := requireIdent(rc.Param("name"))
	if err != nil {
		return nil, err
	}
	db, err := sess(rc).client(rc.Ctx)
	if err != nil {
		return nil, err
	}
	if _, err := queryOne[any](rc.Ctx, db,
		fmt.Sprintf("REMOVE %s %s%s", kind.keyword, kind.prefix, name), nil); err != nil {
		return nil, err
	}
	return map[string]any{"ok": true}, nil
}
