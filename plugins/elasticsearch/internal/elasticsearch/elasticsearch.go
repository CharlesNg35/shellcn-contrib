// Package elasticsearch implements the Elasticsearch protocol plugin.
package elasticsearch

import (
	"github.com/charlesng35/shellcn-contrib/shared/escompat"
	"github.com/charlesng35/shellcn/sdk/plugin"
)

const elasticsearchIconSVG = `<svg viewBox="0 0 128 128"xmlns=http://www.w3.org/2000/svg><path d="M4 64c0 5.535.777 10.879 2.098 16H84c8.836 0 16-7.164 16-16s-7.164-16-16-16H6.098A63.738 63.738 0 0 0 4 64"fill=#343741 /><path d="M111.695 30.648A61.485 61.485 0 0 0 117.922 24C106.188 9.379 88.199 0 68 0 42.715 0 20.957 14.71 10.574 36H98.04a20.123 20.123 0 0 0 13.652-5.352"fill=#fec514 /><path d="M98.04 92H10.577C20.961 113.29 42.715 128 68 128c20.2 0 38.188-9.383 49.922-24a61.1 61.1 0 0 0-6.227-6.648A20.133 20.133 0 0 0 98.04 92"fill=#00bfb3 /></svg>`

func New() plugin.Plugin {
	return escompat.New(escompat.Provider{
		Protocol:    "elasticsearch",
		Title:       "Elasticsearch",
		Description: "Elasticsearch cockpit with cluster health, indices, mappings, settings, shards, documents, JSON DSL search, and document/index operations.",
		DefaultURL:  "http://localhost:9200",
		Product:     escompat.ProductElasticsearch,
		Icon:        plugin.Icon{Type: plugin.IconSVG, Value: elasticsearchIconSVG},
	})
}
