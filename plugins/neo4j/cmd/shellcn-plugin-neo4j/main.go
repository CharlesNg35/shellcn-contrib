package main

import (
	"github.com/charlesng35/shellcn/sdk"

	pluginimpl "github.com/charlesng35/shellcn-contrib/plugins/neo4j/internal/neo4j"
)

func main() {
	sdk.Serve(pluginimpl.New())
}
