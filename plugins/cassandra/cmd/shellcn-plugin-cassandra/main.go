package main

import (
	"github.com/charlesng35/shellcn/sdk"

	pluginimpl "github.com/charlesng35/shellcn-contrib/plugins/cassandra/internal/cassandra"
)

func main() {
	sdk.Serve(pluginimpl.New())
}
