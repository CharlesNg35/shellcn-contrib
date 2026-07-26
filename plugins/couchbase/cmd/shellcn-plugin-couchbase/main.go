package main

import (
	"github.com/charlesng35/shellcn/sdk"

	pluginimpl "github.com/charlesng35/shellcn-contrib/plugins/couchbase/internal/couchbase"
)

func main() {
	sdk.Serve(pluginimpl.New())
}
