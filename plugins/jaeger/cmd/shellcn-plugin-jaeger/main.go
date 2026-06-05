package main

import (
	"github.com/charlesng35/shellcn/sdk"

	pluginimpl "github.com/charlesng35/shellcn-contrib/plugins/jaeger/internal/jaeger"
)

func main() {
	sdk.Serve(pluginimpl.New())
}
