package main

import (
	"github.com/charlesng35/shellcn/sdk"

	pluginimpl "github.com/charlesng35/shellcn-contrib/plugins/mqtt/internal/mqtt"
)

func main() {
	sdk.Serve(pluginimpl.New())
}
