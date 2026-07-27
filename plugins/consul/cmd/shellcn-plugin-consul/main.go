package main

import (
	"github.com/charlesng35/shellcn/sdk"

	pluginimpl "github.com/charlesng35/shellcn-contrib/plugins/consul/internal/consul"
)

func main() {
	sdk.Serve(pluginimpl.New())
}
