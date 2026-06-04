package main

import (
	"github.com/charlesng35/shellcn/sdk"

	pluginimpl "github.com/charlesng35/shellcn-contrib/plugins/rabbitmq/internal/rabbitmq"
)

func main() {
	sdk.Serve(pluginimpl.New())
}
