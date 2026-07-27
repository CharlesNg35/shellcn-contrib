package main

import (
	"github.com/charlesng35/shellcn/sdk"

	pluginimpl "github.com/charlesng35/shellcn-contrib/plugins/pinecone/internal/pinecone"
)

func main() {
	sdk.Serve(pluginimpl.New())
}
