package main

import (
	cloudflareplugin "github.com/charlesng35/shellcn-contrib/plugins/cloudflare/internal/cloudflare"
	"github.com/charlesng35/shellcn/sdk"
)

func main() {
	sdk.Serve(cloudflareplugin.New())
}
