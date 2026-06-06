package prometheus

import (
	"testing"

	"github.com/charlesng35/shellcn/sdk/plugintest"
)

func TestManifestValidates(t *testing.T) {
	plugintest.ValidatePlugin(t, New())
}
