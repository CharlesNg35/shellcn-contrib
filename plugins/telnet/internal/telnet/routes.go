package telnet

import (
	"io"
	"net/url"

	"github.com/charlesng35/shellcn/sdk/plugin"
)

func terminalSchema() *plugin.Schema {
	return &plugin.Schema{Groups: []plugin.Group{{Name: "Terminal", Fields: []plugin.Field{
		{Key: "cols", Label: "Columns", Type: plugin.FieldNumber},
		{Key: "rows", Label: "Rows", Type: plugin.FieldNumber},
	}}}}
}

func shell(rc *plugin.RequestContext, client plugin.ClientStream) error {
	ch, err := rc.Session.OpenChannel(rc.Ctx, plugin.ChannelRequest{Kind: plugin.StreamTerminal, Params: terminalParams(rc.Query())})
	if err != nil {
		return err
	}
	defer func() { _ = ch.Close() }()

	errc := make(chan error, 2)
	go func() {
		_, err := io.Copy(client, ch)
		errc <- err
	}()
	go func() {
		errc <- plugin.CopyTerminalInput(ch, client)
	}()
	select {
	case <-client.Context().Done():
		return nil
	case err := <-errc:
		if err == io.EOF {
			return nil
		}
		return err
	}
}

func terminalParams(q url.Values) map[string]string {
	params := map[string]string{}
	for _, key := range []string{"cols", "rows"} {
		if v := q.Get(key); v != "" {
			params[key] = v
		}
	}
	return params
}
