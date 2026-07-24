package sqlite

import (
	"context"
	"fmt"

	"github.com/charlesng35/shellcn/sdk/plugin"
)

type session struct{}

func (session) HealthCheck(context.Context) error { return nil }
func (session) Close() error                      { return nil }
func (session) OpenChannel(context.Context, plugin.ChannelRequest) (plugin.Channel, error) {
	return nil, fmt.Errorf("%w: no channels", plugin.ErrNotSupported)
}
