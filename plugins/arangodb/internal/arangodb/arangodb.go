// Package arangodb implements the ArangoDB protocol plugin.
package arangodb

import (
	"context"
	"fmt"
	"net/http"
	"strconv"
	"sync"
	"time"

	driver "github.com/arangodb/go-driver/v2/arangodb"
	"github.com/arangodb/go-driver/v2/connection"

	"github.com/charlesng35/shellcn/sdk/plugin"
)

const arangoIconSVG = `<svg xmlns="http://www.w3.org/2000/svg" fill="none" viewBox="0 0 24 24" id="Arangodb-Icon--Streamline-Svg-Logos" height="24" width="24"><desc>Arangodb Icon Streamline Icon: https://streamlinehq.com</desc><path fill="#577138" d="M23.333725 12.049425c-0.217225 -0.7214 -0.325675 -1.08215 -0.48305 -1.4658 -0.303275 -0.742025 -0.68915 -1.345275 -1.2673 -2.09285 -1.35305 -1.746675 -2.029325 -2.621625 -3.132925 -3.3187 -2.39455 -1.5139 -4.914525 -1.246025 -5.20195 -1.21125 -0.86 0.10355 -2.751825 0.480975 -4.37305 2.0452 -0.91845 0.887325 -1.49565 2.15955 -1.76575 2.809 -0.2987 5.17655 4.779575 7.25035 5.97525 8.117325 0.384125 0.182925 1.267775 0.571975 1.984875 0.942275 1.656175 0.85475 2.804225 1.03165 3.396525 1.135175 1.290625 0.226275 3.29805 -0.2847 4.2508 -1.352725 0.912875 -1.0234 1.1787 -2.404075 0.960975 -3.6347 -0.0661 -0.3784 -0.0671 -1.051175 -0.3452 -1.97535l0.0008 0.0024Z" stroke-width="0.25"></path><path fill="#a3b34f" d="M22.6037 11.493775c-0.345825 -0.922725 -0.751225 -1.535025 -1.232525 -2.261175 -0.5891 -0.889375 -1.11565 -1.495625 -1.5385 -1.978525 -0.901775 -1.029575 -1.35305 -1.544375 -1.965825 -1.918175 -1.0577 -0.645475 -2.08015 -0.762025 -2.661325 -0.822675 -1.371625 -0.1422 -2.41995 0.1486 -2.929675 0.294875 -0.5243 0.151025 -1.563275 0.46065 -2.6486 1.31255 -0.9829 0.771875 -1.587575 1.93405 -1.9166 2.559675 -0.302 0.564025 1.555975 5.0908 5.848225 7.766425 0.219625 0.11355 0.831125 0.392675 1.208725 0.5837 0.1229 0.062225 0.2358 0.117925 0.3414 0.169125 0.170525 0.078075 0.329475 0.152825 0.479225 0.22435 0.482075 0.2177 0.7806 0.328375 1.175675 0.535125 1.368775 0.715675 2.81375 0.511625 3.028125 0.478275 0.284075 -0.043675 2.259575 -0.349325 3.07735 -1.92135 0.431425 -0.82905 0.3563 -1.6546 0.253725 -2.785175 -0.029025 -0.3206 -0.126875 -1.1879 -0.520975 -2.238925" stroke-width="0.25"></path><path fill="#dde072" d="M15.975925 17.035325c1.438625 0.550675 1.911825 0.838425 2.527925 0.913525 0.202475 0.024225 2.13415 0.145675 3.266325 -0.924 0.843 -0.79585 0.8835 -1.6006 0.932075 -1.9801 0.168 -1.306525 -0.20355 -2.982075 -0.637525 -3.8046 -0.56975 -1.080575 -0.71695 -1.30765 -2.2453 -3.333 -1.13185 -1.498025 -1.69905 -1.775275 -2.28975 -2.164325 -1.0153 -0.6639 -2.1897 -0.678975 -2.986825 -0.71185 -0.5864 -0.0239 -1.68475 0.191025 -2.73755 0.56005 -1.64505 0.578475 -2.605725 1.547725 -3.280575 2.92015 -0.749175 1.58265 3.039225 6.740625 5.67355 7.795" stroke-width="0.25"></path><path fill="#a3b34f" d="M16.468225 13.93895c0.917325 -0.160375 1.528025 -1.101675 1.714925 -1.864175 0.262325 -1.0785 -0.28025 -1.978525 -0.641825 -2.57875 -0.341225 -0.564825 -0.9413 -1.355125 -2.02775 -1.795925 -1.0002 -0.4065 -1.77685 0.086625 -2.1389 0.2882 -1.595825 2.359625 -0.35965 4.754175 1.98805 5.94985 0.2963 0.0293 0.8724 0.041475 1.105025 0.000975" stroke-width="0.25"></path><path fill="#577138" d="M13.8117 16.162025c0.402675 -0.4808 1.176775 -1.4777 1.489925 -2.0976 0.0204 -0.04035 0.040825 -0.0832 0.061225 -0.127625 0.33775 -0.733925 0.6728 -2.003925 0.704725 -3.0043 0.028625 -0.8959 -0.277725 -1.50135 -0.277725 -1.50135 -0.3125 -0.81475 -1.21125 -1.09375 -1.605375 -1.2446 -0.3236 -0.1172 -0.61355 -0.17705 -0.9078 -0.210725 -0.579425 -0.051975 -1.38305 0.010025 -2.772475 0.2139 -1.376375 0.192775 -2.750225 0.455575 -4.015775 0.760275 -1.5344 0.37395 -2.380275 0.5799 -3.355225 1.051675 -1.1259875 0.54465 -2.18972 1.059125 -2.65021 2.1897 -0.2816935 0.6925 -0.25723975 1.38975 -0.187055 1.853075 0 0 0.303765 3.0313 2.38979 4.576325 1.746675 1.295425 4.32225 1.7737 6.242025 1.24905 1.51405 -0.287875 2.95825 -1.707 4.1587 -2.948725 0.245175 -0.24835 0.540675 -0.539875 0.725675 -0.760125l-0.000425 0.00105Z" stroke-width="0.25"></path><path fill="#a3b34f" d="M11.911025 15.653825c-1.411475 0.828725 -3.48225 1.953125 -6.0737 1.92455 -0.083475 -0.000975 -0.167525 -0.003025 -0.252175 -0.00645 -0.86905 -0.03505 -3.1773725 -0.128025 -4.3651175 -1.67205 -0.327585 -0.4265 -0.52115 -0.89795 -0.62611 -1.2184l-0.0028425 -0.006975 0.00016 -0.0001c-0.063515 -0.1953 -0.0869525 -0.3101 -0.087685 -0.314225 -0.017975 -0.0833 -0.0327725 -0.164675 -0.04546 -0.249l-0.00162 -0.0057c-0.00016 -0.00095 -0.0003175 -0.001775 -0.0004775 -0.002675l0.00016 -0.000075 -0.0004775 -0.001275c-0.06669 -0.442875 -0.0895575 -1.106925 0.17737 -1.762575 0.4369875 -1.071975 1.4256125 -1.55025 2.5708 -2.10395 0.959575 -0.46415 1.7991 -0.668675 3.321875 -1.039925 0.195175 -0.04725 0.38285 -0.09205 0.563875 -0.133625 0.204675 -0.046375 0.40125 -0.0903 0.5899 -0.130225 0.369675 -0.07835 0.722975 -0.147625 1.080575 -0.211825l0.001775 -0.001125c0.656125 -0.1168 1.231725 -0.2026 1.76575 -0.280575 1.376225 -0.201825 2.169075 -0.26375 2.73435 -0.212775 0.310125 0.027325 0.568475 0.087375 0.863975 0.20055 0.40365 0.154525 1.154875 0.442225 1.517875 1.165525 0.109225 0.218 0.173875 0.459525 0.19245 0.719 0.0022 0.025125 0.00365 0.051075 0.0044 0.07695 0.0222 0.651825 -0.24645 1.29255 -0.390775 1.6371 -0.81095 1.9309 -2.318325 2.912225 -3.539425 3.62995" stroke-width="0.25"></path><path fill="#dde072" d="M11.021675 15.749225c1.343825 -0.691675 2.434225 -1.485625 2.99635 -2.10395 0.933675 -1.027525 1.3143 -2.0722 1.396725 -2.921725 0.0924 -0.95195 -0.506225 -1.3078 -0.709325 -1.480875 -0.755825 -0.645175 -1.7832 -0.63485 -2.7169 -0.638275 -0.424275 -0.00135 -1.033075 0.031075 -2.8995 0.437725 -1.48435 0.338375 -1.787975 0.4119 -3.5823 0.902225 -1.19615 0.333 -1.870525 0.5572 -2.421525 0.9032 -0.7702975 0.483675 -1.72923 1.0858 -1.9721775 2.246875 -0.30186 1.44055 0.76362 2.640675 0.9802075 2.86775 1.04277 1.087875 2.480295 1.2184 3.471145 1.22185 0.3892 0.0013 0.75775 -0.01925 1.082625 -0.0373 2.01345 -0.11125 3.3997 -0.896525 4.374675 -1.3967" stroke-width="0.25"></path><path fill="#5e3108" d="M5.8039 9.95325c-0.15415 0.11655 -0.4054 0.36365 -0.534325 0.49575l-0.35855 0.40095c-0.635625 0.7984 -1.08915 2.337375 -0.744875 2.751825 0.176575 0.212925 0.2693 0.236125 0.3325 0.278525 0.88255 0.426825 1.687925 1.01275 3.0043 1.089775l1.09485 -0.001275c0.08865 -0.00365 0.218825 -0.00895 0.31235 -0.01595 0.24405 -0.02195 0.13755 -0.024 0.305975 -0.041975 1.51455 -0.24865 3.5315 -1.320175 3.453675 -2.8201 -0.029775 -0.560525 -0.669925 -1.47865 -1.104525 -1.892775 -0.462075 -0.4408 -1.32575 -0.8689 -1.552025 -0.949725 -0.1059 -0.0327 -0.23245 -0.075875 -0.44365 -0.112975 -0.11135 -0.01985 -0.221025 -0.037325 -0.330125 -0.049925 -0.051975 -0.006675 -0.10405 -0.011725 -0.1561 -0.016775 -0.052525 -0.004875 -0.104425 -0.0077 -0.1564 -0.01095 -1.099625 -0.068425 -2.0865 0.218175 -2.8471 0.7028 -0.063125 0.04025 -0.12525 0.081425 -0.18515 0.12435 -0.031 0.0227 -0.061175 0.0445 -0.091325 0.068" stroke-width="0.25"></path></svg>`

type Plugin struct{}

type row = plugin.TableRow

type actionResult struct {
	OK bool `json:"ok"`
}

// Session holds one ArangoDB connection. The HTTP transport is built on the
// gateway's dialer so every byte leaves through the audited egress path.
type Session struct {
	client    driver.Client
	transport *http.Transport
	opts      Options

	mu      sync.Mutex
	seq     uint64
	running map[string]context.CancelFunc
}

func New() *Plugin { return &Plugin{} }

func (p *Plugin) Manifest() plugin.Manifest {
	return plugin.Manifest{
		APIVersion:  plugin.CurrentAPIVersion,
		Name:        protocolName,
		Version:     "0.1.0",
		Title:       "ArangoDB",
		Description: "Multi-model ArangoDB cockpit: databases, collections, document CRUD, AQL console, named-graph explorer, ArangoSearch views, analyzers, Foxx services, and cluster/shard health.",
		Icon:        plugin.Icon{Type: plugin.IconSVG, Value: arangoIconSVG},
		Category:    plugin.CategoryDatabases,
		Config:      configSchema(),
		Capabilities: []plugin.Capability{
			"documents", "graph", "aql", "arangosearch", "foxx", "cluster", "metrics",
		},
		SupportedTransports: []plugin.Transport{plugin.TransportDirect},
		Layout:              plugin.LayoutSidebarTree,
		Tree:                tree(),
		Resources:           resources(),
		Actions:             actions(),
		Streams:             streams(),
	}
}

func (p *Plugin) Routes() []plugin.Route { return routes() }

func (p *Plugin) Connect(ctx context.Context, cfg plugin.ConnectConfig) (plugin.Session, error) {
	if cfg.Transport != "" && cfg.Transport != plugin.TransportDirect {
		return nil, fmt.Errorf("%w: ArangoDB supports only direct transport", plugin.ErrNotSupported)
	}
	opts, err := parseOptions(cfg)
	if err != nil {
		return nil, err
	}
	if cfg.Net == nil {
		return nil, fmt.Errorf("%w: no network transport", plugin.ErrUnavailable)
	}
	transport := &http.Transport{
		DialContext:           cfg.Net.DialContext,
		TLSClientConfig:       opts.TLSConfig,
		MaxIdleConns:          32,
		MaxIdleConnsPerHost:   8,
		IdleConnTimeout:       90 * time.Second,
		TLSHandshakeTimeout:   opts.Timeout,
		ResponseHeaderTimeout: opts.Timeout,
	}
	auth, err := authentication(opts)
	if err != nil {
		transport.CloseIdleConnections()
		return nil, err
	}
	conn := connection.NewHttpConnection(connection.HttpConfiguration{
		Endpoint:       connection.NewRoundRobinEndpoints([]string{opts.Endpoint}),
		Transport:      transport,
		Authentication: auth,
		ContentType:    connection.ApplicationJSON,
	})
	s := &Session{client: driver.NewClient(conn), transport: transport, opts: opts, running: map[string]context.CancelFunc{}}
	if err := s.HealthCheck(ctx); err != nil {
		_ = s.Close()
		return nil, err
	}
	return s, nil
}

func (s *Session) HealthCheck(ctx context.Context) error {
	ctx, cancel := context.WithTimeout(ctx, s.opts.Timeout)
	defer cancel()
	if _, err := s.client.Version(ctx); err != nil {
		return arangoErr(err)
	}
	return nil
}

func (s *Session) OpenChannel(context.Context, plugin.ChannelRequest) (plugin.Channel, error) {
	return nil, plugin.ErrNotSupported
}

func (s *Session) Close() error {
	s.cancelAll()
	s.transport.CloseIdleConnections()
	return nil
}

// database opens a handle without an existence round-trip; the first real call
// surfaces a 404 that maps to ErrNotFound.
func (s *Session) database(ctx context.Context, name string) (driver.Database, error) {
	name, err := safeDatabaseName(name)
	if err != nil {
		return nil, err
	}
	db, err := s.client.GetDatabase(ctx, name, &driver.GetDatabaseOptions{SkipExistCheck: true})
	if err != nil {
		return nil, arangoErr(err)
	}
	return db, nil
}

func (s *Session) collection(ctx context.Context, database, name string) (driver.Collection, error) {
	db, err := s.database(ctx, database)
	if err != nil {
		return nil, err
	}
	if _, err := safeName(name, "collection"); err != nil {
		return nil, err
	}
	col, err := db.GetCollection(ctx, name, &driver.GetCollectionOptions{SkipExistCheck: true})
	if err != nil {
		return nil, arangoErr(err)
	}
	return col, nil
}

// trackQuery registers a cancel func so the query editor's Cancel button can
// stop an in-flight AQL execution for that database.
func (s *Session) trackQuery(database string, cancel context.CancelFunc) func() {
	s.mu.Lock()
	s.seq++
	key := database + "#" + strconv.FormatUint(s.seq, 10)
	s.running[key] = cancel
	s.mu.Unlock()
	return func() {
		s.mu.Lock()
		delete(s.running, key)
		s.mu.Unlock()
	}
}

func (s *Session) cancelDatabase(database string) int {
	s.mu.Lock()
	defer s.mu.Unlock()
	n := 0
	for key, cancel := range s.running {
		if database != "" && !hasPrefixBefore(key, database) {
			continue
		}
		cancel()
		delete(s.running, key)
		n++
	}
	return n
}

func (s *Session) cancelAll() {
	s.mu.Lock()
	defer s.mu.Unlock()
	for key, cancel := range s.running {
		cancel()
		delete(s.running, key)
	}
}

func authentication(opts Options) (connection.Authentication, error) {
	switch opts.Auth {
	case authNone:
		return nil, nil
	case authPassword, authCredential:
		if opts.Username == "" {
			return nil, fmt.Errorf("%w: username is required", plugin.ErrInvalidInput)
		}
		return connection.NewBasicAuth(opts.Username, opts.Password), nil
	case authJWT, authStoredJWT:
		if opts.Token == "" {
			return nil, fmt.Errorf("%w: JWT token is required", plugin.ErrInvalidInput)
		}
		return connection.NewHeaderAuth("Authorization", "bearer "+opts.Token), nil
	default:
		return nil, fmt.Errorf("%w: unsupported authentication method %q", plugin.ErrInvalidInput, opts.Auth)
	}
}

func unwrap(sess plugin.Session) (*Session, error) {
	if s, ok := sess.(*Session); ok {
		return s, nil
	}
	// rc.Session is the core's borrowed handle, which exposes the live session.
	if h, ok := sess.(interface{ Session() plugin.Session }); ok {
		if s, ok := h.Session().(*Session); ok {
			return s, nil
		}
	}
	return nil, fmt.Errorf("%w: session is not an ArangoDB session", plugin.ErrInvalidInput)
}

func session(rc *plugin.RequestContext) (*Session, error) { return unwrap(rc.Session) }

func icon(name string) plugin.Icon {
	return plugin.Icon{Type: plugin.IconLucide, Value: name}
}

func rid(name string) string { return protocolName + "." + name }

func hasPrefixBefore(key, database string) bool {
	for i := 0; i < len(key); i++ {
		if key[i] == '#' {
			return key[:i] == database
		}
	}
	return false
}
