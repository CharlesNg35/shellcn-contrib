// Package etcd implements the etcd protocol plugin.
package etcd

import (
	"context"

	"github.com/charlesng35/shellcn/sdk/plugin"
)

type Plugin struct{}

func New() *Plugin { return &Plugin{} }

const etcdIconSVG = `<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 -4 256 256"><path fill="#419EDA" d="M252.386416,128.063547 C251.184178,128.164306 249.976215,128.21125 248.692682,128.21125 C241.246821,128.21125 234.023088,126.465143 227.505812,123.267189 C229.675566,110.820018 230.598427,98.2801018 230.356834,85.7859855 C223.291109,75.5658167 215.215504,65.9227222 206.100249,57.0387552 C210.05504,49.6238086 215.901352,43.2439318 223.19951,38.7200816 L226.333344,36.7827608 L223.891083,34.029063 C211.309948,19.8621183 196.294566,8.90915678 179.270875,1.47703537 L175.875983,0 L175.013807,3.58839447 C172.983742,11.9513917 168.740414,19.4957219 162.914712,25.550422 C151.717868,19.5987709 140.020663,14.7886736 127.958208,11.1453196 C115.924377,14.7806586 104.247783,19.5770161 93.0555185,25.5195073 C87.253861,19.4728222 83.0208379,11.9468117 80.9987879,3.60785927 L80.1308865,0.0206097959 L76.74859,1.49077524 C59.9390115,8.81526771 44.5102892,20.0647813 32.1352518,34.0210481 L29.686121,36.7804708 L32.81652,38.7177916 C40.091778,43.224467 45.9220603,49.5665592 49.8699812,56.9414311 C40.7822062,65.7910485 32.715761,75.4032283 25.6557609,85.5764526 C25.3809637,98.0648439 26.25688,110.696359 28.4369384,123.315279 C21.9517226,126.483463 14.7680638,128.210105 7.37143701,128.210105 C6.07301986,128.210105 4.85818689,128.163161 3.67770358,128.064692 L0,127.78417 L0.344641587,131.456148 C2.14685374,150.033589 7.91530662,167.703054 17.4988617,183.979068 L19.3697732,187.155267 L22.1784304,184.7714 C28.6876909,179.250265 36.552618,175.594316 44.9156152,174.120716 C50.4287356,185.393129 56.9631859,195.984274 64.3758425,205.817437 C76.2035754,209.954281 88.5270884,213.042315 101.253637,214.880022 C102.474195,223.296834 101.5021,232.002183 98.1816328,240.051453 L96.7813116,243.462374 L100.382301,244.254706 C109.602895,246.282481 118.904783,247.315261 128.013167,247.315261 L155.636019,244.254706 L159.240443,243.462374 L157.836687,240.044583 C154.52538,231.995313 153.553284,223.279659 154.773842,214.861702 C167.450012,213.022851 179.727725,209.941686 191.511949,205.817437 C198.931475,195.976259 205.47165,185.378244 210.993931,174.090946 C219.383263,175.555387 227.292844,179.213625 233.842179,184.750791 L236.650837,187.131222 L238.512588,183.963038 C248.113318,167.666415 253.880626,149.998095 255.655358,131.450423 L256,127.785315 L252.386416,128.063547 L252.386416,128.063547 Z M167.490086,172.959697 C154.422331,176.513742 141.150767,178.307939 127.958208,178.307939 C114.730154,178.307939 101.47462,176.514887 88.3954147,172.959697 C81.2197707,161.809798 75.5463519,149.865276 71.4633223,137.289866 C67.3974676,124.772849 65.0181812,111.659294 64.327753,98.156443 C72.7743344,87.7130014 82.3796442,78.564542 92.9925442,70.8633483 C103.777192,63.019031 115.509891,56.6460241 127.958208,51.8519565 C140.385915,56.6471691 152.096859,63.011016 162.856317,70.8221287 C173.510437,78.564542 183.158111,87.7839907 191.645912,98.2926967 C190.922279,111.718834 188.514368,124.75682 184.441644,137.253226 C180.368919,149.826346 174.67718,161.808653 167.490086,172.959697 L167.490086,172.959697 Z M138.750871,109.962421 C138.750871,119.194465 146.232227,126.662081 155.451676,126.662081 C164.668834,126.662081 172.142175,119.19561 172.142175,109.962421 C172.142175,100.765872 164.668834,93.2696314 155.451676,93.2696314 C146.232227,93.2696314 138.750871,100.765872 138.750871,109.962421 L138.750871,109.962421 Z M117.172415,109.962421 C117.172415,119.194465 109.692204,126.662081 100.472755,126.662081 C91.2464364,126.662081 83.7868353,119.19561 83.7868353,109.962421 C83.7868353,100.769307 91.2475814,93.2730664 100.472755,93.2730664 C109.692204,93.2730664 117.172415,100.769307 117.172415,109.962421 L117.172415,109.962421 Z"/></svg>`

func (p *Plugin) Manifest() plugin.Manifest {
	return plugin.Manifest{
		APIVersion:          plugin.CurrentAPIVersion,
		Name:                protocolName,
		Version:             "0.1.0",
		Title:               "etcd",
		Description:         "etcd v3 browser with a namespaced key tree, typed value editing, leases, cluster members, RBAC users and roles, a live watch stream, and maintenance.",
		Icon:                plugin.Icon{Type: plugin.IconSVG, Value: etcdIconSVG},
		Category:            plugin.CategoryDatabases,
		Config:              configSchema(),
		Capabilities:        []plugin.Capability{"kv", "keys", "leases", "auth", "watch", "cluster"},
		SupportedTransports: []plugin.Transport{plugin.TransportDirect, plugin.TransportAgent},
		Agent: &plugin.AgentProfile{
			Proxy: plugin.ProxyTarget{Mode: plugin.AgentTCP, Risk: plugin.RiskPrivileged, Forward: true},
			Install: []plugin.InstallArtifact{{
				Label:    "Docker",
				Kind:     "docker",
				Template: "docker run -d --network host shellcn/agent --connect {{.ConnectURL}} --token {{.Token}}",
			}},
		},
		Layout: plugin.LayoutTabs,
		Scope:  []plugin.ScopeFilter{},
		Tabs: []plugin.Panel{
			{Key: "status", Label: "Status", Icon: icon("gauge"), Type: plugin.PanelObjectDetail, Source: &plugin.DataSource{RouteID: "etcd.status"}, Config: plugin.ObjectDetailConfig{RawToggle: true}},
			{Key: "keys", Label: "Keys", Icon: icon("key-round"), Type: plugin.PanelKV, Source: &plugin.DataSource{RouteID: "etcd.keys.list"}, Config: plugin.KVConfig{
				CreateRouteID: "etcd.key.write", ReadRouteID: "etcd.key.read", WriteRouteID: "etcd.key.write", DeleteRouteID: "etcd.key.delete",
				KeyParam: "key", Writable: true, Delimiter: "/",
			}},
			{Key: "leases", Label: "Leases", Icon: icon("clock"), Type: plugin.PanelTable, Source: &plugin.DataSource{RouteID: "etcd.leases.list"}, Config: plugin.TableConfig{
				Columns: leaseColumns(), ActionIDs: []string{"etcd.lease.grant"}, RowActionIDs: []string{"etcd.lease.revoke"}, Exportable: true,
			}},
			{Key: "members", Label: "Cluster", Icon: icon("server"), Type: plugin.PanelTable, Source: &plugin.DataSource{RouteID: "etcd.members.list"}, Config: plugin.TableConfig{
				Columns: memberColumns(), ActionIDs: []string{"etcd.maintenance.compact", "etcd.maintenance.defragment"}, Exportable: true,
			}},
			{Key: "users", Label: "Users", Icon: icon("users"), Type: plugin.PanelTable, Source: &plugin.DataSource{RouteID: "etcd.users.list"}, Config: plugin.TableConfig{
				Columns: userColumns(), ActionIDs: []string{"etcd.user.create"}, RowActionIDs: []string{"etcd.user.grant_role", "etcd.user.revoke_role", "etcd.user.delete"},
			}},
			{Key: "roles", Label: "Roles", Icon: icon("shield"), Type: plugin.PanelTable, Source: &plugin.DataSource{RouteID: "etcd.roles.list"}, Config: plugin.TableConfig{
				Columns: roleColumns(), ActionIDs: []string{"etcd.role.create"}, RowActionIDs: []string{"etcd.role.delete"},
			}},
			{Key: "watch", Label: "Watch", Icon: icon("activity"), Type: plugin.PanelLogStream, Source: &plugin.DataSource{RouteID: "etcd.watch", Method: plugin.MethodWS}},
		},
		Actions: actions(),
		Streams: []plugin.Stream{
			{ID: "etcd.watch", Kind: plugin.StreamLogs, RouteID: "etcd.watch"},
		},
	}
}

func (p *Plugin) Routes() []plugin.Route { return routes() }

func (p *Plugin) Connect(ctx context.Context, cfg plugin.ConnectConfig) (plugin.Session, error) {
	return connect(ctx, cfg)
}

func icon(name string) plugin.Icon {
	return plugin.Icon{Type: plugin.IconLucide, Value: name}
}

func actions() []plugin.Action {
	return []plugin.Action{
		{ID: "etcd.lease.grant", Label: "Grant lease", Icon: icon("plus"), RouteID: "etcd.lease.grant"},
		{ID: "etcd.lease.revoke", Label: "Revoke", Icon: icon("trash-2"), RouteID: "etcd.lease.revoke", Params: map[string]string{"id": "${record.id}"}, Confirm: true, ConfirmText: "Revoke this lease? Keys bound to it are deleted.", Bulk: true},
		{ID: "etcd.user.create", Label: "Add user", Icon: icon("user-plus"), RouteID: "etcd.user.create"},
		{ID: "etcd.user.grant_role", Label: "Grant role", Icon: icon("shield-plus"), RouteID: "etcd.user.grant_role", Params: map[string]string{"username": "${record.username}"}},
		{ID: "etcd.user.revoke_role", Label: "Revoke role", Icon: icon("shield-minus"), RouteID: "etcd.user.revoke_role", Params: map[string]string{"username": "${record.username}"}, Confirm: true, ConfirmText: "Revoke a role from this user?"},
		{ID: "etcd.user.delete", Label: "Delete", Icon: icon("trash-2"), RouteID: "etcd.user.delete", Params: map[string]string{"username": "${record.username}"}, Confirm: true, ConfirmText: "Delete this user?", Bulk: true},
		{ID: "etcd.role.create", Label: "Add role", Icon: icon("plus"), RouteID: "etcd.role.create"},
		{ID: "etcd.role.delete", Label: "Delete", Icon: icon("trash-2"), RouteID: "etcd.role.delete", Params: map[string]string{"name": "${record.name}"}, Confirm: true, ConfirmText: "Delete this role?", Bulk: true},
		{ID: "etcd.maintenance.compact", Label: "Compact", Icon: icon("archive"), RouteID: "etcd.maintenance.compact", Confirm: true, ConfirmText: "Compact history up to a revision? Older revisions are permanently discarded."},
		{ID: "etcd.maintenance.defragment", Label: "Defragment", Icon: icon("hard-drive"), RouteID: "etcd.maintenance.defragment", Confirm: true, ConfirmText: "Defragment the backend database on the connected endpoint?"},
	}
}

func leaseColumns() []plugin.Column {
	return []plugin.Column{
		{Key: "id", Label: "Lease ID", Sortable: true},
		{Key: "ttl", Label: "TTL (s)", Type: plugin.ColumnNumber, Sortable: true},
		{Key: "granted_ttl", Label: "Granted TTL (s)", Type: plugin.ColumnNumber, Sortable: true},
		{Key: "keys", Label: "Keys", Type: plugin.ColumnNumber, Sortable: true},
	}
}

func memberColumns() []plugin.Column {
	return []plugin.Column{
		{Key: "name", Label: "Name", Sortable: true},
		{Key: "id", Label: "Member ID"},
		{Key: "client_urls", Label: "Client URLs"},
		{Key: "peer_urls", Label: "Peer URLs"},
		{Key: "is_learner", Label: "Learner", Type: plugin.ColumnBool},
	}
}

func userColumns() []plugin.Column {
	return []plugin.Column{
		{Key: "username", Label: "User", Sortable: true},
		{Key: "roles", Label: "Roles"},
	}
}

func roleColumns() []plugin.Column {
	return []plugin.Column{
		{Key: "name", Label: "Role", Sortable: true},
		{Key: "permissions", Label: "Permissions", Type: plugin.ColumnNumber},
	}
}

func grantLeaseSchema() *plugin.Schema {
	return &plugin.Schema{Groups: []plugin.Group{{Fields: []plugin.Field{
		{Key: "ttl", Label: "TTL (seconds)", Type: plugin.FieldNumber, Required: true, Default: 60, Validators: []plugin.Validator{{Type: plugin.ValidatorMin, Value: 1}}},
	}}}}
}

func userCreateSchema() *plugin.Schema {
	return &plugin.Schema{Groups: []plugin.Group{{Fields: []plugin.Field{
		{Key: "username", Label: "Username", Type: plugin.FieldText, Required: true},
		{Key: "password", Label: "Password", Type: plugin.FieldPassword, Secret: true},
	}}}}
}

func roleRefSchema() *plugin.Schema {
	return &plugin.Schema{Groups: []plugin.Group{{Fields: []plugin.Field{
		{Key: "role", Label: "Role", Type: plugin.FieldText, Required: true},
	}}}}
}

func roleCreateSchema() *plugin.Schema {
	return &plugin.Schema{Groups: []plugin.Group{{Fields: []plugin.Field{
		{Key: "name", Label: "Role name", Type: plugin.FieldText, Required: true},
	}}}}
}

func compactSchema() *plugin.Schema {
	return &plugin.Schema{Groups: []plugin.Group{{Fields: []plugin.Field{
		{Key: "revision", Label: "Revision", Type: plugin.FieldNumber, Required: true, Help: "Compact history older than this revision."},
		{Key: "physical", Label: "Wait for physical reclaim", Type: plugin.FieldToggle, Default: false},
	}}}}
}
