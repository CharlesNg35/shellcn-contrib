package vault

import "github.com/charlesng35/shellcn/sdk/plugin"

func scope() []plugin.ScopeFilter {
	return []plugin.ScopeFilter{{
		Param:         "namespace",
		Label:         "Namespace",
		Icon:          icon("layers"),
		Control:       plugin.ScopeAutoComplete,
		AllowCustom:   true,
		OptionsSource: &plugin.DataSource{RouteID: "vault.namespaces.list"},
		ValueField:    "path",
		LabelField:    "path",
		AllLabel:      "Connection namespace",
	}}
}

func tree() []plugin.TreeGroup {
	return []plugin.TreeGroup{
		{Key: "server", Label: "Server", Icon: icon("server"), Ref: &plugin.ResourceIdentity{Kind: "system", Name: "Vault", UID: "server"}},
		{Key: "engines", Label: "Secret engines", Icon: icon("database"), Source: plugin.DataSource{RouteID: "vault.tree.mounts"}, ResourceKind: "mount"},
		{Key: "auth", Label: "Auth methods", Icon: icon("key-round"), Source: plugin.DataSource{RouteID: "vault.tree.auth"}, ResourceKind: "auth"},
		{Key: "policies", Label: "Policies", Icon: icon("scroll-text"), ResourceKind: "policy"},
		{Key: "tokens", Label: "Tokens", Icon: icon("ticket"), ResourceKind: "token"},
		{Key: "leases", Label: "Leases", Icon: icon("timer"), ResourceKind: "lease"},
		{Key: "namespaces", Label: "Namespaces", Icon: icon("layers"), ResourceKind: "namespace"},
		{Key: "audit", Label: "Audit devices", Icon: icon("file-clock"), ResourceKind: "audit"},
	}
}

func resources() []plugin.ResourceType {
	return []plugin.ResourceType{
		systemResource(),
		mountResource(),
		authResource(),
		secretResource(),
		policyResource(),
		tokenResource(),
		leaseResource(),
		namespaceResource(),
		auditResource(),
	}
}

func objectDetail(sections ...plugin.ObjectDetailSection) plugin.ObjectDetailConfig {
	return plugin.ObjectDetailConfig{Sections: sections, RawToggle: true}
}

func systemResource() plugin.ResourceType {
	return plugin.ResourceType{
		Kind: "system", Title: "Server",
		List: plugin.DataSource{RouteID: "vault.system.list"},
		Columns: []plugin.Column{
			{Key: "name", Label: "Server", Sortable: true},
			{Key: "version", Label: "Version"},
			{Key: "cluster_name", Label: "Cluster"},
			{Key: "state", Label: "State", Type: plugin.ColumnBadge, Severities: sealSeverities()},
		},
		Detail: plugin.DetailView{
			Header: plugin.HeaderSpec{Title: "Vault", StatusField: "state", Severities: sealSeverities()},
			Tabs: []plugin.Panel{
				{Key: "health", Label: "Health", Icon: icon("activity"), Type: plugin.PanelObjectDetail,
					Source: &plugin.DataSource{RouteID: "vault.system.health"},
					Config: objectDetail(
						plugin.ObjectDetailSection{Title: "Status", Fields: []plugin.ObjectDetailField{
							{Key: "state", Label: "State", Type: plugin.ColumnBadge, Severities: sealSeverities()},
							{Key: "version", Label: "Version", Copy: true},
							{Key: "enterprise", Label: "Enterprise", Type: plugin.ColumnBool},
							{Key: "initialized", Label: "Initialized", Type: plugin.ColumnBool},
							{Key: "sealed", Label: "Sealed", Type: plugin.ColumnBool},
							{Key: "standby", Label: "Standby", Type: plugin.ColumnBool},
							{Key: "performance_standby", Label: "Performance standby", Type: plugin.ColumnBool},
						}},
						plugin.ObjectDetailSection{Title: "Cluster", Fields: []plugin.ObjectDetailField{
							{Key: "cluster_name", Label: "Cluster name", Copy: true},
							{Key: "cluster_id", Label: "Cluster ID", Copy: true},
							{Key: "replication_performance_mode", Label: "Replication (performance)"},
							{Key: "replication_dr_mode", Label: "Replication (DR)"},
							{Key: "server_time_utc", Label: "Server time", Type: plugin.ColumnDateTime},
							{Key: "namespace", Label: "Namespace", Copy: true},
						}},
					)},
				{Key: "seal", Label: "Seal", Icon: icon("lock"), Type: plugin.PanelObjectDetail,
					Source: &plugin.DataSource{RouteID: "vault.system.seal"},
					Config: objectDetail(plugin.ObjectDetailSection{Title: "Seal", Fields: []plugin.ObjectDetailField{
						{Key: "state", Label: "State", Type: plugin.ColumnBadge, Severities: sealSeverities()},
						{Key: "type", Label: "Seal type"},
						{Key: "sealed", Label: "Sealed", Type: plugin.ColumnBool},
						{Key: "initialized", Label: "Initialized", Type: plugin.ColumnBool},
						{Key: "recovery_seal", Label: "Recovery seal", Type: plugin.ColumnBool},
						{Key: "threshold", Label: "Unseal threshold", Type: plugin.ColumnNumber},
						{Key: "shares", Label: "Key shares", Type: plugin.ColumnNumber},
						{Key: "progress", Label: "Unseal progress", Type: plugin.ColumnNumber},
						{Key: "storage_type", Label: "Storage"},
						{Key: "migration", Label: "Seal migration", Type: plugin.ColumnBool},
						{Key: "warnings", Label: "Warnings", Type: plugin.ColumnJSON},
					}})},
			},
		},
	}
}

func mountResource() plugin.ResourceType {
	kvMount := plugin.Condition{AllOf: []plugin.Rule{{Field: "kv", Op: plugin.OpEq, Value: true}}}
	return plugin.ResourceType{
		Kind: "mount", Title: "Secret engines",
		List:    plugin.DataSource{RouteID: "vault.mounts.list"},
		Columns: mountColumns(),
		Actions: plugin.ResourceActions{
			Toolbar: []string{"vault.secret.put"},
			Detail:  []string{"vault.secret.put"},
		},
		Detail: plugin.DetailView{
			Header:     plugin.HeaderSpec{Title: "${resource.name}"},
			DefaultTab: "overview",
			Tabs: []plugin.Panel{
				{Key: "overview", Label: "Overview", Icon: icon("info"), Type: plugin.PanelObjectDetail,
					Source: &plugin.DataSource{RouteID: "vault.mount.read", Params: map[string]string{"mount": "${resource.uid}"}},
					Config: objectDetail(plugin.ObjectDetailSection{Title: "Mount", Fields: []plugin.ObjectDetailField{
						{Key: "path", Label: "Path", Copy: true},
						{Key: "type", Label: "Type", Type: plugin.ColumnBadge},
						{Key: "kv_version", Label: "KV version", Type: plugin.ColumnNumber},
						{Key: "description", Label: "Description"},
						{Key: "accessor", Label: "Accessor", Copy: true},
						{Key: "uuid", Label: "UUID", Copy: true},
						{Key: "local", Label: "Local", Type: plugin.ColumnBool},
						{Key: "seal_wrap", Label: "Seal wrap", Type: plugin.ColumnBool},
						{Key: "external_entropy_access", Label: "External entropy", Type: plugin.ColumnBool},
						{Key: "running_version", Label: "Running version"},
						{Key: "plugin_version", Label: "Plugin version"},
						{Key: "deprecation_status", Label: "Deprecation status", Type: plugin.ColumnBadge, Severities: deprecationSeverities()},
						{Key: "options", Label: "Options", Type: plugin.ColumnJSON},
					}})},
				{Key: "secrets", Label: "Secrets", Icon: icon("key-round"), Type: plugin.PanelKV, VisibleWhen: &kvMount,
					Source: &plugin.DataSource{RouteID: "vault.secrets.list", Params: map[string]string{"mount": "${resource.uid}"}},
					Config: plugin.KVConfig{
						CreateRouteID: "vault.secret.write",
						ReadRouteID:   "vault.secret.read",
						WriteRouteID:  "vault.secret.write",
						DeleteRouteID: "vault.secret.delete",
						KeyParam:      "key",
						Writable:      true,
						ValueTypes:    []string{"json", "string"},
						Delimiter:     "/",
					}},
				{Key: "tune", Label: "Configuration", Icon: icon("sliders-horizontal"), Type: plugin.PanelObjectDetail,
					Source: &plugin.DataSource{RouteID: "vault.mount.config", Params: map[string]string{"mount": "${resource.uid}"}},
					Config: objectDetail(plugin.ObjectDetailSection{Title: "Tune", Fields: []plugin.ObjectDetailField{
						{Key: "default_lease_ttl", Label: "Default lease TTL", Type: plugin.ColumnDuration},
						{Key: "max_lease_ttl", Label: "Max lease TTL", Type: plugin.ColumnDuration},
						{Key: "force_no_cache", Label: "Disable cache", Type: plugin.ColumnBool},
						{Key: "listing_visibility", Label: "Listing visibility"},
						{Key: "token_type", Label: "Token type"},
						{Key: "audit_non_hmac_request_keys", Label: "Audit non-HMAC request keys", Type: plugin.ColumnJSON},
						{Key: "audit_non_hmac_response_keys", Label: "Audit non-HMAC response keys", Type: plugin.ColumnJSON},
						{Key: "passthrough_request_headers", Label: "Passthrough request headers", Type: plugin.ColumnJSON},
						{Key: "allowed_response_headers", Label: "Allowed response headers", Type: plugin.ColumnJSON},
						{Key: "allowed_managed_keys", Label: "Allowed managed keys", Type: plugin.ColumnJSON},
					}})},
			},
		},
	}
}

func authResource() plugin.ResourceType {
	return plugin.ResourceType{
		Kind: "auth", Title: "Auth methods",
		List:    plugin.DataSource{RouteID: "vault.auth.list"},
		Columns: authColumns(),
		Detail: plugin.DetailView{
			Header: plugin.HeaderSpec{Title: "${resource.name}"},
			Tabs: []plugin.Panel{
				{Key: "overview", Label: "Overview", Icon: icon("info"), Type: plugin.PanelObjectDetail,
					Source: &plugin.DataSource{RouteID: "vault.auth.read", Params: map[string]string{"mount": "${resource.uid}"}},
					Config: objectDetail(
						plugin.ObjectDetailSection{Title: "Auth mount", Fields: []plugin.ObjectDetailField{
							{Key: "path", Label: "Path", Copy: true},
							{Key: "type", Label: "Type", Type: plugin.ColumnBadge},
							{Key: "description", Label: "Description"},
							{Key: "accessor", Label: "Accessor", Copy: true},
							{Key: "uuid", Label: "UUID", Copy: true},
							{Key: "local", Label: "Local", Type: plugin.ColumnBool},
							{Key: "running_version", Label: "Running version"},
							{Key: "deprecation_status", Label: "Deprecation status", Type: plugin.ColumnBadge, Severities: deprecationSeverities()},
						}},
						plugin.ObjectDetailSection{Title: "Tune", Fields: []plugin.ObjectDetailField{
							{Key: "default_lease_ttl", Label: "Default lease TTL", Type: plugin.ColumnDuration},
							{Key: "max_lease_ttl", Label: "Max lease TTL", Type: plugin.ColumnDuration},
							{Key: "token_type", Label: "Token type"},
							{Key: "listing_visibility", Label: "Listing visibility"},
						}},
					)},
			},
		},
	}
}

func secretResource() plugin.ResourceType {
	return plugin.ResourceType{
		Kind: "secret", Title: "Secrets",
		List:    plugin.DataSource{RouteID: "vault.secrets.list"},
		Columns: secretColumns(),
		Actions: plugin.ResourceActions{
			Toolbar: []string{"vault.secret.put"},
			Row:     []string{"vault.secret.delete"},
			Detail:  []string{"vault.secret.put", "vault.secret.delete", "vault.secret.versions.delete", "vault.secret.undelete", "vault.secret.destroy", "vault.secret.purge"},
		},
		Detail: plugin.DetailView{
			Header:     plugin.HeaderSpec{Title: "${resource.uid}"},
			DefaultTab: "data",
			Tabs: []plugin.Panel{
				{Key: "data", Label: "Data", Icon: icon("braces"), Type: plugin.PanelObjectDetail,
					Source: &plugin.DataSource{RouteID: "vault.secret.read", Params: map[string]string{"key": "${resource.uid}"}},
					Config: objectDetail(plugin.ObjectDetailSection{Title: "Secret", Fields: []plugin.ObjectDetailField{
						{Key: "key", Label: "Key", Copy: true},
						{Key: "mount", Label: "Engine", Copy: true},
						{Key: "kv_version", Label: "KV version", Type: plugin.ColumnNumber},
						{Key: "version", Label: "Version", Type: plugin.ColumnNumber},
						{Key: "created_time", Label: "Created", Type: plugin.ColumnDateTime},
						{Key: "deletion_time", Label: "Deleted", Type: plugin.ColumnDateTime},
						{Key: "destroyed", Label: "Destroyed", Type: plugin.ColumnBool},
						{Key: "value", Label: "Data", Type: plugin.ColumnJSON},
						{Key: "custom_metadata", Label: "Custom metadata", Type: plugin.ColumnJSON},
					}})},
				{Key: "versions", Label: "Versions", Icon: icon("history"), Type: plugin.PanelTable,
					Source: &plugin.DataSource{RouteID: "vault.secret.versions", Params: map[string]string{"key": "${resource.uid}"}},
					Config: plugin.TableConfig{
						Columns:     versionColumns(),
						DefaultSort: &plugin.SortKey{Field: "version", Desc: true},
						EmptyText:   "This engine does not keep secret versions.",
						Exportable:  false,
					}},
				{Key: "metadata", Label: "Metadata", Icon: icon("tags"), Type: plugin.PanelObjectDetail,
					Source: &plugin.DataSource{RouteID: "vault.secret.metadata", Params: map[string]string{"key": "${resource.uid}"}},
					Config: objectDetail(plugin.ObjectDetailSection{Title: "Metadata", Fields: []plugin.ObjectDetailField{
						{Key: "current_version", Label: "Current version", Type: plugin.ColumnNumber},
						{Key: "oldest_version", Label: "Oldest version", Type: plugin.ColumnNumber},
						{Key: "max_versions", Label: "Max versions", Type: plugin.ColumnNumber},
						{Key: "cas_required", Label: "CAS required", Type: plugin.ColumnBool},
						{Key: "delete_version_after", Label: "Delete version after", Type: plugin.ColumnDuration},
						{Key: "created_time", Label: "Created", Type: plugin.ColumnDateTime},
						{Key: "updated_time", Label: "Updated", Type: plugin.ColumnDateTime},
						{Key: "custom_metadata", Label: "Custom metadata", Type: plugin.ColumnJSON},
					}})},
			},
		},
	}
}

func policyResource() plugin.ResourceType {
	return plugin.ResourceType{
		Kind: "policy", Title: "Policies",
		List:    plugin.DataSource{RouteID: "vault.policies.list"},
		Columns: policyColumns(),
		Actions: plugin.ResourceActions{
			Toolbar: []string{"vault.policy.create"},
			Row:     []string{"vault.policy.delete"},
			Detail:  []string{"vault.policy.delete"},
		},
		Detail: plugin.DetailView{
			Header: plugin.HeaderSpec{Title: "${resource.name}"},
			Tabs: []plugin.Panel{
				{Key: "hcl", Label: "Policy", Icon: icon("scroll-text"), Type: plugin.PanelCodeEditor,
					Source: &plugin.DataSource{RouteID: "vault.policy.read", Params: map[string]string{"name": "${resource.uid}"}},
					Config: plugin.CodeEditorConfig{
						Language:     "hcl",
						SaveRouteID:  "vault.policy.write",
						SaveMethod:   plugin.MethodPut,
						SaveParams:   map[string]string{"name": "${resource.uid}"},
						RefreshField: "content",
						SaveToast:    &plugin.SaveToast{Summary: "Policy saved", Detail: "${response.name} updated", Severity: plugin.SeveritySuccess},
					}},
			},
		},
	}
}

func tokenResource() plugin.ResourceType {
	return plugin.ResourceType{
		Kind: "token", Title: "Tokens",
		List:    plugin.DataSource{RouteID: "vault.tokens.list"},
		Columns: tokenColumns(),
		Actions: plugin.ResourceActions{
			Toolbar: []string{"vault.token.create"},
			Row:     []string{"vault.token.revoke"},
			Detail:  []string{"vault.token.revoke"},
		},
		Detail: plugin.DetailView{
			Header: plugin.HeaderSpec{Title: "${resource.name}"},
			Tabs: []plugin.Panel{
				{Key: "overview", Label: "Overview", Icon: icon("info"), Type: plugin.PanelObjectDetail,
					Source: &plugin.DataSource{RouteID: "vault.token.read", Params: map[string]string{"accessor": "${resource.uid}"}},
					Config: objectDetail(plugin.ObjectDetailSection{Title: "Token", Fields: []plugin.ObjectDetailField{
						{Key: "accessor", Label: "Accessor", Copy: true},
						{Key: "display_name", Label: "Display name"},
						{Key: "type", Label: "Type", Type: plugin.ColumnBadge},
						{Key: "policies", Label: "Policies", Type: plugin.ColumnJSON},
						{Key: "identity_policies", Label: "Identity policies", Type: plugin.ColumnJSON},
						{Key: "ttl", Label: "TTL", Type: plugin.ColumnDuration},
						{Key: "explicit_max_ttl", Label: "Explicit max TTL", Type: plugin.ColumnDuration},
						{Key: "creation_ttl", Label: "Creation TTL", Type: plugin.ColumnDuration},
						{Key: "num_uses", Label: "Remaining uses", Type: plugin.ColumnNumber},
						{Key: "renewable", Label: "Renewable", Type: plugin.ColumnBool},
						{Key: "orphan", Label: "Orphan", Type: plugin.ColumnBool},
						{Key: "issue_time", Label: "Issued", Type: plugin.ColumnDateTime},
						{Key: "expire_time", Label: "Expires", Type: plugin.ColumnDateTime},
						{Key: "entity_id", Label: "Entity ID", Copy: true},
						{Key: "path", Label: "Auth path", Copy: true},
						{Key: "meta", Label: "Metadata", Type: plugin.ColumnJSON},
					}})},
			},
		},
	}
}

func leaseResource() plugin.ResourceType {
	return plugin.ResourceType{
		Kind: "lease", Title: "Leases",
		List:    plugin.DataSource{RouteID: "vault.leases.list"},
		Columns: leaseColumns(),
		Actions: plugin.ResourceActions{
			Row:    []string{"vault.lease.revoke"},
			Detail: []string{"vault.lease.revoke"},
		},
		Detail: plugin.DetailView{
			Header: plugin.HeaderSpec{Title: "${resource.uid}"},
			Tabs: []plugin.Panel{
				{Key: "overview", Label: "Overview", Icon: icon("info"), Type: plugin.PanelObjectDetail,
					Source: &plugin.DataSource{RouteID: "vault.lease.read", Params: map[string]string{"lease_id": "${resource.uid}"}},
					Config: objectDetail(plugin.ObjectDetailSection{Title: "Lease", Fields: []plugin.ObjectDetailField{
						{Key: "id", Label: "Lease ID", Copy: true},
						{Key: "ttl", Label: "TTL", Type: plugin.ColumnDuration},
						{Key: "renewable", Label: "Renewable", Type: plugin.ColumnBool},
						{Key: "issue_time", Label: "Issued", Type: plugin.ColumnDateTime},
						{Key: "expire_time", Label: "Expires", Type: plugin.ColumnDateTime},
						{Key: "last_renewal", Label: "Last renewal", Type: plugin.ColumnDateTime},
					}})},
			},
		},
	}
}

func namespaceResource() plugin.ResourceType {
	return plugin.ResourceType{
		Kind: "namespace", Title: "Namespaces",
		List:    plugin.DataSource{RouteID: "vault.namespaces.list"},
		Columns: namespaceColumns(),
		Detail: plugin.DetailView{
			Header: plugin.HeaderSpec{Title: "${resource.uid}"},
			Tabs: []plugin.Panel{
				{Key: "overview", Label: "Overview", Icon: icon("info"), Type: plugin.PanelObjectDetail,
					Source: &plugin.DataSource{RouteID: "vault.namespace.read", Params: map[string]string{"path": "${resource.uid}"}},
					Config: objectDetail(plugin.ObjectDetailSection{Title: "Namespace", Fields: []plugin.ObjectDetailField{
						{Key: "path", Label: "Path", Copy: true},
						{Key: "id", Label: "ID", Copy: true},
						{Key: "custom_metadata", Label: "Custom metadata", Type: plugin.ColumnJSON},
					}})},
			},
		},
	}
}

func auditResource() plugin.ResourceType {
	return plugin.ResourceType{
		Kind: "audit", Title: "Audit devices",
		List:    plugin.DataSource{RouteID: "vault.audit.list"},
		Columns: auditColumns(),
		Detail: plugin.DetailView{
			Header: plugin.HeaderSpec{Title: "${resource.name}"},
			Tabs: []plugin.Panel{
				{Key: "overview", Label: "Overview", Icon: icon("info"), Type: plugin.PanelObjectDetail,
					Source: &plugin.DataSource{RouteID: "vault.audit.read", Params: map[string]string{"path": "${resource.uid}"}},
					Config: objectDetail(plugin.ObjectDetailSection{Title: "Audit device", Fields: []plugin.ObjectDetailField{
						{Key: "path", Label: "Path", Copy: true},
						{Key: "type", Label: "Type", Type: plugin.ColumnBadge},
						{Key: "description", Label: "Description"},
						{Key: "local", Label: "Local", Type: plugin.ColumnBool},
						{Key: "options", Label: "Options", Type: plugin.ColumnJSON},
					}})},
			},
		},
	}
}

func actions() []plugin.Action {
	return []plugin.Action{
		{ID: "vault.secret.put", Label: "Write secret", Icon: icon("plus"), RouteID: "vault.secret.put",
			OnSuccess: &plugin.ActionSuccess{SelectTab: "data"}},
		{ID: "vault.secret.delete", Label: "Delete", Icon: icon("trash-2"), RouteID: "vault.secret.delete",
			Params: map[string]string{"key": "${resource.uid}"}, Confirm: true,
			ConfirmText: "Delete this secret? KV v2 keeps the versions until they are destroyed; KV v1 removes the value immediately.", Bulk: true},
		{ID: "vault.secret.versions.delete", Label: "Delete versions", Icon: icon("eraser"), RouteID: "vault.secret.versions.delete",
			Params: map[string]string{"key": "${resource.uid}"}, Confirm: true,
			ConfirmText: "Delete these versions? They keep their data and can be undeleted until they are destroyed.",
			OnSuccess:   &plugin.ActionSuccess{SelectTab: "versions"}},
		{ID: "vault.secret.undelete", Label: "Undelete versions", Icon: icon("undo-2"), RouteID: "vault.secret.undelete",
			Params: map[string]string{"key": "${resource.uid}"}, OnSuccess: &plugin.ActionSuccess{SelectTab: "versions"}},
		{ID: "vault.secret.destroy", Label: "Destroy versions", Icon: icon("bomb"), RouteID: "vault.secret.destroy",
			Params: map[string]string{"key": "${resource.uid}"}, Confirm: true,
			ConfirmText: "Permanently destroy these versions? The data cannot be recovered."},
		{ID: "vault.secret.purge", Label: "Destroy secret", Icon: icon("trash"), RouteID: "vault.secret.purge",
			Params: map[string]string{"key": "${resource.uid}"}, Confirm: true,
			ConfirmText: "Permanently destroy every version and the metadata of this secret?",
			OnSuccess:   &plugin.ActionSuccess{Navigate: plugin.NavigateList}},
		{ID: "vault.policy.create", Label: "Create policy", Icon: icon("file-plus"), RouteID: "vault.policy.create"},
		{ID: "vault.policy.delete", Label: "Delete", Icon: icon("trash-2"), RouteID: "vault.policy.delete",
			Params: map[string]string{"name": "${resource.uid}"}, Confirm: true,
			ConfirmText: "Delete this ACL policy? Tokens that reference it lose those capabilities.", Bulk: true},
		{ID: "vault.token.create", Label: "Create token", Icon: icon("ticket-plus"), RouteID: "vault.token.create"},
		{ID: "vault.token.revoke", Label: "Revoke", Icon: icon("trash-2"), RouteID: "vault.token.revoke",
			Params: map[string]string{"accessor": "${resource.uid}"}, Confirm: true,
			ConfirmText: "Revoke this token? Every secret and lease it created is revoked with it.", Bulk: true},
		{ID: "vault.lease.revoke", Label: "Revoke", Icon: icon("trash-2"), RouteID: "vault.lease.revoke",
			Params: map[string]string{"lease_id": "${resource.uid}"}, Confirm: true,
			ConfirmText: "Revoke this lease? The credential it issued stops working immediately.", Bulk: true},
	}
}

func sealSeverities() map[string]plugin.Severity {
	return map[string]plugin.Severity{
		"active":        plugin.SeveritySuccess,
		"unsealed":      plugin.SeveritySuccess,
		"standby":       plugin.SeverityInfo,
		"sealed":        plugin.SeverityDanger,
		"uninitialized": plugin.SeverityWarn,
	}
}

func deprecationSeverities() map[string]plugin.Severity {
	return map[string]plugin.Severity{
		"supported":       plugin.SeveritySuccess,
		"deprecated":      plugin.SeverityWarn,
		"pending removal": plugin.SeverityDanger,
		"removed":         plugin.SeverityDanger,
	}
}

func versionStateSeverities() map[string]plugin.Severity {
	return map[string]plugin.Severity{
		"active":    plugin.SeveritySuccess,
		"deleted":   plugin.SeverityWarn,
		"destroyed": plugin.SeverityDanger,
	}
}

func mountColumns() []plugin.Column {
	return []plugin.Column{
		{Key: "path", Label: "Path", Sortable: true},
		{Key: "type", Label: "Type", Type: plugin.ColumnBadge, Sortable: true},
		{Key: "kv_version", Label: "KV", Type: plugin.ColumnNumber},
		{Key: "description", Label: "Description"},
		{Key: "accessor", Label: "Accessor"},
		{Key: "running_version", Label: "Version"},
		{Key: "local", Label: "Local", Type: plugin.ColumnBool},
	}
}

func authColumns() []plugin.Column {
	return []plugin.Column{
		{Key: "path", Label: "Path", Sortable: true},
		{Key: "type", Label: "Type", Type: plugin.ColumnBadge, Sortable: true},
		{Key: "description", Label: "Description"},
		{Key: "accessor", Label: "Accessor"},
		{Key: "running_version", Label: "Version"},
		{Key: "local", Label: "Local", Type: plugin.ColumnBool},
	}
}

func secretColumns() []plugin.Column {
	return []plugin.Column{
		{Key: "name", Label: "Name", Sortable: true},
		{Key: "key", Label: "Path", Sortable: true},
		{Key: "mount", Label: "Engine"},
	}
}

func versionColumns() []plugin.Column {
	return []plugin.Column{
		{Key: "version", Label: "Version", Type: plugin.ColumnNumber, Sortable: true},
		{Key: "state", Label: "State", Type: plugin.ColumnBadge, Severities: versionStateSeverities()},
		{Key: "created_time", Label: "Created", Type: plugin.ColumnRelativeTime, Sortable: true},
		{Key: "deletion_time", Label: "Deleted", Type: plugin.ColumnRelativeTime},
		{Key: "destroyed", Label: "Destroyed", Type: plugin.ColumnBool},
	}
}

func policyColumns() []plugin.Column {
	return []plugin.Column{
		{Key: "name", Label: "Policy", Sortable: true},
		{Key: "builtin", Label: "Built-in", Type: plugin.ColumnBool},
	}
}

func tokenColumns() []plugin.Column {
	return []plugin.Column{
		{Key: "accessor", Label: "Accessor", Sortable: true},
		{Key: "display_name", Label: "Display name"},
		{Key: "type", Label: "Type", Type: plugin.ColumnBadge},
		{Key: "policies", Label: "Policies", Type: plugin.ColumnJSON},
		{Key: "ttl", Label: "TTL", Type: plugin.ColumnDuration},
		{Key: "expire_time", Label: "Expires", Type: plugin.ColumnRelativeTime},
		{Key: "orphan", Label: "Orphan", Type: plugin.ColumnBool},
	}
}

func leaseColumns() []plugin.Column {
	return []plugin.Column{
		{Key: "id", Label: "Lease ID", Sortable: true},
		{Key: "ttl", Label: "TTL", Type: plugin.ColumnDuration},
		{Key: "renewable", Label: "Renewable", Type: plugin.ColumnBool},
		{Key: "issue_time", Label: "Issued", Type: plugin.ColumnRelativeTime},
		{Key: "expire_time", Label: "Expires", Type: plugin.ColumnRelativeTime},
	}
}

func namespaceColumns() []plugin.Column {
	return []plugin.Column{
		{Key: "path", Label: "Namespace", Sortable: true},
		{Key: "id", Label: "ID"},
	}
}

func auditColumns() []plugin.Column {
	return []plugin.Column{
		{Key: "path", Label: "Path", Sortable: true},
		{Key: "type", Label: "Type", Type: plugin.ColumnBadge, Sortable: true},
		{Key: "description", Label: "Description"},
		{Key: "local", Label: "Local", Type: plugin.ColumnBool},
	}
}

func secretPutSchema() *plugin.Schema {
	return &plugin.Schema{Groups: []plugin.Group{{Name: "Secret", Fields: []plugin.Field{
		{Key: "key", Label: "Path", Type: plugin.FieldText, Required: true, Default: "${resource.uid}", Help: "Full path including the secrets engine mount, for example secret/team/app."},
		{Key: "data", Label: "Data", Type: plugin.FieldJSON, Required: true, Help: "JSON object of key/value pairs stored at this path."},
		{Key: "cas", Label: "Check-and-set version", Type: plugin.FieldNumber, Help: "KV v2 only. Write only when the current version matches; 0 requires the secret to be new.", Validators: []plugin.Validator{{Type: plugin.ValidatorMin, Value: 0}}},
	}}}}
}

func versionsSchema(label, help string) *plugin.Schema {
	return &plugin.Schema{Groups: []plugin.Group{{Name: "Versions", Fields: []plugin.Field{
		{Key: "versions", Label: label, Type: plugin.FieldText, Required: true, Placeholder: "3,4", Help: help},
	}}}}
}

func policyCreateSchema() *plugin.Schema {
	return &plugin.Schema{Groups: []plugin.Group{{Name: "Policy", Fields: []plugin.Field{
		{Key: "name", Label: "Name", Type: plugin.FieldText, Required: true, Placeholder: "app-readonly"},
		{Key: "policy", Label: "HCL", Type: plugin.FieldTextarea, Required: true, Placeholder: "path \"secret/data/app/*\" {\n  capabilities = [\"read\"]\n}"},
	}}}}
}

func tokenCreateSchema() *plugin.Schema {
	return &plugin.Schema{Groups: []plugin.Group{{Name: "Token", Fields: []plugin.Field{
		{Key: "display_name", Label: "Display name", Type: plugin.FieldText, Placeholder: "ci-runner"},
		{Key: "policies", Label: "Policies", Type: plugin.FieldText, Placeholder: "default,app-readonly", Help: "Comma-separated ACL policy names."},
		{Key: "ttl", Label: "TTL", Type: plugin.FieldDuration, Placeholder: "1h"},
		{Key: "explicit_max_ttl", Label: "Explicit max TTL", Type: plugin.FieldDuration},
		{Key: "num_uses", Label: "Use limit", Type: plugin.FieldNumber, Default: 0, Help: "0 means unlimited uses.", Validators: []plugin.Validator{{Type: plugin.ValidatorMin, Value: 0}}},
		{Key: "token_type", Label: "Token type", Type: plugin.FieldSelect, Default: "service", Options: []plugin.Option{
			{Label: "Service", Value: "service"},
			{Label: "Batch", Value: "batch"},
		}},
		{Key: "renewable", Label: "Renewable", Type: plugin.FieldToggle, Default: true},
		{Key: "no_parent", Label: "Orphan token", Type: plugin.FieldToggle, Default: false, Help: "Orphan tokens survive revocation of the creating token."},
	}}}}
}
