package milvus

import (
	"fmt"
	"sort"
	"strings"

	"github.com/milvus-io/milvus/client/v2/milvusclient"

	"github.com/charlesng35/shellcn-contrib/shared/broker"
	"github.com/charlesng35/shellcn/sdk/plugin"
)

func listUsers(rc *plugin.RequestContext) (any, error) {
	s, c, err := clientOf(rc)
	if err != nil {
		return nil, err
	}
	ctx, cancel := withTimeout(rc, s)
	defer cancel()
	names, err := c.ListUsers(ctx, milvusclient.NewListUserOption())
	if err != nil {
		return nil, mapError(err)
	}
	sort.Strings(names)
	rows := make([]row, 0, len(names))
	for _, name := range names {
		item := row{
			"name":  name,
			"value": name,
			"label": name,
			"ref":   plugin.ResourceIdentity{Kind: "user", Name: name, UID: name},
		}
		if user, err := c.DescribeUser(ctx, milvusclient.NewDescribeUserOption(name)); err == nil {
			item["roles"] = user.Roles
			item["role_count"] = len(user.Roles)
		}
		rows = append(rows, item)
	}
	return broker.PageRows(rc, rows)
}

func readUser(rc *plugin.RequestContext) (any, error) {
	name, err := identifier("user", rc.Param("user"))
	if err != nil {
		return nil, err
	}
	s, c, err := clientOf(rc)
	if err != nil {
		return nil, err
	}
	ctx, cancel := withTimeout(rc, s)
	defer cancel()
	user, err := c.DescribeUser(ctx, milvusclient.NewDescribeUserOption(name))
	if err != nil {
		return nil, mapError(err)
	}
	return row{
		"name":       user.UserName,
		"roles":      user.Roles,
		"role_count": len(user.Roles),
		"ref":        plugin.ResourceIdentity{Kind: "user", Name: user.UserName, UID: user.UserName},
	}, nil
}

func listUserRoles(rc *plugin.RequestContext) (any, error) {
	name, err := identifier("user", rc.Param("user"))
	if err != nil {
		return nil, err
	}
	s, c, err := clientOf(rc)
	if err != nil {
		return nil, err
	}
	ctx, cancel := withTimeout(rc, s)
	defer cancel()
	user, err := c.DescribeUser(ctx, milvusclient.NewDescribeUserOption(name))
	if err != nil {
		return nil, mapError(err)
	}
	roles := append([]string(nil), user.Roles...)
	sort.Strings(roles)
	rows := make([]row, 0, len(roles))
	for _, role := range roles {
		rows = append(rows, row{"role": role, "user": user.UserName})
	}
	return broker.PageRows(rc, rows)
}

func createUser(rc *plugin.RequestContext) (any, error) {
	var req struct {
		Name     string `json:"name" validate:"required"`
		Password string `json:"password" validate:"required"`
	}
	if err := rc.Bind(&req); err != nil {
		return nil, err
	}
	name, err := identifier("user", req.Name)
	if err != nil {
		return nil, err
	}
	if len(req.Password) < 6 {
		return nil, fmt.Errorf("%w: password must be at least 6 characters", plugin.ErrInvalidInput)
	}
	s, c, err := writableClient(rc)
	if err != nil {
		return nil, err
	}
	ctx, cancel := withTimeout(rc, s)
	defer cancel()
	err = c.CreateUser(ctx, milvusclient.NewCreateUserOption(name, req.Password))
	s.activity.record("user.create", name, "Created user", err)
	return ok(row{"ok": err == nil, "name": name}, err)
}

func updateUserPassword(rc *plugin.RequestContext) (any, error) {
	name, err := identifier("user", rc.Param("user"))
	if err != nil {
		return nil, err
	}
	var req struct {
		OldPassword string `json:"old_password" validate:"required"`
		NewPassword string `json:"new_password" validate:"required"`
	}
	if err := rc.Bind(&req); err != nil {
		return nil, err
	}
	if len(req.NewPassword) < 6 {
		return nil, fmt.Errorf("%w: password must be at least 6 characters", plugin.ErrInvalidInput)
	}
	s, c, err := writableClient(rc)
	if err != nil {
		return nil, err
	}
	ctx, cancel := withTimeout(rc, s)
	defer cancel()
	err = c.UpdatePassword(ctx, milvusclient.NewUpdatePasswordOption(name, req.OldPassword, req.NewPassword))
	s.activity.record("user.password", name, "Rotated password", err)
	return ok(row{"ok": err == nil, "name": name}, err)
}

func deleteUser(rc *plugin.RequestContext) (any, error) {
	name, err := identifier("user", rc.Param("user"))
	if err != nil {
		return nil, err
	}
	s, c, err := writableClient(rc)
	if err != nil {
		return nil, err
	}
	ctx, cancel := withTimeout(rc, s)
	defer cancel()
	err = c.DropUser(ctx, milvusclient.NewDropUserOption(name))
	s.activity.record("user.delete", name, "Dropped user", err)
	return ok(row{"ok": err == nil, "name": name}, err)
}

func grantUserRole(rc *plugin.RequestContext) (any, error) {
	user, err := identifier("user", rc.Param("user"))
	if err != nil {
		return nil, err
	}
	var req struct {
		Role string `json:"role" validate:"required"`
	}
	if err := rc.Bind(&req); err != nil {
		return nil, err
	}
	role, err := identifier("role", req.Role)
	if err != nil {
		return nil, err
	}
	s, c, err := writableClient(rc)
	if err != nil {
		return nil, err
	}
	ctx, cancel := withTimeout(rc, s)
	defer cancel()
	err = c.GrantRole(ctx, milvusclient.NewGrantRoleOption(user, role))
	s.activity.record("user.role.grant", user, "Granted role "+role, err)
	return ok(row{"ok": err == nil, "user": user, "role": role}, err)
}

func revokeUserRole(rc *plugin.RequestContext) (any, error) {
	user, err := identifier("user", rc.Param("user"))
	if err != nil {
		return nil, err
	}
	role, err := identifier("role", rc.Param("role"))
	if err != nil {
		return nil, err
	}
	s, c, err := writableClient(rc)
	if err != nil {
		return nil, err
	}
	ctx, cancel := withTimeout(rc, s)
	defer cancel()
	err = c.RevokeRole(ctx, milvusclient.NewRevokeRoleOption(user, role))
	s.activity.record("user.role.revoke", user, "Revoked role "+role, err)
	return ok(row{"ok": err == nil, "user": user, "role": role}, err)
}

func listRoles(rc *plugin.RequestContext) (any, error) {
	s, c, err := clientOf(rc)
	if err != nil {
		return nil, err
	}
	ctx, cancel := withTimeout(rc, s)
	defer cancel()
	names, err := c.ListRoles(ctx, milvusclient.NewListRoleOption())
	if err != nil {
		return nil, mapError(err)
	}
	sort.Strings(names)
	rows := make([]row, 0, len(names))
	for _, name := range names {
		item := row{
			"name":  name,
			"value": name,
			"label": name,
			"ref":   plugin.ResourceIdentity{Kind: "role", Name: name, UID: name},
		}
		if role, err := c.DescribeRole(ctx, milvusclient.NewDescribeRoleOption(name)); err == nil {
			item["privilege_count"] = len(role.Privileges)
		}
		rows = append(rows, item)
	}
	return broker.PageRows(rc, rows)
}

func readRole(rc *plugin.RequestContext) (any, error) {
	name, err := identifier("role", rc.Param("role"))
	if err != nil {
		return nil, err
	}
	s, c, err := clientOf(rc)
	if err != nil {
		return nil, err
	}
	ctx, cancel := withTimeout(rc, s)
	defer cancel()
	role, err := c.DescribeRole(ctx, milvusclient.NewDescribeRoleOption(name))
	if err != nil {
		return nil, mapError(err)
	}
	holders := make([]string, 0)
	if users, err := c.ListUsers(ctx, milvusclient.NewListUserOption()); err == nil {
		for _, user := range users {
			described, err := c.DescribeUser(ctx, milvusclient.NewDescribeUserOption(user))
			if err != nil {
				continue
			}
			for _, granted := range described.Roles {
				if granted == name {
					holders = append(holders, user)
					break
				}
			}
		}
	}
	sort.Strings(holders)
	return row{
		"name":            role.RoleName,
		"privilege_count": len(role.Privileges),
		"users":           holders,
		"ref":             plugin.ResourceIdentity{Kind: "role", Name: role.RoleName, UID: role.RoleName},
	}, nil
}

func listRolePrivileges(rc *plugin.RequestContext) (any, error) {
	name, err := identifier("role", rc.Param("role"))
	if err != nil {
		return nil, err
	}
	s, c, err := clientOf(rc)
	if err != nil {
		return nil, err
	}
	ctx, cancel := withTimeout(rc, s)
	defer cancel()
	role, err := c.DescribeRole(ctx, milvusclient.NewDescribeRoleOption(name))
	if err != nil {
		return nil, mapError(err)
	}
	rows := make([]row, 0, len(role.Privileges))
	for _, grant := range role.Privileges {
		rows = append(rows, row{
			"role":        grant.RoleName,
			"privilege":   grant.Privilege,
			"object":      grant.Object,
			"object_name": grant.ObjectName,
			"database":    grant.DbName,
			"grantor":     grant.Grantor,
		})
	}
	return broker.PageRows(rc, rows)
}

func createRole(rc *plugin.RequestContext) (any, error) {
	var req struct {
		Name string `json:"name" validate:"required"`
	}
	if err := rc.Bind(&req); err != nil {
		return nil, err
	}
	name, err := identifier("role", req.Name)
	if err != nil {
		return nil, err
	}
	s, c, err := writableClient(rc)
	if err != nil {
		return nil, err
	}
	ctx, cancel := withTimeout(rc, s)
	defer cancel()
	err = c.CreateRole(ctx, milvusclient.NewCreateRoleOption(name))
	s.activity.record("role.create", name, "Created role", err)
	return ok(row{"ok": err == nil, "name": name}, err)
}

func deleteRole(rc *plugin.RequestContext) (any, error) {
	name, err := identifier("role", rc.Param("role"))
	if err != nil {
		return nil, err
	}
	s, c, err := writableClient(rc)
	if err != nil {
		return nil, err
	}
	ctx, cancel := withTimeout(rc, s)
	defer cancel()
	err = c.DropRole(ctx, milvusclient.NewDropRoleOption(name))
	s.activity.record("role.delete", name, "Dropped role", err)
	return ok(row{"ok": err == nil, "name": name}, err)
}

func grantRolePrivilege(rc *plugin.RequestContext) (any, error) {
	role, privilege, collection, database, s, c, err := privilegeTarget(rc)
	if err != nil {
		return nil, err
	}
	ctx, cancel := withTimeout(rc, s)
	defer cancel()
	err = c.GrantPrivilegeV2(ctx, milvusclient.NewGrantV2Option(role, privilege, database, collection))
	s.activity.record("role.privilege.grant", role, fmt.Sprintf("Granted %s on %s.%s", privilege, database, collection), err)
	return ok(row{"ok": err == nil, "role": role, "privilege": privilege}, err)
}

func revokeRolePrivilege(rc *plugin.RequestContext) (any, error) {
	role, privilege, collection, database, s, c, err := privilegeTarget(rc)
	if err != nil {
		return nil, err
	}
	ctx, cancel := withTimeout(rc, s)
	defer cancel()
	err = c.RevokePrivilegeV2(ctx, milvusclient.NewRevokeV2Option(role, privilege, database, collection))
	s.activity.record("role.privilege.revoke", role, fmt.Sprintf("Revoked %s on %s.%s", privilege, database, collection), err)
	return ok(row{"ok": err == nil, "role": role, "privilege": privilege}, err)
}

func privilegeTarget(rc *plugin.RequestContext) (string, string, string, string, *Session, *milvusclient.Client, error) {
	role, err := identifier("role", rc.Param("role"))
	if err != nil {
		return "", "", "", "", nil, nil, err
	}
	var req struct {
		Privilege  string `json:"privilege" validate:"required"`
		Collection string `json:"collection"`
		DB         string `json:"db"`
	}
	if err := rc.Bind(&req); err != nil {
		return "", "", "", "", nil, nil, err
	}
	privilege := strings.TrimSpace(req.Privilege)
	if !identifierPattern.MatchString(privilege) {
		return "", "", "", "", nil, nil, fmt.Errorf("%w: invalid privilege name %q", plugin.ErrInvalidInput, privilege)
	}
	collection := strings.TrimSpace(req.Collection)
	if collection == "" {
		collection = "*"
	}
	if collection != "*" {
		if collection, err = identifier("collection", collection); err != nil {
			return "", "", "", "", nil, nil, err
		}
	}
	database := strings.TrimSpace(req.DB)
	if database == "" {
		database = databaseName(rc)
	}
	if database != "*" {
		if database, err = identifier("database", database); err != nil {
			return "", "", "", "", nil, nil, err
		}
	}
	s, c, err := writableClient(rc)
	if err != nil {
		return "", "", "", "", nil, nil, err
	}
	return role, privilege, collection, database, s, c, nil
}
