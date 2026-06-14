package dynamodb

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"time"
	"unicode"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsdynamodb "github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	smithy "github.com/aws/smithy-go"

	"github.com/charlesng35/shellcn-contrib/shared/broker"
	"github.com/charlesng35/shellcn-contrib/shared/sqldb"
	"github.com/charlesng35/shellcn/sdk/plugin"
)

type confirmationError struct{ message string }

func (e confirmationError) Error() string { return e.message }

func routes() []plugin.Route {
	return []plugin.Route{
		{ID: rid("tables.tree"), Method: plugin.MethodGet, Path: "/tree/tables", Permission: "dynamodb.tables.read", Risk: plugin.RiskSafe, AuditEvent: rid("tables.tree"), Handle: tablesTree},
		{ID: rid("tables.list"), Method: plugin.MethodGet, Path: "/tables", Permission: "dynamodb.tables.read", Risk: plugin.RiskSafe, AuditEvent: rid("tables.list"), Handle: tablesList},
		{ID: rid("table.read"), Method: plugin.MethodGet, Path: "/tables/{table}", Permission: "dynamodb.tables.read", Risk: plugin.RiskSafe, AuditEvent: rid("table.read"), Handle: tableRead},
		{ID: rid("table.capacity"), Method: plugin.MethodGet, Path: "/tables/{table}/capacity", Permission: "dynamodb.tables.read", Risk: plugin.RiskSafe, AuditEvent: rid("table.capacity"), Handle: tableCapacity},
		{ID: rid("indexes.list"), Method: plugin.MethodGet, Path: "/tables/{table}/indexes", Permission: "dynamodb.indexes.read", Risk: plugin.RiskSafe, AuditEvent: rid("indexes.list"), Handle: indexesList},
		{ID: rid("index.read"), Method: plugin.MethodGet, Path: "/tables/{table}/indexes/{index}", Permission: "dynamodb.indexes.read", Risk: plugin.RiskSafe, AuditEvent: rid("index.read"), Handle: indexRead},
		{ID: rid("items.list"), Method: plugin.MethodGet, Path: "/tables/{table}/items", Permission: "dynamodb.items.read", Risk: plugin.RiskSafe, AuditEvent: rid("items.list"), Handle: itemsList},
		{ID: rid("item.read"), Method: plugin.MethodGet, Path: "/items/{id}", Permission: "dynamodb.items.read", Risk: plugin.RiskSafe, AuditEvent: rid("item.read"), Handle: itemRead},
		{ID: rid("tags.list"), Method: plugin.MethodGet, Path: "/tables/{table}/tags", Permission: "dynamodb.tables.read", Risk: plugin.RiskSafe, AuditEvent: rid("tags.list"), Handle: tagsList},
		{ID: rid("ttl.read"), Method: plugin.MethodGet, Path: "/tables/{table}/ttl", Permission: "dynamodb.tables.read", Risk: plugin.RiskSafe, AuditEvent: rid("ttl.read"), Handle: ttlRead},
		{ID: rid("backups.tree"), Method: plugin.MethodGet, Path: "/tree/backups", Permission: "dynamodb.backups.read", Risk: plugin.RiskSafe, AuditEvent: rid("backups.tree"), Handle: backupsTree},
		{ID: rid("backups.list"), Method: plugin.MethodGet, Path: "/backups", Permission: "dynamodb.backups.read", Risk: plugin.RiskSafe, AuditEvent: rid("backups.list"), Handle: backupsList},
		{ID: rid("backup.read"), Method: plugin.MethodGet, Path: "/backups/{backup}", Permission: "dynamodb.backups.read", Risk: plugin.RiskSafe, AuditEvent: rid("backup.read"), Handle: backupRead},
		{ID: rid("table.create"), Method: plugin.MethodPost, Path: "/tables", Permission: "dynamodb.tables.write", Risk: plugin.RiskWrite, AuditEvent: rid("table.create"), Input: tableCreateSchema(), Handle: tableCreate},
		{ID: rid("table.delete"), Method: plugin.MethodDelete, Path: "/tables/{table}", Permission: "dynamodb.tables.delete", Risk: plugin.RiskDestructive, AuditEvent: rid("table.delete"), Handle: tableDelete},
		{ID: rid("item.put"), Method: plugin.MethodPost, Path: "/tables/{table}/items", Permission: "dynamodb.items.write", Risk: plugin.RiskWrite, AuditEvent: rid("item.put"), Input: itemPutSchema(), Handle: itemPut},
		{ID: rid("item.update"), Method: plugin.MethodPut, Path: "/items/{id}", Permission: "dynamodb.items.write", Risk: plugin.RiskWrite, AuditEvent: rid("item.update"), Handle: itemUpdate},
		{ID: rid("item.delete"), Method: plugin.MethodDelete, Path: "/items/{id}", Permission: "dynamodb.items.delete", Risk: plugin.RiskDestructive, AuditEvent: rid("item.delete"), Handle: itemDelete},
		{ID: rid("gsi.create"), Method: plugin.MethodPost, Path: "/tables/{table}/indexes", Permission: "dynamodb.indexes.write", Risk: plugin.RiskWrite, AuditEvent: rid("gsi.create"), Input: gsiCreateSchema(), Handle: gsiCreate},
		{ID: rid("gsi.delete"), Method: plugin.MethodDelete, Path: "/tables/{table}/indexes/{index}", Permission: "dynamodb.indexes.delete", Risk: plugin.RiskDestructive, AuditEvent: rid("gsi.delete"), Handle: gsiDelete},
		{ID: rid("backup.create"), Method: plugin.MethodPost, Path: "/tables/{table}/backups", Permission: "dynamodb.backups.write", Risk: plugin.RiskWrite, AuditEvent: rid("backup.create"), Input: backupCreateSchema(), Handle: backupCreate},
		{ID: rid("backup.delete"), Method: plugin.MethodDelete, Path: "/backups/{backup}", Permission: "dynamodb.backups.delete", Risk: plugin.RiskDestructive, AuditEvent: rid("backup.delete"), Handle: backupDelete},
		{ID: rid("ttl.update"), Method: plugin.MethodPut, Path: "/tables/{table}/ttl", Permission: "dynamodb.tables.write", Risk: plugin.RiskWrite, AuditEvent: rid("ttl.update"), Input: ttlUpdateSchema(), Handle: ttlUpdate},
		{ID: rid("partiql"), Method: plugin.MethodWS, Path: "/partiql", Permission: "dynamodb.partiql.execute", Risk: plugin.RiskPrivileged, AuditEvent: rid("partiql"), Stream: partiqlStream},
		{ID: rid("completion"), Method: plugin.MethodGet, Path: "/completion", Permission: "dynamodb.tables.read", Risk: plugin.RiskSafe, AuditEvent: rid("completion"), Handle: completionRoute},
	}
}

func ddbSession(rc *plugin.RequestContext) (*Session, error) { return unwrap(rc.Session) }

func tableCreateSchema() *plugin.Schema {
	provisioned := &plugin.Condition{AllOf: []plugin.Rule{{Field: "billing_mode", Op: plugin.OpEq, Value: "PROVISIONED"}}}
	hasSort := &plugin.Condition{AllOf: []plugin.Rule{{Field: "sort_key", Op: plugin.OpNotEmpty}}}
	return &plugin.Schema{Groups: []plugin.Group{{Name: "Table", Fields: []plugin.Field{
		{Key: "name", Label: "Table name", Type: plugin.FieldText, Required: true},
		{Key: "partition_key", Label: "Partition key", Type: plugin.FieldText, Required: true},
		{Key: "partition_key_type", Label: "Partition key type", Type: plugin.FieldSelect, Required: true, Default: "S", Options: scalarTypeOptions()},
		{Key: "sort_key", Label: "Sort key", Type: plugin.FieldText},
		{Key: "sort_key_type", Label: "Sort key type", Type: plugin.FieldSelect, Default: "S", VisibleWhen: hasSort, Options: scalarTypeOptions()},
		{Key: "billing_mode", Label: "Billing mode", Type: plugin.FieldSelect, Required: true, Default: "PAY_PER_REQUEST", Options: billingOptions()},
		{Key: "read_capacity", Label: "Read capacity units", Type: plugin.FieldNumber, Default: 5, VisibleWhen: provisioned, Validators: []plugin.Validator{{Type: plugin.ValidatorMin, Value: 1}}},
		{Key: "write_capacity", Label: "Write capacity units", Type: plugin.FieldNumber, Default: 5, VisibleWhen: provisioned, Validators: []plugin.Validator{{Type: plugin.ValidatorMin, Value: 1}}},
		{Key: "deletion_protection", Label: "Deletion protection", Type: plugin.FieldToggle, Default: false},
	}}}}
}

func itemPutSchema() *plugin.Schema {
	return &plugin.Schema{Groups: []plugin.Group{{Name: "Item", Fields: []plugin.Field{
		{Key: "item", Label: "Item JSON", Type: plugin.FieldJSON, Required: true, Help: "Plain JSON is accepted. DynamoDB JSON is also accepted when every attribute is encoded as S/N/BOOL/M/L/etc."},
		{Key: "condition_expression", Label: "Condition expression", Type: plugin.FieldText, Placeholder: "attribute_not_exists(pk)"},
	}}}}
}

func gsiCreateSchema() *plugin.Schema {
	provisioned := &plugin.Condition{AllOf: []plugin.Rule{{Field: "billing_mode", Op: plugin.OpEq, Value: "PROVISIONED"}}}
	hasSort := &plugin.Condition{AllOf: []plugin.Rule{{Field: "sort_key", Op: plugin.OpNotEmpty}}}
	include := &plugin.Condition{AllOf: []plugin.Rule{{Field: "projection_type", Op: plugin.OpEq, Value: "INCLUDE"}}}
	return &plugin.Schema{Groups: []plugin.Group{{Name: "Global secondary index", Fields: []plugin.Field{
		{Key: "name", Label: "Index name", Type: plugin.FieldText, Required: true},
		{Key: "partition_key", Label: "Partition key", Type: plugin.FieldText, Required: true},
		{Key: "partition_key_type", Label: "Partition key type", Type: plugin.FieldSelect, Required: true, Default: "S", Options: scalarTypeOptions()},
		{Key: "sort_key", Label: "Sort key", Type: plugin.FieldText},
		{Key: "sort_key_type", Label: "Sort key type", Type: plugin.FieldSelect, Default: "S", VisibleWhen: hasSort, Options: scalarTypeOptions()},
		{Key: "projection_type", Label: "Projection", Type: plugin.FieldSelect, Required: true, Default: "ALL", Options: []plugin.Option{{Label: "All attributes", Value: "ALL"}, {Label: "Keys only", Value: "KEYS_ONLY"}, {Label: "Include selected", Value: "INCLUDE"}}},
		{Key: "non_key_attributes", Label: "Projected attributes", Type: plugin.FieldText, VisibleWhen: include, Placeholder: "attr1, attr2"},
		{Key: "billing_mode", Label: "Billing mode", Type: plugin.FieldSelect, Required: true, Default: "PAY_PER_REQUEST", Options: billingOptions()},
		{Key: "read_capacity", Label: "Read capacity units", Type: plugin.FieldNumber, Default: 5, VisibleWhen: provisioned, Validators: []plugin.Validator{{Type: plugin.ValidatorMin, Value: 1}}},
		{Key: "write_capacity", Label: "Write capacity units", Type: plugin.FieldNumber, Default: 5, VisibleWhen: provisioned, Validators: []plugin.Validator{{Type: plugin.ValidatorMin, Value: 1}}},
	}}}}
}

func backupCreateSchema() *plugin.Schema {
	return &plugin.Schema{Groups: []plugin.Group{{Name: "Backup", Fields: []plugin.Field{
		{Key: "name", Label: "Backup name", Type: plugin.FieldText, Required: true},
	}}}}
}

func ttlUpdateSchema() *plugin.Schema {
	enabled := &plugin.Condition{AllOf: []plugin.Rule{{Field: "enabled", Op: plugin.OpEq, Value: true}}}
	return &plugin.Schema{Groups: []plugin.Group{{Name: "TTL", Fields: []plugin.Field{
		{Key: "enabled", Label: "Enabled", Type: plugin.FieldToggle, Default: true},
		{Key: "attribute", Label: "TTL attribute", Type: plugin.FieldText, Required: true, VisibleWhen: enabled, Placeholder: "expires_at"},
	}}}}
}

func scalarTypeOptions() []plugin.Option {
	return []plugin.Option{{Label: "String", Value: "S"}, {Label: "Number", Value: "N"}, {Label: "Binary", Value: "B"}}
}

func billingOptions() []plugin.Option {
	return []plugin.Option{{Label: "On-demand", Value: "PAY_PER_REQUEST"}, {Label: "Provisioned", Value: "PROVISIONED"}}
}

func tablesTree(rc *plugin.RequestContext) (any, error) {
	res, err := tablesList(rc)
	if err != nil {
		return nil, err
	}
	page := res.(plugin.Page[row])
	nodes := make([]plugin.TreeNode, 0, len(page.Items))
	for _, item := range page.Items {
		name := fmt.Sprint(item["name"])
		ref := plugin.ResourceIdentity{Kind: "table", Name: name, UID: name}
		nodes = append(nodes, plugin.TreeNode{Key: "table:" + name, Label: name, Icon: icon("table-2"), Ref: &ref, Leaf: true})
	}
	return plugin.Page[plugin.TreeNode]{Items: nodes, NextCursor: page.NextCursor, Total: page.Total}, nil
}

func tablesList(rc *plugin.RequestContext) (any, error) {
	s, err := ddbSession(rc)
	if err != nil {
		return nil, err
	}
	ctx, cancel := requestContext(rc.Ctx, s)
	defer cancel()
	pager := awsdynamodb.NewListTablesPaginator(s.client, &awsdynamodb.ListTablesInput{})
	rows := []row{}
	for pager.HasMorePages() {
		out, err := pager.NextPage(ctx)
		if err != nil {
			return nil, ddbErr(err)
		}
		for _, name := range out.TableNames {
			if s.opts.TablePrefix != "" && !strings.HasPrefix(name, s.opts.TablePrefix) {
				continue
			}
			item := row{"name": name, "ref": plugin.ResourceIdentity{Kind: "table", Name: name, UID: name}}
			if desc, err := describeTable(ctx, s, name); err == nil {
				mergeRows(item, tableSummary(desc))
			}
			rows = append(rows, item)
		}
	}
	sortRows(rows, "name")
	return broker.PageRows(rc, rows)
}

func tableRead(rc *plugin.RequestContext) (any, error) {
	s, err := ddbSession(rc)
	if err != nil {
		return nil, err
	}
	ctx, cancel := requestContext(rc.Ctx, s)
	defer cancel()
	desc, err := describeTable(ctx, s, tableName(rc))
	if err != nil {
		return nil, err
	}
	return tableDocument(desc), nil
}

func tableCapacity(rc *plugin.RequestContext) (any, error) {
	doc, err := tableRead(rc)
	if err != nil {
		return nil, err
	}
	m := doc.(row)
	return row{
		"billing_mode":             m["billing_mode"],
		"provisioned_throughput":   m["provisioned_throughput"],
		"global_secondary_indexes": m["global_secondary_indexes"],
		"local_secondary_indexes":  m["local_secondary_indexes"],
		"table_size_bytes":         m["size"],
		"item_count":               m["items"],
	}, nil
}

func indexesList(rc *plugin.RequestContext) (any, error) {
	s, err := ddbSession(rc)
	if err != nil {
		return nil, err
	}
	ctx, cancel := requestContext(rc.Ctx, s)
	defer cancel()
	desc, err := describeTable(ctx, s, tableName(rc))
	if err != nil {
		return nil, err
	}
	return broker.PageRows(rc, indexRows(desc))
}

func indexRead(rc *plugin.RequestContext) (any, error) {
	s, err := ddbSession(rc)
	if err != nil {
		return nil, err
	}
	ctx, cancel := requestContext(rc.Ctx, s)
	defer cancel()
	desc, err := describeTable(ctx, s, tableName(rc))
	if err != nil {
		return nil, err
	}
	want := rc.Param("index")
	for _, item := range indexRows(desc) {
		if fmt.Sprint(item["name"]) == want {
			return item, nil
		}
	}
	return nil, plugin.ErrNotFound
}

func itemsList(rc *plugin.RequestContext) (any, error) {
	s, err := ddbSession(rc)
	if err != nil {
		return nil, err
	}
	table := tableName(rc)
	ctx, cancel := requestContext(rc.Ctx, s)
	defer cancel()
	desc, err := describeTable(ctx, s, table)
	if err != nil {
		return nil, err
	}
	page, err := rc.Page()
	if err != nil {
		return nil, err
	}
	exclusiveStartKey, err := decodeKeyCursor(page.Cursor)
	if err != nil {
		return nil, err
	}
	out, err := s.client.Scan(ctx, &awsdynamodb.ScanInput{
		TableName:         aws.String(table),
		Limit:             aws.Int32(int32(page.Limit)),
		ExclusiveStartKey: exclusiveStartKey,
	})
	if err != nil {
		return nil, ddbErr(err)
	}
	rows := make([]row, 0, len(out.Items))
	for _, item := range out.Items {
		display := unmarshalItem(item)
		if key, ok := itemKey(item, desc.KeySchema); ok {
			id, _ := encodeItemID(table, key)
			name := keyDisplay(key, desc.KeySchema)
			display["_key"] = name
			display["ref"] = plugin.ResourceIdentity{Kind: "item", Namespace: table, Name: name, UID: id}
		}
		rows = append(rows, display)
	}
	next, err := encodeKeyCursor(out.LastEvaluatedKey)
	if err != nil {
		return nil, err
	}
	return plugin.Page[row]{Items: rows, NextCursor: next}, nil
}

func itemRead(rc *plugin.RequestContext) (any, error) {
	s, err := ddbSession(rc)
	if err != nil {
		return nil, err
	}
	table, key, err := decodeItemID(rc.Param("id"))
	if err != nil {
		return nil, err
	}
	ctx, cancel := requestContext(rc.Ctx, s)
	defer cancel()
	out, err := s.client.GetItem(ctx, &awsdynamodb.GetItemInput{TableName: aws.String(table), Key: key})
	if err != nil {
		return nil, ddbErr(err)
	}
	if len(out.Item) == 0 {
		return nil, plugin.ErrNotFound
	}
	return unmarshalItem(out.Item), nil
}

func tagsList(rc *plugin.RequestContext) (any, error) {
	s, err := ddbSession(rc)
	if err != nil {
		return nil, err
	}
	ctx, cancel := requestContext(rc.Ctx, s)
	defer cancel()
	desc, err := describeTable(ctx, s, tableName(rc))
	if err != nil {
		return nil, err
	}
	out, err := s.client.ListTagsOfResource(ctx, &awsdynamodb.ListTagsOfResourceInput{ResourceArn: desc.TableArn})
	if err != nil {
		if isUnsupportedLocal(err) {
			return plugin.Page[row]{Items: []row{}, Total: intPtr(0)}, nil
		}
		return nil, ddbErr(err)
	}
	rows := make([]row, 0, len(out.Tags))
	for _, tag := range out.Tags {
		rows = append(rows, row{"key": awsString(tag.Key), "value": awsString(tag.Value)})
	}
	return broker.PageRows(rc, rows)
}

func ttlRead(rc *plugin.RequestContext) (any, error) {
	s, err := ddbSession(rc)
	if err != nil {
		return nil, err
	}
	ctx, cancel := requestContext(rc.Ctx, s)
	defer cancel()
	out, err := s.client.DescribeTimeToLive(ctx, &awsdynamodb.DescribeTimeToLiveInput{TableName: aws.String(tableName(rc))})
	if err != nil {
		return nil, ddbErr(err)
	}
	desc := out.TimeToLiveDescription
	if desc == nil {
		return row{"status": "DISABLED"}, nil
	}
	return row{"status": string(desc.TimeToLiveStatus), "attribute": awsString(desc.AttributeName)}, nil
}

func backupsTree(rc *plugin.RequestContext) (any, error) {
	res, err := backupsList(rc)
	if err != nil {
		return nil, err
	}
	page := res.(plugin.Page[row])
	nodes := make([]plugin.TreeNode, 0, len(page.Items))
	for _, item := range page.Items {
		name, arn := fmt.Sprint(item["name"]), fmt.Sprint(item["arn"])
		ref := plugin.ResourceIdentity{Kind: "backup", Namespace: fmt.Sprint(item["table"]), Name: name, UID: arn}
		nodes = append(nodes, plugin.TreeNode{Key: "backup:" + arn, Label: name, Icon: icon("archive"), Ref: &ref, Leaf: true})
	}
	return plugin.Page[plugin.TreeNode]{Items: nodes, NextCursor: page.NextCursor, Total: page.Total}, nil
}

func backupsList(rc *plugin.RequestContext) (any, error) {
	s, err := ddbSession(rc)
	if err != nil {
		return nil, err
	}
	ctx, cancel := requestContext(rc.Ctx, s)
	defer cancel()
	page, err := rc.Page()
	if err != nil {
		return nil, err
	}
	input := &awsdynamodb.ListBackupsInput{Limit: aws.Int32(int32(page.Limit))}
	if table := tableName(rc); table != "" {
		input.TableName = aws.String(table)
	}
	if page.Cursor != "" {
		input.ExclusiveStartBackupArn = aws.String(page.Cursor)
	}
	out, err := s.client.ListBackups(ctx, input)
	if err != nil {
		if isUnsupportedLocal(err) {
			return plugin.Page[row]{Items: []row{}, Total: intPtr(0)}, nil
		}
		return nil, ddbErr(err)
	}
	rows := make([]row, 0, len(out.BackupSummaries))
	for _, backup := range out.BackupSummaries {
		arn := awsString(backup.BackupArn)
		rows = append(rows, row{
			"name":    awsString(backup.BackupName),
			"arn":     arn,
			"table":   awsString(backup.TableName),
			"status":  string(backup.BackupStatus),
			"size":    awsInt64(backup.BackupSizeBytes),
			"created": backup.BackupCreationDateTime,
			"ref":     plugin.ResourceIdentity{Kind: "backup", Namespace: awsString(backup.TableName), Name: awsString(backup.BackupName), UID: arn},
		})
	}
	return plugin.Page[row]{Items: rows, NextCursor: awsString(out.LastEvaluatedBackupArn)}, nil
}

func backupRead(rc *plugin.RequestContext) (any, error) {
	s, err := ddbSession(rc)
	if err != nil {
		return nil, err
	}
	ctx, cancel := requestContext(rc.Ctx, s)
	defer cancel()
	out, err := s.client.DescribeBackup(ctx, &awsdynamodb.DescribeBackupInput{BackupArn: aws.String(rc.Param("backup"))})
	if err != nil {
		if isUnsupportedLocal(err) {
			return nil, plugin.ErrNotSupported
		}
		return nil, ddbErr(err)
	}
	return backupDocument(out.BackupDescription), nil
}

func tableCreate(rc *plugin.RequestContext) (any, error) {
	s, err := ddbSession(rc)
	if err != nil {
		return nil, err
	}
	if err := ensureWritable(s); err != nil {
		return nil, err
	}
	var req struct {
		Name               string `json:"name" validate:"required"`
		PartitionKey       string `json:"partition_key" validate:"required"`
		PartitionKeyType   string `json:"partition_key_type"`
		SortKey            string `json:"sort_key"`
		SortKeyType        string `json:"sort_key_type"`
		BillingMode        string `json:"billing_mode"`
		ReadCapacity       int64  `json:"read_capacity"`
		WriteCapacity      int64  `json:"write_capacity"`
		DeletionProtection bool   `json:"deletion_protection"`
	}
	if err := rc.Bind(&req); err != nil {
		return nil, err
	}
	input, err := createTableInput(req)
	if err != nil {
		return nil, err
	}
	ctx, cancel := requestContext(rc.Ctx, s)
	defer cancel()
	out, err := s.client.CreateTable(ctx, input)
	if err != nil {
		return nil, ddbErr(err)
	}
	return tableDocument(out.TableDescription), nil
}

func tableDelete(rc *plugin.RequestContext) (any, error) {
	s, err := ddbSession(rc)
	if err != nil {
		return nil, err
	}
	if err := ensureWritable(s); err != nil {
		return nil, err
	}
	ctx, cancel := requestContext(rc.Ctx, s)
	defer cancel()
	_, err = s.client.DeleteTable(ctx, &awsdynamodb.DeleteTableInput{TableName: aws.String(tableName(rc))})
	return actionResult{OK: err == nil}, ddbErr(err)
}

func itemPut(rc *plugin.RequestContext) (any, error) {
	s, err := ddbSession(rc)
	if err != nil {
		return nil, err
	}
	if err := ensureWritable(s); err != nil {
		return nil, err
	}
	var req struct {
		Item                map[string]any `json:"item" validate:"required"`
		ConditionExpression string         `json:"condition_expression"`
	}
	if err := rc.Bind(&req); err != nil {
		return nil, err
	}
	item, err := marshalItem(req.Item)
	if err != nil {
		return nil, err
	}
	input := &awsdynamodb.PutItemInput{TableName: aws.String(tableName(rc)), Item: item}
	if strings.TrimSpace(req.ConditionExpression) != "" {
		input.ConditionExpression = aws.String(strings.TrimSpace(req.ConditionExpression))
	}
	ctx, cancel := requestContext(rc.Ctx, s)
	defer cancel()
	_, err = s.client.PutItem(ctx, input)
	return actionResult{OK: err == nil}, ddbErr(err)
}

func itemUpdate(rc *plugin.RequestContext) (any, error) {
	s, err := ddbSession(rc)
	if err != nil {
		return nil, err
	}
	if err := ensureWritable(s); err != nil {
		return nil, err
	}
	table, _, err := decodeItemID(rc.Param("id"))
	if err != nil {
		return nil, err
	}
	var item map[string]any
	if err := rc.Bind(&item); err != nil {
		return nil, fmt.Errorf("%w: invalid item JSON", plugin.ErrInvalidInput)
	}
	av, err := marshalItem(item)
	if err != nil {
		return nil, err
	}
	ctx, cancel := requestContext(rc.Ctx, s)
	defer cancel()
	_, err = s.client.PutItem(ctx, &awsdynamodb.PutItemInput{TableName: aws.String(table), Item: av})
	return actionResult{OK: err == nil}, ddbErr(err)
}

func itemDelete(rc *plugin.RequestContext) (any, error) {
	s, err := ddbSession(rc)
	if err != nil {
		return nil, err
	}
	if err := ensureWritable(s); err != nil {
		return nil, err
	}
	table, key, err := decodeItemID(rc.Param("id"))
	if err != nil {
		return nil, err
	}
	ctx, cancel := requestContext(rc.Ctx, s)
	defer cancel()
	_, err = s.client.DeleteItem(ctx, &awsdynamodb.DeleteItemInput{TableName: aws.String(table), Key: key})
	return actionResult{OK: err == nil}, ddbErr(err)
}

func gsiCreate(rc *plugin.RequestContext) (any, error) {
	s, err := ddbSession(rc)
	if err != nil {
		return nil, err
	}
	if err := ensureWritable(s); err != nil {
		return nil, err
	}
	var req struct {
		Name             string `json:"name" validate:"required"`
		PartitionKey     string `json:"partition_key" validate:"required"`
		PartitionKeyType string `json:"partition_key_type"`
		SortKey          string `json:"sort_key"`
		SortKeyType      string `json:"sort_key_type"`
		ProjectionType   string `json:"projection_type"`
		NonKeyAttributes string `json:"non_key_attributes"`
		BillingMode      string `json:"billing_mode"`
		ReadCapacity     int64  `json:"read_capacity"`
		WriteCapacity    int64  `json:"write_capacity"`
	}
	if err := rc.Bind(&req); err != nil {
		return nil, err
	}
	update, attrs, err := createGSIInput(req)
	if err != nil {
		return nil, err
	}
	ctx, cancel := requestContext(rc.Ctx, s)
	defer cancel()
	_, err = s.client.UpdateTable(ctx, &awsdynamodb.UpdateTableInput{
		TableName:                   aws.String(tableName(rc)),
		AttributeDefinitions:        attrs,
		GlobalSecondaryIndexUpdates: []types.GlobalSecondaryIndexUpdate{update},
	})
	return actionResult{OK: err == nil}, ddbErr(err)
}

func gsiDelete(rc *plugin.RequestContext) (any, error) {
	s, err := ddbSession(rc)
	if err != nil {
		return nil, err
	}
	if err := ensureWritable(s); err != nil {
		return nil, err
	}
	ctx, cancel := requestContext(rc.Ctx, s)
	defer cancel()
	_, err = s.client.UpdateTable(ctx, &awsdynamodb.UpdateTableInput{
		TableName: aws.String(tableName(rc)),
		GlobalSecondaryIndexUpdates: []types.GlobalSecondaryIndexUpdate{{
			Delete: &types.DeleteGlobalSecondaryIndexAction{IndexName: aws.String(rc.Param("index"))},
		}},
	})
	return actionResult{OK: err == nil}, ddbErr(err)
}

func backupCreate(rc *plugin.RequestContext) (any, error) {
	s, err := ddbSession(rc)
	if err != nil {
		return nil, err
	}
	if err := ensureWritable(s); err != nil {
		return nil, err
	}
	var req struct {
		Name string `json:"name" validate:"required"`
	}
	if err := rc.Bind(&req); err != nil {
		return nil, err
	}
	ctx, cancel := requestContext(rc.Ctx, s)
	defer cancel()
	out, err := s.client.CreateBackup(ctx, &awsdynamodb.CreateBackupInput{TableName: aws.String(tableName(rc)), BackupName: aws.String(req.Name)})
	if err != nil {
		if isUnsupportedLocal(err) {
			return nil, plugin.ErrNotSupported
		}
		return nil, ddbErr(err)
	}
	return backupDetailsDocument(out.BackupDetails), nil
}

func backupDelete(rc *plugin.RequestContext) (any, error) {
	s, err := ddbSession(rc)
	if err != nil {
		return nil, err
	}
	if err := ensureWritable(s); err != nil {
		return nil, err
	}
	ctx, cancel := requestContext(rc.Ctx, s)
	defer cancel()
	_, err = s.client.DeleteBackup(ctx, &awsdynamodb.DeleteBackupInput{BackupArn: aws.String(rc.Param("backup"))})
	if isUnsupportedLocal(err) {
		return nil, plugin.ErrNotSupported
	}
	return actionResult{OK: err == nil}, ddbErr(err)
}

func ttlUpdate(rc *plugin.RequestContext) (any, error) {
	s, err := ddbSession(rc)
	if err != nil {
		return nil, err
	}
	if err := ensureWritable(s); err != nil {
		return nil, err
	}
	var req struct {
		Enabled   bool   `json:"enabled"`
		Attribute string `json:"attribute"`
	}
	if err := rc.Bind(&req); err != nil {
		return nil, err
	}
	spec := &types.TimeToLiveSpecification{Enabled: aws.Bool(req.Enabled)}
	if req.Enabled {
		if strings.TrimSpace(req.Attribute) == "" {
			return nil, fmt.Errorf("%w: TTL attribute is required", plugin.ErrInvalidInput)
		}
		spec.AttributeName = aws.String(strings.TrimSpace(req.Attribute))
	}
	ctx, cancel := requestContext(rc.Ctx, s)
	defer cancel()
	_, err = s.client.UpdateTimeToLive(ctx, &awsdynamodb.UpdateTimeToLiveInput{TableName: aws.String(tableName(rc)), TimeToLiveSpecification: spec})
	return actionResult{OK: err == nil}, ddbErr(err)
}

func partiqlStream(rc *plugin.RequestContext, stream plugin.ClientStream) error {
	s, err := ddbSession(rc)
	if err != nil {
		return err
	}
	enc := json.NewEncoder(stream)
	dec := json.NewDecoder(stream)
	for {
		var req sqldb.QueryRequest
		if err := dec.Decode(&req); err != nil {
			return nil
		}
		result, err := executePartiQL(stream.Context(), s, req)
		params := sqldb.AuditParams(sqldb.QueryAudit{
			Query: req.Query, Statements: sqldb.SplitStatements(req.Query), Confirmed: req.Confirm,
			ReadOnlyMode: s.opts.ReadOnly, RequiresReview: statementNeedsReview(req.Query),
		})
		rc.Audit(queryAuditResult(err), params, err)
		if err != nil {
			payload := map[string]any{"error": err.Error()}
			var confirmErr confirmationError
			if errors.As(err, &confirmErr) {
				payload["requiresConfirmation"] = true
				payload["confirmMessage"] = "This DynamoDB PartiQL statement can write or delete data. Review it before running."
			}
			if err := enc.Encode(payload); err != nil {
				return err
			}
			continue
		}
		if err := enc.Encode(result); err != nil {
			return err
		}
	}
}

func executePartiQL(ctx context.Context, s *Session, req sqldb.QueryRequest) (sqldb.QueryResult, error) {
	statement := strings.TrimSpace(req.Query)
	if statement == "" {
		return sqldb.QueryResult{}, fmt.Errorf("%w: statement is empty", plugin.ErrInvalidInput)
	}
	statement, limit, err := normalizePartiQLStatement(statement, int32(s.opts.PageLimit))
	if err != nil {
		return sqldb.QueryResult{}, err
	}
	if s.opts.ReadOnly && !sqldb.IsReadOnlyStatement(statement) {
		return sqldb.QueryResult{}, fmt.Errorf("%w: read-only mode blocks write statements", plugin.ErrForbidden)
	}
	if s.opts.ConfirmWrites && !req.Confirm && statementNeedsReview(statement) {
		return sqldb.QueryResult{}, confirmationError{message: "statement requires confirmation"}
	}
	start := time.Now()
	ctx, cancel := requestContext(ctx, s)
	defer cancel()
	out, err := s.client.ExecuteStatement(ctx, &awsdynamodb.ExecuteStatementInput{
		Statement:              aws.String(statement),
		Limit:                  aws.Int32(limit),
		ReturnConsumedCapacity: types.ReturnConsumedCapacityTotal,
	})
	if err != nil {
		return sqldb.QueryResult{}, ddbErr(err)
	}
	rows := make([]row, 0, len(out.Items))
	for _, item := range out.Items {
		rows = append(rows, unmarshalItem(item))
	}
	columns, matrix := matrixRows(rows)
	if len(rows) == 0 && out.ConsumedCapacity != nil {
		rows = append(rows, row{"consumed_capacity": awsFloat64(out.ConsumedCapacity.CapacityUnits), "table": awsString(out.ConsumedCapacity.TableName)})
		columns, matrix = matrixRows(rows)
	}
	return sqldb.QueryResult{Columns: columns, Rows: matrix, RowCount: int64(len(matrix)), ElapsedMS: time.Since(start).Milliseconds(), Statement: statement}, nil
}

func completionRoute(_ *plugin.RequestContext) (any, error) {
	return []sqldb.CompletionItem{
		{Label: "SELECT", Type: "keyword", Apply: `SELECT * FROM "table"`},
		{Label: "INSERT", Type: "keyword", Apply: `INSERT INTO "table" VALUE {'pk':'id-1','name':'Ada'}`},
		{Label: "UPDATE", Type: "keyword", Apply: `UPDATE "table" SET name='Ada' WHERE pk='id-1'`},
		{Label: "DELETE", Type: "keyword", Apply: `DELETE FROM "table" WHERE pk='id-1'`},
	}, nil
}

func normalizePartiQLStatement(statement string, defaultLimit int32) (string, int32, error) {
	trimmed := strings.TrimSpace(statement)
	trimmed = strings.TrimSpace(strings.TrimSuffix(trimmed, ";"))
	start, rawLimit, ok := trailingLimitClause(trimmed)
	if !ok {
		return trimmed, defaultLimit, nil
	}
	limit64, err := strconv.ParseInt(rawLimit, 10, 32)
	if err != nil || limit64 <= 0 {
		return "", 0, fmt.Errorf("%w: LIMIT must be a positive integer", plugin.ErrInvalidInput)
	}
	return strings.TrimSpace(trimmed[:start]), int32(limit64), nil
}

func trailingLimitClause(statement string) (int, string, bool) {
	i := len(statement) - 1
	for i >= 0 && unicode.IsSpace(rune(statement[i])) {
		i--
	}
	endDigits := i + 1
	for i >= 0 && statement[i] >= '0' && statement[i] <= '9' {
		i--
	}
	if endDigits == i+1 {
		return 0, "", false
	}
	rawLimit := statement[i+1 : endDigits]
	for i >= 0 && unicode.IsSpace(rune(statement[i])) {
		i--
	}
	endWord := i + 1
	for i >= 0 && ((statement[i] >= 'a' && statement[i] <= 'z') || (statement[i] >= 'A' && statement[i] <= 'Z')) {
		i--
	}
	startWord := i + 1
	if !strings.EqualFold(statement[startWord:endWord], "limit") {
		return 0, "", false
	}
	if i >= 0 && !unicode.IsSpace(rune(statement[i])) {
		return 0, "", false
	}
	if !outsideQuotedText(statement, startWord) {
		return 0, "", false
	}
	return startWord, rawLimit, true
}

func outsideQuotedText(statement string, index int) bool {
	inSingle, inDouble := false, false
	for i := 0; i < index; i++ {
		switch statement[i] {
		case '\'':
			if inSingle && i+1 < index && statement[i+1] == '\'' {
				i++
				continue
			}
			if !inDouble {
				inSingle = !inSingle
			}
		case '"':
			if inDouble && i+1 < index && statement[i+1] == '"' {
				i++
				continue
			}
			if !inSingle {
				inDouble = !inDouble
			}
		}
	}
	return !inSingle && !inDouble
}

func ensureWritable(s *Session) error {
	if s.opts.ReadOnly {
		return fmt.Errorf("%w: connection is read-only", plugin.ErrForbidden)
	}
	return nil
}

func createTableInput(req struct {
	Name               string `json:"name" validate:"required"`
	PartitionKey       string `json:"partition_key" validate:"required"`
	PartitionKeyType   string `json:"partition_key_type"`
	SortKey            string `json:"sort_key"`
	SortKeyType        string `json:"sort_key_type"`
	BillingMode        string `json:"billing_mode"`
	ReadCapacity       int64  `json:"read_capacity"`
	WriteCapacity      int64  `json:"write_capacity"`
	DeletionProtection bool   `json:"deletion_protection"`
},
) (*awsdynamodb.CreateTableInput, error) {
	name := strings.TrimSpace(req.Name)
	if name == "" {
		return nil, fmt.Errorf("%w: table name is required", plugin.ErrInvalidInput)
	}
	pk := strings.TrimSpace(req.PartitionKey)
	sk := strings.TrimSpace(req.SortKey)
	attrs := []types.AttributeDefinition{{AttributeName: aws.String(pk), AttributeType: scalarType(req.PartitionKeyType)}}
	keys := []types.KeySchemaElement{{AttributeName: aws.String(pk), KeyType: types.KeyTypeHash}}
	if sk != "" {
		attrs = append(attrs, types.AttributeDefinition{AttributeName: aws.String(sk), AttributeType: scalarType(req.SortKeyType)})
		keys = append(keys, types.KeySchemaElement{AttributeName: aws.String(sk), KeyType: types.KeyTypeRange})
	}
	input := &awsdynamodb.CreateTableInput{
		TableName:                 aws.String(name),
		AttributeDefinitions:      attrs,
		KeySchema:                 keys,
		BillingMode:               billingMode(req.BillingMode),
		DeletionProtectionEnabled: aws.Bool(req.DeletionProtection),
	}
	if input.BillingMode == types.BillingModeProvisioned {
		input.ProvisionedThroughput = throughput(req.ReadCapacity, req.WriteCapacity)
	}
	return input, nil
}

func createGSIInput(req struct {
	Name             string `json:"name" validate:"required"`
	PartitionKey     string `json:"partition_key" validate:"required"`
	PartitionKeyType string `json:"partition_key_type"`
	SortKey          string `json:"sort_key"`
	SortKeyType      string `json:"sort_key_type"`
	ProjectionType   string `json:"projection_type"`
	NonKeyAttributes string `json:"non_key_attributes"`
	BillingMode      string `json:"billing_mode"`
	ReadCapacity     int64  `json:"read_capacity"`
	WriteCapacity    int64  `json:"write_capacity"`
},
) (types.GlobalSecondaryIndexUpdate, []types.AttributeDefinition, error) {
	name := strings.TrimSpace(req.Name)
	pk := strings.TrimSpace(req.PartitionKey)
	if name == "" || pk == "" {
		return types.GlobalSecondaryIndexUpdate{}, nil, fmt.Errorf("%w: index name and partition key are required", plugin.ErrInvalidInput)
	}
	attrs := []types.AttributeDefinition{{AttributeName: aws.String(pk), AttributeType: scalarType(req.PartitionKeyType)}}
	keys := []types.KeySchemaElement{{AttributeName: aws.String(pk), KeyType: types.KeyTypeHash}}
	if sk := strings.TrimSpace(req.SortKey); sk != "" {
		attrs = append(attrs, types.AttributeDefinition{AttributeName: aws.String(sk), AttributeType: scalarType(req.SortKeyType)})
		keys = append(keys, types.KeySchemaElement{AttributeName: aws.String(sk), KeyType: types.KeyTypeRange})
	}
	create := &types.CreateGlobalSecondaryIndexAction{
		IndexName:  aws.String(name),
		KeySchema:  keys,
		Projection: projection(req.ProjectionType, req.NonKeyAttributes),
	}
	if billingMode(req.BillingMode) == types.BillingModeProvisioned {
		create.ProvisionedThroughput = throughput(req.ReadCapacity, req.WriteCapacity)
	}
	return types.GlobalSecondaryIndexUpdate{Create: create}, attrs, nil
}

func describeTable(ctx context.Context, s *Session, table string) (*types.TableDescription, error) {
	out, err := s.client.DescribeTable(ctx, &awsdynamodb.DescribeTableInput{TableName: aws.String(table)})
	if err != nil {
		return nil, ddbErr(err)
	}
	if out.Table == nil {
		return nil, plugin.ErrNotFound
	}
	return out.Table, nil
}

func tableSummary(desc *types.TableDescription) row {
	return row{
		"name":         awsString(desc.TableName),
		"arn":          awsString(desc.TableArn),
		"status":       string(desc.TableStatus),
		"billing_mode": billingModeText(desc),
		"items":        awsInt64(desc.ItemCount),
		"size":         awsInt64(desc.TableSizeBytes),
		"created":      desc.CreationDateTime,
	}
}

func tableDocument(desc *types.TableDescription) row {
	doc := tableSummary(desc)
	doc["key_schema"] = keySchemaText(desc.KeySchema)
	doc["attribute_definitions"] = attributeDefinitionRows(desc.AttributeDefinitions)
	doc["provisioned_throughput"] = desc.ProvisionedThroughput
	doc["deletion_protection"] = awsBool(desc.DeletionProtectionEnabled)
	doc["stream_arn"] = awsString(desc.LatestStreamArn)
	doc["stream_label"] = awsString(desc.LatestStreamLabel)
	doc["global_secondary_indexes"] = indexRows(desc)
	doc["local_secondary_indexes"] = localIndexRows(desc)
	doc["replicas"] = desc.Replicas
	doc["restore_summary"] = desc.RestoreSummary
	doc["sse_description"] = desc.SSEDescription
	return doc
}

func indexRows(desc *types.TableDescription) []row {
	rows := []row{}
	for _, idx := range desc.GlobalSecondaryIndexes {
		name := awsString(idx.IndexName)
		rows = append(rows, row{
			"name":       name,
			"table":      awsString(desc.TableName),
			"kind":       "global",
			"status":     string(idx.IndexStatus),
			"key_schema": keySchemaText(idx.KeySchema),
			"projection": string(idx.Projection.ProjectionType),
			"items":      awsInt64(idx.ItemCount),
			"size":       awsInt64(idx.IndexSizeBytes),
			"ref":        plugin.ResourceIdentity{Kind: "index", Namespace: awsString(desc.TableName), Name: name, UID: awsString(desc.TableName) + "." + name},
		})
	}
	for _, idx := range desc.LocalSecondaryIndexes {
		name := awsString(idx.IndexName)
		rows = append(rows, row{
			"name":       name,
			"table":      awsString(desc.TableName),
			"kind":       "local",
			"status":     "ACTIVE",
			"key_schema": keySchemaText(idx.KeySchema),
			"projection": string(idx.Projection.ProjectionType),
			"items":      awsInt64(idx.ItemCount),
			"size":       awsInt64(idx.IndexSizeBytes),
			"ref":        plugin.ResourceIdentity{Kind: "index", Namespace: awsString(desc.TableName), Name: name, UID: awsString(desc.TableName) + "." + name},
		})
	}
	return rows
}

func localIndexRows(desc *types.TableDescription) []row {
	rows := []row{}
	for _, idx := range desc.LocalSecondaryIndexes {
		rows = append(rows, row{"name": awsString(idx.IndexName), "key_schema": keySchemaText(idx.KeySchema), "projection": string(idx.Projection.ProjectionType)})
	}
	return rows
}

func backupDocument(desc *types.BackupDescription) row {
	if desc == nil {
		return row{}
	}
	details := desc.BackupDetails
	source := desc.SourceTableDetails
	return row{
		"name":          awsString(details.BackupName),
		"arn":           awsString(details.BackupArn),
		"status":        string(details.BackupStatus),
		"type":          string(details.BackupType),
		"size":          awsInt64(details.BackupSizeBytes),
		"created":       details.BackupCreationDateTime,
		"expires":       details.BackupExpiryDateTime,
		"table":         awsString(source.TableName),
		"table_arn":     awsString(source.TableArn),
		"billing_mode":  string(source.BillingMode),
		"key_schema":    keySchemaText(source.KeySchema),
		"item_count":    awsInt64(source.ItemCount),
		"table_created": source.TableCreationDateTime,
	}
}

func backupDetailsDocument(details *types.BackupDetails) row {
	if details == nil {
		return row{}
	}
	return row{
		"name":    awsString(details.BackupName),
		"arn":     awsString(details.BackupArn),
		"status":  string(details.BackupStatus),
		"type":    string(details.BackupType),
		"size":    awsInt64(details.BackupSizeBytes),
		"created": details.BackupCreationDateTime,
		"expires": details.BackupExpiryDateTime,
	}
}

func matrixRows(rows []row) ([]string, [][]any) {
	colset := map[string]bool{}
	for _, item := range rows {
		for key := range item {
			if key != "ref" {
				colset[key] = true
			}
		}
	}
	columns := make([]string, 0, len(colset))
	for key := range colset {
		columns = append(columns, key)
	}
	sort.Strings(columns)
	matrix := make([][]any, 0, len(rows))
	for _, item := range rows {
		line := make([]any, len(columns))
		for i, key := range columns {
			line[i] = item[key]
		}
		matrix = append(matrix, line)
	}
	return columns, matrix
}

func keySchemaText(keys []types.KeySchemaElement) string {
	parts := make([]string, 0, len(keys))
	for _, key := range keys {
		parts = append(parts, strings.ToLower(string(key.KeyType))+":"+awsString(key.AttributeName))
	}
	return strings.Join(parts, ", ")
}

func attributeDefinitionRows(attrs []types.AttributeDefinition) []row {
	rows := make([]row, 0, len(attrs))
	for _, attr := range attrs {
		rows = append(rows, row{"name": awsString(attr.AttributeName), "type": string(attr.AttributeType)})
	}
	return rows
}

func projection(kind, rawAttributes string) *types.Projection {
	pt := types.ProjectionType(strings.TrimSpace(kind))
	if pt == "" {
		pt = types.ProjectionTypeAll
	}
	p := &types.Projection{ProjectionType: pt}
	if pt == types.ProjectionTypeInclude {
		p.NonKeyAttributes = splitCSV(rawAttributes)
	}
	return p
}

func scalarType(raw string) types.ScalarAttributeType {
	switch strings.ToUpper(strings.TrimSpace(raw)) {
	case "N":
		return types.ScalarAttributeTypeN
	case "B":
		return types.ScalarAttributeTypeB
	default:
		return types.ScalarAttributeTypeS
	}
}

func billingMode(raw string) types.BillingMode {
	if strings.EqualFold(strings.TrimSpace(raw), "PROVISIONED") {
		return types.BillingModeProvisioned
	}
	return types.BillingModePayPerRequest
}

func billingModeText(desc *types.TableDescription) string {
	if desc.BillingModeSummary != nil && desc.BillingModeSummary.BillingMode != "" {
		return string(desc.BillingModeSummary.BillingMode)
	}
	return string(types.BillingModeProvisioned)
}

func throughput(read, write int64) *types.ProvisionedThroughput {
	if read <= 0 {
		read = 5
	}
	if write <= 0 {
		write = 5
	}
	return &types.ProvisionedThroughput{ReadCapacityUnits: aws.Int64(read), WriteCapacityUnits: aws.Int64(write)}
}

func splitCSV(raw string) []string {
	parts := strings.Split(raw, ",")
	out := []string{}
	for _, part := range parts {
		if value := strings.TrimSpace(part); value != "" {
			out = append(out, value)
		}
	}
	return out
}

func tableName(rc *plugin.RequestContext) string {
	if table := strings.TrimSpace(rc.Param("table")); table != "" {
		return table
	}
	return strings.TrimSpace(rc.Query().Get("p.table"))
}

func requestContext(ctx context.Context, s *Session) (context.Context, context.CancelFunc) {
	return context.WithTimeout(ctx, s.opts.Timeout)
}

func statementNeedsReview(statement string) bool {
	return !sqldb.IsReadOnlyStatement(statement) || sqldb.IsDestructiveStatement(statement)
}

func queryAuditResult(err error) plugin.AuditResult {
	if err == nil {
		return plugin.AuditAllowed
	}
	var confirmErr confirmationError
	if errors.As(err, &confirmErr) {
		return plugin.AuditDenied
	}
	return plugin.AuditError
}

func encodeKeyCursor(key map[string]types.AttributeValue) (string, error) {
	if len(key) == 0 {
		return "", nil
	}
	raw := map[string]any{}
	for name, av := range key {
		raw[name] = avToDDBJSON(av)
	}
	data, err := json.Marshal(raw)
	if err != nil {
		return "", err
	}
	return url.QueryEscape(string(data)), nil
}

func decodeKeyCursor(cursor string) (map[string]types.AttributeValue, error) {
	cursor = strings.TrimSpace(cursor)
	if cursor == "" {
		return nil, nil
	}
	data, err := url.QueryUnescape(cursor)
	if err != nil {
		return nil, fmt.Errorf("%w: invalid cursor", plugin.ErrInvalidInput)
	}
	var raw map[string]any
	if err := json.Unmarshal([]byte(data), &raw); err != nil {
		return nil, fmt.Errorf("%w: invalid cursor", plugin.ErrInvalidInput)
	}
	out := map[string]types.AttributeValue{}
	for name, value := range raw {
		av, err := jsonToAV(value)
		if err != nil {
			return nil, err
		}
		out[name] = av
	}
	return out, nil
}

func sortRows(rows []row, key string) {
	sort.Slice(rows, func(i, j int) bool {
		return strings.ToLower(fmt.Sprint(rows[i][key])) < strings.ToLower(fmt.Sprint(rows[j][key]))
	})
}

func mergeRows(dst row, src row) {
	for key, value := range src {
		dst[key] = value
	}
}

func ddbErr(err error) error {
	if err == nil {
		return nil
	}
	var api smithy.APIError
	if errors.As(err, &api) {
		message := strings.TrimSpace(api.ErrorMessage())
		if message == "" {
			message = api.ErrorCode()
		}
		switch api.ErrorCode() {
		case "ResourceNotFoundException", "BackupNotFoundException":
			return fmt.Errorf("%w: %s", plugin.ErrNotFound, message)
		case "AccessDeniedException", "UnrecognizedClientException", "IncompleteSignatureException":
			return fmt.Errorf("%w: %s", plugin.ErrUnauthorized, message)
		case "ValidationException", "ConditionalCheckFailedException", "ResourceInUseException", "LimitExceededException":
			return fmt.Errorf("%w: %s", plugin.ErrInvalidInput, message)
		default:
			return fmt.Errorf("%w: %s", plugin.ErrUnavailable, message)
		}
	}
	return err
}

func isUnsupportedLocal(err error) bool {
	if err == nil {
		return false
	}
	text := strings.ToLower(err.Error())
	return strings.Contains(text, "not currently supported in dynamodb local") ||
		strings.Contains(text, "an unknown operation was requested")
}

func awsString(v *string) string {
	if v == nil {
		return ""
	}
	return *v
}

func awsInt64(v *int64) int64 {
	if v == nil {
		return 0
	}
	return *v
}

func awsFloat64(v *float64) float64 {
	if v == nil {
		return 0
	}
	return *v
}

func awsBool(v *bool) bool {
	return v != nil && *v
}

func intPtr(v int) *int { return &v }
