package s3compat

import (
	"context"
	"errors"
	"fmt"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awss3 "github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	smithy "github.com/aws/smithy-go"

	"github.com/charlesng35/shellcn/sdk/plugin"
)

const (
	minPresignExpiry     = 1 * time.Second
	maxPresignExpiry     = 7 * 24 * time.Hour // S3 SigV4 presign hard limit
	defaultPresignExpiry = 15 * time.Minute
)

// bucketNameRe enforces the S3 bucket naming rules common to AWS and MinIO:
// 3-63 chars, lowercase letters/digits/hyphens/dots, starting and ending with a
// letter or digit. It deliberately rejects uppercase and underscores so a name
// is portable across both backends.
var bucketNameRe = regexp.MustCompile(`^[a-z0-9][a-z0-9.-]{1,61}[a-z0-9]$`)

func icon(name string) plugin.Icon {
	return plugin.Icon{Type: plugin.IconLucide, Value: name}
}

func routeID(protocol, suffix string) string {
	return protocol + "." + suffix
}

// BucketTab is the bucket-administration tab: a table of buckets with create,
// delete, and versioning affordances. It coexists with the file browser tab so
// the object browser stays scoped to the connection's bucket while admins manage
// the account's buckets here.
func BucketTab(protocol string) plugin.Panel {
	return plugin.Panel{
		Key: "buckets", Label: "Buckets", Icon: icon("database"),
		Type:   plugin.PanelTable,
		Source: &plugin.DataSource{RouteID: routeID(protocol, "buckets.list")},
		Config: plugin.TableConfig{
			Columns: bucketColumns(),
			ActionIDs: []string{
				routeID(protocol, "bucket.create"),
			},
			RowActionIDs: []string{
				routeID(protocol, "bucket.versioning.set"),
				routeID(protocol, "bucket.delete"),
			},
			Exportable: true,
			EmptyText:  "No buckets.",
		},
	}
}

func bucketColumns() []plugin.Column {
	return []plugin.Column{
		{Key: "name", Label: "Name", Sortable: true},
		{Key: "region", Label: "Region", Sortable: true},
		{Key: "createdAt", Label: "Created", Type: plugin.ColumnDateTime, Sortable: true},
	}
}

func Actions(protocol string) []plugin.Action {
	return []plugin.Action{
		{ID: routeID(protocol, "bucket.create"), Label: "Create bucket", Icon: icon("plus"), RouteID: routeID(protocol, "bucket.create")},
		{ID: routeID(protocol, "bucket.delete"), Label: "Delete", Icon: icon("trash-2"), RouteID: routeID(protocol, "bucket.delete"), Params: bucketParams(), Confirm: true, ConfirmText: "Delete this bucket? The bucket must be empty."},
		{ID: routeID(protocol, "bucket.versioning.set"), Label: "Set versioning", Icon: icon("history"), RouteID: routeID(protocol, "bucket.versioning.set"), Params: bucketParams()},
	}
}

func bucketParams() map[string]string {
	return map[string]string{"bucket": "${resource.name}"}
}

func AdminRoutes(protocol string) []plugin.Route {
	return []plugin.Route{
		{ID: routeID(protocol, "buckets.list"), Method: plugin.MethodGet, Path: "/buckets", Permission: protocol + ".buckets.read", Risk: plugin.RiskSafe, AuditEvent: routeID(protocol, "buckets.list"), Handle: listBuckets},
		{ID: routeID(protocol, "bucket.create"), Method: plugin.MethodPost, Path: "/buckets", Permission: protocol + ".buckets.write", Risk: plugin.RiskWrite, AuditEvent: routeID(protocol, "bucket.create"), Input: bucketCreateSchema(), Handle: createBucket},
		{ID: routeID(protocol, "bucket.delete"), Method: plugin.MethodDelete, Path: "/buckets/{bucket}", Permission: protocol + ".buckets.delete", Risk: plugin.RiskDestructive, AuditEvent: routeID(protocol, "bucket.delete"), Handle: deleteBucket},
		{ID: routeID(protocol, "bucket.versioning"), Method: plugin.MethodGet, Path: "/buckets/{bucket}/versioning", Permission: protocol + ".buckets.read", Risk: plugin.RiskSafe, AuditEvent: routeID(protocol, "bucket.versioning"), Handle: getVersioning},
		{ID: routeID(protocol, "bucket.versioning.set"), Method: plugin.MethodPut, Path: "/buckets/{bucket}/versioning", Permission: protocol + ".buckets.write", Risk: plugin.RiskWrite, AuditEvent: routeID(protocol, "bucket.versioning.set"), Input: versioningSchema(), Handle: setVersioning},
		{ID: routeID(protocol, "bucket.versions"), Method: plugin.MethodGet, Path: "/buckets/{bucket}/versions", Permission: protocol + ".buckets.read", Risk: plugin.RiskSafe, AuditEvent: routeID(protocol, "bucket.versions"), Handle: listVersions},
		{ID: routeID(protocol, "object.presign"), Method: plugin.MethodGet, Path: "/presign/{path}", Permission: protocol + ".files.read", Risk: plugin.RiskSafe, AuditEvent: routeID(protocol, "object.presign"), Handle: presignObject},
	}
}

func bucketCreateSchema() *plugin.Schema {
	return &plugin.Schema{Groups: []plugin.Group{{Name: "Bucket", Fields: []plugin.Field{
		{Key: "name", Label: "Bucket name", Type: plugin.FieldText, Required: true, Placeholder: "my-bucket", Help: "Lowercase letters, numbers, dots and hyphens; 3-63 characters."},
		{Key: "region", Label: "Region", Type: plugin.FieldText, Placeholder: "us-east-1"},
	}}}}
}

func versioningSchema() *plugin.Schema {
	return &plugin.Schema{Groups: []plugin.Group{{Name: "Versioning", Fields: []plugin.Field{
		{Key: "status", Label: "Versioning", Type: plugin.FieldSelect, Required: true, Options: []plugin.Option{
			{Label: "Enabled", Value: string(types.BucketVersioningStatusEnabled)},
			{Label: "Suspended", Value: string(types.BucketVersioningStatusSuspended)},
		}},
	}}}}
}

type bucketEntry struct {
	Name      string    `json:"name"`
	Region    string    `json:"region,omitempty"`
	CreatedAt time.Time `json:"createdAt,omitzero"`
}

type versioningState struct {
	Status string `json:"status"`
}

type objectVersionEntry struct {
	Key          string    `json:"key"`
	VersionID    string    `json:"versionId"`
	IsLatest     bool      `json:"isLatest"`
	DeleteMarker bool      `json:"deleteMarker,omitempty"`
	Size         int64     `json:"size,omitempty"`
	ModTime      time.Time `json:"modTime,omitzero"`
}

func listBuckets(rc *plugin.RequestContext) (any, error) {
	c, err := adminClient(rc)
	if err != nil {
		return nil, err
	}
	out, err := c.s3.ListBuckets(rc.Ctx, &awss3.ListBucketsInput{})
	if err != nil {
		return nil, mapAdminError(err)
	}
	items := make([]bucketEntry, 0, len(out.Buckets))
	for _, b := range out.Buckets {
		entry := bucketEntry{Name: aws.ToString(b.Name), Region: aws.ToString(b.BucketRegion)}
		if b.CreationDate != nil {
			entry.CreatedAt = *b.CreationDate
		}
		items = append(items, entry)
	}
	return plugin.Page[bucketEntry]{Items: items}, nil
}

type bucketCreateRequest struct {
	Name   string `json:"name" validate:"required"`
	Region string `json:"region"`
}

func createBucket(rc *plugin.RequestContext) (any, error) {
	c, err := adminClient(rc)
	if err != nil {
		return nil, err
	}
	var req bucketCreateRequest
	if err := rc.Bind(&req); err != nil {
		return nil, err
	}
	name, err := validateBucketName(req.Name)
	if err != nil {
		return nil, err
	}
	region := strings.TrimSpace(req.Region)
	if region == "" {
		region = c.region
	}
	_, err = c.s3.CreateBucket(rc.Ctx, createBucketInput(name, region))
	if err != nil {
		return nil, mapAdminError(err)
	}
	return bucketEntry{Name: name, Region: region}, nil
}

// createBucketInput builds the CreateBucket request, attaching a location
// constraint for every region except us-east-1 (the API rejects an explicit
// constraint for the default region on AWS).
func createBucketInput(name, region string) *awss3.CreateBucketInput {
	in := &awss3.CreateBucketInput{Bucket: aws.String(name)}
	if region != "" && region != "us-east-1" {
		in.CreateBucketConfiguration = &types.CreateBucketConfiguration{
			LocationConstraint: types.BucketLocationConstraint(region),
		}
	}
	return in
}

func deleteBucket(rc *plugin.RequestContext) (any, error) {
	c, err := adminClient(rc)
	if err != nil {
		return nil, err
	}
	name, err := validateBucketName(rc.Param("bucket"))
	if err != nil {
		return nil, err
	}
	_, err = c.s3.DeleteBucket(rc.Ctx, &awss3.DeleteBucketInput{Bucket: aws.String(name)})
	if err != nil {
		return nil, mapAdminError(err)
	}
	return map[string]bool{"ok": true}, nil
}

func getVersioning(rc *plugin.RequestContext) (any, error) {
	c, err := adminClient(rc)
	if err != nil {
		return nil, err
	}
	name, err := validateBucketName(rc.Param("bucket"))
	if err != nil {
		return nil, err
	}
	out, err := c.s3.GetBucketVersioning(rc.Ctx, &awss3.GetBucketVersioningInput{Bucket: aws.String(name)})
	if err != nil {
		return nil, mapAdminError(err)
	}
	status := string(out.Status)
	if status == "" {
		status = "Disabled"
	}
	return versioningState{Status: status}, nil
}

type versioningRequest struct {
	Status string `json:"status" validate:"required"`
}

func setVersioning(rc *plugin.RequestContext) (any, error) {
	c, err := adminClient(rc)
	if err != nil {
		return nil, err
	}
	name, err := validateBucketName(rc.Param("bucket"))
	if err != nil {
		return nil, err
	}
	var req versioningRequest
	if err := rc.Bind(&req); err != nil {
		return nil, err
	}
	status, err := validateVersioningStatus(req.Status)
	if err != nil {
		return nil, err
	}
	_, err = c.s3.PutBucketVersioning(rc.Ctx, &awss3.PutBucketVersioningInput{
		Bucket:                  aws.String(name),
		VersioningConfiguration: &types.VersioningConfiguration{Status: status},
	})
	if err != nil {
		return nil, mapAdminError(err)
	}
	return versioningState{Status: string(status)}, nil
}

func listVersions(rc *plugin.RequestContext) (any, error) {
	c, err := adminClient(rc)
	if err != nil {
		return nil, err
	}
	name, err := validateBucketName(rc.Param("bucket"))
	if err != nil {
		return nil, err
	}
	in := &awss3.ListObjectVersionsInput{Bucket: aws.String(name)}
	if prefix := strings.TrimSpace(rc.Query().Get("prefix")); prefix != "" {
		in.Prefix = aws.String(prefix)
	}
	items, err := collectVersions(rc.Ctx, c.s3, in)
	if err != nil {
		return nil, mapAdminError(err)
	}
	return plugin.Page[objectVersionEntry]{Items: items}, nil
}

func collectVersions(ctx context.Context, api *awss3.Client, in *awss3.ListObjectVersionsInput) ([]objectVersionEntry, error) {
	pager := awss3.NewListObjectVersionsPaginator(api, in)
	var items []objectVersionEntry
	for pager.HasMorePages() {
		page, err := pager.NextPage(ctx)
		if err != nil {
			return nil, err
		}
		for _, v := range page.Versions {
			entry := objectVersionEntry{
				Key:       aws.ToString(v.Key),
				VersionID: aws.ToString(v.VersionId),
				IsLatest:  aws.ToBool(v.IsLatest),
				Size:      aws.ToInt64(v.Size),
			}
			if v.LastModified != nil {
				entry.ModTime = *v.LastModified
			}
			items = append(items, entry)
		}
		for _, m := range page.DeleteMarkers {
			entry := objectVersionEntry{
				Key:          aws.ToString(m.Key),
				VersionID:    aws.ToString(m.VersionId),
				IsLatest:     aws.ToBool(m.IsLatest),
				DeleteMarker: true,
			}
			if m.LastModified != nil {
				entry.ModTime = *m.LastModified
			}
			items = append(items, entry)
		}
	}
	return items, nil
}

func presignObject(rc *plugin.RequestContext) (any, error) {
	c, err := adminClient(rc)
	if err != nil {
		return nil, err
	}
	key, err := presignKey(c, rc.Param("path"))
	if err != nil {
		return nil, err
	}
	expiry, err := parseExpiry(rc.Query().Get("expiry"))
	if err != nil {
		return nil, err
	}
	method := strings.ToLower(strings.TrimSpace(rc.Query().Get("method")))
	expires := awss3.WithPresignExpires(expiry)
	switch method {
	case "", "get", "download":
		req, err := c.presign.PresignGetObject(rc.Ctx, &awss3.GetObjectInput{Bucket: aws.String(c.bucket), Key: aws.String(key)}, expires)
		if err != nil {
			return nil, mapAdminError(err)
		}
		return presignResult(req.URL, "GET", expiry), nil
	default:
		return nil, fmt.Errorf("%w: method must be get", plugin.ErrInvalidInput)
	}
}

func presignResult(url, method string, expiry time.Duration) map[string]any {
	return map[string]any{
		"url":       url,
		"method":    method,
		"expiresIn": int(expiry.Seconds()),
	}
}

// adminClient unwraps the request's session to the bucket-scoped client. Mirrors
// the borrowed-handle unwrap used across the shared plugins.
func adminClient(rc *plugin.RequestContext) (*Client, error) {
	if s, ok := rc.Session.(*Session); ok {
		return s.fs, nil
	}
	if h, ok := rc.Session.(interface{ Session() plugin.Session }); ok {
		if s, ok := h.Session().(*Session); ok {
			return s.fs, nil
		}
	}
	return nil, fmt.Errorf("%w: object store session unavailable", plugin.ErrUnavailable)
}

func validateBucketName(raw string) (string, error) {
	name := strings.TrimSpace(raw)
	if name == "" {
		return "", fmt.Errorf("%w: bucket is required", plugin.ErrInvalidInput)
	}
	if !bucketNameRe.MatchString(name) || strings.Contains(name, "..") {
		return "", fmt.Errorf("%w: invalid bucket name", plugin.ErrInvalidInput)
	}
	return name, nil
}

func validateVersioningStatus(raw string) (types.BucketVersioningStatus, error) {
	switch strings.ToLower(strings.TrimSpace(raw)) {
	case "enabled":
		return types.BucketVersioningStatusEnabled, nil
	case "suspended":
		return types.BucketVersioningStatusSuspended, nil
	default:
		return "", fmt.Errorf("%w: versioning status must be Enabled or Suspended", plugin.ErrInvalidInput)
	}
}

// parseExpiry reads a presign expiry in seconds, applying the default when blank
// and clamping to the SigV4-allowed window.
func parseExpiry(raw string) (time.Duration, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return defaultPresignExpiry, nil
	}
	secs, err := strconv.Atoi(raw)
	if err != nil || secs <= 0 {
		return 0, fmt.Errorf("%w: expiry must be a positive number of seconds", plugin.ErrInvalidInput)
	}
	d := time.Duration(secs) * time.Second
	if d < minPresignExpiry {
		d = minPresignExpiry
	}
	if d > maxPresignExpiry {
		d = maxPresignExpiry
	}
	return d, nil
}

// presignKey resolves a browser path to the bucket-scoped object key, rejecting
// empty or directory paths (a presigned URL targets a single object).
func presignKey(c *Client, raw string) (string, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" || raw == "." || raw == "/" || strings.HasSuffix(raw, "/") || strings.ContainsRune(raw, 0) {
		return "", fmt.Errorf("%w: object path is required", plugin.ErrInvalidInput)
	}
	key := c.key(raw)
	if key == "" || strings.HasSuffix(key, "/") {
		return "", fmt.Errorf("%w: object path is required", plugin.ErrInvalidInput)
	}
	return key, nil
}

func mapAdminError(err error) error {
	if err == nil {
		return nil
	}
	if isNotFound(err) {
		return fmt.Errorf("%w: %v", plugin.ErrNotFound, err)
	}
	var apiErr smithy.APIError
	if errors.As(err, &apiErr) {
		switch apiErr.ErrorCode() {
		case "AccessDenied", "InvalidAccessKeyId", "SignatureDoesNotMatch":
			return fmt.Errorf("%w: %s", plugin.ErrForbidden, apiErr.ErrorMessage())
		case "BucketAlreadyExists", "BucketAlreadyOwnedByYou", "BucketNotEmpty":
			return fmt.Errorf("%w: %s", plugin.ErrConflict, apiErr.ErrorMessage())
		case "InvalidBucketName", "InvalidArgument":
			return fmt.Errorf("%w: %s", plugin.ErrInvalidInput, apiErr.ErrorMessage())
		}
	}
	return fmt.Errorf("%w: %v", plugin.ErrUnavailable, err)
}
