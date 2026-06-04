// Package s3compat adapts S3-compatible object storage to ShellCN's shared file
// browser contract.
package s3compat

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"path"
	"sort"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/credentials"
	transfermanager "github.com/aws/aws-sdk-go-v2/feature/s3/transfermanager"
	awss3 "github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	smithy "github.com/aws/smithy-go"

	"github.com/charlesng35/shellcn-contrib/shared/filesystem"
	"github.com/charlesng35/shellcn/sdk/plugin"
)

type Options struct {
	Endpoint      string
	Region        string
	Bucket        string
	Prefix        string
	AccessKeyID   string
	SecretKey     string
	SessionToken  string
	UsePathStyle  bool
	VerifyTLS     bool
	RequireBucket bool
}

type Session struct {
	fs *Client
}

func (s *Session) Filesystem() (filesystem.Client, error) {
	return s.fs, nil
}

func (s *Session) HealthCheck(ctx context.Context) error {
	_, err := s.fs.s3.HeadBucket(ctx, &awss3.HeadBucketInput{Bucket: aws.String(s.fs.bucket)})
	return err
}

func (s *Session) OpenChannel(context.Context, plugin.ChannelRequest) (plugin.Channel, error) {
	return nil, plugin.ErrNotSupported
}

func (s *Session) Close() error {
	return nil
}

type Client struct {
	s3       *awss3.Client
	presign  *awss3.PresignClient
	uploader *transfermanager.Client
	region   string
	bucket   string
	prefix   string
}

func Connect(_ context.Context, cfg plugin.ConnectConfig, opts Options) (plugin.Session, error) {
	if err := normalizeOptions(cfg, &opts); err != nil {
		return nil, err
	}
	httpClient := &http.Client{Transport: &http.Transport{
		DialContext: cfg.Net.DialContext,
		TLSClientConfig: &tls.Config{
			ServerName:         endpointHost(opts.Endpoint),
			InsecureSkipVerify: !opts.VerifyTLS,
		},
	}}
	s3opts := awss3.Options{
		Region:       opts.Region,
		Credentials:  aws.NewCredentialsCache(credentials.NewStaticCredentialsProvider(opts.AccessKeyID, opts.SecretKey, opts.SessionToken)),
		HTTPClient:   httpClient,
		UsePathStyle: opts.UsePathStyle,
	}
	if opts.Endpoint != "" {
		s3opts.BaseEndpoint = aws.String(opts.Endpoint)
	}
	api := awss3.New(s3opts)
	return &Session{fs: &Client{
		s3:       api,
		presign:  awss3.NewPresignClient(api),
		uploader: transfermanager.New(api),
		region:   opts.Region,
		bucket:   opts.Bucket,
		prefix:   normalizePrefix(opts.Prefix),
	}}, nil
}

func normalizeOptions(cfg plugin.ConnectConfig, opts *Options) error {
	if opts.Endpoint == "" {
		opts.Endpoint = strings.TrimSpace(cfg.String("endpoint"))
	}
	if opts.Endpoint != "" {
		u, err := url.Parse(opts.Endpoint)
		if err != nil || u.Scheme == "" || u.Host == "" {
			return fmt.Errorf("%w: endpoint must be an absolute URL", plugin.ErrInvalidInput)
		}
		if u.Scheme != "http" && u.Scheme != "https" {
			return fmt.Errorf("%w: endpoint scheme must be http or https", plugin.ErrInvalidInput)
		}
	}
	if opts.Region == "" {
		opts.Region = strings.TrimSpace(cfg.String("region"))
	}
	if opts.Region == "" {
		opts.Region = "us-east-1"
	}
	if opts.Bucket == "" {
		opts.Bucket = strings.TrimSpace(cfg.String("bucket"))
	}
	if opts.Bucket == "" && opts.RequireBucket {
		return fmt.Errorf("%w: bucket is required", plugin.ErrInvalidInput)
	}
	if opts.Prefix == "" {
		opts.Prefix = strings.TrimSpace(cfg.String("prefix"))
	}
	opts.AccessKeyID = strings.TrimSpace(cfg.String("access_key_id"))
	opts.SecretKey = cfg.String("secret_access_key")
	opts.SessionToken = cfg.String("session_token")
	auth := strings.TrimSpace(cfg.String("auth"))
	if auth == "" {
		auth = "access_key"
	}
	switch auth {
	case "access_key":
	case "credential":
		if identity := cfg.CredentialIdentityFor(plugin.CredentialField); identity != "" {
			opts.AccessKeyID = identity
		}
		if secret := cfg.CredentialSecretFor(plugin.CredentialField); secret != "" {
			opts.SecretKey = secret
		}
	default:
		return fmt.Errorf("%w: unsupported authentication method %q", plugin.ErrInvalidInput, auth)
	}
	if opts.AccessKeyID == "" {
		return fmt.Errorf("%w: access key id is required", plugin.ErrInvalidInput)
	}
	if opts.SecretKey == "" {
		return fmt.Errorf("%w: secret access key is required", plugin.ErrInvalidInput)
	}
	opts.VerifyTLS = boolValue(cfg, "verify_tls", opts.VerifyTLS)
	opts.UsePathStyle = boolValue(cfg, "path_style", opts.UsePathStyle)
	return nil
}

func AuthFields(protocol string) []plugin.Field {
	staticCredentials := &plugin.Condition{AnyOf: []plugin.Rule{{Field: "auth", Op: plugin.OpEq, Value: "access_key"}, {Field: "auth", Op: plugin.OpEq, Value: "credential"}}}
	return []plugin.Field{
		{Key: "auth", Label: "Authentication", Type: plugin.FieldSelect, Required: true, Default: "access_key", Options: []plugin.Option{
			{Label: "Access key", Value: "access_key"},
			{Label: "Stored access key", Value: "credential"},
		}},
		{Key: "access_key_id", Label: "Access key ID", Type: plugin.FieldText, Required: true, VisibleWhen: &plugin.Condition{AllOf: []plugin.Rule{{Field: "auth", Op: plugin.OpEq, Value: "access_key"}}}},
		{Key: "secret_access_key", Label: "Secret access key", Type: plugin.FieldPassword, Required: true, Secret: true, VisibleWhen: &plugin.Condition{AllOf: []plugin.Rule{{Field: "auth", Op: plugin.OpEq, Value: "access_key"}}}},
		{Key: "session_token", Label: "Session token", Type: plugin.FieldPassword, Secret: true, VisibleWhen: staticCredentials},
		{Key: "credential_id", Label: "Access key credential", Type: plugin.FieldCredentialRef, Credential: &plugin.CredentialSelector{
			Kinds: []plugin.CredentialKind{plugin.CredentialCloudAccessKey}, Protocols: []string{protocol}, Required: true,
		}, VisibleWhen: &plugin.Condition{AllOf: []plugin.Rule{{Field: "auth", Op: plugin.OpEq, Value: "credential"}}}},
	}
}

func (c *Client) Home(context.Context) (string, error) {
	return "/", nil
}

func (c *Client) ReadDir(ctx context.Context, p string) ([]os.FileInfo, error) {
	prefix := c.dirPrefix(p)
	pager := awss3.NewListObjectsV2Paginator(c.s3, &awss3.ListObjectsV2Input{
		Bucket:    aws.String(c.bucket),
		Prefix:    aws.String(prefix),
		Delimiter: aws.String("/"),
	})
	var infos []os.FileInfo
	seen := map[string]bool{}
	for pager.HasMorePages() {
		out, err := pager.NextPage(ctx)
		if err != nil {
			return nil, err
		}
		for _, common := range out.CommonPrefixes {
			key := aws.ToString(common.Prefix)
			name := path.Base(strings.TrimSuffix(key, "/"))
			if name == "" || seen[name] {
				continue
			}
			seen[name] = true
			infos = append(infos, objectInfo{name: name, dir: true, modTime: time.Now()})
		}
		for _, obj := range out.Contents {
			key := aws.ToString(obj.Key)
			if key == prefix {
				continue
			}
			name := strings.TrimPrefix(key, prefix)
			if strings.Contains(name, "/") || name == "" || seen[name] {
				continue
			}
			seen[name] = true
			modTime := time.Time{}
			if obj.LastModified != nil {
				modTime = *obj.LastModified
			}
			infos = append(infos, objectInfo{name: name, size: aws.ToInt64(obj.Size), modTime: modTime})
		}
	}
	sort.Slice(infos, func(i, j int) bool {
		if infos[i].IsDir() != infos[j].IsDir() {
			return infos[i].IsDir()
		}
		return strings.ToLower(infos[i].Name()) < strings.ToLower(infos[j].Name())
	})
	return infos, nil
}

func (c *Client) Stat(ctx context.Context, p string) (os.FileInfo, error) {
	key := c.key(p)
	if key == c.prefix || strings.HasSuffix(p, "/") {
		return objectInfo{name: pathBase(p), dir: true, modTime: time.Now()}, nil
	}
	out, err := c.s3.HeadObject(ctx, &awss3.HeadObjectInput{Bucket: aws.String(c.bucket), Key: aws.String(key)})
	if err == nil {
		modTime := time.Time{}
		if out.LastModified != nil {
			modTime = *out.LastModified
		}
		return objectInfo{name: pathBase(p), size: aws.ToInt64(out.ContentLength), modTime: modTime}, nil
	}
	if !isNotFound(err) {
		return nil, err
	}
	children, listErr := c.s3.ListObjectsV2(ctx, &awss3.ListObjectsV2Input{
		Bucket:  aws.String(c.bucket),
		Prefix:  aws.String(strings.TrimSuffix(key, "/") + "/"),
		MaxKeys: aws.Int32(1),
	})
	if listErr != nil {
		return nil, listErr
	}
	if aws.ToInt32(children.KeyCount) > 0 {
		return objectInfo{name: pathBase(p), dir: true, modTime: time.Now()}, nil
	}
	return nil, os.ErrNotExist
}

func (c *Client) Open(ctx context.Context, p string) (io.ReadCloser, error) {
	out, err := c.s3.GetObject(ctx, &awss3.GetObjectInput{Bucket: aws.String(c.bucket), Key: aws.String(c.key(p))})
	if err != nil {
		return nil, err
	}
	return out.Body, nil
}

func (c *Client) OpenRange(ctx context.Context, p string, offset, length int64) (io.ReadCloser, error) {
	in := &awss3.GetObjectInput{Bucket: aws.String(c.bucket), Key: aws.String(c.key(p))}
	if offset > 0 || length > 0 {
		spec := fmt.Sprintf("bytes=%d-", offset)
		if length > 0 {
			spec = fmt.Sprintf("bytes=%d-%d", offset, offset+length-1)
		}
		in.Range = aws.String(spec)
	}
	out, err := c.s3.GetObject(ctx, in)
	if err != nil {
		return nil, err
	}
	return out.Body, nil
}

func (c *Client) Write(ctx context.Context, p string, r io.Reader) error {
	_, err := c.uploader.UploadObject(ctx, &transfermanager.UploadObjectInput{Bucket: aws.String(c.bucket), Key: aws.String(c.key(p)), Body: r})
	return err
}

func (c *Client) Mkdir(ctx context.Context, p string) error {
	_, err := c.s3.PutObject(ctx, &awss3.PutObjectInput{
		Bucket: aws.String(c.bucket),
		Key:    aws.String(strings.TrimSuffix(c.key(p), "/") + "/"),
		Body:   strings.NewReader(""),
	})
	return err
}

func (c *Client) Rename(ctx context.Context, from, to string) error {
	info, err := c.Stat(ctx, from)
	if err != nil {
		return err
	}
	if info.IsDir() {
		return c.renamePrefix(ctx, strings.TrimSuffix(c.key(from), "/")+"/", strings.TrimSuffix(c.key(to), "/")+"/")
	}
	fromKey := c.key(from)
	toKey := c.key(to)
	_, err = c.s3.CopyObject(ctx, &awss3.CopyObjectInput{
		Bucket:     aws.String(c.bucket),
		Key:        aws.String(toKey),
		CopySource: aws.String(url.PathEscape(c.bucket + "/" + fromKey)),
	})
	if err != nil {
		return err
	}
	_, err = c.s3.DeleteObject(ctx, &awss3.DeleteObjectInput{Bucket: aws.String(c.bucket), Key: aws.String(fromKey)})
	return err
}

func (c *Client) Remove(ctx context.Context, p string, isDir bool) error {
	if isDir {
		return c.deletePrefix(ctx, strings.TrimSuffix(c.key(p), "/")+"/")
	}
	_, err := c.s3.DeleteObject(ctx, &awss3.DeleteObjectInput{Bucket: aws.String(c.bucket), Key: aws.String(c.key(p))})
	return err
}

func (c *Client) MapError(err error) error {
	if isNotFound(err) {
		return plugin.ErrNotFound
	}
	var apiErr smithy.APIError
	if errors.As(err, &apiErr) {
		switch apiErr.ErrorCode() {
		case "AccessDenied", "InvalidAccessKeyId", "SignatureDoesNotMatch":
			return plugin.ErrForbidden
		}
	}
	return nil
}

func (c *Client) renamePrefix(ctx context.Context, fromPrefix, toPrefix string) error {
	keys, err := c.listKeys(ctx, fromPrefix)
	if err != nil {
		return err
	}
	for _, key := range keys {
		dst := toPrefix + strings.TrimPrefix(key, fromPrefix)
		_, err := c.s3.CopyObject(ctx, &awss3.CopyObjectInput{
			Bucket:     aws.String(c.bucket),
			Key:        aws.String(dst),
			CopySource: aws.String(url.PathEscape(c.bucket + "/" + key)),
		})
		if err != nil {
			return err
		}
	}
	return c.deleteKeys(ctx, keys)
}

func (c *Client) deletePrefix(ctx context.Context, prefix string) error {
	keys, err := c.listKeys(ctx, prefix)
	if err != nil {
		return err
	}
	return c.deleteKeys(ctx, keys)
}

func (c *Client) listKeys(ctx context.Context, prefix string) ([]string, error) {
	var keys []string
	p := awss3.NewListObjectsV2Paginator(c.s3, &awss3.ListObjectsV2Input{Bucket: aws.String(c.bucket), Prefix: aws.String(prefix)})
	for p.HasMorePages() {
		page, err := p.NextPage(ctx)
		if err != nil {
			return nil, err
		}
		for _, obj := range page.Contents {
			keys = append(keys, aws.ToString(obj.Key))
		}
	}
	return keys, nil
}

func (c *Client) deleteKeys(ctx context.Context, keys []string) error {
	for len(keys) > 0 {
		n := len(keys)
		if n > 1000 {
			n = 1000
		}
		batch := keys[:n]
		keys = keys[n:]
		objects := make([]types.ObjectIdentifier, 0, len(batch))
		for _, key := range batch {
			objects = append(objects, types.ObjectIdentifier{Key: aws.String(key)})
		}
		if len(objects) == 0 {
			continue
		}
		_, err := c.s3.DeleteObjects(ctx, &awss3.DeleteObjectsInput{
			Bucket: aws.String(c.bucket),
			Delete: &types.Delete{Objects: objects, Quiet: aws.Bool(true)},
		})
		if err != nil {
			return err
		}
	}
	return nil
}

func (c *Client) key(p string) string {
	p = strings.TrimPrefix(path.Clean("/"+strings.TrimPrefix(p, "/")), "/")
	if p == "." {
		p = ""
	}
	if c.prefix == "" {
		return p
	}
	if p == "" {
		return c.prefix
	}
	return c.prefix + p
}

func (c *Client) dirPrefix(p string) string {
	key := c.key(p)
	if key == "" || strings.HasSuffix(key, "/") {
		return key
	}
	return key + "/"
}

type objectInfo struct {
	name    string
	size    int64
	dir     bool
	modTime time.Time
}

func (i objectInfo) Name() string {
	return i.name
}

func (i objectInfo) Size() int64 {
	if i.dir {
		return 0
	}
	return i.size
}

func (i objectInfo) Mode() os.FileMode {
	if i.dir {
		return os.ModeDir | 0o755
	}
	return 0o644
}

func (i objectInfo) ModTime() time.Time {
	return i.modTime
}

func (i objectInfo) IsDir() bool {
	return i.dir
}

func (i objectInfo) Sys() any {
	return nil
}

func normalizePrefix(prefix string) string {
	prefix = strings.Trim(strings.TrimSpace(prefix), "/")
	if prefix == "" {
		return ""
	}
	return prefix + "/"
}

func pathBase(p string) string {
	p = strings.TrimRight(p, "/")
	if p == "" || p == "." {
		return "/"
	}
	return path.Base(p)
}

func boolValue(cfg plugin.ConnectConfig, key string, fallback bool) bool {
	switch v := cfg.Config[key].(type) {
	case bool:
		return v
	case string:
		return strings.EqualFold(v, "true")
	default:
		return fallback
	}
}

func endpointHost(endpoint string) string {
	u, err := url.Parse(endpoint)
	if err != nil {
		return ""
	}
	return u.Hostname()
}

func isNotFound(err error) bool {
	if os.IsNotExist(err) {
		return true
	}
	var apiErr smithy.APIError
	if errors.As(err, &apiErr) {
		switch apiErr.ErrorCode() {
		case "NoSuchKey", "NotFound", "NoSuchBucket":
			return true
		}
	}
	return false
}
