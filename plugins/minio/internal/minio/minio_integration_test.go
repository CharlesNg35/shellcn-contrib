package minio

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/json"
	"io"
	"net"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/credentials"
	awss3 "github.com/aws/aws-sdk-go-v2/service/s3"
	s3types "github.com/aws/aws-sdk-go-v2/service/s3/types"

	"github.com/charlesng35/shellcn-contrib/shared/filesystem"
	"github.com/charlesng35/shellcn/sdk/plugin"
	"github.com/charlesng35/shellcn/sdk/plugintest"
)

const (
	itAccessKey = "shellcnadmin"
	itSecretKey = "shellcnadmin123"
)

func TestMinIOPluginIntegration(t *testing.T) {
	if os.Getenv("SHELLCN_MINIO_INTEGRATION") != "1" {
		t.Skip("set SHELLCN_MINIO_INTEGRATION=1 to run against MinIO")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 180*time.Second)
	defer cancel()

	endpoint := minioEndpoint(ctx, t)
	bucket := "shellcn-it-" + time.Now().UTC().Format("20060102150405")
	cfg := map[string]any{
		"endpoint":          endpoint,
		"region":            "us-east-1",
		"bucket":            bucket,
		"auth":              "access_key",
		"access_key_id":     itAccessKey,
		"secret_access_key": itSecretKey,
		"path_style":        true,
		"verify_tls":        false,
	}

	p := New()
	sess, err := p.Connect(ctx, plugin.ConnectConfig{Config: cfg, Net: plugintest.DirectTransport()})
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer func() { _ = sess.Close() }()
	routes := plugintest.RouteMap(p.Routes())

	// Create the bucket.
	createBody, _ := json.Marshal(map[string]any{"name": bucket})
	call(ctx, t, routes["minio.bucket.create"], sess, nil, nil, createBody)
	defer callNoFail(context.Background(), routes["minio.bucket.delete"], sess, map[string]string{"bucket": bucket})

	if !bucketListed(call(ctx, t, routes["minio.buckets.list"], sess, nil, nil, nil), bucket) {
		t.Fatalf("created bucket %q not present in list", bucket)
	}

	// Enable versioning and read it back.
	verBody, _ := json.Marshal(map[string]any{"status": "Enabled"})
	call(ctx, t, routes["minio.bucket.versioning.set"], sess, map[string]string{"bucket": bucket}, nil, verBody)
	ver := asMap(call(ctx, t, routes["minio.bucket.versioning"], sess, map[string]string{"bucket": bucket}, nil, nil))
	if ver["status"] != "Enabled" {
		t.Fatalf("versioning status after enable: got %v want Enabled", ver["status"])
	}

	// Upload a large object through the multipart-capable uploader.
	const objectKey = "data/large.bin"
	payload := make([]byte, 12<<20) // 12 MiB > 5 MiB part size forces multipart
	if _, err := rand.Read(payload); err != nil {
		t.Fatalf("rand: %v", err)
	}
	fsClient, err := sess.(interface {
		Filesystem() (filesystem.Client, error)
	}).Filesystem()
	if err != nil {
		t.Fatalf("filesystem: %v", err)
	}
	if err := fsClient.Write(ctx, "/"+objectKey, bytes.NewReader(payload)); err != nil {
		t.Fatalf("multipart write: %v", err)
	}

	// Presign a GET and fetch it back; assert the bytes match.
	presign := asMap(call(ctx, t, routes["minio.object.presign"], sess,
		map[string]string{"path": objectKey},
		url.Values{"method": []string{"get"}, "expiry": []string{"120"}}, nil))
	signedURL, _ := presign["url"].(string)
	if signedURL == "" {
		t.Fatalf("presign returned no url: %#v", presign)
	}
	fetched := fetch(ctx, t, signedURL)
	if !bytes.Equal(fetched, payload) {
		t.Fatalf("presigned GET returned %d bytes, want %d", len(fetched), len(payload))
	}

	// List object versions: the uploaded object must have a version id.
	versions := pageItems(call(ctx, t, routes["minio.bucket.versions"], sess, map[string]string{"bucket": bucket}, nil, nil))
	found := false
	for _, v := range versions {
		if strings.HasSuffix(asString(v["key"]), "large.bin") && asString(v["versionId"]) != "" {
			found = true
		}
	}
	if !found {
		t.Fatalf("expected a versioned object, got %#v", versions)
	}

	// A versioned bucket retains every version (and a delete marker after a plain
	// delete), so purge all versions before the bucket can be deleted.
	purgeVersions(ctx, t, endpoint, bucket)
	call(ctx, t, routes["minio.bucket.delete"], sess, map[string]string{"bucket": bucket}, nil, nil)
	if bucketListed(call(ctx, t, routes["minio.buckets.list"], sess, nil, nil, nil), bucket) {
		t.Fatalf("bucket %q still present after delete", bucket)
	}
}

func minioEndpoint(ctx context.Context, t *testing.T) string {
	t.Helper()
	if endpoint := os.Getenv("SHELLCN_MINIO_ENDPOINT"); endpoint != "" {
		return endpoint
	}
	if _, err := exec.LookPath("docker"); err != nil {
		t.Skip("docker CLI unavailable and SHELLCN_MINIO_ENDPOINT is not set")
	}
	name := "shellcn-minio-it-" + time.Now().UTC().Format("20060102150405")
	run(ctx, t, "docker", "run", "-d", "--rm", "--name", name,
		"-e", "MINIO_ROOT_USER="+itAccessKey,
		"-e", "MINIO_ROOT_PASSWORD="+itSecretKey,
		"-p", "127.0.0.1::9000",
		"minio/minio:latest", "server", "/data")
	t.Cleanup(func() {
		cleanupCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		_ = exec.CommandContext(cleanupCtx, "docker", "rm", "-f", name).Run()
	})
	out := run(ctx, t, "docker", "port", name, "9000/tcp")
	host, port, err := net.SplitHostPort(strings.TrimSpace(strings.SplitN(out, "\n", 2)[0]))
	if err != nil {
		t.Fatalf("unexpected docker port output: %q", out)
	}
	endpoint := "http://" + net.JoinHostPort(host, port)
	waitReady(ctx, t, endpoint)
	return endpoint
}

func waitReady(ctx context.Context, t *testing.T, endpoint string) {
	t.Helper()
	deadline := time.Now().Add(120 * time.Second)
	for {
		req, _ := http.NewRequestWithContext(ctx, http.MethodGet, endpoint+"/minio/health/ready", nil)
		resp, err := http.DefaultClient.Do(req)
		if err == nil {
			_ = resp.Body.Close()
			if resp.StatusCode == http.StatusOK {
				return
			}
		}
		if time.Now().After(deadline) {
			t.Fatalf("MinIO did not become ready at %s", endpoint)
		}
		time.Sleep(500 * time.Millisecond)
	}
}

func fetch(ctx context.Context, t *testing.T, rawURL string) []byte {
	t.Helper()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, rawURL, nil)
	if err != nil {
		t.Fatalf("request: %v", err)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("fetch presigned url: %v", err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("presigned GET status %d", resp.StatusCode)
	}
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("read body: %v", err)
	}
	return body
}

func purgeVersions(ctx context.Context, t *testing.T, endpoint, bucket string) {
	t.Helper()
	api := awss3.New(awss3.Options{
		Region:       "us-east-1",
		BaseEndpoint: aws.String(endpoint),
		UsePathStyle: true,
		Credentials:  credentials.NewStaticCredentialsProvider(itAccessKey, itSecretKey, ""),
	})
	out, err := api.ListObjectVersions(ctx, &awss3.ListObjectVersionsInput{Bucket: aws.String(bucket)})
	if err != nil {
		t.Fatalf("list versions for purge: %v", err)
	}
	var ids []s3types.ObjectIdentifier
	for _, v := range out.Versions {
		ids = append(ids, s3types.ObjectIdentifier{Key: v.Key, VersionId: v.VersionId})
	}
	for _, m := range out.DeleteMarkers {
		ids = append(ids, s3types.ObjectIdentifier{Key: m.Key, VersionId: m.VersionId})
	}
	if len(ids) == 0 {
		return
	}
	if _, err := api.DeleteObjects(ctx, &awss3.DeleteObjectsInput{
		Bucket: aws.String(bucket),
		Delete: &s3types.Delete{Objects: ids, Quiet: aws.Bool(true)},
	}); err != nil {
		t.Fatalf("purge versions: %v", err)
	}
}

func bucketListed(page any, name string) bool {
	for _, b := range pageItems(page) {
		if asString(b["name"]) == name {
			return true
		}
	}
	return false
}

func asString(v any) string {
	s, _ := v.(string)
	return s
}

func asMap(v any) map[string]any {
	data, _ := json.Marshal(v)
	var out map[string]any
	_ = json.Unmarshal(data, &out)
	return out
}

func call(ctx context.Context, t *testing.T, route plugin.Route, sess plugin.Session, params map[string]string, query url.Values, body []byte) any {
	t.Helper()
	out, err := route.Handle(plugin.NewRequestContext(ctx, plugin.User{}, sess, params, query, body))
	if err != nil {
		t.Fatalf("%s: %v", route.ID, err)
	}
	return out
}

func callNoFail(ctx context.Context, route plugin.Route, sess plugin.Session, params map[string]string) {
	_, _ = route.Handle(plugin.NewRequestContext(ctx, plugin.User{}, sess, params, nil, nil))
}

func pageItems(page any) []map[string]any {
	data, _ := json.Marshal(page)
	var decoded struct {
		Items []map[string]any `json:"items"`
	}
	_ = json.Unmarshal(data, &decoded)
	return decoded.Items
}

func run(ctx context.Context, t *testing.T, name string, args ...string) string {
	t.Helper()
	cmd := exec.CommandContext(ctx, name, args...)
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("%s %s: %v\n%s", name, strings.Join(args, " "), err, out)
	}
	return string(out)
}
