package s3compat

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/credentials"
	awss3 "github.com/aws/aws-sdk-go-v2/service/s3"

	"github.com/charlesng35/shellcn/sdk/plugin"
)

// fakeS3 is a minimal S3 endpoint: it records every request and replies with
// canned XML so the paging contract can be asserted without a live server.
type fakeS3 struct {
	t       *testing.T
	handler func(w http.ResponseWriter, r *http.Request)
	ops     []string
	queries []url.Values
}

func (f *fakeS3) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	f.queries = append(f.queries, q)
	switch {
	case r.Method == http.MethodPost && q.Has("delete"):
		f.ops = append(f.ops, "delete")
	case q.Get("versions") != "" || q.Has("versions"):
		f.ops = append(f.ops, "versions")
	default:
		f.ops = append(f.ops, "list")
	}
	w.Header().Set("Content-Type", "application/xml")
	f.handler(w, r)
}

func newFakeClient(t *testing.T, handler func(w http.ResponseWriter, r *http.Request)) (*Client, *fakeS3) {
	t.Helper()
	fake := &fakeS3{t: t, handler: handler}
	srv := httptest.NewServer(fake)
	t.Cleanup(srv.Close)
	api := awss3.New(awss3.Options{
		Region:       "us-east-1",
		Credentials:  credentials.NewStaticCredentialsProvider("key", "secret", ""),
		HTTPClient:   srv.Client(),
		BaseEndpoint: aws.String(srv.URL),
		UsePathStyle: true,
	})
	return &Client{s3: api, bucket: "bucket"}, fake
}

func TestReadDirPageIssuesOneBoundedList(t *testing.T) {
	c, fake := newFakeClient(t, func(w http.ResponseWriter, _ *http.Request) {
		fmt.Fprint(w, `<?xml version="1.0" encoding="UTF-8"?>
<ListBucketResult><Name>bucket</Name><IsTruncated>true</IsTruncated>
<NextContinuationToken>tok-2</NextContinuationToken>
<CommonPrefixes><Prefix>logs/</Prefix></CommonPrefixes>
<Contents><Key>a.txt</Key><Size>4</Size></Contents>
<Contents><Key>b.txt</Key><Size>8</Size></Contents>
</ListBucketResult>`)
	})

	infos, next, truncated, err := c.ReadDirPage(context.Background(), "/", "tok-1", 2)
	if err != nil {
		t.Fatalf("ReadDirPage: %v", err)
	}
	if len(fake.ops) != 1 {
		t.Fatalf("ReadDirPage made %d requests, want 1", len(fake.ops))
	}
	q := fake.queries[0]
	if q.Get("max-keys") != "2" || q.Get("delimiter") != "/" || q.Get("continuation-token") != "tok-1" {
		t.Fatalf("request did not bound the listing: %v", q)
	}
	if next != "tok-2" || !truncated {
		t.Fatalf("next = %q truncated = %v; want tok-2/true", next, truncated)
	}
	if len(infos) != 3 || !infos[0].IsDir() || infos[0].Name() != "logs" {
		t.Fatalf("unexpected page: %#v", infos)
	}
}

func TestReadDirPageClampsLimit(t *testing.T) {
	c, fake := newFakeClient(t, func(w http.ResponseWriter, _ *http.Request) {
		fmt.Fprint(w, `<ListBucketResult><Name>bucket</Name><IsTruncated>false</IsTruncated></ListBucketResult>`)
	})
	if _, _, _, err := c.ReadDirPage(context.Background(), "/", "", 100000); err != nil {
		t.Fatalf("ReadDirPage: %v", err)
	}
	if got := fake.queries[0].Get("max-keys"); got != fmt.Sprint(plugin.MaxPageLimit) {
		t.Fatalf("max-keys = %q, want %d", got, plugin.MaxPageLimit)
	}
}

func TestListVersionsReturnsOnePageWithCursor(t *testing.T) {
	c, fake := newFakeClient(t, func(w http.ResponseWriter, _ *http.Request) {
		fmt.Fprint(w, `<?xml version="1.0" encoding="UTF-8"?>
<ListVersionsResult><Name>bucket</Name><IsTruncated>true</IsTruncated>
<NextKeyMarker>k9</NextKeyMarker><NextVersionIdMarker>v9</NextVersionIdMarker>
<Version><Key>k1</Key><VersionId>v1</VersionId><IsLatest>true</IsLatest><Size>10</Size></Version>
<DeleteMarker><Key>k2</Key><VersionId>v2</VersionId><IsLatest>false</IsLatest></DeleteMarker>
</ListVersionsResult>`)
	})

	rc := plugin.NewRequestContext(t.Context(), plugin.User{}, &Session{fs: c},
		map[string]string{"bucket": "bucket"}, url.Values{"limit": {"2"}}, nil)
	out, err := listVersions(rc)
	if err != nil {
		t.Fatalf("listVersions: %v", err)
	}
	page, ok := out.(versionsPage)
	if !ok {
		t.Fatalf("listVersions returned %T, want versionsPage", out)
	}
	if len(fake.ops) != 1 {
		t.Fatalf("listVersions made %d requests, want 1", len(fake.ops))
	}
	if got := fake.queries[0].Get("max-keys"); got != "2" {
		t.Fatalf("max-keys = %q, want 2", got)
	}
	if len(page.Items) != 2 || !page.Items[1].DeleteMarker {
		t.Fatalf("unexpected versions page: %#v", page.Items)
	}
	if page.Total != nil {
		t.Fatalf("version listings must not count the bucket, got %d", *page.Total)
	}
	if !page.Truncated || page.NextCursor == "" {
		t.Fatalf("truncated listing must expose a cursor: %#v", page)
	}

	key, version, err := decodeVersionCursor(page.NextCursor)
	if err != nil || key != "k9" || version != "v9" {
		t.Fatalf("cursor round-trip = %q/%q, %v", key, version, err)
	}

	resume := plugin.NewRequestContext(t.Context(), plugin.User{}, &Session{fs: c},
		map[string]string{"bucket": "bucket"}, url.Values{"cursor": {page.NextCursor}}, nil)
	if _, err := listVersions(resume); err != nil {
		t.Fatalf("resume: %v", err)
	}
	q := fake.queries[1]
	if q.Get("key-marker") != "k9" || q.Get("version-id-marker") != "v9" {
		t.Fatalf("resume did not carry the markers: %v", q)
	}
}

func TestListVersionsRejectsBadCursor(t *testing.T) {
	c, _ := newFakeClient(t, func(http.ResponseWriter, *http.Request) {})
	rc := plugin.NewRequestContext(t.Context(), plugin.User{}, &Session{fs: c},
		map[string]string{"bucket": "bucket"}, url.Values{"cursor": {"!!!"}}, nil)
	if _, err := listVersions(rc); err == nil {
		t.Fatal("invalid cursor accepted")
	}
}

// TestDeletePrefixStreamsIntoBatches pins the interleaving: a delete must be
// issued while the listing is still being walked, never after it is fully
// materialized.
func TestDeletePrefixStreamsIntoBatches(t *testing.T) {
	page := 0
	c, fake := newFakeClient(t, func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodPost {
			fmt.Fprint(w, `<DeleteResult></DeleteResult>`)
			return
		}
		page++
		var b strings.Builder
		b.WriteString(`<ListBucketResult><Name>bucket</Name>`)
		if page == 1 {
			b.WriteString(`<IsTruncated>true</IsTruncated><NextContinuationToken>p2</NextContinuationToken>`)
		} else {
			b.WriteString(`<IsTruncated>false</IsTruncated>`)
		}
		count := 1200
		if page == 2 {
			count = 300
		}
		for i := range count {
			fmt.Fprintf(&b, `<Contents><Key>p/%d-%d</Key><Size>1</Size></Contents>`, page, i)
		}
		b.WriteString(`</ListBucketResult>`)
		fmt.Fprint(w, b.String())
	})

	if err := c.deletePrefix(context.Background(), "p/"); err != nil {
		t.Fatalf("deletePrefix: %v", err)
	}
	want := []string{"list", "delete", "list", "delete"}
	if strings.Join(fake.ops, ",") != strings.Join(want, ",") {
		t.Fatalf("ops = %v, want %v (deletes must interleave with the listing)", fake.ops, want)
	}
}
