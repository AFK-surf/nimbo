package s3store

import (
	"context"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/johannesboyne/gofakes3"
	"github.com/johannesboyne/gofakes3/backend/s3mem"

	"github.com/afk-surf/nimbo/internal/node"
)

func fakeAWS(t *testing.T) *AWS {
	t.Helper()
	backend := s3mem.New()
	srv := httptest.NewServer(gofakes3.New(backend).Server())
	t.Cleanup(srv.Close)
	if err := backend.CreateBucket("b"); err != nil {
		t.Fatal(err)
	}
	client := s3.New(s3.Options{
		Region:       "us-east-1",
		BaseEndpoint: aws.String(srv.URL),
		UsePathStyle: true,
		Credentials:  credentials.NewStaticCredentialsProvider("k", "s", ""),
	})
	return &AWS{Client: client, Bucket: "b", Prefix: "nimbo"}
}

func testStore(t *testing.T, st Store) {
	ctx := context.Background()
	if r := st.Do(ctx, node.S3Request{Op: node.S3Get, Key: "members/x.json"}); !r.NotFound || r.Err != nil {
		t.Fatalf("get missing: %+v", r)
	}
	p := st.Do(ctx, node.S3Request{Op: node.S3Put, Key: "members/a.json", Body: []byte(`{"a":1}`)})
	if p.Err != nil || p.Date.IsZero() {
		t.Fatalf("put: %+v", p)
	}
	if d := time.Since(p.Date); d < -2*time.Second || d > 5*time.Second {
		t.Fatalf("date header off by %v", d)
	}
	st.Do(ctx, node.S3Request{Op: node.S3Put, Key: "members/b.json", Body: []byte(`{"b":2}`)})
	st.Do(ctx, node.S3Request{Op: node.S3Put, Key: "other.json", Body: []byte(`{}`)})
	l := st.Do(ctx, node.S3Request{Op: node.S3List, Key: "members/"})
	if l.Err != nil || len(l.Objects) != 2 || l.Objects[0].Key != "members/a.json" || l.Objects[0].ETag == "" || l.Objects[0].LastModified.IsZero() {
		t.Fatalf("list: %+v", l)
	}
	g := st.Do(ctx, node.S3Request{Op: node.S3Get, Key: "members/a.json"})
	if g.Err != nil || string(g.Body) != `{"a":1}` || g.ETag != l.Objects[0].ETag {
		t.Fatalf("get: %+v", g)
	}
	if d := st.Do(ctx, node.S3Request{Op: node.S3Delete, Key: "members/a.json"}); d.Err != nil {
		t.Fatal(d.Err)
	}
	if l := st.Do(ctx, node.S3Request{Op: node.S3List, Key: "members/"}); len(l.Objects) != 1 {
		t.Fatalf("after delete: %+v", l)
	}
	ok, err := st.PutIfAbsent(ctx, "cluster.json", []byte(`{}`))
	if err != nil || !ok {
		t.Fatal("first PutIfAbsent", ok, err)
	}
	ok, err = st.PutIfAbsent(ctx, "cluster.json", []byte(`{}`))
	if err != nil || ok {
		t.Fatal("second PutIfAbsent must not write", ok, err)
	}
}

func TestMem(t *testing.T) { testStore(t, NewMem()) }

func TestAWSAgainstFakeS3(t *testing.T) { testStore(t, fakeAWS(t)) }
