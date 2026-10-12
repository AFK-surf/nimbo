// Package s3store runs the Broker's S3 requests (design §4) against an
// S3-compatible store and reports each response's Date header, which the
// Broker uses as its reference clock.
package s3store

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net/http"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsmiddleware "github.com/aws/aws-sdk-go-v2/aws/middleware"
	awshttp "github.com/aws/aws-sdk-go-v2/aws/transport/http"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/aws/smithy-go"
	"github.com/aws/smithy-go/middleware"
	smithyhttp "github.com/aws/smithy-go/transport/http"

	"github.com/afk-surf/nimbo/internal/node"
)

// Store runs S3 requests. Keys are relative to the cluster prefix.
type Store interface {
	Do(ctx context.Context, req node.S3Request) node.S3Response
	// PutIfAbsent writes an object only if it does not exist (for
	// cluster.json). It reports whether it wrote.
	PutIfAbsent(ctx context.Context, key string, body []byte) (bool, error)
}

// AWS is a Store backed by aws-sdk-go-v2.
type AWS struct {
	Client *s3.Client
	Bucket string
	Prefix string // without trailing slash
}

func (a *AWS) key(k string) string {
	if a.Prefix == "" {
		return k
	}
	return a.Prefix + "/" + k
}

func dateOf(md middleware.Metadata) time.Time {
	if raw, ok := awsmiddleware.GetRawResponse(md).(*smithyhttp.Response); ok && raw != nil {
		if t, err := http.ParseTime(raw.Header.Get("Date")); err == nil {
			return t
		}
	}
	return time.Time{}
}

func dateOfErr(err error) time.Time {
	var re *awshttp.ResponseError
	if errors.As(err, &re) && re.Response != nil {
		if t, err := http.ParseTime(re.Response.Header.Get("Date")); err == nil {
			return t
		}
	}
	return time.Time{}
}

func notFound(err error) bool {
	var nsk *types.NoSuchKey
	if errors.As(err, &nsk) {
		return true
	}
	var ae smithy.APIError
	if errors.As(err, &ae) {
		switch ae.ErrorCode() {
		case "NoSuchKey", "NotFound":
			return true
		}
	}
	return false
}

// Do implements Store.
func (a *AWS) Do(ctx context.Context, req node.S3Request) node.S3Response {
	switch req.Op {
	case node.S3Put:
		out, err := a.Client.PutObject(ctx, &s3.PutObjectInput{
			Bucket:      &a.Bucket,
			Key:         aws.String(a.key(req.Key)),
			Body:        bytes.NewReader(req.Body),
			ContentType: aws.String("application/json"),
		})
		if err != nil {
			return node.S3Response{Err: err, Date: dateOfErr(err)}
		}
		return node.S3Response{ETag: aws.ToString(out.ETag), Date: dateOf(out.ResultMetadata)}
	case node.S3Get:
		out, err := a.Client.GetObject(ctx, &s3.GetObjectInput{Bucket: &a.Bucket, Key: aws.String(a.key(req.Key))})
		if err != nil {
			if notFound(err) {
				return node.S3Response{NotFound: true, Date: dateOfErr(err)}
			}
			return node.S3Response{Err: err, Date: dateOfErr(err)}
		}
		defer out.Body.Close()
		b, err := io.ReadAll(io.LimitReader(out.Body, 1<<20))
		if err != nil {
			return node.S3Response{Err: err}
		}
		return node.S3Response{Body: b, ETag: aws.ToString(out.ETag), LastModified: aws.ToTime(out.LastModified), Date: dateOf(out.ResultMetadata)}
	case node.S3Delete:
		out, err := a.Client.DeleteObject(ctx, &s3.DeleteObjectInput{Bucket: &a.Bucket, Key: aws.String(a.key(req.Key))})
		if err != nil {
			return node.S3Response{Err: err, Date: dateOfErr(err)}
		}
		return node.S3Response{Date: dateOf(out.ResultMetadata)}
	case node.S3List:
		var resp node.S3Response
		p := s3.NewListObjectsV2Paginator(a.Client, &s3.ListObjectsV2Input{Bucket: &a.Bucket, Prefix: aws.String(a.key(req.Key))})
		strip := a.key("")
		for p.HasMorePages() {
			out, err := p.NextPage(ctx)
			if err != nil {
				return node.S3Response{Err: err, Date: dateOfErr(err)}
			}
			resp.Date = dateOf(out.ResultMetadata)
			for _, o := range out.Contents {
				resp.Objects = append(resp.Objects, node.S3Object{
					Key:          strings.TrimPrefix(aws.ToString(o.Key), strip),
					ETag:         aws.ToString(o.ETag),
					LastModified: aws.ToTime(o.LastModified),
				})
			}
		}
		return resp
	}
	return node.S3Response{Err: errors.New("s3store: bad op")}
}

// PutIfAbsent implements Store with a conditional If-None-Match PUT.
func (a *AWS) PutIfAbsent(ctx context.Context, key string, body []byte) (bool, error) {
	_, err := a.Client.PutObject(ctx, &s3.PutObjectInput{
		Bucket:      &a.Bucket,
		Key:         aws.String(a.key(key)),
		Body:        bytes.NewReader(body),
		IfNoneMatch: aws.String("*"),
		ContentType: aws.String("application/json"),
	})
	if err != nil {
		var ae smithy.APIError
		if errors.As(err, &ae) && (ae.ErrorCode() == "PreconditionFailed" || ae.ErrorCode() == "ConditionalRequestConflict") {
			return false, nil
		}
		return false, err
	}
	return true, nil
}

// Mem is an in-memory Store for tests and single-host development. It
// can be shared by several Brokers in one process.
type Mem struct {
	mu   sync.Mutex
	objs map[string]memObj
	// Now overrides the clock (for tests).
	Now func() time.Time
}

type memObj struct {
	body []byte
	etag string
	lm   time.Time
}

func NewMem() *Mem { return &Mem{objs: map[string]memObj{}} }

func (m *Mem) now() time.Time {
	if m.Now != nil {
		return m.Now()
	}
	return time.Now()
}

// Do implements Store.
func (m *Mem) Do(_ context.Context, req node.S3Request) node.S3Response {
	m.mu.Lock()
	defer m.mu.Unlock()
	now := m.now()
	date := now.UTC().Truncate(time.Second)
	switch req.Op {
	case node.S3Put:
		etag := etagOf(req.Body)
		m.objs[req.Key] = memObj{body: append([]byte(nil), req.Body...), etag: etag, lm: now}
		return node.S3Response{ETag: etag, Date: date}
	case node.S3Get:
		o, ok := m.objs[req.Key]
		if !ok {
			return node.S3Response{NotFound: true, Date: date}
		}
		return node.S3Response{Body: append([]byte(nil), o.body...), ETag: o.etag, LastModified: o.lm, Date: date}
	case node.S3Delete:
		delete(m.objs, req.Key)
		return node.S3Response{Date: date}
	case node.S3List:
		var keys []string
		for k := range m.objs {
			if strings.HasPrefix(k, req.Key) {
				keys = append(keys, k)
			}
		}
		sort.Strings(keys)
		resp := node.S3Response{Date: date}
		for _, k := range keys {
			o := m.objs[k]
			resp.Objects = append(resp.Objects, node.S3Object{Key: k, ETag: o.etag, LastModified: o.lm})
		}
		return resp
	}
	return node.S3Response{Err: errors.New("s3store: bad op")}
}

// PutIfAbsent implements Store.
func (m *Mem) PutIfAbsent(_ context.Context, key string, body []byte) (bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if _, ok := m.objs[key]; ok {
		return false, nil
	}
	m.objs[key] = memObj{body: append([]byte(nil), body...), etag: etagOf(body), lm: m.now()}
	return true, nil
}

func etagOf(b []byte) string {
	h := uint64(14695981039346656037)
	for _, c := range b {
		h ^= uint64(c)
		h *= 1099511628211
	}
	const hexd = "0123456789abcdef"
	out := make([]byte, 16)
	for i := range out {
		out[15-i] = hexd[h&15]
		h >>= 4
	}
	return `"` + string(out) + `"`
}
