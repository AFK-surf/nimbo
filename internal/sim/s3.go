package sim

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"sort"
	"strings"
	"time"

	"github.com/afk-surf/nimbo/internal/node"
)

// S3 is a strongly consistent in-memory object store with request latency,
// a one-second-resolution Date header, and injectable failures.
type S3 struct {
	w    *World
	objs map[string]s3obj
}

type s3obj struct {
	body []byte
	etag string
	lm   time.Time
}

func newS3(w *World) *S3 { return &S3{w: w, objs: map[string]s3obj{}} }

var errS3 = errors.New("simulated S3 failure")

func (s *S3) do(e *brokerEnv, req node.S3Request, done func(node.S3Response)) {
	w := s.w
	b := e.b
	lat := w.dur(5*time.Millisecond, 60*time.Millisecond)
	fail := b.s3Broken || (w.faultsOn && w.chance(w.cfg.Faults.S3ErrorRate))
	body := append([]byte(nil), req.Body...)
	// The request takes effect half-way through its latency.
	w.After(lat/2, func() {
		var resp node.S3Response
		if fail {
			resp.Err = errS3
		} else {
			resp = s.apply(req.Op, req.Key, body)
		}
		resp.Date = w.Now().Truncate(time.Second)
		w.After(lat/2, func() {
			if e.live() {
				done(resp)
			}
		})
	})
}

func (s *S3) apply(op node.S3Op, key string, body []byte) node.S3Response {
	now := s.w.Now()
	switch op {
	case node.S3Put:
		sum := sha256.Sum256(body)
		s.objs[key] = s3obj{body: body, etag: hex.EncodeToString(sum[:8]), lm: now}
		return node.S3Response{ETag: s.objs[key].etag, LastModified: now}
	case node.S3Get:
		o, ok := s.objs[key]
		if !ok {
			return node.S3Response{NotFound: true}
		}
		return node.S3Response{Body: append([]byte(nil), o.body...), ETag: o.etag, LastModified: o.lm}
	case node.S3Delete:
		delete(s.objs, key)
		return node.S3Response{}
	case node.S3List:
		var keys []string
		for k := range s.objs {
			if strings.HasPrefix(k, key) {
				keys = append(keys, k)
			}
		}
		sort.Strings(keys)
		var out []node.S3Object
		for _, k := range keys {
			o := s.objs[k]
			out = append(out, node.S3Object{Key: k, ETag: o.etag, LastModified: o.lm})
		}
		return node.S3Response{Objects: out}
	}
	return node.S3Response{Err: errors.New("bad op")}
}
