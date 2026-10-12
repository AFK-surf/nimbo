// Package schema compiles JSON Schemas (draft 2020-12) from blobs and caches
// them by hash. Remote references are refused.
package schema

import (
	"bytes"
	"container/list"
	"errors"
	"fmt"

	"github.com/santhosh-tekuri/jsonschema/v6"

	"github.com/afk-surf/nimbo/internal/ids"
)

type noLoader struct{}

func (noLoader) Load(url string) (any, error) {
	return nil, fmt.Errorf("remote $ref not allowed: %s", url)
}

// Compile compiles one schema document.
func Compile(b []byte) (*jsonschema.Schema, error) {
	doc, err := jsonschema.UnmarshalJSON(bytes.NewReader(b))
	if err != nil {
		return nil, err
	}
	if _, ok := doc.(map[string]any); !ok {
		if _, ok := doc.(bool); !ok {
			return nil, errors.New("schema must be an object or boolean")
		}
	}
	c := jsonschema.NewCompiler()
	c.UseLoader(noLoader{})
	c.DefaultDraft(jsonschema.Draft2020)
	const url = "mem:///schema.json"
	if err := c.AddResource(url, doc); err != nil {
		return nil, err
	}
	return c.Compile(url)
}

// Validate checks a JSON document against a compiled schema.
func Validate(s *jsonschema.Schema, doc []byte) error {
	v, err := jsonschema.UnmarshalJSON(bytes.NewReader(doc))
	if err != nil {
		return err
	}
	if err := s.Validate(v); err != nil {
		var ve *jsonschema.ValidationError
		if errors.As(err, &ve) {
			// Report a stable summary: the instance location of the first
			// leaf error.
			for len(ve.Causes) > 0 {
				ve = ve.Causes[0]
			}
			return fmt.Errorf("at /%s", joinLoc(ve.InstanceLocation))
		}
		return err
	}
	return nil
}

func joinLoc(p []string) string {
	var b bytes.Buffer
	for i, s := range p {
		if i > 0 {
			b.WriteByte('/')
		}
		b.WriteString(s)
	}
	return b.String()
}

// Cache is a bounded LRU of compiled schemas by blob hash. Not safe for
// concurrent use.
type Cache struct {
	max int
	ll  *list.List
	m   map[ids.Hash]*list.Element
}

type entry struct {
	h ids.Hash
	s *jsonschema.Schema
}

func NewCache(max int) *Cache {
	return &Cache{max: max, ll: list.New(), m: map[ids.Hash]*list.Element{}}
}

// Get returns the compiled schema for blob bytes b with hash h.
func (c *Cache) Get(h ids.Hash, b []byte) (*jsonschema.Schema, error) {
	if e, ok := c.m[h]; ok {
		c.ll.MoveToFront(e)
		return e.Value.(*entry).s, nil
	}
	s, err := Compile(b)
	if err != nil {
		return nil, err
	}
	c.m[h] = c.ll.PushFront(&entry{h: h, s: s})
	for c.ll.Len() > c.max {
		e := c.ll.Back()
		c.ll.Remove(e)
		delete(c.m, e.Value.(*entry).h)
	}
	return s, nil
}
