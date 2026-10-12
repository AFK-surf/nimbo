package schema

import "testing"

func TestCompileValidate(t *testing.T) {
	s, err := Compile([]byte(`{"type":"object","required":["n"],"properties":{"n":{"type":"integer"}}}`))
	if err != nil {
		t.Fatal(err)
	}
	if err := Validate(s, []byte(`{"n":1}`)); err != nil {
		t.Fatal(err)
	}
	if err := Validate(s, []byte(`{"n":"x"}`)); err == nil {
		t.Fatal("expected violation")
	}
	if _, err := Compile([]byte(`{"$ref":"https://example.com/s.json"}`)); err == nil {
		t.Fatal("remote ref must fail")
	}
}
