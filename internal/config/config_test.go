package config

import (
	"os"
	"path/filepath"
	"testing"
)

func TestNodeKeyOrder(t *testing.T) {
	dir := t.TempDir()
	os.Unsetenv(EnvNodeKey)
	k1, src, err := LoadNodeKey(dir)
	if err != nil || src != "file (created)" {
		t.Fatal(src, err)
	}
	k2, src, err := LoadNodeKey(dir)
	if err != nil || src != "file" || k2 != k1 {
		t.Fatal("state file not reused", src, err)
	}
	k3, _ := GenerateNodeKey()
	t.Setenv(EnvNodeKey, k3.String())
	k4, src, err := LoadNodeKey(dir)
	if err != nil || src != "env" || k4 != k3 {
		t.Fatal("env must win", src, err)
	}
	t.Setenv(EnvNodeKey, "privkey:zz")
	if _, _, err := LoadNodeKey(dir); err == nil {
		t.Fatal("invalid env key must be fatal")
	}
	if _, err := os.Stat(filepath.Join(dir, "node.key")); err != nil {
		t.Fatal(err)
	}
}
