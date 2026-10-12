// Package config loads the Broker's node key (design §5) and models
// cluster.json (design §4).
package config

import (
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"golang.org/x/crypto/curve25519"
)

// EnvNodeKey is the environment variable holding the node private key.
const EnvNodeKey = "NIMBOD_NODE_KEY"

// NodeKey is a Curve25519 key pair; it is also the WireGuard key.
type NodeKey struct {
	Private [32]byte
	Public  [32]byte
}

// GenerateNodeKey creates a fresh key.
func GenerateNodeKey() (NodeKey, error) {
	var k NodeKey
	if _, err := rand.Read(k.Private[:]); err != nil {
		return k, err
	}
	// Clamp as WireGuard does.
	k.Private[0] &= 248
	k.Private[31] = (k.Private[31] & 127) | 64
	return k, k.derive()
}

func (k *NodeKey) derive() error {
	pub, err := curve25519.X25519(k.Private[:], curve25519.Basepoint)
	if err != nil {
		return err
	}
	copy(k.Public[:], pub)
	return nil
}

// String is Tailscale's text form, privkey:<64 hex>.
func (k NodeKey) String() string { return "privkey:" + hex.EncodeToString(k.Private[:]) }

// ParseNodeKey parses privkey:<64 hex>.
func ParseNodeKey(s string) (NodeKey, error) {
	var k NodeKey
	h, ok := strings.CutPrefix(strings.TrimSpace(s), "privkey:")
	if !ok {
		return k, errors.New("node key must be privkey:<64 hex>")
	}
	b, err := hex.DecodeString(h)
	if err != nil || len(b) != 32 {
		return k, errors.New("node key must be privkey:<64 hex>")
	}
	copy(k.Private[:], b)
	return k, k.derive()
}

// LoadNodeKey applies the order of design §5: NIMBOD_NODE_KEY, then
// stateDir/node.key (created on first start), then a fresh key. An invalid
// NIMBOD_NODE_KEY is an error; it never falls back silently. source says
// where the key came from.
func LoadNodeKey(stateDir string) (k NodeKey, source string, err error) {
	if v, ok := os.LookupEnv(EnvNodeKey); ok {
		k, err = ParseNodeKey(v)
		if err != nil {
			return k, "", fmt.Errorf("%s: %w", EnvNodeKey, err)
		}
		return k, "env", nil
	}
	if stateDir != "" {
		p := filepath.Join(stateDir, "node.key")
		b, err := os.ReadFile(p)
		if err == nil {
			k, err = ParseNodeKey(string(b))
			if err != nil {
				return k, "", fmt.Errorf("%s: %w", p, err)
			}
			return k, "file", nil
		}
		if !errors.Is(err, os.ErrNotExist) {
			return k, "", err
		}
		k, err = GenerateNodeKey()
		if err != nil {
			return k, "", err
		}
		if err := os.MkdirAll(stateDir, 0o700); err != nil {
			return k, "", err
		}
		if err := os.WriteFile(p, []byte(k.String()+"\n"), 0o600); err != nil {
			return k, "", err
		}
		return k, "file (created)", nil
	}
	k, err = GenerateNodeKey()
	return k, "ephemeral", err
}

// Cluster is cluster.json.
type Cluster struct {
	ClusterID   string `json:"cluster_id"`
	WGPSK       string `json:"wg_psk"`        // hex, 32 bytes
	DERPMeshKey string `json:"derp_mesh_key"` // hex, 32 bytes
	Version     int    `json:"version"`
	// Mesh is "tailscale" (default) or "tcp" (development only).
	Mesh string `json:"mesh,omitempty"`
}

// NewCluster creates a cluster.json with fresh secrets.
func NewCluster(mesh string) (*Cluster, error) {
	var id [16]byte
	var psk, dk [32]byte
	for _, b := range [][]byte{id[:], psk[:], dk[:]} {
		if _, err := rand.Read(b); err != nil {
			return nil, err
		}
	}
	return &Cluster{ClusterID: hex.EncodeToString(id[:]), WGPSK: hex.EncodeToString(psk[:]), DERPMeshKey: hex.EncodeToString(dk[:]), Version: 1, Mesh: mesh}, nil
}

// ParseCluster parses and validates cluster.json.
func ParseCluster(b []byte) (*Cluster, error) {
	var c Cluster
	if err := json.Unmarshal(b, &c); err != nil {
		return nil, err
	}
	if c.Version != 1 {
		return nil, fmt.Errorf("cluster.json: unsupported version %d", c.Version)
	}
	if _, err := c.PSK(); err != nil {
		return nil, err
	}
	if c.Mesh == "" {
		c.Mesh = "tailscale"
	}
	return &c, nil
}

// PSK returns the WireGuard pre-shared key.
func (c *Cluster) PSK() ([]byte, error) {
	b, err := hex.DecodeString(c.WGPSK)
	if err != nil || len(b) != 32 {
		return nil, errors.New("cluster.json: wg_psk must be 32 bytes of hex")
	}
	return b, nil
}
