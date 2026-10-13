package node

import "time"

// MeshPeer is the data plane's view of one peer Broker, for /debug/stats.
type MeshPeer struct {
	ID     string // full Broker ID
	Short  string // abbreviated ID, as in logs
	Region string `json:",omitempty"` // the peer's -region label
	// Path is "direct" (UDP straight to Addr), "derp" (through the DERP
	// relay in DERP), "peer-relay" (through a Tailscale UDP peer relay) or
	// "idle" (nothing sent recently, so no current path).
	Path string
	// Addr is the UDP endpoint in use when Path is "direct" or "peer-relay".
	Addr string `json:",omitempty"`
	// DERP is the region code of the peer's home relay, used when Path is
	// "derp".
	DERP string `json:",omitempty"`
	// LastHandshake is the last WireGuard handshake; LastWrite the last
	// packet sent to the peer.
	LastHandshake *time.Time `json:",omitempty"`
	LastWrite     *time.Time `json:",omitempty"`
	RxBytes       int64
	TxBytes       int64
	// Latency is a disco ping round trip, only measured on request.
	LatencyMs *float64 `json:",omitempty"`
	// PingPath is the path the ping took ("direct", "derp" or
	// "peer-relay"); PingErr explains a failed ping.
	PingPath string `json:",omitempty"`
	PingErr  string `json:",omitempty"`
}
