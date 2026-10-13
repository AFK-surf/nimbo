package node

import (
	"encoding/json"
	"net/netip"
	"time"
)

// Tunables are the protocol timings and sizes of design §14.
type Tunables struct {
	Replicas       int
	VirtualIDs     int
	LookupFanout   int
	LookupWait     time.Duration
	LookupRetry    time.Duration
	HandoffWindow  time.Duration
	RPCTimeout     time.Duration
	CapTTL         time.Duration
	TouchThreshold time.Duration
	Grace          time.Duration

	PresenceReconcile    time.Duration
	PresenceTombstoneTTL time.Duration
	LocationCacheTTL     time.Duration
	MaxReroutes          int
	HoldBackstop         time.Duration

	MemberHeartbeat  time.Duration
	MemberPoll       time.Duration // full refresh backstop: finds crashed Brokers
	MemberSignalPoll time.Duration // membership signal check
	MemberTTL        time.Duration
	FenceMargin      time.Duration
	MaxSkew          time.Duration
	FastJoinNegTTL   time.Duration
	FastJoinPerSec   float64
	RejoinRetry      time.Duration

	SuspectBase time.Duration
	SuspectMax  time.Duration

	DefaultMsgTTL  time.Duration
	MaxMsgTTL      time.Duration
	DurableTimeout time.Duration
	DrainTimeout   time.Duration
	HandshakeTTL   time.Duration

	CacheEntries  int
	SchemaCache   int
	BatchOps      int
	MaxInflight   int
	SweepInterval time.Duration
}

// DefaultTunables returns the defaults of design §14.
func DefaultTunables() Tunables {
	return Tunables{
		Replicas:       3,
		VirtualIDs:     16,
		LookupFanout:   4,
		LookupWait:     time.Second,
		LookupRetry:    200 * time.Millisecond,
		HandoffWindow:  10 * time.Second,
		RPCTimeout:     2 * time.Second,
		CapTTL:         60 * time.Second,
		TouchThreshold: 10 * time.Second,
		Grace:          15 * time.Second,

		PresenceReconcile:    5 * time.Minute,
		PresenceTombstoneTTL: 10 * time.Minute,
		LocationCacheTTL:     30 * time.Second,
		MaxReroutes:          2,
		HoldBackstop:         2 * time.Second,

		MemberHeartbeat:  15 * time.Second,
		MemberPoll:       30 * time.Second,
		MemberSignalPoll: 5 * time.Second,
		MemberTTL:        60 * time.Second,
		FenceMargin:      15 * time.Second,
		MaxSkew:          3 * time.Second,
		FastJoinNegTTL:   60 * time.Second,
		FastJoinPerSec:   10,
		RejoinRetry:      time.Second,

		SuspectBase: 10 * time.Second,
		SuspectMax:  2 * time.Minute,

		DefaultMsgTTL:  10 * time.Second,
		MaxMsgTTL:      60 * time.Second,
		DurableTimeout: 5 * time.Second,
		DrainTimeout:   30 * time.Second,
		HandshakeTTL:   10 * time.Second,

		CacheEntries:  100000,
		SchemaCache:   1024,
		BatchOps:      512,
		MaxInflight:   4,
		SweepInterval: time.Second,
	}
}

// Limits are the abuse limits of design §6.4.
type Limits struct {
	MaxPayloadBytes   int
	MaxCarriedCaps    int
	MaxSchemaBytes    int
	MaxBaseBytes      int
	BlobBytesPer10s   int
	ActionsPerSec     float64
	ActionsBurst      float64
	MintsPerSec       float64
	MintsBurst        float64
	CapsUsedPer10s    int
	UnknownPer10s     int
	MaxLiveCaps       int
	SessionsPerIP     int
	HandshakesPerSec  float64
	HandshakeBurst    float64
	IPAllowlist       []netip.Prefix
	MintAdmission     float64 // records/s per Broker
	MaxPeerQueue      int     // replication ops queued per peer
	HeldPerTarget     int
	MaxPendingPerSess int // queued sends awaiting a result per session
}

// DefaultLimits returns the defaults of design §6.4 and §14.
func DefaultLimits() Limits {
	return Limits{
		MaxPayloadBytes:   64 << 10,
		MaxCarriedCaps:    16,
		MaxSchemaBytes:    32 << 10,
		MaxBaseBytes:      4 << 10,
		BlobBytesPer10s:   64 << 10,
		ActionsPerSec:     1000,
		ActionsBurst:      2000,
		MintsPerSec:       100,
		MintsBurst:        200,
		CapsUsedPer10s:    4096,
		UnknownPer10s:     100,
		MaxLiveCaps:       10000,
		SessionsPerIP:     64,
		HandshakesPerSec:  20,
		HandshakeBurst:    200,
		MintAdmission:     20000,
		MaxPeerQueue:      100000,
		HeldPerTarget:     100,
		MaxPendingPerSess: 4096,
	}
}

// MemberInfo is the JSON member object in S3 (design §4).
type MemberInfo struct {
	NodePub     string   `json:"node_pub"` // hex of the 32-byte node public key
	DiscoPub    string   `json:"disco_pub,omitempty"`
	Incarnation string   `json:"incarnation"` // hex uint64
	Draining    bool     `json:"draining"`
	Version     string   `json:"version"`
	WSURLs      []string `json:"ws_urls,omitempty"`
	Region      string   `json:"region,omitempty"`
	// Endpoints lists the mesh's direct endpoints ("ip:port"): UDP for
	// the Tailscale mesh, TCP for the development mesh.
	Endpoints []string `json:"endpoints,omitempty"`
	// DERP is the DERP relay region this Broker chose as its home, as a
	// Tailscale DERPRegion object, so peers can reach it through that relay.
	// Brokers are not relays. Opaque to the node.
	DERP json.RawMessage `json:"derp,omitempty"`
}

// Config configures a Node.
type Config struct {
	// NodePub is the node public key; the Broker ID is its SHA-256.
	NodePub []byte
	// Member holds the static fields of this Broker's member object.
	Member MemberInfo
	// PublicNames are the authorities ("host[:port]") endpoints may dial.
	PublicNames []string
	Tunables    Tunables
	Limits      Limits
	// Debug enables verbose logging of routing and presence decisions.
	Debug bool
}
