// Package wire defines the endpoint WebSocket protocol (design §6): JSON text
// frames, the X25519 handshake proof, and error codes. It is shared by the
// Broker and the Go SDK; docs/protocol.md specifies it for other SDKs.
package wire

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/json"
)

// Subprotocol is the WebSocket subprotocol name.
const Subprotocol = "nimbo.v1"

// Error codes (design §6.3).
const (
	ErrBadRequest      = "bad_request"
	ErrInvalidCap      = "invalid_cap"
	ErrNotHolder       = "not_holder"
	ErrExpired         = "expired"
	ErrConsumed        = "consumed"
	ErrTargetGone      = "target_gone"
	ErrSchemaViolation = "schema_violation"
	ErrBaseConflict    = "base_conflict"
	ErrTooLarge        = "too_large"
	ErrRateLimited     = "rate_limited"
	ErrUnavailable     = "unavailable"
)

// Bounce reasons.
const (
	BounceTTLExpired   = "ttl_expired"
	BounceReceiverBusy = "receiver_busy"
	BounceTooLarge     = "too_large"
	BounceTargetGone   = "target_gone"
	BounceUnavailable  = "unavailable"
)

// In is any endpoint → broker frame. Only the fields of its op are set.
type In struct {
	Op  string `json:"op"`
	Rid uint64 `json:"rid,omitempty"`

	// hello
	Key             string `json:"key,omitempty"`
	Authority       string `json:"authority,omitempty"`
	Proof           string `json:"proof,omitempty"`
	Client          string `json:"client,omitempty"`
	MaxMessageBytes int    `json:"max_message_bytes,omitempty"`

	// send, transfer, inspect
	Cap     string          `json:"cap,omitempty"`
	Payload json.RawMessage `json:"payload,omitempty"`
	Caps    []string        `json:"caps,omitempty"` // send: carried; renew: caps to renew
	TTLms   int64           `json:"ttl_ms,omitempty"`
	Reply   *ReplyOpts      `json:"reply,omitempty"`

	// mint
	Schema    json.RawMessage `json:"schema,omitempty"`
	Base      json.RawMessage `json:"base,omitempty"`
	AutoReply bool            `json:"auto_reply,omitempty"`
	Durable   bool            `json:"durable,omitempty"`

	// transfer
	To string `json:"to,omitempty"`
}

// ReplyOpts configure the single-use reply cap of a send.
type ReplyOpts struct {
	Schema    json.RawMessage `json:"schema,omitempty"`
	Base      json.RawMessage `json:"base,omitempty"`
	AutoReply bool            `json:"auto_reply,omitempty"`
}

// Challenge is the first broker frame.
type Challenge struct {
	Op     string `json:"op"` // "challenge"
	V      int    `json:"v"`
	Broker string `json:"broker"`
	Eph    string `json:"eph"`
	Nonce  string `json:"nonce"`
}

// Limits advertised in welcome.
type Limits struct {
	MaxPayloadBytes int   `json:"max_payload_bytes"`
	MaxCarriedCaps  int   `json:"max_carried_caps"`
	MaxSchemaBytes  int   `json:"max_schema_bytes"`
	MaxBaseBytes    int   `json:"max_base_bytes"`
	ActionsPerSec   int   `json:"actions_per_sec"`
	MintsPerSec     int   `json:"mints_per_sec"`
	MaxLiveCaps     int   `json:"max_live_caps"`
	DefaultTTLms    int64 `json:"default_ttl_ms"`
	MaxTTLms        int64 `json:"max_ttl_ms"`
}

type Welcome struct {
	Op      string   `json:"op"` // "welcome"
	Session string   `json:"session"`
	Limits  Limits   `json:"limits"`
	Brokers []string `json:"brokers,omitempty"`
}

type Error struct {
	Code   string `json:"code"`
	Detail string `json:"detail,omitempty"`
}

// RenewItem is the per-cap answer of renew.
type RenewItem struct {
	Cap   string `json:"cap"`
	OK    bool   `json:"ok"`
	TTLms int64  `json:"ttl_ms,omitempty"`
	Error *Error `json:"error,omitempty"`
}

// CapInfo is the inspect answer.
type CapInfo struct {
	Lineage   string          `json:"lineage"`
	Kind      string          `json:"kind"`
	SingleUse bool            `json:"single_use,omitempty"`
	Target    string          `json:"target"`
	Holder    string          `json:"holder"`
	Issuer    string          `json:"issuer"`
	MintedAt  int64           `json:"minted_at"`
	Schema    json.RawMessage `json:"schema,omitempty"`
	Base      json.RawMessage `json:"base,omitempty"`
	AutoReply bool            `json:"auto_reply,omitempty"`
	TTLms     int64           `json:"ttl_ms"`
}

// Result answers a request.
type Result struct {
	Op      string      `json:"op"` // "result"
	Rid     uint64      `json:"rid"`
	OK      bool        `json:"ok"`
	Error   *Error      `json:"error,omitempty"`
	Msg     string      `json:"msg,omitempty"`
	ReplyID string      `json:"reply_id,omitempty"`
	Cap     string      `json:"cap,omitempty"`
	Renewed []RenewItem `json:"renewed,omitempty"`
	Info    *CapInfo    `json:"info,omitempty"`
}

type Deliver struct {
	Op      string          `json:"op"` // "deliver"
	Msg     string          `json:"msg"`
	On      string          `json:"on"`
	Payload json.RawMessage `json:"payload"`
	Caps    []string        `json:"caps,omitempty"`
	Reply   string          `json:"reply,omitempty"`
}

type Bounce struct {
	Op     string `json:"op"` // "bounce"
	Msg    string `json:"msg"`
	Reason string `json:"reason"`
}

type Closing struct {
	Op           string `json:"op"` // "closing"
	Reason       string `json:"reason"`
	RetryAfterMs int64  `json:"retry_after_ms"`
}

// Out is used to decode any broker → endpoint frame.
type Out struct {
	Op string `json:"op"`
	// challenge
	V      int    `json:"v,omitempty"`
	Broker string `json:"broker,omitempty"`
	Eph    string `json:"eph,omitempty"`
	Nonce  string `json:"nonce,omitempty"`
	// welcome
	Session string   `json:"session,omitempty"`
	Limits  *Limits  `json:"limits,omitempty"`
	Brokers []string `json:"brokers,omitempty"`
	// result
	Rid     uint64      `json:"rid,omitempty"`
	OK      bool        `json:"ok,omitempty"`
	Error   *Error      `json:"error,omitempty"`
	Msg     string      `json:"msg,omitempty"`
	ReplyID string      `json:"reply_id,omitempty"`
	Cap     string      `json:"cap,omitempty"`
	Renewed []RenewItem `json:"renewed,omitempty"`
	Info    *CapInfo    `json:"info,omitempty"`
	// deliver
	On      string          `json:"on,omitempty"`
	Payload json.RawMessage `json:"payload,omitempty"`
	Caps    []string        `json:"caps,omitempty"`
	Reply   string          `json:"reply,omitempty"`
	// bounce, closing
	Reason       string `json:"reason,omitempty"`
	RetryAfterMs int64  `json:"retry_after_ms,omitempty"`
}

// Proof computes the hello proof:
// HMAC-SHA256(SHA-256("nimbo/v1/auth" ‖ ss), nonce ‖ authority ‖ E_pub ‖ B_eph_pub).
func Proof(ss, nonce []byte, authority string, ePub, bEph []byte) []byte {
	k := sha256.Sum256(append([]byte("nimbo/v1/auth"), ss...))
	m := hmac.New(sha256.New, k[:])
	m.Write(nonce)
	m.Write([]byte(authority))
	m.Write(ePub)
	m.Write(bEph)
	return m.Sum(nil)
}
