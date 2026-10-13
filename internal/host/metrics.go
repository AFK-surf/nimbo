package host

import (
	"encoding/json"
	"fmt"
	"net/http"
	"time"

	"github.com/afk-surf/nimbo/internal/node"
)

// PeerPather is implemented by meshes that can report how each peer is
// reached; /debug/stats includes it. A positive ping also measures each
// peer's round trip, waiting at most that long.
type PeerPather interface {
	PeerPaths(ping time.Duration) []node.MeshPeer
}

// Snapshot reads the node's counters on the loop.
func (h *Host) Snapshot() node.Snapshot {
	var s node.Snapshot
	h.Call(func() { s = h.node.Snapshot() })
	return s
}

func b2f(b bool) int {
	if b {
		return 1
	}
	return 0
}

// MetricsHandler serves Prometheus metrics, /healthz and /debug/stats.
func (h *Host) MetricsHandler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/metrics", func(w http.ResponseWriter, r *http.Request) {
		s := h.Snapshot()
		w.Header().Set("Content-Type", "text/plain; version=0.0.4")
		c := func(name, help string, v any) {
			fmt.Fprintf(w, "# HELP nimbo_%s %s\n# TYPE nimbo_%s counter\nnimbo_%s %v\n", name, help, name, name, v)
		}
		g := func(name, help string, v any) {
			fmt.Fprintf(w, "# HELP nimbo_%s %s\n# TYPE nimbo_%s gauge\nnimbo_%s %v\n", name, help, name, name, v)
		}
		c("messages_delivered_total", "Messages written to local sessions.", s.Delivered)
		c("messages_forwarded_total", "Messages accepted by another Broker.", s.Forwarded)
		c("messages_bounced_total", "Bounces generated.", s.Bounced)
		c("messages_dropped_total", "Forwards with unknown outcome, dropped (at most once).", s.Dropped)
		c("messages_held_total", "Messages held for a reconnecting target.", s.Held)
		c("caps_minted_total", "Caps minted.", s.Minted)
		c("caps_transferred_total", "Caps transferred.", s.Transferred)
		c("reply_caps_total", "Single-use reply caps created.", s.Replies)
		c("lookups_remote_total", "Record lookups sent to other Brokers.", s.LookupsRemote)
		c("lookups_unresolved_total", "Lookups that ended unavailable.", s.LookupsUnresolved)
		c("hinted_writes_total", "Hinted copies written for suspect Brokers.", s.HintedWrites)
		c("view_changes_total", "Membership view changes.", s.ViewChanges)
		c("fenced_total", "Times this Broker fenced itself.", s.Fenced)
		c("streams_opened_total", "Streams opened by local holders.", s.StreamsOpened)
		c("stream_offers_total", "Stream offers delivered to local targets.", s.StreamOffers)
		c("stream_attaches_total", "Data sockets attached with a valid token.", s.StreamAttaches)
		c("streams_paired_total", "Holder halves paired with a target half.", s.StreamsPaired)
		c("streams_spliced_total", "Half-streams spliced (bytes flowing).", s.StreamsSpliced)
		g("members", "Live members in this Broker's view.", s.Members)
		g("sessions", "Established endpoint sessions.", s.Sessions)
		g("records", "Cap and blob records held or cached.", s.Records)
		g("presence_keys", "Endpoints with presence registers held here.", s.Presence)
		g("replication_queue_ops", "Replication ops waiting for acknowledgement.", s.QueuedOps)
		g("held_messages", "Messages held for reconnecting targets.", s.HeldMessages)
		g("suspect_peers", "Peers currently suspected.", s.Suspects)
		g("pending_half_streams", "Half-streams waiting to attach or pair.", s.PendingStreams)
		g("stream_legs", "Data sockets and mesh data connections.", s.StreamLegs)
		g("is_fenced", "1 if this Broker has fenced itself.", b2f(s.FencedNow))
		g("is_draining", "1 if this Broker is draining.", b2f(s.Draining))
		g("s3_clock_offset_seconds", "Estimated S3 time minus local time.", s.S3Offset.Seconds())
	})
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) {
		if s := h.Snapshot(); !s.Ready {
			http.Error(w, "not ready", http.StatusServiceUnavailable)
			return
		}
		fmt.Fprintln(w, "ok")
	})
	mux.HandleFunc("/debug/stats", func(w http.ResponseWriter, r *http.Request) {
		out := struct {
			node.Snapshot
			// MeshPeers is the data plane's path to each peer, when the
			// mesh reports it (the Tailscale mesh does).
			MeshPeers []node.MeshPeer `json:",omitempty"`
		}{Snapshot: h.Snapshot()}
		if pp, ok := h.opts.Mesh.(PeerPather); ok {
			var ping time.Duration
			if r.URL.Query().Has("ping") {
				ping = 2 * time.Second
			}
			out.MeshPeers = pp.PeerPaths(ping)
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(out)
	})
	return mux
}
