package mesh

import (
	"context"
	_ "embed"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"slices"
	"sort"
	"strconv"
	"strings"

	"tailscale.com/tailcfg"
)

// Brokers do not relay. Each Broker chooses its own DERP relays (by default
// Tailscale's public ones), uses the nearest as its home region, and
// publishes that region in its member object. Peers add every published
// region to their own map, so a Broker is reachable through the relay it
// chose. Region IDs are local to each Broker's map.

// DefaultDERPMapURL serves Tailscale's map of its public DERP relays.
const DefaultDERPMapURL = "https://controlplane.tailscale.com/derpmap/default"

// derpmap_default.json is a copy of DefaultDERPMapURL, used when it cannot
// be fetched.
//
//go:embed derpmap_default.json
var defaultDERPMap []byte

// Region IDs are 16-bit (magicsock encodes them as a port). Regions only
// peers use get IDs from peerRegionBase up; a Broker's own regions must
// have lower IDs.
const (
	peerRegionBase = 40000
	maxRegionID    = 65535
)

// LoadDERPMap loads the DERP map at src: an http(s) URL or a JSON file
// ("" means DefaultDERPMapURL). If the default map cannot be fetched, it
// returns the copy built into the binary along with the fetch error.
func LoadDERPMap(ctx context.Context, src string) (*tailcfg.DERPMap, error) {
	if src == "" {
		src = DefaultDERPMapURL
	}
	var dm *tailcfg.DERPMap
	var err error
	if strings.HasPrefix(src, "https://") || strings.HasPrefix(src, "http://") {
		dm, err = fetchDERPMap(ctx, src)
	} else {
		var b []byte
		if b, err = os.ReadFile(src); err == nil {
			dm, err = parseDERPMap(b)
		}
	}
	if err != nil && src == DefaultDERPMapURL {
		builtin, perr := parseDERPMap(defaultDERPMap)
		if perr != nil {
			panic("nimbo: built-in DERP map: " + perr.Error())
		}
		return builtin, err
	}
	return dm, err
}

// RelayMap is a map with one region: the DERP relay at hostport (port 443
// if omitted), e.g. a self-hosted derper.
func RelayMap(hostport string) (*tailcfg.DERPMap, error) {
	host, port := hostport, 443
	if h, p, err := net.SplitHostPort(hostport); err == nil {
		n, err := strconv.Atoi(p)
		if err != nil || n <= 0 || n > 65535 {
			return nil, fmt.Errorf("DERP relay %q: bad port", hostport)
		}
		host, port = h, n
	}
	if host == "" {
		return nil, fmt.Errorf("DERP relay %q: no host", hostport)
	}
	const id = 900
	return &tailcfg.DERPMap{Regions: map[tailcfg.DERPRegionID]*tailcfg.DERPRegion{
		id: {RegionID: id, RegionCode: "relay", RegionName: host, Nodes: []*tailcfg.DERPNode{
			{Name: "900a", RegionID: id, HostName: host, DERPPort: port},
		}},
	}}, nil
}

// SkipCertVerify returns a copy of dm whose relays are dialed without
// verifying their TLS certificates (e.g. a derper with a self-signed
// certificate). The setting is published with the home region, so peers
// dial it the same way.
func SkipCertVerify(dm *tailcfg.DERPMap) *tailcfg.DERPMap {
	c := dm.Clone()
	for _, r := range c.Regions {
		for _, n := range r.Nodes {
			n.InsecureForTests = true
		}
	}
	return c
}

func fetchDERPMap(ctx context.Context, url string) (*tailcfg.DERPMap, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, err
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("DERP map %s: %s", url, resp.Status)
	}
	b, err := io.ReadAll(io.LimitReader(resp.Body, 4<<20))
	if err != nil {
		return nil, err
	}
	return parseDERPMap(b)
}

func parseDERPMap(b []byte) (*tailcfg.DERPMap, error) {
	var dm tailcfg.DERPMap
	if err := json.Unmarshal(b, &dm); err != nil {
		return nil, err
	}
	return &dm, checkOwnMap(&dm)
}

func checkOwnMap(dm *tailcfg.DERPMap) error {
	if len(dm.Regions) == 0 {
		return errors.New("DERP map has no regions")
	}
	for id, r := range dm.Regions {
		if id <= 0 || id >= peerRegionBase || r == nil || r.RegionID != id || len(r.Nodes) == 0 || slices.Contains(r.Nodes, nil) {
			return fmt.Errorf("DERP map: bad region %d (IDs must be 1..%d and match, with nodes)", id, peerRegionBase-1)
		}
	}
	return nil
}

// regionKey identifies a relay region by its nodes, independent of the ID a
// map gives it.
func regionKey(r *tailcfg.DERPRegion) string {
	var parts []string
	for _, n := range r.Nodes {
		if n == nil {
			continue
		}
		parts = append(parts, fmt.Sprintf("%s|%s|%s|%s|%d|%d|%t|%t", n.HostName, n.CertName, n.IPv4, n.IPv6, n.DERPPort, n.STUNPort, n.STUNOnly, n.InsecureForTests))
	}
	sort.Strings(parts)
	return strings.Join(parts, "\n")
}

// publishedRegion renders a home region for a member object: the relay's
// nodes, without this Broker's local IDs or flags.
func publishedRegion(r *tailcfg.DERPRegion) json.RawMessage {
	c := r.Clone()
	c.RegionID, c.Avoid, c.NoMeasureNoHome = 0, false, false
	for _, n := range c.Nodes {
		n.RegionID = 0
	}
	b, _ := json.Marshal(c)
	return b
}

// derpIndex builds a Broker's DERP map: its own regions, plus the regions
// peers publish, under IDs that stay stable while the Broker runs.
type derpIndex struct {
	own     *tailcfg.DERPMap
	ownKeys map[string]tailcfg.DERPRegionID
	peer    map[string]tailcfg.DERPRegionID
	next    tailcfg.DERPRegionID
}

func (x *derpIndex) setOwn(dm *tailcfg.DERPMap) {
	x.own = dm
	x.ownKeys = map[string]tailcfg.DERPRegionID{}
	if dm == nil {
		return
	}
	for id, r := range dm.Regions {
		x.ownKeys[regionKey(r)] = id
	}
}

// build returns the map for the given published peer regions, and the
// local region ID of each (0 if it published none or an invalid one).
// A peer region equal to one of the Broker's own keeps that region's ID;
// any other is added with NoMeasureNoHome, so it is used only to reach the
// peers that chose it, never as this Broker's home.
func (x *derpIndex) build(published []json.RawMessage) (*tailcfg.DERPMap, []tailcfg.DERPRegionID) {
	ids := make([]tailcfg.DERPRegionID, len(published))
	var dm *tailcfg.DERPMap
	if x.own != nil {
		dm = x.own.Clone()
	}
	for i, raw := range published {
		if len(raw) == 0 {
			continue
		}
		var r tailcfg.DERPRegion
		if json.Unmarshal(raw, &r) != nil {
			continue
		}
		nodes := r.Nodes[:0]
		for _, n := range r.Nodes {
			if n != nil && n.HostName != "" {
				nodes = append(nodes, n)
			}
		}
		if r.Nodes = nodes; len(nodes) == 0 {
			continue
		}
		key := regionKey(&r)
		if id, ok := x.ownKeys[key]; ok {
			ids[i] = id
			continue
		}
		id, ok := x.peer[key]
		if !ok {
			if x.peer == nil {
				x.peer, x.next = map[string]tailcfg.DERPRegionID{}, peerRegionBase
			}
			if x.next > maxRegionID {
				continue // out of IDs: reach this peer directly only
			}
			id = x.next
			x.next++
			x.peer[key] = id
		}
		ids[i] = id
		if dm == nil {
			dm = &tailcfg.DERPMap{Regions: map[tailcfg.DERPRegionID]*tailcfg.DERPRegion{}}
		}
		if dm.Regions[id] == nil {
			r.RegionID, r.Avoid, r.NoMeasureNoHome = id, false, true
			if r.RegionCode == "" {
				r.RegionCode = fmt.Sprintf("peer%d", id-peerRegionBase)
			}
			for j, n := range r.Nodes {
				n.RegionID = id
				if n.Name == "" {
					n.Name = fmt.Sprintf("%d%c", id, 'a'+j%26)
				}
			}
			dm.Regions[id] = &r
		}
	}
	return dm, ids
}
