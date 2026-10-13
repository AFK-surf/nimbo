package mesh

import (
	"context"
	"encoding/json"
	"testing"

	"tailscale.com/tailcfg"
)

func relayRegion(id tailcfg.DERPRegionID, host string) *tailcfg.DERPRegion {
	return &tailcfg.DERPRegion{RegionID: id, RegionCode: host, Nodes: []*tailcfg.DERPNode{{Name: "a", RegionID: id, HostName: host, DERPPort: 443}}}
}

func TestDERPIndex(t *testing.T) {
	own := &tailcfg.DERPMap{Regions: map[tailcfg.DERPRegionID]*tailcfg.DERPRegion{
		1: relayRegion(1, "nyc.example"),
		2: relayRegion(2, "ord.example"),
	}}
	if err := checkOwnMap(own); err != nil {
		t.Fatal(err)
	}
	var x derpIndex
	x.setOwn(own)
	// Peers publish without IDs; one chose a relay this Broker also has.
	sameAsOwn := publishedRegion(relayRegion(7, "ord.example"))
	other := publishedRegion(relayRegion(7, "self-hosted.example"))
	insecure := publishedRegion(SkipCertVerify(&tailcfg.DERPMap{Regions: map[tailcfg.DERPRegionID]*tailcfg.DERPRegion{
		3: relayRegion(3, "self-hosted.example")}}).Regions[3])
	dm, ids := x.build([]json.RawMessage{sameAsOwn, other, nil, other, json.RawMessage(`{"Nodes":[null]}`), insecure})

	if ids[0] != 2 {
		t.Errorf("a peer on one of our relays: region %d, want our ID 2", ids[0])
	}
	if ids[1] < peerRegionBase || ids[3] != ids[1] {
		t.Errorf("peers on the same other relay: regions %d, %d", ids[1], ids[3])
	}
	if ids[2] != 0 || ids[4] != 0 {
		t.Errorf("no or invalid relay published: regions %d, %d, want 0", ids[2], ids[4])
	}
	if ids[5] == ids[1] || ids[5] < peerRegionBase {
		t.Errorf("the same host without certificate verification is a different relay: %d vs %d", ids[5], ids[1])
	}
	r := dm.Regions[ids[1]]
	if r == nil || !r.NoMeasureNoHome || r.Nodes[0].RegionID != ids[1] || r.Nodes[0].HostName != "self-hosted.example" {
		t.Fatalf("peer region: %+v", r)
	}
	if !dm.Regions[ids[5]].Nodes[0].InsecureForTests {
		t.Error("a published opt-out of certificate verification was lost")
	}
	if dm.Regions[1].NoMeasureNoHome || dm.Regions[2].NoMeasureNoHome {
		t.Error("own regions must stay eligible as home")
	}
	if len(own.Regions) != 2 {
		t.Error("build modified the own map")
	}
	// IDs stay stable across rebuilds, whatever the order.
	_, again := x.build([]json.RawMessage{insecure, other})
	if again[0] != ids[5] || again[1] != ids[1] {
		t.Errorf("IDs changed across rebuilds: %v then %v", ids, again)
	}
}

func TestPublishedRegionHasNoLocalState(t *testing.T) {
	r := relayRegion(5, "relay.example")
	r.NoMeasureNoHome = true
	var got tailcfg.DERPRegion
	if err := json.Unmarshal(publishedRegion(r), &got); err != nil {
		t.Fatal(err)
	}
	if got.RegionID != 0 || got.Nodes[0].RegionID != 0 || got.NoMeasureNoHome {
		t.Errorf("published %+v", got)
	}
	if r.RegionID != 5 || !r.NoMeasureNoHome {
		t.Error("publishing modified the region")
	}
}

func TestOwnMapChecks(t *testing.T) {
	for _, dm := range []*tailcfg.DERPMap{
		{},
		{Regions: map[tailcfg.DERPRegionID]*tailcfg.DERPRegion{peerRegionBase: relayRegion(peerRegionBase, "x")}},
		{Regions: map[tailcfg.DERPRegionID]*tailcfg.DERPRegion{3: relayRegion(4, "x")}},
		{Regions: map[tailcfg.DERPRegionID]*tailcfg.DERPRegion{3: {RegionID: 3}}},
	} {
		if checkOwnMap(dm) == nil {
			t.Errorf("accepted %+v", dm)
		}
	}
	m, err := RelayMap("derp.example:8443")
	if err != nil || checkOwnMap(m) != nil || m.Regions[900].Nodes[0].DERPPort != 8443 {
		t.Fatalf("RelayMap: %+v, %v", m, err)
	}
	if _, err := RelayMap("derp.example:x"); err == nil {
		t.Error("RelayMap accepted a bad port")
	}
}

func TestBuiltinDERPMap(t *testing.T) {
	dm, err := parseDERPMap(defaultDERPMap)
	if err != nil || len(dm.Regions) < 10 {
		t.Fatalf("built-in map: %d regions, %v", len(dm.Regions), err)
	}
	// An unreachable default URL falls back to the built-in copy; a custom
	// source does not.
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if dm, err := LoadDERPMap(ctx, ""); dm == nil || err == nil {
		t.Errorf("default map without network: %v, %v", dm != nil, err)
	}
	if dm, err := LoadDERPMap(ctx, "https://derp.example/map"); dm != nil || err == nil {
		t.Errorf("custom map without network: %v, %v", dm != nil, err)
	}
}
