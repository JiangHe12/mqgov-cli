package kafka

import "testing"

func TestCapabilitiesAdvertiseACL(t *testing.T) {
	backend := &Broker{opts: Options{Cluster: "test"}}
	caps := backend.Capabilities()
	if !caps.SupportsACL {
		t.Fatalf("SupportsACL = false, want true")
	}
}
