package rabbitmq

import (
	"testing"

	"github.com/JiangHe12/mqgov-cli/internal/mqgov"
)

func TestSupportsACLFalse(t *testing.T) {
	backend := &Broker{opts: Options{Cluster: "test"}}
	if backend.Capabilities().SupportsACL {
		t.Fatalf("SupportsACL = true, want false")
	}
	if _, ok := mqgov.SupportsACL(backend); ok {
		t.Fatalf("SupportsACL capability assertion = true, want false")
	}
}
