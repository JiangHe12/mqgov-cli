package rocketmq

import (
	"context"
	"testing"

	"github.com/JiangHe12/opskit-core/apperrors"

	"github.com/JiangHe12/mqgov-cli/internal/mqgov"
)

func TestCapabilitiesAreHonestPartialImplementation(t *testing.T) {
	backend := &Broker{opts: Options{Cluster: "test"}}

	caps := backend.Capabilities()
	if caps.SupportsOffsets || caps.SupportsPartitions || caps.SupportsACL {
		t.Fatalf("capabilities = %+v, want all optional capabilities false", caps)
	}
	if _, ok := mqgov.SupportsACL(backend); ok {
		t.Fatalf("SupportsACL capability assertion = true, want false")
	}
}

func TestACLCapabilityFailsClosed(t *testing.T) {
	backend := &Broker{opts: Options{Cluster: "test"}}

	caps := backend.Capabilities()
	if caps.SupportsACL {
		t.Fatalf("SupportsACL = true, want false")
	}
	if contains(caps.ResourceTypes, "acl") {
		t.Fatalf("ResourceTypes = %v, want no acl resource", caps.ResourceTypes)
	}
	if contains(caps.Verbs, "grant-acl") || contains(caps.Verbs, "revoke-acl") {
		t.Fatalf("Verbs = %v, want no ACL mutation verbs", caps.Verbs)
	}
	if _, ok := mqgov.SupportsACL(backend); ok {
		t.Fatalf("RocketMQ implements ACLManager; want fail-closed SupportsACL gate")
	}
}

func TestSchemaCapabilityFailsClosed(t *testing.T) {
	backend := &Broker{opts: Options{Cluster: "test"}}

	caps := backend.Capabilities()
	if caps.SupportsSchema {
		t.Fatalf("SupportsSchema = true, want false")
	}
	if contains(caps.ResourceTypes, "schema") {
		t.Fatalf("ResourceTypes = %v, want no schema resource", caps.ResourceTypes)
	}
	if contains(caps.Verbs, "check-schema") {
		t.Fatalf("Verbs = %v, want no schema verbs", caps.Verbs)
	}
	if _, ok := mqgov.SupportsSchema(backend); ok {
		t.Fatalf("RocketMQ implements SchemaManager; want fail-closed SupportsSchema gate")
	}
}

func TestUnsupportedGroupOperationsFailClosed(t *testing.T) {
	backend := &Broker{}

	_, err := backend.ListGroups(context.Background(), mqgov.GroupListOptions{})
	if got := apperrors.AsAppError(err).Code; got != apperrors.CodeNotImplemented {
		t.Fatalf("ListGroups() code = %s, want %s", got, apperrors.CodeNotImplemented)
	}
	if _, err := backend.CreateGroup(context.Background(), mqgov.GroupCoordinate{}); apperrors.AsAppError(err).Code != apperrors.CodeNotImplemented {
		t.Fatalf("CreateGroup() error = %v, want NotImplemented", err)
	}
	if err := backend.DeleteGroup(context.Background(), mqgov.GroupCoordinate{}); apperrors.AsAppError(err).Code != apperrors.CodeNotImplemented {
		t.Fatalf("DeleteGroup() error = %v, want NotImplemented", err)
	}
}

func TestInternalTopicDetection(t *testing.T) {
	for _, topic := range []string{"RMQ_SYS_TRACE_TOPIC", "%RETRY%orders", "%DLQ%orders", "SCHEDULE_TOPIC_XXXX"} {
		if !isInternalTopic(topic) {
			t.Fatalf("isInternalTopic(%q) = false, want true", topic)
		}
	}
	if isInternalTopic("orders") {
		t.Fatalf("isInternalTopic(orders) = true, want false")
	}
}

func TestNewRejectsUnsupportedTLS(t *testing.T) {
	_, err := New(Options{NameServers: []string{"127.0.0.1:9876"}, TLS: true})
	if got := apperrors.AsAppError(err).Code; got != apperrors.CodeNotImplemented {
		t.Fatalf("New(TLS) code = %s, want %s", got, apperrors.CodeNotImplemented)
	}
}

func contains(items []string, want string) bool {
	for _, item := range items {
		if item == want {
			return true
		}
	}
	return false
}
