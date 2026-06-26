//go:build integration

package kafka

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/JiangHe12/mqgov-cli/internal/mqgov"
)

func TestKafkaACLIntegration(t *testing.T) {
	brokers := os.Getenv("KAFKA_ACL_BROKERS")
	if strings.TrimSpace(brokers) == "" {
		t.Skip("KAFKA_ACL_BROKERS not set")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	backend, err := New(Options{
		Brokers:       []string{brokers},
		Cluster:       "integration-acl",
		Username:      os.Getenv("KAFKA_ACL_USERNAME"),
		Password:      os.Getenv("KAFKA_ACL_PASSWORD"),
		SASLMechanism: getenvDefault("KAFKA_ACL_SASL_MECHANISM", "SCRAM-SHA-256"),
		TLS:           os.Getenv("KAFKA_ACL_TLS") == "true",
		CACertFile:    os.Getenv("KAFKA_ACL_CA_CERT_FILE"),
		TLSPinPath:    filepath.Join(t.TempDir(), "tls_known_hosts"),
		Timeout:       10 * time.Second,
	})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	manager, ok := mqgov.SupportsACL(backend)
	if !ok {
		t.Fatalf("SupportsACL() = false, want true")
	}
	binding := mqgov.ACLBinding{
		Principal:    "User:mqgov-acl-it",
		Host:         "*",
		ResourceType: "topic",
		ResourceName: fmt.Sprintf("mqgov-acl-it-%d", time.Now().UnixNano()),
		PatternType:  "literal",
		Operation:    "read",
		Permission:   "allow",
	}
	defer func() { _ = manager.RevokeACL(context.Background(), binding) }()

	if err := manager.GrantACL(ctx, binding); err != nil {
		t.Fatalf("GrantACL() error = %v", err)
	}
	items, err := manager.ListACLs(ctx, mqgov.ACLFilter{Principal: binding.Principal, ResourceType: binding.ResourceType, ResourceName: binding.ResourceName})
	if err != nil {
		t.Fatalf("ListACLs(after grant) error = %v", err)
	}
	if !containsACL(items, binding) {
		t.Fatalf("ListACLs(after grant) = %+v, want %+v", items, binding)
	}
	if err := manager.RevokeACL(ctx, binding); err != nil {
		t.Fatalf("RevokeACL() error = %v", err)
	}
	items, err = manager.ListACLs(ctx, mqgov.ACLFilter{Principal: binding.Principal, ResourceType: binding.ResourceType, ResourceName: binding.ResourceName})
	if err != nil {
		t.Fatalf("ListACLs(after revoke) error = %v", err)
	}
	if containsACL(items, binding) {
		t.Fatalf("ListACLs(after revoke) = %+v, still contains %+v", items, binding)
	}
}

func getenvDefault(name, fallback string) string {
	if value := os.Getenv(name); value != "" {
		return value
	}
	return fallback
}

func containsACL(items []mqgov.ACLBinding, want mqgov.ACLBinding) bool {
	for _, item := range items {
		if item.Principal == want.Principal &&
			item.Host == want.Host &&
			item.ResourceType == want.ResourceType &&
			item.ResourceName == want.ResourceName &&
			item.PatternType == want.PatternType &&
			item.Operation == want.Operation &&
			item.Permission == want.Permission {
			return true
		}
	}
	return false
}
