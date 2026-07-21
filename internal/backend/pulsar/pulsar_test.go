package pulsar

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/JiangHe12/opskit-core/v2/apperrors"

	"github.com/JiangHe12/mqgov-cli/internal/mqgov"
)

type closeIdleTransport struct {
	http.RoundTripper
	calls int
}

func (transport *closeIdleTransport) CloseIdleConnections() {
	transport.calls++
}

func TestCloseIsIdempotent(t *testing.T) {
	t.Parallel()
	transport := &closeIdleTransport{}
	backend := &Broker{httpClient: &http.Client{Transport: transport}}

	backend.Close()
	backend.Close()
	if transport.calls != 1 {
		t.Fatalf("CloseIdleConnections() calls = %d, want 1", transport.calls)
	}
}

func TestListGroupsNotSupportedWithoutTopic(t *testing.T) {
	backend := &Broker{}

	_, err := backend.ListGroups(context.Background(), mqgov.GroupListOptions{})
	if got := apperrors.AsAppError(err).Code; got != apperrors.CodeNotImplemented {
		t.Fatalf("ListGroups() code = %s, want %s", got, apperrors.CodeNotImplemented)
	}
	if got := apperrors.AsAppError(err).Message; got != "Pulsar subscriptions are per-topic; list is not supported without a topic" {
		t.Fatalf("ListGroups() message = %q", got)
	}
}

func TestSupportsACLTrue(t *testing.T) {
	backend := &Broker{opts: Options{Cluster: "test"}}
	if !backend.Capabilities().SupportsACL {
		t.Fatalf("SupportsACL = false, want true")
	}
	if _, ok := mqgov.SupportsACL(backend); !ok {
		t.Fatalf("SupportsACL capability assertion = false, want true")
	}
}

func TestSupportsSchemaTrue(t *testing.T) {
	backend := &Broker{opts: Options{Cluster: "test"}}
	if !backend.Capabilities().SupportsSchema {
		t.Fatalf("SupportsSchema = false, want true")
	}
	if _, ok := mqgov.SupportsSchema(backend); !ok {
		t.Fatalf("SupportsSchema capability assertion = false, want true")
	}
}

func TestPulsarTLSRejectsExplicitPlaintextEndpoints(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		opts Options
	}{
		{
			name: "service URL",
			opts: Options{
				TLS:        true,
				ServiceURL: "pulsar://pulsar.example:6650",
				AdminURL:   "https://pulsar.example:8443",
			},
		},
		{
			name: "admin URL",
			opts: Options{
				TLS:        true,
				ServiceURL: "pulsar+ssl://pulsar.example:6651",
				AdminURL:   "http://pulsar.example:8080",
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			_, err := New(tt.opts)
			if err == nil {
				t.Fatal("New() error = nil, want plaintext endpoint rejection")
			}
			if got := apperrors.AsAppError(err).Code; got != apperrors.CodeUsageError {
				t.Fatalf("New() error = %v, code = %s, want %s", err, got, apperrors.CodeUsageError)
			}
		})
	}
}

func TestPulsarTLSDefaultsUseSecureEndpoints(t *testing.T) {
	t.Parallel()

	backend, err := New(Options{
		TLS:        true,
		TLSPinPath: filepath.Join(t.TempDir(), "tls_known_hosts"),
	})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	t.Cleanup(backend.client.Close)
	if !strings.HasPrefix(backend.opts.ServiceURL, "pulsar+ssl://") {
		t.Fatalf("ServiceURL = %q, want pulsar+ssl", backend.opts.ServiceURL)
	}
	if !strings.HasPrefix(backend.opts.AdminURL, "https://") {
		t.Fatalf("AdminURL = %q, want https", backend.opts.AdminURL)
	}
	if backend.tlsConfig == nil {
		t.Fatal("tlsConfig = nil, want TLS config")
	}
}

func TestPulsarCompatibilityFailsClosed(t *testing.T) {
	t.Parallel()
	officialResponse, err := os.ReadFile("testdata/compatibility_response.json")
	if err != nil {
		t.Fatalf("read official compatibility fixture: %v", err)
	}

	tests := []struct {
		name       string
		body       string
		compatible bool
		wantErr    bool
	}{
		{name: "official Java Bean response", body: string(officialResponse), compatible: true},
		{name: "official field response", body: `{"isCompatibility":false,"schemaCompatibilityStrategy":"FULL"}`, compatible: false},
		{name: "compatible legacy alias", body: `{"compatible":true}`, compatible: true},
		{name: "snake case legacy alias", body: `{"is_compatible":false}`, compatible: false},
		{name: "all aliases agree", body: `{"compatibility":true,"isCompatibility":true,"compatible":true,"is_compatible":true}`, compatible: true},
		{name: "duplicate alias agrees", body: `{"compatibility":true,"compatibility":true}`, compatible: true},
		{name: "empty", body: "", wantErr: true},
		{name: "malformed", body: `{`, wantErr: true},
		{name: "missing compatibility field", body: `{"message":"accepted"}`, wantErr: true},
		{name: "null compatibility field", body: `{"compatibility":null}`, wantErr: true},
		{name: "non boolean compatibility field", body: `{"compatibility":"true"}`, wantErr: true},
		{name: "duplicate alias conflicts", body: `{"compatibility":false,"compatibility":true}`, wantErr: true},
		{name: "case folded duplicate conflicts", body: `{"compatibility":false,"Compatibility":true}`, wantErr: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			compatible, _, err := pulsarCompatibility([]byte(tt.body))
			if tt.wantErr {
				if err == nil {
					t.Fatal("pulsarCompatibility() error = nil, want fail-closed error")
				}
				return
			}
			if err != nil {
				t.Fatalf("pulsarCompatibility() error = %v", err)
			}
			if compatible != tt.compatible {
				t.Fatalf("pulsarCompatibility() compatible = %t, want %t", compatible, tt.compatible)
			}
		})
	}
}

func TestPulsarCompatibilityRejectsEveryAliasConflict(t *testing.T) {
	t.Parallel()
	aliases := []string{"compatibility", "isCompatibility", "compatible", "is_compatible"}
	for i := range aliases {
		for j := i + 1; j < len(aliases); j++ {
			left, right := aliases[i], aliases[j]
			t.Run(left+"_vs_"+right, func(t *testing.T) {
				t.Parallel()
				body := fmt.Sprintf(`{"%s":true,"%s":false}`, left, right)
				if _, _, err := pulsarCompatibility([]byte(body)); err == nil {
					t.Fatal("pulsarCompatibility() error = nil, want conflicting aliases to fail closed")
				}
			})
		}
	}
}

func TestPulsarOffsetResetRefusesTargetsWithoutReliableImpact(t *testing.T) {
	t.Parallel()
	backend := &Broker{}
	for _, target := range []string{"", "earliest", "datetime:2026-07-20T00:00:00Z", "offset:10", "shift:-1"} {
		t.Run(target, func(t *testing.T) {
			t.Parallel()
			_, err := backend.PlanOffsetReset(context.Background(), mqgov.OffsetPlanRequest{
				Group:     mqgov.GroupCoordinate{Group: "billing"},
				Topic:     mqgov.TopicCoordinate{Topic: "orders"},
				To:        target,
				DryRun:    true,
				Partition: -1,
			})
			if got := apperrors.AsAppError(err).Code; got != apperrors.CodeNotImplemented {
				t.Fatalf("PlanOffsetReset(%q) code = %s, want %s; err=%v", target, got, apperrors.CodeNotImplemented, err)
			}
		})
	}
}

func TestPulsarLatestResetImpactUsesPerPartitionBacklog(t *testing.T) {
	t.Parallel()
	stats := topicStats{
		Subscriptions: map[string]subscriptionStat{"billing": {MsgBacklog: 999}},
		Partitions: map[string]topicStats{
			"persistent://public/default/orders-partition-1": {
				Subscriptions: map[string]subscriptionStat{"billing": {MsgBacklog: 2}},
			},
			"persistent://public/default/orders-partition-0": {
				Subscriptions: map[string]subscriptionStat{"billing": {MsgBacklog: 5}},
			},
		},
	}

	impact, total, err := pulsarLatestResetImpact(stats, "billing")
	if err != nil {
		t.Fatalf("pulsarLatestResetImpact() error = %v", err)
	}
	if total != 7 || len(impact) != 2 {
		t.Fatalf("impact = %+v total=%d, want two partitions totaling 7", impact, total)
	}
	if impact[0].Partition != 0 || impact[0].Count != 5 || impact[1].Partition != 1 || impact[1].Count != 2 {
		t.Fatalf("impact = %+v, want partition 0=5 and partition 1=2", impact)
	}
}

func TestPulsarLatestResetImpactFailsClosedOnUnparseablePartition(t *testing.T) {
	t.Parallel()
	stats := topicStats{Partitions: map[string]topicStats{
		"orders-shard-a": {Subscriptions: map[string]subscriptionStat{"billing": {MsgBacklog: 1}}},
	}}

	_, _, err := pulsarLatestResetImpact(stats, "billing")
	if got := apperrors.AsAppError(err).Code; got != apperrors.CodeNotImplemented {
		t.Fatalf("pulsarLatestResetImpact() code = %s, want %s; err=%v", got, apperrors.CodeNotImplemented, err)
	}
}

func TestPulsarDLQMutationCapabilitiesFailClosed(t *testing.T) {
	t.Parallel()
	backend := &Broker{}
	capabilities := backend.Capabilities()
	if capabilities.SupportsDLQRedrive || capabilities.SupportsDLQPurge {
		t.Fatalf("DLQ mutation capabilities = %+v, want redrive/purge disabled", capabilities)
	}
	if _, err := backend.RedriveDLQ(context.Background(), mqgov.DLQRedriveRequest{}); apperrors.AsAppError(err).Code != apperrors.CodeNotImplemented {
		t.Fatalf("RedriveDLQ() error = %v, want NotImplemented", err)
	}
	if _, err := backend.PurgeDLQ(context.Background(), mqgov.DLQPurgeRequest{}); apperrors.AsAppError(err).Code != apperrors.CodeNotImplemented {
		t.Fatalf("PurgeDLQ() error = %v, want NotImplemented", err)
	}
}

func TestPulsarSchemaDescribeAndCheck(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodGet && r.URL.Path == "/admin/v2/schemas/public/default/orders/schema":
			_ = json.NewEncoder(w).Encode(pulsarSchemaInfo{Version: float64(0), Type: "AVRO", Data: `{"type":"record","name":"Order"}`})
		case r.Method == http.MethodGet && r.URL.Path == "/admin/v2/schemas/public/default/orders/versions":
			_ = json.NewEncoder(w).Encode([]int{0})
		case r.Method == http.MethodPost && r.URL.Path == "/admin/v2/schemas/public/default/orders/compatibility":
			var payload pulsarSchemaPayload
			if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
				t.Fatalf("Decode(schema check) error = %v", err)
			}
			if payload.Type != "AVRO" || payload.Schema == "" {
				t.Fatalf("schema check payload = %+v, want AVRO schema", payload)
			}
			_ = json.NewEncoder(w).Encode(map[string]bool{"compatible": true})
		default:
			t.Fatalf("unexpected request %s %s", r.Method, r.URL.Path)
		}
	}))
	t.Cleanup(server.Close)

	backend := &Broker{opts: Options{AdminURL: server.URL, Tenant: "public", Namespace: "default"}, httpClient: server.Client()}
	desc, err := backend.DescribeSchema(context.Background(), mqgov.SchemaDescribeRequest{Subject: "orders", Version: "latest"})
	if err != nil {
		t.Fatalf("DescribeSchema() error = %v", err)
	}
	if desc.Subject != "orders" || desc.Version != "0" || desc.SchemaHash == "" || len(desc.Versions) != 1 {
		t.Fatalf("DescribeSchema() = %+v, want schema metadata", desc)
	}
	check, err := backend.CheckCompatibility(context.Background(), mqgov.SchemaCheckRequest{Subject: "orders", Type: "AVRO", Schema: `{"type":"record","name":"Order"}`})
	if err != nil {
		t.Fatalf("CheckCompatibility() error = %v", err)
	}
	if !check.Compatible || check.SchemaHash == "" {
		t.Fatalf("CheckCompatibility() = %+v, want compatible with hash", check)
	}
}

func TestPulsarACLGrantListRevoke(t *testing.T) {
	permissions := map[string][]string{}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/admin/v2/namespaces/public/default/permissions" && r.URL.Path != "/admin/v2/namespaces/public/default/permissions/role-a" {
			http.NotFound(w, r)
			return
		}
		switch {
		case r.URL.Path == "/admin/v2/namespaces/public/default/permissions" && r.Method == http.MethodGet:
			_ = json.NewEncoder(w).Encode(permissions)
		case r.URL.Path == "/admin/v2/namespaces/public/default/permissions/role-a" && r.Method == http.MethodPost:
			var actions []string
			if err := json.NewDecoder(r.Body).Decode(&actions); err != nil {
				t.Fatalf("Decode(actions) error = %v", err)
			}
			permissions["role-a"] = actions
			w.WriteHeader(http.StatusNoContent)
		case r.URL.Path == "/admin/v2/namespaces/public/default/permissions/role-a" && r.Method == http.MethodDelete:
			delete(permissions, "role-a")
			w.WriteHeader(http.StatusNoContent)
		default:
			http.Error(w, "bad method", http.StatusMethodNotAllowed)
		}
	}))
	t.Cleanup(server.Close)

	backend := &Broker{
		opts:       Options{AdminURL: server.URL, Tenant: "public", Namespace: "default"},
		httpClient: server.Client(),
	}
	ctx := context.Background()
	produce := mqgov.ACLBinding{Principal: "role-a", ResourceType: "namespace", ResourceName: "public/default", PatternType: "literal", Operation: "produce", Permission: "allow"}
	consume := mqgov.ACLBinding{Principal: "role-a", ResourceType: "namespace", ResourceName: "public/default", PatternType: "literal", Operation: "consume", Permission: "allow"}

	if err := backend.GrantACL(ctx, produce); err != nil {
		t.Fatalf("GrantACL(produce) error = %v", err)
	}
	if err := backend.GrantACL(ctx, consume); err != nil {
		t.Fatalf("GrantACL(consume) error = %v", err)
	}
	if got := permissions["role-a"]; len(got) != 2 || got[0] != "consume" || got[1] != "produce" {
		t.Fatalf("actions after grants = %+v, want consume+produce", got)
	}
	items, err := backend.ListACLs(ctx, mqgov.ACLFilter{Principal: "role-a", ResourceType: "namespace", ResourceName: "public/default", Operation: "produce"})
	if err != nil {
		t.Fatalf("ListACLs() error = %v", err)
	}
	if len(items) != 1 || items[0].Operation != "produce" {
		t.Fatalf("ListACLs() = %+v, want one produce binding", items)
	}
	if err := backend.RevokeACL(ctx, produce); err != nil {
		t.Fatalf("RevokeACL(produce) error = %v", err)
	}
	if got := permissions["role-a"]; len(got) != 1 || got[0] != "consume" {
		t.Fatalf("actions after revoke produce = %+v, want consume preserved", got)
	}
	if err := backend.RevokeACL(ctx, consume); err != nil {
		t.Fatalf("RevokeACL(consume) error = %v", err)
	}
	if _, ok := permissions["role-a"]; ok {
		t.Fatalf("role permissions still exist after all actions revoked: %+v", permissions)
	}
}

func TestPulsarACLValidation(t *testing.T) {
	backend := &Broker{}
	tests := []struct {
		name    string
		binding mqgov.ACLBinding
	}{
		{name: "deny rejected", binding: mqgov.ACLBinding{Principal: "role-a", ResourceType: "namespace", ResourceName: "public/default", PatternType: "literal", Operation: "produce", Permission: "deny"}},
		{name: "regex rejected", binding: mqgov.ACLBinding{Principal: "role-a", ResourceType: "namespace", ResourceName: "public/default", PatternType: "regex", Operation: "produce", Permission: "allow"}},
		{name: "unknown action rejected", binding: mqgov.ACLBinding{Principal: "role-a", ResourceType: "namespace", ResourceName: "public/default", PatternType: "literal", Operation: "admin", Permission: "allow"}},
		{name: "unknown resource rejected", binding: mqgov.ACLBinding{Principal: "role-a", ResourceType: "cluster", ResourceName: "public/default", PatternType: "literal", Operation: "produce", Permission: "allow"}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := backend.GrantACL(context.Background(), tt.binding)
			if apperrors.AsAppError(err).Code != apperrors.CodeUsageError {
				t.Fatalf("GrantACL() error = %v, want UsageError", err)
			}
		})
	}
}

func TestAlterNonPartitionedTopicReturnsClearError(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/admin/v2/persistent/public/default/plain/partitions":
			http.NotFound(w, r)
		case "/admin/v2/persistent/public/default/plain/stats":
			_, _ = w.Write([]byte(`{"subscriptions":{}}`))
		default:
			http.NotFound(w, r)
		}
	}))
	t.Cleanup(server.Close)

	backend := &Broker{
		opts:       Options{AdminURL: server.URL, Tenant: "public", Namespace: "default"},
		httpClient: server.Client(),
	}

	_, err := backend.AlterTopic(context.Background(), mqgov.TopicAlterRequest{
		Coordinate: mqgov.TopicCoordinate{Topic: "plain"},
		Partitions: 3,
	})
	appErr := apperrors.AsAppError(err)
	if appErr.Code != apperrors.CodeBackendError {
		t.Fatalf("AlterTopic() code = %s, want %s", appErr.Code, apperrors.CodeBackendError)
	}
	if appErr.Message != "cannot update partitions on a non-partitioned Pulsar topic" {
		t.Fatalf("AlterTopic() message = %q", appErr.Message)
	}
}
