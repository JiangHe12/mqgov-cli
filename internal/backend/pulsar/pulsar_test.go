package pulsar

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/JiangHe12/opskit-core/apperrors"

	"github.com/JiangHe12/mqgov-cli/internal/mqgov"
)

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
