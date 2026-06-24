package rabbitmq

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/JiangHe12/opskit-core/apperrors"

	"github.com/JiangHe12/mqgov-cli/internal/mqgov"
)

func TestSupportsACLTrue(t *testing.T) {
	backend := &Broker{opts: Options{Cluster: "test"}}
	if !backend.Capabilities().SupportsACL {
		t.Fatalf("SupportsACL = false, want true")
	}
	if _, ok := mqgov.SupportsACL(backend); !ok {
		t.Fatalf("SupportsACL capability assertion = false, want true")
	}
}

func TestSupportsSchemaFalse(t *testing.T) {
	backend := &Broker{opts: Options{Cluster: "test"}}
	if backend.Capabilities().SupportsSchema {
		t.Fatalf("SupportsSchema = true, want false")
	}
	if _, ok := mqgov.SupportsSchema(backend); ok {
		t.Fatalf("SupportsSchema capability assertion = true, want false")
	}
}

func TestRabbitMQACLGrantListRevoke(t *testing.T) {
	permissions := map[string]rabbitMQPermission{}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		path := r.URL.EscapedPath()
		if path == "/api/permissions" && r.Method == http.MethodGet {
			out := make([]rabbitMQPermission, 0, len(permissions))
			for _, permission := range permissions {
				out = append(out, permission)
			}
			_ = json.NewEncoder(w).Encode(out)
			return
		}
		if path != "/api/permissions/%2F/svc" {
			http.NotFound(w, r)
			return
		}
		switch r.Method {
		case http.MethodGet:
			permission, ok := permissions["svc"]
			if !ok {
				http.NotFound(w, r)
				return
			}
			_ = json.NewEncoder(w).Encode(permission)
		case http.MethodPut:
			var body struct {
				Configure string `json:"configure"`
				Write     string `json:"write"`
				Read      string `json:"read"`
			}
			if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
				t.Fatalf("Decode(put) error = %v", err)
			}
			permissions["svc"] = rabbitMQPermission{User: "svc", Vhost: "/", Configure: body.Configure, Write: body.Write, Read: body.Read}
			w.WriteHeader(http.StatusNoContent)
		case http.MethodDelete:
			delete(permissions, "svc")
			w.WriteHeader(http.StatusNoContent)
		default:
			http.Error(w, "bad method", http.StatusMethodNotAllowed)
		}
	}))
	defer server.Close()

	backend := &Broker{
		opts:       Options{Username: "guest", Password: "guest"},
		manageURL:  server.URL,
		httpClient: server.Client(),
	}
	ctx := t.Context()
	writeBinding := mqgov.ACLBinding{Principal: "svc", Vhost: "/", ResourceType: "vhost", ResourceName: "^orders$", PatternType: "regex", Operation: "write", Permission: "allow"}
	readBinding := mqgov.ACLBinding{Principal: "svc", Vhost: "/", ResourceType: "vhost", ResourceName: "^orders-read$", PatternType: "regex", Operation: "read", Permission: "allow"}

	if err := backend.GrantACL(ctx, writeBinding); err != nil {
		t.Fatalf("GrantACL(write) error = %v", err)
	}
	if err := backend.GrantACL(ctx, readBinding); err != nil {
		t.Fatalf("GrantACL(read) error = %v", err)
	}
	items, err := backend.ListACLs(ctx, mqgov.ACLFilter{Principal: "svc", Vhost: "/"})
	if err != nil {
		t.Fatalf("ListACLs() error = %v", err)
	}
	if len(items) != 2 {
		t.Fatalf("ListACLs() len = %d, want 2: %+v", len(items), items)
	}
	if got := permissions["svc"].Write; got != "^orders$" {
		t.Fatalf("write regex = %q, want ^orders$", got)
	}
	if got := permissions["svc"].Read; got != "^orders-read$" {
		t.Fatalf("read regex = %q, want ^orders-read$", got)
	}

	if err := backend.RevokeACL(ctx, writeBinding); err != nil {
		t.Fatalf("RevokeACL(write) error = %v", err)
	}
	if _, ok := permissions["svc"]; !ok {
		t.Fatal("permission deleted after revoking one scope, want read scope preserved")
	}
	if permissions["svc"].Write != "" || permissions["svc"].Read != "^orders-read$" {
		t.Fatalf("permission after write revoke = %+v", permissions["svc"])
	}
	if err := backend.RevokeACL(ctx, readBinding); err != nil {
		t.Fatalf("RevokeACL(read) error = %v", err)
	}
	if _, ok := permissions["svc"]; ok {
		t.Fatalf("permission still exists after all scopes revoked: %+v", permissions["svc"])
	}
}

func TestRabbitMQACLValidation(t *testing.T) {
	backend := &Broker{}
	tests := []struct {
		name    string
		binding mqgov.ACLBinding
	}{
		{name: "deny rejected", binding: mqgov.ACLBinding{Principal: "svc", Vhost: "/", ResourceType: "vhost", ResourceName: "^orders$", PatternType: "regex", Operation: "read", Permission: "deny"}},
		{name: "literal rejected", binding: mqgov.ACLBinding{Principal: "svc", Vhost: "/", ResourceType: "vhost", ResourceName: "orders", PatternType: "literal", Operation: "read", Permission: "allow"}},
		{name: "unknown operation rejected", binding: mqgov.ACLBinding{Principal: "svc", Vhost: "/", ResourceType: "vhost", ResourceName: "^orders$", PatternType: "regex", Operation: "delete", Permission: "allow"}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := backend.GrantACL(t.Context(), tt.binding)
			if apperrors.AsAppError(err).Code != apperrors.CodeUsageError {
				t.Fatalf("GrantACL() error = %v, want UsageError", err)
			}
		})
	}
}

func TestRabbitMQManagementAuthError(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		http.Error(w, "forbidden", http.StatusForbidden)
	}))
	defer server.Close()
	backend := &Broker{manageURL: server.URL, httpClient: server.Client()}
	_, err := backend.ListACLs(t.Context(), mqgov.ACLFilter{Vhost: "/"})
	if apperrors.AsAppError(err).Code != apperrors.CodeAuthFailed {
		t.Fatalf("ListACLs() error = %v, want auth failed", err)
	}
}

func TestRabbitMQACLListFilters(t *testing.T) {
	permission := rabbitMQPermission{User: "svc", Vhost: "/", Configure: "^cfg$", Write: "^write$", Read: "^read$"}
	items := rabbitMQACLBindings(permission, mqgov.ACLFilter{Operation: "write", PatternType: "regex"})
	if len(items) != 1 || items[0].Operation != "write" || items[0].ResourceName != "^write$" {
		t.Fatalf("filtered bindings = %+v, want write only", items)
	}
	if got := rabbitMQACLBindings(permission, mqgov.ACLFilter{ResourceType: "topic"}); len(got) != 0 {
		t.Fatalf("topic filter returned %+v, want none", got)
	}
	if !strings.Contains(aclSortKey(items[0]), "svc") {
		t.Fatalf("aclSortKey() = %q, want stable key with principal", aclSortKey(items[0]))
	}
}
