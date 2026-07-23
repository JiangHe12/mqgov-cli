package rabbitmq

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	"github.com/JiangHe12/opskit-core/v2/apperrors"

	"github.com/JiangHe12/mqgov-cli/internal/mqgov"
)

type closeIdleTransport struct {
	http.RoundTripper
	calls int
}

func TestRedriveRejectsOversizedBatchBeforeBrokerAccess(t *testing.T) {
	t.Parallel()
	backend := &Broker{}

	_, err := backend.RedriveDLQ(t.Context(), mqgov.DLQRedriveRequest{Count: mqgov.MaxMessageBatchSize + 1})

	if code := apperrors.AsAppError(err).Code; code != apperrors.CodeUsageError {
		t.Fatalf("RedriveDLQ() error = %v, code = %s, want USAGE_ERROR", err, code)
	}
}

func TestRabbitMQPublishingPreservesMessageKey(t *testing.T) {
	t.Parallel()
	key := []byte("order-42")
	requestHeaders := map[string][]byte{"trace-id": []byte("abc")}
	publishing := rabbitMQPublishing(mqgov.MessageProduceRequest{
		Key:     key,
		Body:    []byte("body"),
		Headers: requestHeaders,
	})

	storedKey, ok := publishing.Headers[rabbitMQMessageKeyHeader].([]byte)
	if !ok || !slices.Equal(storedKey, key) {
		t.Fatalf("RabbitMQ key header = %#v, want %q", publishing.Headers[rabbitMQMessageKeyHeader], key)
	}
	if storedTrace, ok := publishing.Headers["trace-id"].([]byte); !ok || !slices.Equal(storedTrace, requestHeaders["trace-id"]) {
		t.Fatalf("RabbitMQ trace header = %#v, want %q", publishing.Headers["trace-id"], requestHeaders["trace-id"])
	}
	key[0] = 'X'
	if string(storedKey) != "order-42" {
		t.Fatalf("RabbitMQ key header changed with caller buffer: %q", storedKey)
	}
	if _, exists := requestHeaders[rabbitMQMessageKeyHeader]; exists {
		t.Fatal("rabbitMQPublishing() mutated caller headers")
	}
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

func TestRabbitMQPeekIsNotImplementedWithoutConnecting(t *testing.T) {
	t.Parallel()
	backend := &Broker{}

	if _, err := backend.Peek(t.Context(), mqgov.MessagePeekRequest{Coordinate: mqgov.TopicCoordinate{Topic: "orders"}, Count: 1}); apperrors.AsAppError(err).Code != apperrors.CodeNotImplemented {
		t.Fatalf("Peek() error = %v, want %s", err, apperrors.CodeNotImplemented)
	}
}

func TestRabbitMQDLQPeekIsNotImplementedWithoutConnecting(t *testing.T) {
	t.Parallel()
	backend := &Broker{}

	if _, err := backend.PeekDLQ(t.Context(), mqgov.DLQPeekRequest{DLQ: mqgov.TopicCoordinate{Topic: "orders.dlq"}, Count: 1}); apperrors.AsAppError(err).Code != apperrors.CodeNotImplemented {
		t.Fatalf("PeekDLQ() error = %v, want %s", err, apperrors.CodeNotImplemented)
	}
	capabilities := backend.Capabilities()
	if capabilities.SupportsDLQPeek || slices.Contains(capabilities.Verbs, "peek") {
		t.Fatalf("capabilities advertise unsupported peek: %+v", capabilities)
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

func TestSupportsSchemaFalse(t *testing.T) {
	backend := &Broker{opts: Options{Cluster: "test"}}
	if backend.Capabilities().SupportsSchema {
		t.Fatalf("SupportsSchema = true, want false")
	}
	if _, ok := mqgov.SupportsSchema(backend); ok {
		t.Fatalf("SupportsSchema capability assertion = true, want false")
	}
}

func TestRabbitMQCredentialsBuildAMQPAndManagementAuth(t *testing.T) {
	backend, err := New(Options{
		Host:     "rabbit.example",
		Port:     5678,
		VHost:    "team",
		Username: "svc",
		Password: "secret",
	})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	if backend.amqpURL != "amqp://svc:secret@rabbit.example:5678/team" {
		t.Fatalf("amqpURL = %q, want explicit username/password", backend.amqpURL)
	}
	if backend.opts.Username != "svc" || backend.opts.Password != "secret" {
		t.Fatalf("management credentials = %q/%q, want svc/secret", backend.opts.Username, backend.opts.Password)
	}
}

func TestRabbitMQExplicitCredentialsOverrideAMQPURLUserInfo(t *testing.T) {
	backend, err := New(Options{
		AMQPURL:  "amqp://url-user:url-pass@rabbit.example:5672/%2F",
		Username: "flag-user",
		Password: "env-pass",
	})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	if !strings.Contains(backend.amqpURL, "flag-user:env-pass@rabbit.example:5672") {
		t.Fatalf("amqpURL = %q, want explicit credentials to override URL userinfo", backend.amqpURL)
	}
	if backend.opts.Username != "flag-user" || backend.opts.Password != "env-pass" {
		t.Fatalf("management credentials = %q/%q, want flag-user/env-pass", backend.opts.Username, backend.opts.Password)
	}
}

func TestRabbitMQAMQPURLUserInfoFeedsManagementCredentials(t *testing.T) {
	backend, err := New(Options{AMQPURL: "amqp://url-user:url-pass@rabbit.example:5672/%2F"})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	if backend.opts.Username != "url-user" || backend.opts.Password != "url-pass" {
		t.Fatalf("management credentials = %q/%q, want AMQP URL userinfo", backend.opts.Username, backend.opts.Password)
	}
	if !strings.Contains(backend.amqpURL, "url-user:url-pass@rabbit.example:5672") {
		t.Fatalf("amqpURL = %q, want URL userinfo preserved", backend.amqpURL)
	}
}

func TestRabbitMQDefaultsGuestCredentials(t *testing.T) {
	backend, err := New(Options{Host: "rabbit.example"})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	if backend.opts.Username != "guest" || backend.opts.Password != "guest" {
		t.Fatalf("default credentials = %q/%q, want guest/guest", backend.opts.Username, backend.opts.Password)
	}
	if !strings.Contains(backend.amqpURL, "guest:guest@rabbit.example:5672") {
		t.Fatalf("amqpURL = %q, want guest credentials", backend.amqpURL)
	}
}

func TestRabbitMQDLQRedriveRejectsSuccessfulNoOp(t *testing.T) {
	t.Parallel()
	backend := &Broker{}
	dlq := mqgov.TopicCoordinate{Topic: "orders.dlq"}
	_, err := backend.RedriveDLQ(t.Context(), mqgov.DLQRedriveRequest{DLQ: dlq, Target: dlq, Count: 1})
	if got := apperrors.AsAppError(err).Code; got != apperrors.CodeUsageError {
		t.Fatalf("RedriveDLQ(same target) code = %s, want %s; err=%v", got, apperrors.CodeUsageError, err)
	}
}

func TestRabbitMQRedriveFailureReportsPartialAndUncertainOutcomes(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name    string
		outcome mqgov.BatchOutcome
		cause   error
		want    apperrors.ErrorCode
	}{
		{
			name:    "partial success",
			outcome: mqgov.BatchOutcome{Succeeded: 2, Failed: 1},
			cause:   apperrors.New(apperrors.CodeResourceNotFound, "target missing", nil),
			want:    apperrors.CodePartialFailure,
		},
		{
			name:    "uncertain publish",
			outcome: mqgov.BatchOutcome{Uncertain: 1},
			cause:   apperrors.New(apperrors.CodeBackendUnreachable, "confirmation timed out", nil),
			want:    apperrors.CodePartialFailure,
		},
		{
			name:    "deterministic failure before success",
			outcome: mqgov.BatchOutcome{Failed: 1},
			cause:   apperrors.New(apperrors.CodeResourceNotFound, "target missing", nil),
			want:    apperrors.CodeResourceNotFound,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			if got := apperrors.AsAppError(rabbitMQRedriveFailure(tt.outcome, tt.cause)).Code; got != tt.want {
				t.Fatalf("rabbitMQRedriveFailure() code = %s, want %s", got, tt.want)
			}
		})
	}
}

func TestRabbitMQTLSRejectsExplicitPlaintextEndpoints(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		opts Options
	}{
		{
			name: "AMQP URL",
			opts: Options{
				TLS:           true,
				AMQPURL:       "amqp://rabbit.example:5672/%2F",
				ManagementURL: "https://rabbit.example:15671",
			},
		},
		{
			name: "management URL",
			opts: Options{
				TLS:           true,
				AMQPURL:       "amqps://rabbit.example:5671/%2F",
				ManagementURL: "http://rabbit.example:15672",
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

func TestRabbitMQTLSDefaultsUseSecureEndpoints(t *testing.T) {
	t.Parallel()

	backend, err := New(Options{
		Host:       "rabbit.example",
		TLS:        true,
		TLSPinPath: filepath.Join(t.TempDir(), "tls_known_hosts"),
	})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	if !strings.HasPrefix(backend.amqpURL, "amqps://") {
		t.Fatalf("amqpURL = %q, want amqps", backend.amqpURL)
	}
	if !strings.HasPrefix(backend.manageURL, "https://") {
		t.Fatalf("manageURL = %q, want https", backend.manageURL)
	}
	if backend.tlsConfig == nil {
		t.Fatal("tlsConfig = nil, want TLS config")
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
