package rabbitmq

import (
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/JiangHe12/opskit-core/v2/apperrors"
	amqp "github.com/rabbitmq/amqp091-go"

	"github.com/JiangHe12/mqgov-cli/internal/mqgov"
)

type peekAcknowledger struct {
	nackTag      uint64
	nackMultiple bool
	nackRequeue  bool
	nackCalls    int
	nackErr      error
}

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

func (*peekAcknowledger) Ack(uint64, bool) error { return nil }

func (ack *peekAcknowledger) Nack(tag uint64, multiple, requeue bool) error {
	ack.nackTag = tag
	ack.nackMultiple = multiple
	ack.nackRequeue = requeue
	ack.nackCalls++
	return ack.nackErr
}

func (*peekAcknowledger) Reject(uint64, bool) error { return nil }

type peekDeliveryGetter struct {
	deliveries []amqp.Delivery
	index      int
}

func (getter *peekDeliveryGetter) Get(string, bool) (amqp.Delivery, bool, error) {
	if getter.index >= len(getter.deliveries) {
		return amqp.Delivery{}, false, nil
	}
	delivery := getter.deliveries[getter.index]
	getter.index++
	return delivery, true, nil
}

func TestRabbitMQPeekCollectsDistinctOrderedBatchBeforeRequeue(t *testing.T) {
	t.Parallel()
	ack := &peekAcknowledger{}
	keys := []string{"first", "second", "third"}
	bodies := []string{"one", "two", "three"}
	getter := &peekDeliveryGetter{deliveries: []amqp.Delivery{
		{Acknowledger: ack, DeliveryTag: 1, RoutingKey: keys[0], Body: []byte(bodies[0]), Timestamp: time.Unix(1, 0)},
		{Acknowledger: ack, DeliveryTag: 2, RoutingKey: keys[1], Body: []byte(bodies[1]), Timestamp: time.Unix(2, 0)},
		{Acknowledger: ack, DeliveryTag: 3, RoutingKey: keys[2], Body: []byte(bodies[2]), Timestamp: time.Unix(3, 0)},
	}}

	messages, err := rabbitMQPeekMessages(getter, "orders", 5)
	if err != nil {
		t.Fatalf("rabbitMQPeekMessages() error = %v", err)
	}
	if len(messages) != 3 {
		t.Fatalf("len(messages) = %d, want 3", len(messages))
	}
	for index := range bodies {
		want := mqgov.FingerprintMessageAt(0, int64(index), []byte(keys[index]), []byte(bodies[index]), time.Unix(int64(index+1), 0))
		if messages[index] != want {
			t.Fatalf("messages[%d] = %+v, want %+v", index, messages[index], want)
		}
	}
	if ack.nackCalls != 1 || ack.nackTag != 3 || !ack.nackMultiple || !ack.nackRequeue {
		t.Fatalf("batch requeue = calls:%d tag:%d multiple:%t requeue:%t", ack.nackCalls, ack.nackTag, ack.nackMultiple, ack.nackRequeue)
	}
}

func TestRabbitMQPeekFailsWhenBatchCannotBeRestored(t *testing.T) {
	t.Parallel()
	ack := &peekAcknowledger{nackErr: errors.New("injected nack failure")}
	getter := &peekDeliveryGetter{deliveries: []amqp.Delivery{{
		Acknowledger: ack,
		DeliveryTag:  1,
		Body:         []byte("one"),
	}}}

	if _, err := rabbitMQPeekMessages(getter, "orders", 1); err == nil || !strings.Contains(err.Error(), "restore peeked RabbitMQ messages") {
		t.Fatalf("rabbitMQPeekMessages() error = %v, want restore failure", err)
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
