package kafka

import (
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"

	"github.com/JiangHe12/opskit-core/v2/apperrors"
	"github.com/twmb/franz-go/pkg/kadm"

	"github.com/JiangHe12/mqgov-cli/internal/mqgov"
)

type closeIdleTransport struct {
	http.RoundTripper
	calls int
}

func TestPeekRejectsOversizedBatchBeforeClientCreation(t *testing.T) {
	t.Parallel()
	backend := &Broker{}

	_, err := backend.Peek(t.Context(), mqgov.MessagePeekRequest{Count: mqgov.MaxMessageBatchSize + 1})
	if code := apperrors.AsAppError(err).Code; code != apperrors.CodeUsageError {
		t.Fatalf("Peek() error = %v, code = %s, want USAGE_ERROR", err, code)
	}
	_, err = backend.Tail(t.Context(), mqgov.MessageTailRequest{MaxMessages: mqgov.MaxMessageBatchSize + 1}, nil)
	if code := apperrors.AsAppError(err).Code; code != apperrors.CodeUsageError {
		t.Fatalf("Tail() error = %v, code = %s, want USAGE_ERROR", err, code)
	}
	_, err = backend.MirrorMessages(t.Context(), mqgov.MessageMirrorRequest{Limit: mqgov.MaxMirrorBatchSize + 1}, nil)
	if code := apperrors.AsAppError(err).Code; code != apperrors.CodeUsageError {
		t.Fatalf("MirrorMessages() error = %v, code = %s, want USAGE_ERROR", err, code)
	}
}

func (transport *closeIdleTransport) CloseIdleConnections() {
	transport.calls++
}

func TestCloseIsIdempotent(t *testing.T) {
	t.Parallel()
	transport := &closeIdleTransport{}
	backend := &Broker{srHTTP: &http.Client{Transport: transport}}

	backend.Close()
	backend.Close()
	if transport.calls != 1 {
		t.Fatalf("CloseIdleConnections() calls = %d, want 1", transport.calls)
	}
}

func TestCapabilitiesAdvertiseACL(t *testing.T) {
	backend := &Broker{opts: Options{Cluster: "test"}}
	caps := backend.Capabilities()
	if !caps.SupportsACL {
		t.Fatalf("SupportsACL = false, want true")
	}
}

func TestKafkaDLQRedriveFailsClosed(t *testing.T) {
	t.Parallel()
	backend := &Broker{opts: Options{Cluster: "test"}}
	if backend.Capabilities().SupportsDLQRedrive {
		t.Fatal("SupportsDLQRedrive = true, want false until exact move semantics are available")
	}
	if _, err := backend.RedriveDLQ(t.Context(), mqgov.DLQRedriveRequest{}); apperrors.AsAppError(err).Code != apperrors.CodeNotImplemented {
		t.Fatalf("RedriveDLQ() error = %v, want NotImplemented", err)
	}
}

func TestAppliedDeleteRecordsImpactCountsOnlySuccessfulPartitions(t *testing.T) {
	t.Parallel()
	start := kadm.ListedOffsets{
		"orders-dlq": {
			0: {Topic: "orders-dlq", Partition: 0, Offset: 4},
			1: {Topic: "orders-dlq", Partition: 1, Offset: 7},
		},
	}
	responses := kadm.DeleteRecordsResponses{
		"orders-dlq": {
			0: {Topic: "orders-dlq", Partition: 0, LowWatermark: 10},
			1: {Topic: "orders-dlq", Partition: 1, Err: errors.New("partition unavailable")},
		},
	}

	impact, total := appliedDeleteRecordsImpact(start, responses)
	if total != 6 {
		t.Fatalf("total = %d, want 6", total)
	}
	if len(impact) != 1 {
		t.Fatalf("len(impact) = %d, want 1; impact=%+v", len(impact), impact)
	}
	if got := impact[0]; got.Partition != 0 || got.From != 4 || got.To != 10 || got.Count != 6 {
		t.Fatalf("impact[0] = %+v, want partition=0 from=4 to=10 count=6", got)
	}
}

func TestKafkaOffsetCommitOutcomePreservesPerPartitionCounts(t *testing.T) {
	t.Parallel()
	plan := mqgov.OffsetPlan{
		Topic: mqgov.TopicCoordinate{Topic: "orders"},
		Impact: []mqgov.PartitionImpact{
			{Partition: 0},
			{Partition: 1},
			{Partition: 2},
		},
	}
	responses := kadm.OffsetResponses{
		"orders": {
			0: {Offset: kadm.Offset{Topic: "orders", Partition: 0}},
			1: {Offset: kadm.Offset{Topic: "orders", Partition: 1}, Err: errors.New("partition unavailable")},
		},
	}

	outcome, err := kafkaOffsetCommitOutcome(plan, responses)
	if outcome != (mqgov.BatchOutcome{Succeeded: 1, Failed: 1, Uncertain: 1}) {
		t.Fatalf("outcome = %+v", outcome)
	}
	if err == nil {
		t.Fatal("kafkaOffsetCommitOutcome() error = nil, want first failure")
	}
	partialErr := kafkaOffsetCommitPartialFailure(outcome, err)
	if got := apperrors.AsAppError(partialErr).Code; got != apperrors.CodePartialFailure {
		t.Fatalf("partial failure code = %s, want %s", got, apperrors.CodePartialFailure)
	}
}

func TestSupportsSchemaRequiresSchemaRegistryURL(t *testing.T) {
	backend := &Broker{opts: Options{Cluster: "test"}}
	if backend.Capabilities().SupportsSchema {
		t.Fatalf("SupportsSchema = true without Schema Registry URL")
	}
	if _, ok := mqgov.SupportsSchema(backend); !ok {
		t.Fatalf("SupportsSchema type assertion = false, want true for Kafka backend")
	}
	backend.opts.SchemaRegistryURL = "https://schema-registry.example"
	if !backend.Capabilities().SupportsSchema {
		t.Fatalf("SupportsSchema = false with Schema Registry URL")
	}
}

func TestSchemaRegistryCredentialsRequireHTTPS(t *testing.T) {
	_, err := New(Options{
		Brokers:                []string{"127.0.0.1:9092"},
		SchemaRegistryURL:      "http://schema-registry.example",
		SchemaRegistryUsername: "sr-user",
		SchemaRegistryPassword: "sr-pass",
	})
	if got := apperrors.AsAppError(err).Code; got != apperrors.CodeUsageError {
		t.Fatalf("New(http with credentials) code = %s, want %s; err=%v", got, apperrors.CodeUsageError, err)
	}

	anonymous, err := New(Options{Brokers: []string{"127.0.0.1:9092"}, SchemaRegistryURL: "http://schema-registry.example"})
	if err != nil {
		t.Fatalf("New(anonymous http) error = %v", err)
	}
	anonymous.client.Close()

	withTLS, err := New(Options{
		Brokers:                []string{"127.0.0.1:9092"},
		SchemaRegistryURL:      "https://schema-registry.example",
		SchemaRegistryUsername: "sr-user",
		SchemaRegistryPassword: "sr-pass",
	})
	if err != nil {
		t.Fatalf("New(https with credentials) error = %v", err)
	}
	withTLS.client.Close()
}

func TestKafkaTLSConfigPinsDialHostWhenServerNameEmpty(t *testing.T) {
	server := httptest.NewTLSServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {}))
	defer server.Close()

	path := filepath.Join(t.TempDir(), "tls_known_hosts")
	base := &tls.Config{MinVersion: tls.VersionTLS12}
	cfg, err := kafkaTLSConfigForHost(base, path, "broker.example:9093")
	if err != nil {
		t.Fatalf("kafkaTLSConfigForHost() error = %v", err)
	}
	if base.ServerName != "" {
		t.Fatalf("base ServerName = %q, want unchanged", base.ServerName)
	}
	if cfg.ServerName != "broker.example" {
		t.Fatalf("ServerName = %q, want broker.example", cfg.ServerName)
	}
	if cfg.InsecureSkipVerify {
		t.Fatal("InsecureSkipVerify = true, want false")
	}
	if err := cfg.VerifyConnection(tls.ConnectionState{PeerCertificates: []*x509.Certificate{server.Certificate()}}); err != nil {
		t.Fatalf("VerifyConnection(empty ServerName) error = %v", err)
	}
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("ReadFile(pin) error = %v", err)
	}
	if !strings.HasPrefix(string(data), "broker.example:9093\t") {
		t.Fatalf("pin file = %q, want broker.example:9093 key", data)
	}
}

func TestSchemaRegistryRequests(t *testing.T) {
	var sawCheck bool
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if got, want := r.Header.Get("Authorization"), "Basic c3ItdXNlcjpzci1wYXNz"; got != want {
			t.Fatalf("Authorization = %q, want %q", got, want)
		}
		switch {
		case r.Method == http.MethodGet && r.URL.EscapedPath() == "/subjects":
			_ = json.NewEncoder(w).Encode([]string{"orders-value"})
		case r.Method == http.MethodGet && r.URL.EscapedPath() == "/subjects/orders-value/versions":
			_ = json.NewEncoder(w).Encode([]int{1})
		case r.Method == http.MethodGet && r.URL.EscapedPath() == "/subjects/orders-value/versions/latest":
			_ = json.NewEncoder(w).Encode(schemaRegistryVersion{Subject: "orders-value", ID: 7, Version: 1, SchemaType: "AVRO", Schema: `{"type":"record","name":"Order"}`})
		case r.Method == http.MethodPost && r.URL.EscapedPath() == "/compatibility/subjects/orders-value/versions/latest":
			var payload map[string]string
			if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
				t.Fatalf("decode check payload: %v", err)
			}
			if payload["schema"] == "" || payload["schemaType"] != "AVRO" {
				t.Fatalf("check payload = %+v, want schema and schemaType", payload)
			}
			sawCheck = true
			_ = json.NewEncoder(w).Encode(schemaRegistryCompatibility{Compatible: true})
		default:
			t.Fatalf("unexpected request %s %s", r.Method, r.URL.EscapedPath())
		}
	}))
	defer server.Close()

	backend := &Broker{
		opts:   Options{SchemaRegistryURL: server.URL, SchemaRegistryUsername: "sr-user", SchemaRegistryPassword: "sr-pass"},
		srHTTP: server.Client(),
	}

	subjects, err := backend.ListSchemas(t.Context(), mqgov.SchemaListOptions{})
	if err != nil {
		t.Fatalf("ListSchemas() error = %v", err)
	}
	if len(subjects) != 1 || subjects[0].Subject != "orders-value" {
		t.Fatalf("ListSchemas() = %+v, want orders-value", subjects)
	}
	desc, err := backend.DescribeSchema(t.Context(), mqgov.SchemaDescribeRequest{Subject: "orders-value", Version: "latest"})
	if err != nil {
		t.Fatalf("DescribeSchema() error = %v", err)
	}
	if desc.ID != 7 || desc.Version != "1" || desc.SchemaHash == "" || len(desc.Versions) != 1 {
		t.Fatalf("DescribeSchema() = %+v, want version metadata and hash", desc)
	}
	check, err := backend.CheckCompatibility(t.Context(), mqgov.SchemaCheckRequest{Subject: "orders-value", Version: "latest", Type: "AVRO", Schema: `{"type":"record","name":"Order"}`})
	if err != nil {
		t.Fatalf("CheckCompatibility() error = %v", err)
	}
	if !check.Compatible || check.SchemaHash == "" || !sawCheck {
		t.Fatalf("CheckCompatibility() = %+v, sawCheck=%v", check, sawCheck)
	}
}

func TestSchemaRegistryErrorsExposeOnlyResponseFingerprint(t *testing.T) {
	t.Parallel()
	const responseBody = `{"error":"registry-secret"}`
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
		_, _ = w.Write([]byte(responseBody))
	}))
	t.Cleanup(server.Close)
	backend := &Broker{
		opts:   Options{SchemaRegistryURL: server.URL},
		srHTTP: server.Client(),
	}

	_, err := backend.schemaRegistryJSON(t.Context(), http.MethodGet, "/subjects", nil)
	if got := apperrors.AsAppError(err).Code; got != apperrors.CodeBackendError {
		t.Fatalf("schemaRegistryJSON() code = %s, want %s; err=%v", got, apperrors.CodeBackendError, err)
	}
	detail := err.Error()
	if strings.Contains(detail, "registry-secret") {
		t.Fatalf("schemaRegistryJSON() leaked response body: %v", err)
	}
	for _, want := range []string{
		"status 500",
		"response-bytes=" + strconv.Itoa(len(responseBody)),
		"response-sha256=" + mqgov.SHA256Hex([]byte(responseBody)),
	} {
		if !strings.Contains(detail, want) {
			t.Fatalf("schemaRegistryJSON() error = %q, want %q", detail, want)
		}
	}
}
