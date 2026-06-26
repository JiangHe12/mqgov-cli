package kafka

import (
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/JiangHe12/opskit-core/apperrors"

	"github.com/JiangHe12/mqgov-cli/internal/mqgov"
)

func TestCapabilitiesAdvertiseACL(t *testing.T) {
	backend := &Broker{opts: Options{Cluster: "test"}}
	caps := backend.Capabilities()
	if !caps.SupportsACL {
		t.Fatalf("SupportsACL = false, want true")
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
