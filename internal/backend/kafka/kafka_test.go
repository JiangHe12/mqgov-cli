package kafka

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
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
