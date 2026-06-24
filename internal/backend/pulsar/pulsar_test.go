package pulsar

import (
	"context"
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

func TestSupportsACLFalse(t *testing.T) {
	backend := &Broker{opts: Options{Cluster: "test"}}
	if backend.Capabilities().SupportsACL {
		t.Fatalf("SupportsACL = true, want false")
	}
	if _, ok := mqgov.SupportsACL(backend); ok {
		t.Fatalf("SupportsACL capability assertion = true, want false")
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
