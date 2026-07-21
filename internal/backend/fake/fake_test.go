package fake

import (
	"testing"

	"github.com/JiangHe12/opskit-core/v2/apperrors"

	"github.com/JiangHe12/mqgov-cli/internal/mqgov"
)

func TestDLQRedrivePlanAndExecutionUseMoveSemantics(t *testing.T) {
	t.Parallel()
	backend := New("test", "")
	request := mqgov.DLQRedriveRequest{
		DLQ:    mqgov.TopicCoordinate{Cluster: "test", Topic: "orders.dlq"},
		Target: mqgov.TopicCoordinate{Cluster: "test", Topic: "orders"},
		Count:  1,
		DryRun: true,
	}

	preview, err := backend.RedriveDLQ(t.Context(), request)
	if err != nil {
		t.Fatalf("RedriveDLQ(dry-run) error = %v", err)
	}
	if preview.Total != 1 || preview.Fingerprint.Count != 1 {
		t.Fatalf("RedriveDLQ(dry-run) = %+v, want count 1", preview)
	}
	before, err := backend.PeekDLQ(t.Context(), mqgov.DLQPeekRequest{DLQ: request.DLQ, Count: 1})
	if err != nil || before.Count != 1 {
		t.Fatalf("PeekDLQ(after preview) = %+v, err=%v; preview must not remove messages", before, err)
	}

	request.DryRun = false
	applied, err := backend.RedriveDLQ(t.Context(), request)
	if err != nil {
		t.Fatalf("RedriveDLQ() error = %v", err)
	}
	if applied.Total != preview.Total || applied.Fingerprint.Count != preview.Fingerprint.Count {
		t.Fatalf("applied = %+v, preview = %+v", applied, preview)
	}
	after, err := backend.PeekDLQ(t.Context(), mqgov.DLQPeekRequest{DLQ: request.DLQ, Count: 1})
	if err != nil {
		t.Fatalf("PeekDLQ(after redrive) error = %v", err)
	}
	if after.Count != 0 {
		t.Fatalf("PeekDLQ(after redrive).Count = %d, want 0", after.Count)
	}
	target, err := backend.Peek(t.Context(), mqgov.MessagePeekRequest{Coordinate: request.Target, Count: 1})
	if err != nil || target.Count != 1 {
		t.Fatalf("Peek(target) = %+v, err=%v; redrive must copy before removing", target, err)
	}
}

func TestDLQRedriveRejectsSuccessfulNoOp(t *testing.T) {
	t.Parallel()
	backend := New("test", "")
	dlq := mqgov.TopicCoordinate{Cluster: "test", Topic: "orders.dlq"}
	_, err := backend.RedriveDLQ(t.Context(), mqgov.DLQRedriveRequest{DLQ: dlq, Target: dlq, Count: 1})
	if got := apperrors.AsAppError(err).Code; got != apperrors.CodeUsageError {
		t.Fatalf("RedriveDLQ(same target) code = %s, want %s; err=%v", got, apperrors.CodeUsageError, err)
	}
}

func TestDLQPurgePlanAndExecutionCountsMatch(t *testing.T) {
	t.Parallel()
	backend := New("test", "")
	request := mqgov.DLQPurgeRequest{
		DLQ:    mqgov.TopicCoordinate{Cluster: "test", Topic: "orders.dlq"},
		DryRun: true,
	}
	preview, err := backend.PurgeDLQ(t.Context(), request)
	if err != nil {
		t.Fatalf("PurgeDLQ(dry-run) error = %v", err)
	}
	request.DryRun = false
	applied, err := backend.PurgeDLQ(t.Context(), request)
	if err != nil {
		t.Fatalf("PurgeDLQ() error = %v", err)
	}
	if preview.Total != 1 || applied.Total != preview.Total || applied.Fingerprint.Count != preview.Fingerprint.Count {
		t.Fatalf("preview = %+v, applied = %+v", preview, applied)
	}
}

func TestPeekHonorsCountOffsetAndOrder(t *testing.T) {
	t.Parallel()
	backend := New("test", "")
	coordinate := mqgov.TopicCoordinate{Cluster: "test", Topic: "orders"}
	for _, body := range []string{"zero", "one", "two", "three"} {
		if _, err := backend.Produce(t.Context(), mqgov.MessageProduceRequest{Coordinate: coordinate, Body: []byte(body)}); err != nil {
			t.Fatalf("Produce(%q) error = %v", body, err)
		}
	}

	result, err := backend.Peek(t.Context(), mqgov.MessagePeekRequest{
		Coordinate: coordinate,
		Partition:  0,
		Offset:     1,
		Count:      2,
	})
	if err != nil {
		t.Fatalf("Peek() error = %v", err)
	}
	if result.Count != 2 || len(result.Messages) != 2 {
		t.Fatalf("Peek() count = %d messages=%d, want 2", result.Count, len(result.Messages))
	}
	for index, body := range []string{"one", "two"} {
		message := result.Messages[index]
		if message.Offset != int64(index+1) || message.BodySHA256 != mqgov.SHA256Hex([]byte(body)) {
			t.Fatalf("messages[%d] = %+v, want offset=%d body=%q", index, message, index+1, body)
		}
	}
}
