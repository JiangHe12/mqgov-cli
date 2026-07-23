package cmd

import (
	"bytes"
	"context"
	"errors"
	"testing"
	"time"

	"github.com/JiangHe12/opskit-core/v2/apperrors"
	"github.com/JiangHe12/opskit-core/v2/audit"

	"github.com/JiangHe12/mqgov-cli/internal/mqclass"
	"github.com/JiangHe12/mqgov-cli/internal/mqgov"
	"github.com/JiangHe12/mqgov-cli/internal/mqgovctx"
)

func TestDirectCancellationDoesNotHidePartialFailure(t *testing.T) {
	if !isDirectCancellation(context.Canceled) {
		t.Fatal("direct context cancellation should keep the quiet signal-exit path")
	}
	if isDirectCancellation(errors.Join(context.Canceled)) {
		t.Fatal("wrapped context cancellation must remain a reported non-zero error")
	}
	partial := apperrors.New(
		apperrors.CodePartialFailure,
		"mutation outcome is uncertain",
		context.Canceled,
	)
	if isDirectCancellation(partial) {
		t.Fatal("partial failure wrapping cancellation must remain a reported non-zero error")
	}
}

func TestRocketMQTopicCreateAuthorizationClosesR2AndR3Loop(t *testing.T) {
	f := newDefaultFlags()
	f.Yes = true
	f.NonInter = true

	target, allow := topicCreateAuthorizationTarget(mqgovctx.Context{}, "rocketmq", mqgov.TopicCoordinate{Topic: "orders"})
	if err := classifyAndAuthorize(f, mqgovctx.Context{}, mqclass.OperationCreateTopic, target, allow); apperrors.AsAppError(err).Code != apperrors.CodeAuthorizationRequired {
		t.Fatalf("RocketMQ create without ticket error = %v, want authorization required", err)
	}
	f.Ticket = "OPS-123"
	if err := classifyAndAuthorize(f, mqgovctx.Context{}, mqclass.OperationCreateTopic, target, allow); err != nil {
		t.Fatalf("RocketMQ R2 create with ticket error = %v", err)
	}

	protected := mqgovctx.Context{Topics: []string{"orders"}}
	target, allow = topicCreateAuthorizationTarget(protected, "rocketmq", mqgov.TopicCoordinate{Topic: "orders"})
	if err := classifyAndAuthorize(f, protected, mqclass.OperationCreateTopic, target, allow); apperrors.AsAppError(err).Code != apperrors.CodeAuthorizationRequired {
		t.Fatalf("protected RocketMQ create without allow flag error = %v, want authorization required", err)
	}
	f.AllowTopicUpsert = true
	if err := classifyAndAuthorize(f, protected, mqclass.OperationCreateTopic, target, allow); err != nil {
		t.Fatalf("protected RocketMQ R3 create with exact allow flag error = %v", err)
	}
}

func TestPulsarSystemScopeEscalatesTopicCreateBeforeMutation(t *testing.T) {
	f := newDefaultFlags()
	f.Yes = true
	f.NonInter = true
	coordinate := mqgov.TopicCoordinate{Namespace: "public/system", Topic: "orders"}

	target, allow := topicCreateAuthorizationTarget(mqgovctx.Context{}, "pulsar", coordinate)
	if !target.InternalTopic {
		t.Fatalf("Pulsar system namespace target = %+v, want internal topic", target)
	}
	if err := classifyAndAuthorize(f, mqgovctx.Context{}, mqclass.OperationCreateTopic, target, allow); apperrors.AsAppError(err).Code != apperrors.CodeAuthorizationRequired {
		t.Fatalf("Pulsar system namespace create without ticket error = %v, want authorization required", err)
	}
	f.Ticket = "OPS-123"
	if err := classifyAndAuthorize(f, mqgovctx.Context{}, mqclass.OperationCreateTopic, target, allow); err != nil {
		t.Fatalf("Pulsar system namespace R2 create with ticket error = %v", err)
	}
}

func TestRocketMQTopicDeleteFailsBeforeAuthorizationOrMutationIntent(t *testing.T) {
	t.Setenv("ROCKETMQ_NAMESRV_ADDR", "127.0.0.1:9876")
	for _, args := range [][]string{
		{"--backend", "rocketmq", "topic", "delete", "orders"},
		{"--backend", "rocketmq", "--plan", "topic", "delete", "orders"},
		{"--backend", "rocketmq", "--dry-run", "topic", "delete", "orders"},
	} {
		_, err := runCommandForTest(t, args...)
		if got := apperrors.AsAppError(err).Code; got != apperrors.CodeNotImplemented {
			t.Fatalf("RocketMQ topic delete %v error = %v, want NotImplemented", args, err)
		}
	}
}

func TestTopicCreatePartialFailureAuditIsUncertain(t *testing.T) {
	var records []safeAuditRecord
	f := newDefaultFlags()
	f.mutationAudit = &mutationAuditRuntime{
		appendRecord: func(_ string, record safeAuditRecord, _ audit.Options) error {
			records = append(records, record)
			return nil
		},
		now:    func() time.Time { return time.Unix(1700000000, int64(len(records))).UTC() },
		random: bytes.NewReader(bytes.Repeat([]byte{0x62}, 16)),
	}
	handle, err := beginMutationAudit(f, mutationAuditSpec{
		Action:    "mq.topic.create",
		Target:    audit.EventTarget{ResourceType: "topic", Resource: "orders"},
		AuditPath: privateMutationAuditPath(t),
	})
	if err != nil {
		t.Fatalf("beginMutationAudit() error = %v", err)
	}
	operationErr := apperrors.New(
		apperrors.CodePartialFailure,
		"RocketMQ topic create request returned without a client-reported error but confirmation failed",
		apperrors.New(apperrors.CodeResourceNotFound, "topic not found", nil),
	)
	if err := finishMutationAudit(handle, topicCreateAuditOutcome(operationErr), operationErr); !errors.Is(err, operationErr) {
		t.Fatalf("finishMutationAudit() error = %v, want original partial failure", err)
	}
	if len(records) != 2 || records[1].Outcome == nil {
		t.Fatalf("audit records = %+v, want intent and outcome", records)
	}
	outcome := records[1].Outcome
	if outcome.Status != audit.StatusFailed || outcome.ErrorCode != string(apperrors.CodePartialFailure) ||
		outcome.Uncertain != 1 || outcome.Succeeded != 0 || outcome.Failed != 0 {
		t.Fatalf("topic create outcome = %+v, want one uncertain partial failure", outcome)
	}
}

func TestPurgeMutationOutcomeNeverUsesAffectedMessageCountAsPartitionCount(t *testing.T) {
	reported := mqgov.BatchOutcome{Succeeded: 1, Failed: 1, Uncertain: 1}
	outcome, total := purgeMutationOutcome(reported, 99, errors.New("partial failure"))
	if outcome != reported || total != 3 {
		t.Fatalf("purgeMutationOutcome() = %+v total=%d, want reported partition outcome and total 3", outcome, total)
	}

	outcome, total = purgeMutationOutcome(mqgov.BatchOutcome{}, 2, errors.New("legacy backend failure"))
	if outcome != (mqgov.BatchOutcome{Succeeded: 2, Uncertain: 1}) || total != 3 {
		t.Fatalf("legacy purge outcome = %+v total=%d, want two completed resources and one uncertain resource", outcome, total)
	}
}
