package cmd

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/JiangHe12/opskit-core/v2/apperrors"
	"github.com/spf13/cobra"

	"github.com/JiangHe12/opskit-core/v2/audit"
	"github.com/JiangHe12/opskit-core/v2/safety"

	"github.com/JiangHe12/mqgov-cli/internal/mqclass"
	"github.com/JiangHe12/mqgov-cli/internal/mqgov"
	"github.com/JiangHe12/mqgov-cli/internal/mqgovctx"
)

func newMessageCmd(f *cliFlags) *cobra.Command {
	cmd := &cobra.Command{Use: "message", Short: "Produce or read message fingerprints"}
	cmd.AddCommand(newMessagePeekCmd(f), newMessageTailCmd(f), newMessageProduceCmd(f), newMessageMirrorCmd(f))
	return cmd
}

func newMessagePeekCmd(f *cliFlags) *cobra.Command {
	var partition int
	var offset int64
	var count int
	cmd := &cobra.Command{
		Use:   "peek TOPIC",
		Short: "Peek message fingerprints without bodies",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			if err := validatePeekWindow(partition, offset, count); err != nil {
				return err
			}
			backend, meta, err := buildBroker(f)
			if err != nil {
				return err
			}
			defer backend.Close()
			opTarget := operationTargetFromBroker(f, backend)
			topic := args[0]
			resolved, err := resolveTopicTarget(cmd.Context(), backend, f, meta, topic, false)
			if err != nil {
				return err
			}
			if err := classifyAndAuthorize(f, meta, mqclass.OperationPeek, resolved.Classification, ""); err != nil {
				return err
			}
			result, err := backend.Peek(cmd.Context(), mqgov.MessagePeekRequest{Coordinate: resolved.Coordinate, Partition: partition, Offset: offset, Count: count})
			if err != nil {
				return err
			}
			appendAuditWarn(f, auditEventMessage, meta, audit.EventTarget{ResourceType: "message", Resource: topic}, audit.StatusSuccess, fmt.Sprintf("peek count=%d", result.Count), nil)
			return targetJSONData(f, "MessagePeekResult", result, opTarget, operationTargetRead)
		},
	}
	cmd.Flags().IntVar(&partition, "partition", 0, "Partition")
	cmd.Flags().Int64Var(&offset, "offset", 0, "Offset")
	cmd.Flags().IntVar(&count, "count", 1, "Maximum messages")
	return cmd
}

func validatePeekWindow(partition int, offset int64, count int) error {
	if partition < 0 {
		return apperrors.New(apperrors.CodeUsageError, "peek partition must be non-negative", nil)
	}
	if offset < 0 {
		return apperrors.New(apperrors.CodeUsageError, "peek offset must be non-negative", nil)
	}
	if count <= 0 {
		return apperrors.New(apperrors.CodeUsageError, "peek count must be positive", nil)
	}
	return nil
}

func newMessageTailCmd(f *cliFlags) *cobra.Command {
	var partition int
	var from string
	var follow bool
	var maxMessages int
	var timeout time.Duration
	cmd := &cobra.Command{
		Use:   "tail TOPIC",
		Short: "Tail message fingerprints without bodies",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			backend, meta, err := buildBroker(f)
			if err != nil {
				return err
			}
			defer backend.Close()
			opTarget := operationTargetFromBroker(f, backend)
			tailer, ok := mqgov.SupportsTail(backend)
			if !ok {
				return apperrors.New(apperrors.CodeNotImplemented, "backend does not support non-destructive message tail", nil)
			}
			topic := args[0]
			resolved, err := resolveTopicTarget(cmd.Context(), backend, f, meta, topic, false)
			if err != nil {
				return err
			}
			if err := classifyAndAuthorize(f, meta, mqclass.OperationTail, resolved.Classification, ""); err != nil {
				return err
			}
			if timeout <= 0 {
				timeout = 30 * time.Second
			}
			runCtx, cancel := context.WithTimeout(cmd.Context(), timeout)
			defer cancel()
			req := mqgov.MessageTailRequest{Coordinate: resolved.Coordinate, Partition: partition, From: from, Follow: follow, MaxMessages: maxMessages}
			if err := printOperationTarget(newPrinter(f), opTarget, operationTargetRead); err != nil {
				return err
			}
			result, tailErr := tailer.Tail(runCtx, req, func(fp mqgov.MessageFingerprint) error {
				return printTailFingerprint(f, fp, opTarget)
			})
			auditStatus := audit.StatusSuccess
			var auditErr error
			if tailErr != nil && !errors.Is(tailErr, context.Canceled) && !errors.Is(tailErr, context.DeadlineExceeded) {
				auditStatus = audit.StatusFailed
				auditErr = tailErr
			}
			appendAuditWarn(f, auditEventMessage, meta, audit.EventTarget{ResourceType: "message", Resource: topic}, auditStatus, tailAuditDiff(result), auditErr)
			if auditErr != nil {
				return tailErr
			}
			return printTailResult(f, result, opTarget)
		},
	}
	cmd.Flags().IntVar(&partition, "partition", -1, "Partition to tail (-1 = all partitions when supported)")
	cmd.Flags().StringVar(&from, "from", "earliest", "Start position: earliest | latest | offset:N")
	cmd.Flags().BoolVar(&follow, "follow", false, "Keep reading new messages until timeout, max-messages, or cancellation")
	cmd.Flags().IntVar(&maxMessages, "max-messages", 100, "Maximum messages to emit (0 = unlimited until timeout)")
	cmd.Flags().DurationVar(&timeout, "timeout", 30*time.Second, "Maximum tail duration")
	return cmd
}

func printTailFingerprint(f *cliFlags, fp mqgov.MessageFingerprint, target operationTarget) error {
	if f.Output == "json" {
		return newPrinter(f).JSONData("MessageFingerprint", targetDataForOutput(f, fp, target))
	}
	return newPrinter(f).Info(fmt.Sprintf("partition=%d offset=%d key-sha256=%s body-sha256=%s size=%d timestamp=%s", fp.Partition, fp.Offset, fp.KeySHA256, fp.BodySHA256, fp.Size, fp.Timestamp))
}

func printTailResult(f *cliFlags, result mqgov.MessageTailResult, target operationTarget) error {
	if f.Output == "json" {
		return newPrinter(f).JSONData("MessageTailResult", targetDataForOutput(f, result, target))
	}
	return newPrinter(f).Info(fmt.Sprintf("tail complete count=%d totalSize=%d", result.Count, result.TotalSize))
}

func tailAuditDiff(result mqgov.MessageTailResult) string {
	return fmt.Sprintf("tail count=%d totalSize=%d impact=%v", result.Count, result.TotalSize, result.Impact)
}

func newMessageProduceCmd(f *cliFlags) *cobra.Command {
	var key string
	var body string
	cmd := &cobra.Command{
		Use:   "produce TOPIC",
		Short: "Produce a message",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			if contextPlanOnly(f) {
				return printBrokerChangePlan(f, "produce", "message", args[0], map[string]any{
					"keySha256":  mqgov.SHA256Hex([]byte(key)),
					"bodySha256": mqgov.SHA256Hex([]byte(body)),
					"size":       len(body),
				})
			}
			backend, meta, err := buildBroker(f)
			if err != nil {
				return err
			}
			defer backend.Close()
			opTarget := operationTargetFromBroker(f, backend)
			topic := args[0]
			resolved, err := resolveTopicTarget(cmd.Context(), backend, f, meta, topic, false)
			if err != nil {
				return err
			}
			allow := safety.AllowFlag("")
			if resolved.Classification.InternalTopic {
				allow = allowInternalProduce
			}
			if err := classifyAndAuthorize(f, meta, mqclass.OperationProduce, resolved.Classification, allow); err != nil {
				return err
			}
			request := mqgov.MessageProduceRequest{Coordinate: resolved.Coordinate, Key: []byte(key), Body: []byte(body)}
			handle, err := beginMutationAudit(f, mutationAuditSpec{
				Action:   "mq.message.produce",
				Context:  meta,
				Target:   audit.EventTarget{ResourceType: "message", Resource: topic},
				Metadata: mutationMessageMetadata(request.Key, request.Body),
			})
			if err != nil {
				return err
			}
			result, operationErr := backend.Produce(cmd.Context(), request)
			if err := finishMutationAudit(handle, mutationAuditOutcome{}, operationErr); err != nil {
				return err
			}
			return targetJSONData(f, "MessageProduceResult", result, opTarget, operationTargetWrite)
		},
	}
	cmd.Flags().StringVar(&key, "key", "", "Message key")
	cmd.Flags().StringVar(&body, "body", "", "Message body")
	return cmd
}

func newMessageMirrorCmd(f *cliFlags) *cobra.Command {
	var toContext string
	var toTopic string
	var limit int
	var from string
	var partition int
	cmd := &cobra.Command{
		Use:   "mirror SOURCE_TOPIC",
		Short: "Copy a bounded batch of messages to another context",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			sourceBackend, sourceMeta, err := buildBroker(f)
			if err != nil {
				return err
			}
			defer sourceBackend.Close()
			source, ok := mqgov.SupportsMirrorSource(sourceBackend)
			if !ok {
				return apperrors.New(apperrors.CodeNotImplemented, "source backend does not support non-destructive message mirror", nil)
			}
			if toContext == "" {
				return apperrors.New(apperrors.CodeUsageError, "--to-context is required", nil)
			}
			if toTopic == "" {
				return apperrors.New(apperrors.CodeUsageError, "--to-topic is required", nil)
			}
			if limit <= 0 {
				return apperrors.New(apperrors.CodeUsageError, "--limit must be positive", nil)
			}
			sourceTopic := args[0]
			if hasMirrorTargetPattern(toTopic) {
				return apperrors.New(apperrors.CodeUsageError, "mirror target topic must not contain wildcards", nil)
			}
			sourcePreflightTarget := declaredTopicTarget(sourceMeta, sourceTopic, false)
			sourcePreflightTarget.ProtectedTopic = sourcePreflightTarget.ProtectedTopic || sourceMeta.Protected
			if err := classifyAndAuthorize(f, sourceMeta, mqclass.OperationPeek, sourcePreflightTarget, ""); err != nil {
				return err
			}
			targetBackend, targetMeta, targetFlags, err := buildMirrorTargetBroker(f, toContext)
			if err != nil {
				return err
			}
			defer targetBackend.Close()
			opTarget := operationTargetFromBroker(targetFlags, targetBackend)
			dryRun := f.DryRun || f.Plan
			targetPreflightTarget := declaredTopicTarget(targetMeta, toTopic, dryRun)
			allow := safety.AllowFlag("")
			if targetPreflightTarget.InternalTopic && !dryRun {
				allow = allowInternalProduce
			}
			if err := classifyAndAuthorize(targetFlags, targetMeta, mqclass.OperationMirror, targetPreflightTarget, allow); err != nil {
				return err
			}
			sourceResolved, err := resolveTopicTarget(cmd.Context(), sourceBackend, f, sourceMeta, sourceTopic, false)
			if err != nil {
				return err
			}
			sourceResolved.Classification.ProtectedTopic = sourceResolved.Classification.ProtectedTopic || sourceMeta.Protected
			targetResolved, err := resolveTopicTarget(cmd.Context(), targetBackend, targetFlags, targetMeta, toTopic, dryRun)
			if err != nil {
				return err
			}
			if !sameTopicClassification(sourcePreflightTarget, sourceResolved.Classification) {
				if err := classifyAndAuthorize(f, sourceMeta, mqclass.OperationPeek, sourceResolved.Classification, ""); err != nil {
					return err
				}
			}
			allow = ""
			if targetResolved.Classification.InternalTopic && !dryRun {
				allow = allowInternalProduce
			}
			if !sameTopicClassification(targetPreflightTarget, targetResolved.Classification) {
				if err := classifyAndAuthorize(targetFlags, targetMeta, mqclass.OperationMirror, targetResolved.Classification, allow); err != nil {
					return err
				}
			}
			acc := newMirrorAccumulator()
			req := mqgov.MessageMirrorRequest{
				Source:    sourceResolved.Coordinate,
				Target:    targetResolved.Coordinate,
				From:      from,
				Partition: partition,
				Limit:     limit,
				DryRun:    dryRun,
			}
			var handle *mutationAuditHandle
			if !dryRun {
				handle, err = beginMutationAudit(f, mirrorTargetMutationAuditSpec(toContext, targetMeta, req, limit))
				if err != nil {
					return err
				}
			}
			succeeded := 0
			failed := 0
			result, err := source.MirrorMessages(cmd.Context(), req, func(msg mqgov.Message) error {
				acc.Add(msg.Body)
				if dryRun {
					return nil
				}
				_, produceErr := targetBackend.Produce(cmd.Context(), mqgov.MessageProduceRequest{Coordinate: req.Target, Key: msg.Key, Body: msg.Body, Headers: msg.Headers})
				if produceErr != nil {
					failed++
					return produceErr
				}
				succeeded++
				return nil
			})
			result.Fingerprint = acc.Fingerprints()
			sourceAuditStatus := audit.StatusSuccess
			if err != nil {
				sourceAuditStatus = audit.StatusFailed
			}
			appendMirrorReadAudit(f, sourceMeta, f.contextName(), req, result, sourceAuditStatus, err)
			if !dryRun {
				if err != nil && failed == 0 {
					failed = 1
				}
				if auditErr := finishBatchMutationAudit(handle, limit, succeeded, failed, err); auditErr != nil {
					return auditErr
				}
			}
			if err != nil {
				if dryRun {
					appendMirrorTargetPreviewAudit(f, targetMeta, toContext, req, result, audit.StatusFailed, err)
				}
				return err
			}
			if dryRun {
				appendMirrorTargetPreviewAudit(f, targetMeta, toContext, req, result, audit.StatusSuccess, nil)
			}
			return targetJSONData(f, "MessageMirrorResult", result, opTarget, operationTargetWrite)
		},
	}
	cmd.Flags().StringVar(&toContext, "to-context", "", "Target context name")
	cmd.Flags().StringVar(&toTopic, "to-topic", "", "Target topic")
	cmd.Flags().IntVar(&limit, "limit", 100, "Maximum messages to copy")
	cmd.Flags().StringVar(&from, "from", "earliest", "Start position: earliest | latest | offset:N | timestamp:<RFC3339>")
	cmd.Flags().IntVar(&partition, "partition", -1, "Source partition (-1 = all partitions when supported)")
	return cmd
}

func buildMirrorTargetBroker(f *cliFlags, name string) (mqgov.Broker, mqgovctx.Context, *cliFlags, error) {
	cfg, err := mqgovctx.Load()
	if err != nil {
		return nil, mqgovctx.Context{}, nil, err
	}
	item, ok := cfg.Contexts[name]
	if !ok {
		return nil, mqgovctx.Context{}, nil, apperrors.New(apperrors.CodeUsageError, "target context not found", nil)
	}
	local := fleetLocalFlags(f, name)
	backend, err := buildBrokerFromContext(&local, item, name)
	return backend, item, &local, err
}

type mirrorAccumulator struct {
	bodyHashes []byte
	totalSize  int
	totalCount int64
}

func newMirrorAccumulator() *mirrorAccumulator {
	return &mirrorAccumulator{}
}

func (a *mirrorAccumulator) Add(body []byte) {
	bodyHash := sha256.Sum256(body)
	a.bodyHashes = append(a.bodyHashes, bodyHash[:]...)
	a.totalSize += len(body)
	a.totalCount++
}

func (a *mirrorAccumulator) Fingerprints() mqgov.ResourceFingerprints {
	sum := sha256.Sum256(a.bodyHashes)
	if a.totalCount == 0 {
		sum = sha256.Sum256(nil)
	}
	return mqgov.ResourceFingerprints{BodySHA256: hex.EncodeToString(sum[:]), Count: a.totalCount, Size: a.totalSize}
}

func mirrorAuditDiff(result mqgov.MessageMirrorResult) string {
	return fmt.Sprintf("mirror source=%s target=%s count=%d body-sha256=%s dryRun=%t", result.Source.Topic, result.Target.Topic, result.Count, result.Fingerprint.BodySHA256, result.DryRun)
}

type mirrorAuditBinding struct {
	Request     mqgov.MessageMirrorRequest
	Count       int64
	Impact      []mqgov.PartitionImpact
	Fingerprint mqgov.ResourceFingerprints
}

func mirrorTargetMutationAuditSpec(
	contextName string,
	meta mqgovctx.Context,
	request mqgov.MessageMirrorRequest,
	limit int,
) mutationAuditSpec {
	metadata := mutationValueMetadata("mq.message.mirror.target", request)
	metadata.Items = limit
	return mutationAuditSpec{
		Action:      "mq.message.mirror",
		ContextName: contextName,
		Context:     meta,
		Target:      audit.EventTarget{ResourceType: "message", Resource: request.Target.Topic},
		Metadata:    metadata,
	}
}

func appendMirrorReadAudit(
	f *cliFlags,
	meta mqgovctx.Context,
	contextName string,
	request mqgov.MessageMirrorRequest,
	result mqgov.MessageMirrorResult,
	status string,
	operationErr error,
) {
	metadata := mutationValueMetadata("mq.message.mirror.source", mirrorAuditBinding{
		Request:     request,
		Count:       result.Count,
		Impact:      result.Impact,
		Fingerprint: result.Fingerprint,
	})
	metadata.Items = int(result.Count)
	appendAuditWarnForContext(
		f,
		auditEventMessage,
		meta,
		contextName,
		audit.EventTarget{ResourceType: "message", Resource: request.Source.Topic},
		status,
		mirrorAuditDiff(result),
		metadata,
		operationErr,
	)
}

func appendMirrorTargetPreviewAudit(
	f *cliFlags,
	meta mqgovctx.Context,
	contextName string,
	request mqgov.MessageMirrorRequest,
	result mqgov.MessageMirrorResult,
	status string,
	operationErr error,
) {
	metadata := mutationValueMetadata("mq.message.mirror.target-preview", mirrorAuditBinding{
		Request:     request,
		Count:       result.Count,
		Impact:      result.Impact,
		Fingerprint: result.Fingerprint,
	})
	metadata.Items = int(result.Count)
	appendAuditWarnForContext(
		f,
		auditEventMessage,
		meta,
		contextName,
		audit.EventTarget{ResourceType: "message", Resource: request.Target.Topic},
		status,
		mirrorAuditDiff(result),
		metadata,
		operationErr,
	)
}

func hasMirrorTargetPattern(topic string) bool {
	return strings.ContainsAny(topic, "*?[")
}
