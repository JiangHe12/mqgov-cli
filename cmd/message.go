package cmd

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/JiangHe12/opskit-core/apperrors"
	"github.com/spf13/cobra"

	"github.com/JiangHe12/opskit-core/audit"
	"github.com/JiangHe12/opskit-core/safety"

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
			backend, meta, err := buildBroker(f)
			if err != nil {
				return err
			}
			opTarget := operationTargetFromBroker(f, backend)
			topic := args[0]
			if err := classifyAndAuthorize(f, meta, mqclass.OperationPeek, mqclass.Target{Topic: topic}, ""); err != nil {
				return err
			}
			result, err := backend.Peek(cmd.Context(), mqgov.MessagePeekRequest{Coordinate: topicCoord(f, meta, topic), Partition: partition, Offset: offset, Count: count})
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
			opTarget := operationTargetFromBroker(f, backend)
			tailer, ok := mqgov.SupportsTail(backend)
			if !ok {
				return apperrors.New(apperrors.CodeNotImplemented, "backend does not support non-destructive message tail", nil)
			}
			topic := args[0]
			desc, _ := backend.DescribeTopic(cmd.Context(), topicCoord(f, meta, topic))
			target := mqclass.Target{Topic: topic, ProtectedTopic: isProtectedTopic(meta, topic, desc), InternalTopic: desc.Internal}
			if err := classifyAndAuthorize(f, meta, mqclass.OperationTail, target, ""); err != nil {
				return err
			}
			if timeout <= 0 {
				timeout = 30 * time.Second
			}
			runCtx, cancel := context.WithTimeout(cmd.Context(), timeout)
			defer cancel()
			req := mqgov.MessageTailRequest{Coordinate: topicCoord(f, meta, topic), Partition: partition, From: from, Follow: follow, MaxMessages: maxMessages}
			printOperationTarget(newPrinter(f), opTarget, operationTargetRead)
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
	newPrinter(f).Info(fmt.Sprintf("partition=%d offset=%d key-sha256=%s body-sha256=%s size=%d timestamp=%s", fp.Partition, fp.Offset, fp.KeySHA256, fp.BodySHA256, fp.Size, fp.Timestamp))
	return nil
}

func printTailResult(f *cliFlags, result mqgov.MessageTailResult, target operationTarget) error {
	if f.Output == "json" {
		return newPrinter(f).JSONData("MessageTailResult", targetDataForOutput(f, result, target))
	}
	newPrinter(f).Info(fmt.Sprintf("tail complete count=%d totalSize=%d", result.Count, result.TotalSize))
	return nil
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
			backend, meta, err := buildBroker(f)
			if err != nil {
				return err
			}
			opTarget := operationTargetFromBroker(f, backend)
			topic := args[0]
			desc, _ := backend.DescribeTopic(cmd.Context(), topicCoord(f, meta, topic))
			target := mqclass.Target{Topic: topic, ProtectedTopic: isProtectedTopic(meta, topic, desc), InternalTopic: desc.Internal}
			allow := safety.AllowFlag("")
			if target.InternalTopic {
				allow = allowInternalProduce
			}
			if err := classifyAndAuthorize(f, meta, mqclass.OperationProduce, target, allow); err != nil {
				return err
			}
			result, err := backend.Produce(cmd.Context(), mqgov.MessageProduceRequest{Coordinate: topicCoord(f, meta, topic), Key: []byte(key), Body: []byte(body)})
			if err != nil {
				appendAuditWarn(f, auditEventMessage, meta, audit.EventTarget{ResourceType: "message", Resource: topic}, audit.StatusFailed, "produce", err)
				return err
			}
			appendAuditWarn(f, auditEventMessage, meta, audit.EventTarget{ResourceType: "message", Resource: topic}, audit.StatusSuccess, fmt.Sprintf("produce key-sha256=%s body-sha256=%s size=%d", result.Fingerprint.KeySHA256, result.Fingerprint.BodySHA256, result.Fingerprint.Size), nil)
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
			sourceTarget := mqclass.Target{Topic: sourceTopic, ProtectedTopic: sourceMeta.Protected || protectedTopicName(sourceMeta, sourceTopic), InternalTopic: isInternalMessageTopic(sourceTopic)}
			if err := classifyAndAuthorize(f, sourceMeta, mqclass.OperationPeek, sourceTarget, ""); err != nil {
				return err
			}
			targetBackend, targetMeta, targetFlags, err := buildMirrorTargetBroker(f, toContext)
			if err != nil {
				return err
			}
			opTarget := operationTargetFromBroker(targetFlags, targetBackend)
			dryRun := f.DryRun || f.Plan
			targetDesc, _ := targetBackend.DescribeTopic(cmd.Context(), topicCoord(targetFlags, targetMeta, toTopic))
			target := mqclass.Target{Topic: toTopic, ProtectedTopic: isProtectedTopic(targetMeta, toTopic, targetDesc), InternalTopic: targetDesc.Internal, Plan: dryRun}
			allow := safety.AllowFlag("")
			if target.InternalTopic && !dryRun {
				allow = allowInternalProduce
			}
			if err := classifyAndAuthorize(f, targetMeta, mqclass.OperationMirror, target, allow); err != nil {
				return err
			}
			acc := newMirrorAccumulator()
			req := mqgov.MessageMirrorRequest{
				Source:    topicCoord(f, sourceMeta, sourceTopic),
				Target:    topicCoord(targetFlags, targetMeta, toTopic),
				From:      from,
				Partition: partition,
				Limit:     limit,
				DryRun:    dryRun,
			}
			result, err := source.MirrorMessages(cmd.Context(), req, func(msg mqgov.Message) error {
				acc.Add(msg.Body)
				if dryRun {
					return nil
				}
				_, err := targetBackend.Produce(cmd.Context(), mqgov.MessageProduceRequest{Coordinate: req.Target, Key: msg.Key, Body: msg.Body, Headers: msg.Headers})
				return err
			})
			result.Fingerprint = acc.Fingerprints()
			if err != nil {
				appendAuditWarn(f, auditEventMessage, sourceMeta, audit.EventTarget{ResourceType: "message", Resource: sourceTopic + "->" + toContext + "/" + toTopic}, audit.StatusFailed, mirrorAuditDiff(result), err)
				return err
			}
			appendAuditWarn(f, auditEventMessage, sourceMeta, audit.EventTarget{ResourceType: "message", Resource: sourceTopic + "->" + toContext + "/" + toTopic}, audit.StatusSuccess, mirrorAuditDiff(result), nil)
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

func hasMirrorTargetPattern(topic string) bool {
	return strings.ContainsAny(topic, "*?[")
}

func protectedTopicName(meta mqgovctx.Context, topic string) bool {
	for _, protected := range meta.Topics {
		if protected == topic {
			return true
		}
	}
	return false
}

func isInternalMessageTopic(topic string) bool {
	name := strings.ToLower(strings.TrimSpace(topic))
	return strings.HasPrefix(name, "__") || strings.HasPrefix(name, "_system") || strings.Contains(name, "consumer_offsets")
}
