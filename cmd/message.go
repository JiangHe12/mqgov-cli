package cmd

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/JiangHe12/opskit-core/apperrors"
	"github.com/spf13/cobra"

	"github.com/JiangHe12/opskit-core/audit"
	"github.com/JiangHe12/opskit-core/safety"

	"github.com/JiangHe12/mqgov-cli/internal/mqclass"
	"github.com/JiangHe12/mqgov-cli/internal/mqgov"
)

func newMessageCmd(f *cliFlags) *cobra.Command {
	cmd := &cobra.Command{Use: "message", Short: "Produce or read message fingerprints"}
	cmd.AddCommand(newMessagePeekCmd(f), newMessageTailCmd(f), newMessageProduceCmd(f))
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
			topic := args[0]
			if err := classifyAndAuthorize(f, meta, mqclass.OperationPeek, mqclass.Target{Topic: topic}, ""); err != nil {
				return err
			}
			result, err := backend.Peek(cmd.Context(), mqgov.MessagePeekRequest{Coordinate: topicCoord(f, meta, topic), Partition: partition, Offset: offset, Count: count})
			if err != nil {
				return err
			}
			appendAuditWarn(f, auditEventMessage, meta, audit.EventTarget{ResourceType: "message", Resource: topic}, audit.StatusSuccess, fmt.Sprintf("peek count=%d", result.Count), nil)
			return newPrinter(f).JSONData("MessagePeekResult", result)
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
			result, tailErr := tailer.Tail(runCtx, req, func(fp mqgov.MessageFingerprint) error {
				return printTailFingerprint(f, fp)
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
			return printTailResult(f, result)
		},
	}
	cmd.Flags().IntVar(&partition, "partition", -1, "Partition to tail (-1 = all partitions when supported)")
	cmd.Flags().StringVar(&from, "from", "earliest", "Start position: earliest | latest | offset:N")
	cmd.Flags().BoolVar(&follow, "follow", false, "Keep reading new messages until timeout, max-messages, or cancellation")
	cmd.Flags().IntVar(&maxMessages, "max-messages", 100, "Maximum messages to emit (0 = unlimited until timeout)")
	cmd.Flags().DurationVar(&timeout, "timeout", 30*time.Second, "Maximum tail duration")
	return cmd
}

func printTailFingerprint(f *cliFlags, fp mqgov.MessageFingerprint) error {
	if f.Output == "json" {
		return newPrinter(f).JSONData("MessageFingerprint", fp)
	}
	newPrinter(f).Info(fmt.Sprintf("partition=%d offset=%d key-sha256=%s body-sha256=%s size=%d timestamp=%s", fp.Partition, fp.Offset, fp.KeySHA256, fp.BodySHA256, fp.Size, fp.Timestamp))
	return nil
}

func printTailResult(f *cliFlags, result mqgov.MessageTailResult) error {
	if f.Output == "json" {
		return newPrinter(f).JSONData("MessageTailResult", result)
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
			return newPrinter(f).JSONData("MessageProduceResult", result)
		},
	}
	cmd.Flags().StringVar(&key, "key", "", "Message key")
	cmd.Flags().StringVar(&body, "body", "", "Message body")
	return cmd
}
