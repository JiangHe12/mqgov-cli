package cmd

import (
	"fmt"

	"github.com/spf13/cobra"

	"github.com/JiangHe12/opskit-core/audit"
	"github.com/JiangHe12/opskit-core/safety"

	"github.com/JiangHe12/mqgov-cli/internal/mqclass"
	"github.com/JiangHe12/mqgov-cli/internal/mqgov"
)

func newMessageCmd(f *cliFlags) *cobra.Command {
	cmd := &cobra.Command{Use: "message", Short: "Produce or peek messages"}
	cmd.AddCommand(newMessagePeekCmd(f), newMessageProduceCmd(f))
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
