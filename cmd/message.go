package cmd

import (
	"bytes"
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

const (
	maxPeekMessages          = mqgov.MaxMessageBatchSize
	maxTailMessages          = mqgov.MaxMessageBatchSize
	tailBufferInitialLimit   = 256
	maxMirrorMessages        = mqgov.MaxMirrorBatchSize
	mirrorBufferInitialLimit = 64
	maxMirrorBufferedBytes   = 64 * 1024 * 1024
	mirrorMessageOverhead    = 256
	mirrorHeaderOverhead     = 128
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
			topic := args[0]
			result, opTarget, err := runMandatoryBrokerRead(f, readAuditSpec{
				Action: "mq.message.peek",
				Target: audit.EventTarget{ResourceType: "message", Resource: topic},
				Metadata: mutationValueMetadata("mq.message.peek", struct {
					Topic     string
					Partition int
					Offset    int64
					Count     int
				}{Topic: topic, Partition: partition, Offset: offset, Count: count}),
			}, func(meta mqgovctx.Context) error {
				target := declaredTopicTarget(meta, firstNonEmpty(f.Backend, meta.Backend, defaultFakeBackend), topic, false)
				return classifyAndAuthorize(f, meta, mqclass.OperationPeek, target, "")
			}, func(backend mqgov.Broker, meta mqgovctx.Context) (mqgov.MessagePeekResult, error) {
				resolved, resolveErr := resolveTopicTarget(cmd.Context(), backend, f, meta, topic, false)
				if resolveErr != nil {
					return mqgov.MessagePeekResult{}, resolveErr
				}
				if authorizeErr := classifyAndAuthorize(f, meta, mqclass.OperationPeek, resolved.Classification, ""); authorizeErr != nil {
					return mqgov.MessagePeekResult{}, authorizeErr
				}
				return backend.Peek(cmd.Context(), mqgov.MessagePeekRequest{Coordinate: resolved.Coordinate, Partition: partition, Offset: offset, Count: count})
			}, func(result mqgov.MessagePeekResult) int {
				return result.Count
			})
			if err != nil {
				return err
			}
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
	if count > maxPeekMessages {
		return apperrors.New(apperrors.CodeUsageError, fmt.Sprintf("peek count must not exceed %d", maxPeekMessages), nil)
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
			if err := validateTailWindow(maxMessages); err != nil {
				return err
			}
			topic := args[0]
			if timeout <= 0 {
				timeout = 30 * time.Second
			}
			readResult, opTarget, err := runMandatoryBrokerRead(f, readAuditSpec{
				Action: "mq.message.tail",
				Target: audit.EventTarget{ResourceType: "message", Resource: topic},
				Metadata: mutationValueMetadata("mq.message.tail", struct {
					Topic       string
					Partition   int
					From        string
					Follow      bool
					MaxMessages int
					Timeout     time.Duration
				}{
					Topic:       topic,
					Partition:   partition,
					From:        from,
					Follow:      follow,
					MaxMessages: maxMessages,
					Timeout:     timeout,
				}),
			}, func(meta mqgovctx.Context) error {
				target := declaredTopicTarget(meta, firstNonEmpty(f.Backend, meta.Backend, defaultFakeBackend), topic, false)
				return classifyAndAuthorize(f, meta, mqclass.OperationTail, target, "")
			}, func(backend mqgov.Broker, meta mqgovctx.Context) (messageTailReadResult, error) {
				tailer, ok := mqgov.SupportsTail(backend)
				if !ok {
					return messageTailReadResult{}, apperrors.New(apperrors.CodeNotImplemented, "backend does not support non-destructive message tail", nil)
				}
				resolved, resolveErr := resolveTopicTarget(cmd.Context(), backend, f, meta, topic, false)
				if resolveErr != nil {
					return messageTailReadResult{}, resolveErr
				}
				if authorizeErr := classifyAndAuthorize(f, meta, mqclass.OperationTail, resolved.Classification, ""); authorizeErr != nil {
					return messageTailReadResult{}, authorizeErr
				}
				runCtx, cancel := context.WithTimeout(cmd.Context(), timeout)
				defer cancel()
				req := mqgov.MessageTailRequest{Coordinate: resolved.Coordinate, Partition: partition, From: from, Follow: follow, MaxMessages: maxMessages}
				fingerprints := make([]mqgov.MessageFingerprint, 0, tailBufferCapacity(maxMessages))
				result, tailErr := tailer.Tail(runCtx, req, func(fp mqgov.MessageFingerprint) error {
					if len(fingerprints) >= maxMessages {
						return apperrors.New(apperrors.CodeBackendError, "backend emitted more messages than requested", nil)
					}
					fingerprints = append(fingerprints, fp)
					return nil
				})
				if tailErr != nil && !errors.Is(tailErr, context.Canceled) && !errors.Is(tailErr, context.DeadlineExceeded) {
					return messageTailReadResult{Result: result, Fingerprints: fingerprints}, tailErr
				}
				return messageTailReadResult{Result: result, Fingerprints: fingerprints}, nil
			}, func(result messageTailReadResult) int {
				return len(result.Fingerprints)
			})
			if err != nil {
				return err
			}
			if err := printOperationTarget(newPrinter(f), opTarget, operationTargetRead); err != nil {
				return err
			}
			for _, fingerprint := range readResult.Fingerprints {
				if err := printTailFingerprint(f, fingerprint, opTarget); err != nil {
					return err
				}
			}
			return printTailResult(f, readResult.Result, opTarget)
		},
	}
	cmd.Flags().IntVar(&partition, "partition", -1, "Partition to tail (-1 = all partitions when supported)")
	cmd.Flags().StringVar(&from, "from", "earliest", "Start position: earliest | latest | offset:N")
	cmd.Flags().BoolVar(&follow, "follow", false, "Keep reading new messages until timeout, max-messages, or cancellation")
	cmd.Flags().IntVar(&maxMessages, "max-messages", 100, "Maximum messages to emit")
	cmd.Flags().DurationVar(&timeout, "timeout", 30*time.Second, "Maximum tail duration")
	return cmd
}

func validateTailWindow(maxMessages int) error {
	if maxMessages <= 0 {
		return apperrors.New(apperrors.CodeUsageError, "tail max-messages must be positive", nil)
	}
	if maxMessages > maxTailMessages {
		return apperrors.New(
			apperrors.CodeUsageError,
			fmt.Sprintf("tail max-messages must not exceed %d", maxTailMessages),
			nil,
		)
	}
	return nil
}

func tailBufferCapacity(maxMessages int) int {
	if maxMessages < tailBufferInitialLimit {
		return maxMessages
	}
	return tailBufferInitialLimit
}

type messageTailReadResult struct {
	Result       mqgov.MessageTailResult
	Fingerprints []mqgov.MessageFingerprint
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
			topic := args[0]
			preflight, err := runMandatoryBrokerPreflight(f, readAuditSpec{
				Action:   "mq.message.produce.preflight",
				Target:   audit.EventTarget{ResourceType: "message", Resource: topic},
				Metadata: mutationValueMetadata("mq.message.produce.preflight", map[string]string{"topic": topic}),
			}, func(backend mqgov.Broker, meta mqgovctx.Context) (resolvedTopicTarget, error) {
				return resolveTopicTarget(cmd.Context(), backend, f, meta, topic, false)
			}, func(resolvedTopicTarget) int { return 1 })
			if err != nil {
				return err
			}
			defer preflight.Backend.Close()
			resolved := preflight.Value
			allow := safety.AllowFlag("")
			if resolved.Classification.InternalTopic {
				allow = allowInternalProduce
			}
			if err := classifyAndAuthorize(f, preflight.Context, mqclass.OperationProduce, resolved.Classification, allow); err != nil {
				return err
			}
			request := mqgov.MessageProduceRequest{Coordinate: resolved.Coordinate, Key: []byte(key), Body: []byte(body)}
			handle, err := beginMutationAudit(f, mutationAuditSpec{
				Action:   "mq.message.produce",
				Context:  preflight.Context,
				Target:   audit.EventTarget{ResourceType: "message", Resource: topic},
				Metadata: mutationMessageMetadata(request.Key, request.Body),
			})
			if err != nil {
				return err
			}
			result, operationErr := preflight.Backend.Produce(cmd.Context(), request)
			if err := finishMutationAudit(handle, mutationAuditOutcome{}, operationErr); err != nil {
				return err
			}
			return targetJSONData(f, "MessageProduceResult", result, preflight.Target, operationTargetWrite)
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
			sourceTopic := args[0]
			if err := validateMirrorWindow(toContext, toTopic, limit); err != nil {
				return err
			}
			dryRun := f.DryRun || f.Plan
			sourceMeta, sourceContextName, err := resolvedContext(f)
			if err != nil {
				return err
			}

			var sourceBackend mqgov.Broker
			var targetBackend mqgov.Broker
			defer func() {
				if targetBackend != nil {
					targetBackend.Close()
				}
				if sourceBackend != nil {
					sourceBackend.Close()
				}
			}()

			var bufferedPhase mirrorReadPhase
			readPhase, err := runMandatoryRead(f, mirrorSourceReadAuditSpec(
				sourceContextName,
				sourceMeta,
				sourceTopic,
				toContext,
				toTopic,
				from,
				partition,
				limit,
				dryRun,
			), func() (mirrorReadPhase, error) {
				sourcePreflightTarget := declaredTopicTarget(
					sourceMeta,
					firstNonEmpty(f.Backend, sourceMeta.Backend, defaultFakeBackend),
					sourceTopic,
					false,
				)
				sourcePreflightTarget.ProtectedTopic = sourcePreflightTarget.ProtectedTopic || sourceMeta.Protected
				if authorizeErr := classifyAndAuthorize(f, sourceMeta, mqclass.OperationPeek, sourcePreflightTarget, ""); authorizeErr != nil {
					return mirrorReadPhase{}, authorizeErr
				}
				targetMeta, targetFlags, err := resolveMirrorTargetForCommand(f, toContext)
				if err != nil {
					return mirrorReadPhase{}, err
				}
				targetPreflightTarget := declaredTopicTarget(
					targetMeta,
					firstNonEmpty(targetFlags.Backend, targetMeta.Backend, defaultFakeBackend),
					toTopic,
					dryRun,
				)
				allow := safety.AllowFlag("")
				if targetPreflightTarget.InternalTopic && !dryRun {
					allow = allowInternalProduce
				}
				if authorizeErr := classifyAndAuthorize(targetFlags, targetMeta, mqclass.OperationMirror, targetPreflightTarget, allow); authorizeErr != nil {
					return mirrorReadPhase{}, authorizeErr
				}
				sourceBackend, err = buildMirrorSourceBroker(f, sourceMeta, sourceContextName)
				if err != nil {
					return mirrorReadPhase{}, err
				}
				defer func() {
					sourceBackend.Close()
					sourceBackend = nil
				}()
				var buildErr error
				targetBackend, buildErr = buildMirrorTargetBrokerForCommand(f, targetMeta, targetFlags, toContext)
				if buildErr != nil {
					return mirrorReadPhase{}, buildErr
				}
				bufferedPhase, buildErr = readMirrorSource(
					cmd.Context(),
					f,
					sourceBackend,
					sourceMeta,
					sourceTopic,
					targetBackend,
					targetMeta,
					targetFlags,
					toTopic,
					from,
					partition,
					limit,
					dryRun,
				)
				return bufferedPhase, buildErr
			}, func(phase mirrorReadPhase) int {
				return len(phase.Messages)
			})
			if err != nil {
				wipeMirrorMessages(bufferedPhase.Messages)
				return err
			}
			defer wipeMirrorMessages(readPhase.Messages)

			if dryRun {
				appendMirrorTargetPreviewAudit(f, readPhase.TargetMeta, toContext, readPhase.Request, readPhase.Result, audit.StatusSuccess, nil)
				return targetJSONData(f, "MessageMirrorResult", readPhase.Result, readPhase.OperationTarget, operationTargetWrite)
			}

			handle, err := beginMutationAudit(f, mirrorTargetMutationAuditSpec(toContext, readPhase.TargetMeta, readPhase.Request, limit))
			if err != nil {
				return err
			}
			succeeded, failed, operationErr := produceMirroredMessages(cmd.Context(), targetBackend, readPhase.Request.Target, readPhase.Messages)
			if auditErr := finishBatchMutationAudit(handle, limit, succeeded, failed, operationErr); auditErr != nil {
				return auditErr
			}
			return targetJSONData(f, "MessageMirrorResult", readPhase.Result, readPhase.OperationTarget, operationTargetWrite)
		},
	}
	cmd.Flags().StringVar(&toContext, "to-context", "", "Target context name")
	cmd.Flags().StringVar(&toTopic, "to-topic", "", "Target topic")
	cmd.Flags().IntVar(&limit, "limit", 100, "Maximum messages to copy")
	cmd.Flags().StringVar(&from, "from", "earliest", "Start position: earliest | latest | offset:N | timestamp:<RFC3339>")
	cmd.Flags().IntVar(&partition, "partition", -1, "Source partition (-1 = all partitions when supported)")
	return cmd
}

type mirrorCommandRuntime struct {
	buildSource      func(*cliFlags, mqgovctx.Context, string) (mqgov.Broker, error)
	resolveTarget    func(*cliFlags, string) (mqgovctx.Context, *cliFlags, error)
	buildTarget      func(*cliFlags, mqgovctx.Context, *cliFlags, string) (mqgov.Broker, error)
	maxBufferedBytes int
}

type mirrorReadPhase struct {
	Request         mqgov.MessageMirrorRequest
	Result          mqgov.MessageMirrorResult
	Messages        []mqgov.Message
	TargetMeta      mqgovctx.Context
	OperationTarget operationTarget
}

type mirrorSourceReadAuditBinding struct {
	SourceContext string
	SourceTopic   string
	TargetContext string
	TargetTopic   string
	From          string
	Partition     int
	Limit         int
	DryRun        bool
}

func validateMirrorWindow(toContext, toTopic string, limit int) error {
	if toContext == "" {
		return apperrors.New(apperrors.CodeUsageError, "--to-context is required", nil)
	}
	if toTopic == "" {
		return apperrors.New(apperrors.CodeUsageError, "--to-topic is required", nil)
	}
	if limit <= 0 {
		return apperrors.New(apperrors.CodeUsageError, "--limit must be positive", nil)
	}
	if limit > maxMirrorMessages {
		return apperrors.New(
			apperrors.CodeUsageError,
			fmt.Sprintf("mirror limit must not exceed %d", maxMirrorMessages),
			nil,
		)
	}
	if hasMirrorTargetPattern(toTopic) {
		return apperrors.New(apperrors.CodeUsageError, "mirror target topic must not contain wildcards", nil)
	}
	return nil
}

func mirrorSourceReadAuditSpec(
	contextName string,
	meta mqgovctx.Context,
	sourceTopic string,
	targetContext string,
	targetTopic string,
	from string,
	partition int,
	limit int,
	dryRun bool,
) readAuditSpec {
	return readAuditSpec{
		Action:      "mq.message.mirror.source",
		ContextName: contextName,
		Context:     meta,
		Target:      audit.EventTarget{ResourceType: "message", Resource: sourceTopic},
		Metadata: mutationValueMetadata("mq.message.mirror.source", mirrorSourceReadAuditBinding{
			SourceContext: contextName,
			SourceTopic:   sourceTopic,
			TargetContext: targetContext,
			TargetTopic:   targetTopic,
			From:          from,
			Partition:     partition,
			Limit:         limit,
			DryRun:        dryRun,
		}),
	}
}

func buildMirrorSourceBroker(f *cliFlags, meta mqgovctx.Context, contextName string) (mqgov.Broker, error) {
	if f.mirrorRuntime != nil && f.mirrorRuntime.buildSource != nil {
		return f.mirrorRuntime.buildSource(f, meta, contextName)
	}
	return buildBrokerForResolvedContext(f, meta, contextName)
}

func resolveMirrorTargetForCommand(f *cliFlags, name string) (mqgovctx.Context, *cliFlags, error) {
	if f.mirrorRuntime != nil && f.mirrorRuntime.resolveTarget != nil {
		return f.mirrorRuntime.resolveTarget(f, name)
	}
	return resolveMirrorTarget(f, name)
}

func buildMirrorTargetBrokerForCommand(
	f *cliFlags,
	item mqgovctx.Context,
	targetFlags *cliFlags,
	name string,
) (mqgov.Broker, error) {
	if f.mirrorRuntime != nil && f.mirrorRuntime.buildTarget != nil {
		return f.mirrorRuntime.buildTarget(f, item, targetFlags, name)
	}
	return buildBrokerFromContext(targetFlags, item, name)
}

func readMirrorSource(
	ctx context.Context,
	f *cliFlags,
	sourceBackend mqgov.Broker,
	sourceMeta mqgovctx.Context,
	sourceTopic string,
	targetBackend mqgov.Broker,
	targetMeta mqgovctx.Context,
	targetFlags *cliFlags,
	targetTopic string,
	from string,
	partition int,
	limit int,
	dryRun bool,
) (mirrorReadPhase, error) {
	source, ok := mqgov.SupportsMirrorSource(sourceBackend)
	if !ok {
		return mirrorReadPhase{}, apperrors.New(apperrors.CodeNotImplemented, "source backend does not support non-destructive message mirror", nil)
	}
	sourceResolved, targetResolved, err := resolveAuthorizedMirrorTargets(
		ctx,
		f,
		sourceBackend,
		sourceMeta,
		sourceTopic,
		targetBackend,
		targetMeta,
		targetFlags,
		targetTopic,
		dryRun,
	)
	if err != nil {
		return mirrorReadPhase{}, err
	}
	request := mqgov.MessageMirrorRequest{
		Source:    sourceResolved.Coordinate,
		Target:    targetResolved.Coordinate,
		From:      from,
		Partition: partition,
		Limit:     limit,
		DryRun:    dryRun,
	}
	buffer := newMirrorReadBuffer(limit, mirrorBufferedByteLimit(f))
	result, operationErr := source.MirrorMessages(ctx, request, buffer.Add)
	result.Fingerprint = buffer.accumulator.Fingerprints()
	phase := mirrorReadPhase{
		Request:         request,
		Result:          result,
		Messages:        buffer.messages,
		TargetMeta:      targetMeta,
		OperationTarget: operationTargetFromBroker(targetFlags, targetBackend),
	}
	if operationErr != nil {
		return phase, operationErr
	}
	return phase, validateMirrorReadResult(request, result, len(buffer.messages))
}

func resolveAuthorizedMirrorTargets(
	ctx context.Context,
	f *cliFlags,
	sourceBackend mqgov.Broker,
	sourceMeta mqgovctx.Context,
	sourceTopic string,
	targetBackend mqgov.Broker,
	targetMeta mqgovctx.Context,
	targetFlags *cliFlags,
	targetTopic string,
	dryRun bool,
) (resolvedTopicTarget, resolvedTopicTarget, error) {
	sourcePreflightTarget := declaredTopicTarget(sourceMeta, sourceBackend.Describe().Backend, sourceTopic, false)
	sourcePreflightTarget.ProtectedTopic = sourcePreflightTarget.ProtectedTopic || sourceMeta.Protected
	if err := classifyAndAuthorize(f, sourceMeta, mqclass.OperationPeek, sourcePreflightTarget, ""); err != nil {
		return resolvedTopicTarget{}, resolvedTopicTarget{}, err
	}
	targetPreflightTarget := declaredTopicTarget(targetMeta, targetBackend.Describe().Backend, targetTopic, dryRun)
	allow := mirrorTargetAllow(targetPreflightTarget, dryRun)
	if err := classifyAndAuthorize(targetFlags, targetMeta, mqclass.OperationMirror, targetPreflightTarget, allow); err != nil {
		return resolvedTopicTarget{}, resolvedTopicTarget{}, err
	}
	sourceResolved, err := resolveTopicTarget(ctx, sourceBackend, f, sourceMeta, sourceTopic, false)
	if err != nil {
		return resolvedTopicTarget{}, resolvedTopicTarget{}, err
	}
	sourceResolved.Classification.ProtectedTopic = sourceResolved.Classification.ProtectedTopic || sourceMeta.Protected
	targetResolved, err := resolveTopicTarget(ctx, targetBackend, targetFlags, targetMeta, targetTopic, dryRun)
	if err != nil {
		return resolvedTopicTarget{}, resolvedTopicTarget{}, err
	}
	if !sameTopicClassification(sourcePreflightTarget, sourceResolved.Classification) {
		if err := classifyAndAuthorize(f, sourceMeta, mqclass.OperationPeek, sourceResolved.Classification, ""); err != nil {
			return resolvedTopicTarget{}, resolvedTopicTarget{}, err
		}
	}
	allow = mirrorTargetAllow(targetResolved.Classification, dryRun)
	if !sameTopicClassification(targetPreflightTarget, targetResolved.Classification) {
		if err := classifyAndAuthorize(targetFlags, targetMeta, mqclass.OperationMirror, targetResolved.Classification, allow); err != nil {
			return resolvedTopicTarget{}, resolvedTopicTarget{}, err
		}
	}
	return sourceResolved, targetResolved, nil
}

func mirrorTargetAllow(target mqclass.Target, dryRun bool) safety.AllowFlag {
	if target.InternalTopic && !dryRun {
		return allowInternalProduce
	}
	return ""
}

func validateMirrorReadResult(request mqgov.MessageMirrorRequest, result mqgov.MessageMirrorResult, buffered int) error {
	if result.Source != request.Source ||
		result.Target != request.Target ||
		result.DryRun != request.DryRun ||
		result.Count != int64(buffered) {
		return apperrors.New(apperrors.CodeValidationFailed, "mirror source returned an inconsistent bounded-read result", nil)
	}
	return nil
}

type mirrorReadBuffer struct {
	messages    []mqgov.Message
	buffered    int
	maxMessages int
	maxBytes    int
	accumulator *mirrorAccumulator
}

func mirrorBufferedByteLimit(f *cliFlags) int {
	if f.mirrorRuntime != nil && f.mirrorRuntime.maxBufferedBytes > 0 {
		return f.mirrorRuntime.maxBufferedBytes
	}
	return maxMirrorBufferedBytes
}

func newMirrorReadBuffer(limit, maxBytes int) *mirrorReadBuffer {
	capacity := limit
	if capacity > mirrorBufferInitialLimit {
		capacity = mirrorBufferInitialLimit
	}
	return &mirrorReadBuffer{
		messages:    make([]mqgov.Message, 0, capacity),
		maxMessages: limit,
		maxBytes:    maxBytes,
		accumulator: newMirrorAccumulator(),
	}
}

func (buffer *mirrorReadBuffer) Add(message mqgov.Message) error {
	if len(buffer.messages) >= buffer.maxMessages {
		return apperrors.New(apperrors.CodeValidationFailed, "mirror source emitted more messages than requested", nil)
	}
	messageBytes, ok := boundedMirrorMessageBytes(message, buffer.maxBytes-buffer.buffered)
	if !ok {
		return apperrors.New(
			apperrors.CodeValidationFailed,
			fmt.Sprintf("mirror source exceeded the %d-byte safe buffer limit", buffer.maxBytes),
			nil,
		)
	}
	cloned := cloneMirrorMessage(message)
	buffer.messages = append(buffer.messages, cloned)
	buffer.buffered += messageBytes
	buffer.accumulator.Add(cloned.Body)
	return nil
}

func boundedMirrorMessageBytes(message mqgov.Message, remaining int) (int, bool) {
	total := 0
	add := func(size int) bool {
		if size < 0 || size > remaining-total {
			return false
		}
		total += size
		return true
	}
	if !add(mirrorMessageOverhead) || !add(len(message.Key)) || !add(len(message.Body)) {
		return 0, false
	}
	for key, value := range message.Headers {
		if !add(mirrorHeaderOverhead) || !add(len(key)) || !add(len(value)) {
			return 0, false
		}
	}
	return total, true
}

func cloneMirrorMessage(message mqgov.Message) mqgov.Message {
	cloned := message
	cloned.Key = bytes.Clone(message.Key)
	cloned.Body = bytes.Clone(message.Body)
	if message.Headers != nil {
		cloned.Headers = make(map[string][]byte, len(message.Headers))
		for key, value := range message.Headers {
			cloned.Headers[key] = bytes.Clone(value)
		}
	}
	return cloned
}

func wipeMirrorMessages(messages []mqgov.Message) {
	for index := range messages {
		clear(messages[index].Key)
		clear(messages[index].Body)
		for key, value := range messages[index].Headers {
			clear(value)
			delete(messages[index].Headers, key)
		}
		messages[index] = mqgov.Message{}
	}
}

func produceMirroredMessages(
	ctx context.Context,
	target mqgov.Broker,
	coordinate mqgov.TopicCoordinate,
	messages []mqgov.Message,
) (succeeded int, failed int, operationErr error) {
	for _, message := range messages {
		if _, err := target.Produce(ctx, mqgov.MessageProduceRequest{
			Coordinate: coordinate,
			Key:        message.Key,
			Body:       message.Body,
			Headers:    message.Headers,
		}); err != nil {
			return succeeded, 1, err
		}
		succeeded++
	}
	return succeeded, 0, nil
}

func resolveMirrorTarget(f *cliFlags, name string) (mqgovctx.Context, *cliFlags, error) {
	cfg, err := mqgovctx.Load()
	if err != nil {
		return mqgovctx.Context{}, nil, err
	}
	item, ok := cfg.Contexts[name]
	if !ok {
		return mqgovctx.Context{}, nil, apperrors.New(apperrors.CodeUsageError, "target context not found", nil)
	}
	local := fleetLocalFlags(f, name)
	return item, &local, nil
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
