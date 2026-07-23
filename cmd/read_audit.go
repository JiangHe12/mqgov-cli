package cmd

import (
	"errors"

	"github.com/JiangHe12/opskit-core/v2/apperrors"
	"github.com/JiangHe12/opskit-core/v2/audit"
	"github.com/JiangHe12/opskit-core/v2/safety"

	"github.com/JiangHe12/mqgov-cli/internal/mqgov"
	"github.com/JiangHe12/mqgov-cli/internal/mqgovctx"
)

type readAuditSpec struct {
	Action      string
	ContextName string
	Context     mqgovctx.Context
	Target      audit.EventTarget
	Metadata    mutationAuditMetadata
	AuditPath   string
}

type readAuditHandle struct {
	mutation            *mutationAuditHandle
	flags               *cliFlags
	diagnostics         *readDiagnosticBuffer
	previousDiagnostics *readDiagnosticBuffer
}

type mandatoryBrokerReadResult[T any] struct {
	value  T
	target operationTarget
}

type mandatoryBrokerPreflightResult[T any] struct {
	Backend mqgov.Broker
	Context mqgovctx.Context
	Target  operationTarget
	Value   T
}

func beginReadAudit(f *cliFlags, spec readAuditSpec) (*readAuditHandle, error) {
	metadata := spec.Metadata
	if metadata.Items == 0 {
		metadata.Items = 1
	}
	handle, err := beginMutationAudit(f, mutationAuditSpec{
		Action:      spec.Action,
		ContextName: spec.ContextName,
		Context:     spec.Context,
		Target:      spec.Target,
		Metadata:    metadata,
		AuditPath:   spec.AuditPath,
		Read:        true,
	})
	if err != nil {
		return nil, mandatoryReadAuditError("failed to persist mandatory read intent", err, nil)
	}
	diagnostics := &readDiagnosticBuffer{}
	previousDiagnostics := f.readDiagnostics
	f.readDiagnostics = diagnostics
	return &readAuditHandle{
		mutation:            handle,
		flags:               f,
		diagnostics:         diagnostics,
		previousDiagnostics: previousDiagnostics,
	}, nil
}

func finishReadAudit(handle *readAuditHandle, resultCount int, operationErr error) error {
	if err := persistReadAuditOutcome(handle, singleReadAuditOutcome(resultCount, operationErr), operationErr); err != nil {
		return err
	}
	return operationErr
}

func singleReadAuditOutcome(resultCount int, operationErr error) mutationAuditOutcome {
	outcome := mutationAuditOutcome{
		ResultCount: resultCount,
		counted:     true,
	}
	if operationErr == nil {
		outcome.Succeeded = 1
	} else {
		outcome.Failed = 1
	}
	return outcome
}

func finishReadAuditBatch(
	handle *readAuditHandle,
	succeeded int,
	failed int,
	resultCount int,
	operationErr error,
) error {
	if err := persistReadAuditBatch(handle, succeeded, failed, resultCount, operationErr); err != nil {
		return err
	}
	return operationErr
}

func persistReadAuditBatch(
	handle *readAuditHandle,
	succeeded int,
	failed int,
	resultCount int,
	operationErr error,
) error {
	status := audit.StatusSuccess
	if failed > 0 || operationErr != nil {
		status = audit.StatusFailed
		if succeeded > 0 {
			status = audit.StatusPartialFailed
		}
	}
	total := handle.mutation.spec.Metadata.Items
	skipped := total - succeeded - failed
	if skipped < 0 {
		skipped = 0
	}
	if err := persistReadAuditOutcome(handle, mutationAuditOutcome{
		Status:      status,
		Succeeded:   succeeded,
		Failed:      failed,
		Skipped:     skipped,
		ResultCount: resultCount,
		counted:     true,
	}, operationErr); err != nil {
		return err
	}
	return nil
}

func persistReadAuditOutcome(handle *readAuditHandle, outcome mutationAuditOutcome, operationErr error) error {
	if handle == nil || handle.mutation == nil {
		return apperrors.New(apperrors.CodeValidationFailed, "read audit handle is required", nil)
	}
	finishErr := finishMutationAudit(handle.mutation, outcome, operationErr)
	durable := finishErr == nil || operationErr != nil && errors.Is(finishErr, operationErr)
	handle.completeDiagnostics(durable)
	if finishErr == nil {
		return nil
	}
	if operationErr != nil && errors.Is(finishErr, operationErr) {
		return nil
	}
	return mandatoryReadAuditError("failed to persist mandatory read outcome", finishErr, operationErr)
}

func (handle *readAuditHandle) completeDiagnostics(durable bool) {
	if handle == nil || handle.diagnostics == nil {
		return
	}
	if handle.flags != nil && handle.flags.readDiagnostics == handle.diagnostics {
		handle.flags.readDiagnostics = handle.previousDiagnostics
	}
	var write func([]byte) (int, error)
	if handle.flags != nil {
		write = handle.flags.readDiagnosticWrite
	}
	handle.diagnostics.complete(durable, write)
	handle.diagnostics = nil
}

func mandatoryReadAuditError(message string, auditErr, operationErr error) error {
	cause := auditErr
	if operationErr != nil {
		cause = errors.Join(auditErr, operationErr)
	}
	return apperrors.New(apperrors.CodeLocalIOError, message, cause)
}

func runMandatoryRead[T any](
	f *cliFlags,
	spec readAuditSpec,
	operation func() (T, error),
	resultCount func(T) int,
) (T, error) {
	var zero T
	handle, err := beginReadAudit(f, spec)
	if err != nil {
		return zero, err
	}
	result, operationErr := operation()
	count := 0
	if resultCount != nil {
		count = resultCount(result)
	}
	if err := finishReadAudit(handle, count, operationErr); err != nil {
		return zero, err
	}
	return result, nil
}

func runMandatoryBrokerRead[T any](
	f *cliFlags,
	spec readAuditSpec,
	preBuildAuthorize func(mqgovctx.Context) error,
	operation func(mqgov.Broker, mqgovctx.Context) (T, error),
	resultCount func(T) int,
) (T, operationTarget, error) {
	var zero T
	meta, contextName, err := resolvedContext(f)
	if err != nil {
		return zero, operationTarget{}, err
	}
	spec.Context = meta
	if spec.ContextName == "" {
		spec.ContextName = contextName
	}
	result, err := runMandatoryRead(f, spec, func() (mandatoryBrokerReadResult[T], error) {
		if preBuildAuthorize != nil {
			if authorizeErr := preBuildAuthorize(meta); authorizeErr != nil {
				return mandatoryBrokerReadResult[T]{}, authorizeErr
			}
		}
		backend, buildErr := buildBrokerForResolvedContext(f, meta, contextName)
		if buildErr != nil {
			return mandatoryBrokerReadResult[T]{}, buildErr
		}
		defer backend.Close()
		value, operationErr := operation(backend, meta)
		return mandatoryBrokerReadResult[T]{
			value:  value,
			target: operationTargetFromDescription(contextName, backend.Describe()),
		}, operationErr
	}, func(result mandatoryBrokerReadResult[T]) int {
		if resultCount == nil {
			return 0
		}
		return resultCount(result.value)
	})
	if err != nil {
		return zero, operationTarget{}, err
	}
	return result.value, result.target, nil
}

func runMandatoryBrokerList[T any](
	f *cliFlags,
	spec readAuditSpec,
	preBuildAuthorize func(mqgovctx.Context) error,
	operation func(mqgov.Broker, mqgovctx.Context) ([]T, error),
) ([]T, operationTarget, error) {
	return runMandatoryBrokerRead(f, spec, preBuildAuthorize, operation, func(items []T) int {
		return len(items)
	})
}

func runMandatorySchemaRead[T any](
	f *cliFlags,
	spec readAuditSpec,
	preBuildAuthorize func(mqgovctx.Context) error,
	operation func(mqgov.SchemaManager) (T, error),
) (T, operationTarget, error) {
	return runMandatoryBrokerRead(f, spec, preBuildAuthorize, func(backend mqgov.Broker, _ mqgovctx.Context) (T, error) {
		var zero T
		manager, err := schemaManager(backend)
		if err != nil {
			return zero, err
		}
		return operation(manager)
	}, func(T) int {
		return 1
	})
}

func runMandatoryBrokerPreflight[T any](
	f *cliFlags,
	spec readAuditSpec,
	operation func(mqgov.Broker, mqgovctx.Context) (T, error),
	resultCount func(T) int,
) (mandatoryBrokerPreflightResult[T], error) {
	var zero mandatoryBrokerPreflightResult[T]
	meta, contextName, err := resolvedContext(f)
	if err != nil {
		return zero, err
	}
	spec.Context = meta
	if spec.ContextName == "" {
		spec.ContextName = contextName
	}
	var opened mqgov.Broker
	result, err := runMandatoryRead(f, spec, func() (mandatoryBrokerPreflightResult[T], error) {
		if authorizeErr := authorize(f, safety.R0, meta, ""); authorizeErr != nil {
			return zero, authorizeErr
		}
		backend, buildErr := buildBrokerForResolvedContext(f, meta, contextName)
		if buildErr != nil {
			return zero, buildErr
		}
		opened = backend
		value, operationErr := operation(backend, meta)
		return mandatoryBrokerPreflightResult[T]{
			Backend: backend,
			Context: meta,
			Target:  operationTargetFromDescription(contextName, backend.Describe()),
			Value:   value,
		}, operationErr
	}, func(result mandatoryBrokerPreflightResult[T]) int {
		if resultCount == nil {
			return 0
		}
		return resultCount(result.Value)
	})
	if err != nil {
		if opened != nil {
			opened.Close()
		}
		return zero, err
	}
	return result, nil
}
