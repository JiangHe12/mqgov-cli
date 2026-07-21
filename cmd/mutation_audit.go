package cmd

import (
	"bytes"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/JiangHe12/opskit-core/v2/apperrors"
	"github.com/JiangHe12/opskit-core/v2/audit"
	"github.com/JiangHe12/opskit-core/v2/lockfile"

	"github.com/JiangHe12/mqgov-cli/internal/mqgovctx"
)

const (
	codeAuditIncomplete              apperrors.ErrorCode = "AUDIT_INCOMPLETE"
	mutationAuditAPIVersion                              = "mqgov-cli.io/mutation-audit/v1"
	mutationAuditKind                                    = "MutationAuditRecord"
	safeAuditKind                                        = "AuditRecord"
	mutationAuditPhaseIntent                             = "intent"
	mutationAuditPhaseOutcome                            = "outcome"
	mutationAuditSpoolSuffix                             = ".outcome-spool"
	mutationAuditSpoolLockBase                           = "queue"
	mutationAuditSpoolSequenceFile                       = "sequence"
	mutationAuditIndeterminateSuffix                     = ".indeterminate"
	maxMutationSpoolRecordSize                           = 1024 * 1024
)

type mutationAuditMetadata struct {
	PayloadFingerprint string `json:"payloadFingerprint,omitempty"`
	PayloadBytes       int    `json:"payloadBytes,omitempty"`
	KeyFingerprint     string `json:"keyFingerprint,omitempty"`
	KeyBytes           int    `json:"keyBytes,omitempty"`
	BodyFingerprint    string `json:"bodyFingerprint,omitempty"`
	BodyBytes          int    `json:"bodyBytes,omitempty"`
	Revision           string `json:"revision,omitempty"`
	Items              int    `json:"items,omitempty"`
	Creates            int    `json:"creates,omitempty"`
	Updates            int    `json:"updates,omitempty"`
	Deletes            int    `json:"deletes,omitempty"`
}

type mutationAuditOutcome struct {
	Status             string `json:"status"`
	ErrorCode          string `json:"errorCode,omitempty"`
	ErrorFingerprint   string `json:"errorFingerprint,omitempty"`
	ErrorBytes         int    `json:"errorBytes,omitempty"`
	Succeeded          int    `json:"succeeded,omitempty"`
	Failed             int    `json:"failed,omitempty"`
	Skipped            int    `json:"skipped,omitempty"`
	Uncertain          int    `json:"uncertain,omitempty"`
	Revision           string `json:"revision,omitempty"`
	CompensationStatus string `json:"compensationStatus,omitempty"`
	counted            bool
}

type mutationAuditSpec struct {
	Action      string
	ContextName string
	Context     mqgovctx.Context
	Target      audit.EventTarget
	Metadata    mutationAuditMetadata
	AuditPath   string
}

// safeAuditRecord is the only audit shape emitted or returned by mqgov. The
// embedded legacy fields Ticket, Reason, Diff, and Error must always be empty.
type safeAuditRecord struct {
	APIVersion string `json:"apiVersion"`
	Kind       string `json:"kind"`
	audit.Event
	MutationID        string                `json:"mutationId,omitempty"`
	Phase             string                `json:"phase,omitempty"`
	Action            string                `json:"action,omitempty"`
	TicketFingerprint string                `json:"ticketFingerprint,omitempty"`
	TicketBytes       int                   `json:"ticketBytes,omitempty"`
	ReasonFingerprint string                `json:"reasonFingerprint,omitempty"`
	ReasonBytes       int                   `json:"reasonBytes,omitempty"`
	DetailFingerprint string                `json:"detailFingerprint,omitempty"`
	DetailBytes       int                   `json:"detailBytes,omitempty"`
	ErrorCode         string                `json:"errorCode,omitempty"`
	ErrorFingerprint  string                `json:"errorFingerprint,omitempty"`
	ErrorBytes        int                   `json:"errorBytes,omitempty"`
	Metadata          mutationAuditMetadata `json:"metadata,omitempty"`
	Outcome           *mutationAuditOutcome `json:"outcome,omitempty"`
}

type mutationAuditHandle struct {
	f    *cliFlags
	id   string
	path string
	spec mutationAuditSpec
}

type mutationAuditRuntime struct {
	appendRecordWithResult func(string, safeAuditRecord, audit.Options) (audit.AppendResult, error)
	// appendRecord is retained as a narrow test seam for callers that only need
	// to model pre-commit success or failure. Production always uses the core
	// commit-state-aware API above.
	appendRecord func(string, safeAuditRecord, audit.Options) error
	now          func() time.Time
	random       io.Reader
}

type mutationAuditAppendError struct {
	state audit.AppendCommitState
	cause error
}

func (err *mutationAuditAppendError) Error() string {
	return "mutation audit append failed with state " + string(err.state) + ": " + err.cause.Error()
}

func (err *mutationAuditAppendError) Unwrap() error {
	return err.cause
}

var productionMutationAuditRuntime = mutationAuditRuntime{
	appendRecordWithResult: func(path string, record safeAuditRecord, options audit.Options) (audit.AppendResult, error) {
		return audit.AppendRecordWithResult(path, record, options)
	},
	now:    func() time.Time { return time.Now().UTC() },
	random: rand.Reader,
}

var (
	// The core locks provide cross-process exclusion. These process-local locks
	// prevent concurrent workers from contending on those bounded-retry file
	// locks and preserve deterministic queue replay order.
	mutationAuditAppendMu sync.Mutex
	mutationAuditSpoolMu  sync.Mutex
)

func beginMutationAudit(f *cliFlags, spec mutationAuditSpec) (*mutationAuditHandle, error) {
	if strings.TrimSpace(spec.Action) == "" {
		return nil, apperrors.New(apperrors.CodeValidationFailed, "mutation audit action is required", nil)
	}
	path := strings.TrimSpace(spec.AuditPath)
	if path == "" {
		var err error
		path, err = audit.DefaultPath()
		if err != nil {
			return nil, apperrors.New(apperrors.CodeLocalIOError, "failed to resolve mutation audit path", nil)
		}
	}
	var handle *mutationAuditHandle
	err := withMutationAuditQueue(path, func(spoolPath string) error {
		if err := replayMutationAuditSpoolLocked(f, path, spoolPath); err != nil {
			return auditIncompleteError("", false)
		}
		id, err := newMutationID(mutationAuditRuntimeFor(f).random)
		if err != nil {
			return err
		}
		handle = &mutationAuditHandle{f: f, id: id, path: path, spec: spec}
		if err := appendMutationAuditRecord(f, path, handle.record(mutationAuditPhaseIntent, nil)); err != nil {
			if mutationAuditAppendMayExist(err) {
				skipped := 1
				if spec.Metadata.Items > 0 {
					skipped = spec.Metadata.Items
				}
				outcome := mutationAuditOutcome{
					Status:    audit.StatusFailed,
					ErrorCode: string(codeAuditIncomplete),
					Skipped:   skipped,
					counted:   true,
				}
				record := handle.record(mutationAuditPhaseOutcome, &outcome)
				spoolErr := writeMutationSpoolRecord(f, spoolPath, record)
				mutationID := handle.id
				handle = nil
				return auditIncompleteError(mutationID, spoolErr != nil)
			}
			handle = nil
			return apperrors.New(apperrors.CodeLocalIOError, "failed to persist mutation intent", err)
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return handle, nil
}

func finishMutationAudit(handle *mutationAuditHandle, outcome mutationAuditOutcome, operationErr error) error {
	if handle == nil {
		return apperrors.New(apperrors.CodeValidationFailed, "mutation audit handle is required", nil)
	}
	outcome = normalizeMutationAuditOutcome(outcome, operationErr)
	queueEntered := false
	err := withMutationAuditQueue(handle.path, func(spoolPath string) error {
		queueEntered = true
		if err := replayMutationAuditSpoolLocked(handle.f, handle.path, spoolPath); err != nil {
			record := handle.record(mutationAuditPhaseOutcome, &outcome)
			spoolErr := writeMutationSpoolRecord(handle.f, spoolPath, record)
			return auditIncompleteError(handle.id, spoolErr != nil)
		}
		record := handle.record(mutationAuditPhaseOutcome, &outcome)
		if err := appendMutationAuditRecord(handle.f, handle.path, record); err != nil {
			if mutationAuditAppendMayExist(err) {
				return auditIncompleteError(handle.id, false)
			}
			spoolErr := writeMutationSpoolRecord(handle.f, spoolPath, record)
			return auditIncompleteError(handle.id, spoolErr != nil)
		}
		return nil
	})
	if err != nil {
		if !queueEntered {
			spoolErr := spoolMutationAuditOutcomeEmergency(handle.f, handle.path, func() safeAuditRecord {
				return handle.record(mutationAuditPhaseOutcome, &outcome)
			})
			return auditIncompleteError(handle.id, spoolErr != nil)
		}
		return err
	}
	return operationErr
}

func normalizeMutationAuditOutcome(outcome mutationAuditOutcome, operationErr error) mutationAuditOutcome {
	if outcome.Status == "" {
		outcome.Status = mutationAuditStatus(operationErr)
	}
	if operationErr != nil {
		outcome = mutationAuditErrorOutcome(outcome, operationErr)
	}
	if !outcome.counted && mutationAuditCountsEmpty(outcome) {
		if operationErr == nil {
			outcome.Succeeded = 1
		} else {
			outcome.Failed = 1
		}
	}
	return outcome
}

func mutationAuditStatus(operationErr error) string {
	if operationErr != nil {
		return audit.StatusFailed
	}
	return audit.StatusSuccess
}

func mutationAuditErrorOutcome(outcome mutationAuditOutcome, operationErr error) mutationAuditOutcome {
	appErr := apperrors.AsAppError(operationErr)
	if outcome.ErrorCode == "" {
		outcome.ErrorCode = string(appErr.Code)
	}
	if outcome.ErrorFingerprint == "" {
		outcome.ErrorFingerprint, outcome.ErrorBytes = sensitiveAuditFingerprint("error", appErr.Error())
	}
	return outcome
}

func mutationAuditCountsEmpty(outcome mutationAuditOutcome) bool {
	return outcome.Succeeded == 0 &&
		outcome.Failed == 0 &&
		outcome.Skipped == 0 &&
		outcome.Uncertain == 0
}

func finishBatchMutationAudit(
	handle *mutationAuditHandle,
	total int,
	succeeded int,
	failed int,
	operationErr error,
) error {
	skipped := total - succeeded - failed
	if skipped < 0 {
		skipped = 0
	}
	status := audit.StatusSuccess
	if failed > 0 || operationErr != nil {
		status = audit.StatusFailed
		if succeeded > 0 {
			status = audit.StatusPartialFailed
		}
	}
	return finishMutationAudit(handle, mutationAuditOutcome{
		Status:    status,
		Succeeded: succeeded,
		Failed:    failed,
		Skipped:   skipped,
		counted:   true,
	}, operationErr)
}

func (handle *mutationAuditHandle) record(phase string, outcome *mutationAuditOutcome) safeAuditRecord {
	contextName := handle.spec.ContextName
	if contextName == "" {
		contextName = handle.f.contextName()
	}
	ticketFingerprint, ticketBytes := sensitiveAuditFingerprint("ticket", handle.f.Ticket)
	reasonFingerprint, reasonBytes := sensitiveAuditFingerprint("reason", handle.f.Reason)
	status := audit.StatusPending
	if outcome != nil {
		status = outcome.Status
	}
	return safeAuditRecord{
		APIVersion: mutationAuditAPIVersion,
		Kind:       mutationAuditKind,
		Event: audit.Event{
			Timestamp: mutationAuditRuntimeFor(handle.f).now().UTC(),
			EventType: audit.EventType(handle.spec.Action + "." + phase),
			Operator:  currentOperator(handle.f),
			Context: audit.EventContext{
				Name:      contextName,
				Env:       handle.spec.Context.Env,
				Protected: handle.spec.Context.Protected,
			},
			Target: handle.spec.Target,
			Status: status,
		},
		MutationID:        handle.id,
		Phase:             phase,
		Action:            handle.spec.Action,
		TicketFingerprint: ticketFingerprint,
		TicketBytes:       ticketBytes,
		ReasonFingerprint: reasonFingerprint,
		ReasonBytes:       reasonBytes,
		Metadata:          handle.spec.Metadata,
		Outcome:           outcome,
	}
}

func newSafeAuditRecord(
	f *cliFlags,
	typ audit.EventType,
	ctx mqgovctx.Context,
	contextName string,
	target audit.EventTarget,
	status string,
	detail string,
	operationErr error,
) safeAuditRecord {
	if contextName == "" {
		contextName = f.contextName()
	}
	ticketFingerprint, ticketBytes := sensitiveAuditFingerprint("ticket", f.Ticket)
	reasonFingerprint, reasonBytes := sensitiveAuditFingerprint("reason", f.Reason)
	detailFingerprint, detailBytes := sensitiveAuditFingerprint("detail", detail)
	record := safeAuditRecord{
		APIVersion: mutationAuditAPIVersion,
		Kind:       safeAuditKind,
		Event: audit.Event{
			Timestamp: mutationAuditRuntimeFor(f).now().UTC(),
			EventType: typ,
			Operator:  currentOperator(f),
			Context: audit.EventContext{
				Name:      contextName,
				Env:       ctx.Env,
				Protected: ctx.Protected,
			},
			Target: target,
			Status: status,
		},
		TicketFingerprint: ticketFingerprint,
		TicketBytes:       ticketBytes,
		ReasonFingerprint: reasonFingerprint,
		ReasonBytes:       reasonBytes,
		DetailFingerprint: detailFingerprint,
		DetailBytes:       detailBytes,
	}
	if operationErr != nil {
		appErr := apperrors.AsAppError(operationErr)
		record.ErrorCode = string(appErr.Code)
		record.ErrorFingerprint, record.ErrorBytes = sensitiveAuditFingerprint("error", appErr.Error())
	}
	return record
}

func mutationPayloadMetadata(action string, payload []byte) mutationAuditMetadata {
	return mutationAuditMetadata{
		PayloadFingerprint: mutationAuditFingerprint("payload:"+action, payload),
		PayloadBytes:       len(payload),
	}
}

func mutationValueMetadata(action string, value any) mutationAuditMetadata {
	payload, err := json.Marshal(value)
	if err != nil {
		return mutationAuditMetadata{}
	}
	return mutationPayloadMetadata(action, payload)
}

func mutationMessageMetadata(key, body []byte) mutationAuditMetadata {
	return mutationAuditMetadata{
		KeyFingerprint:  mutationAuditFingerprint("message-key", key),
		KeyBytes:        len(key),
		BodyFingerprint: mutationAuditFingerprint("message-body", body),
		BodyBytes:       len(body),
		Items:           1,
	}
}

func sensitiveAuditFingerprint(domain, value string) (string, int) {
	if value == "" {
		return "", 0
	}
	return mutationAuditFingerprint(domain, []byte(value)), len([]byte(value))
}

func mutationAuditFingerprint(domain string, value []byte) string {
	hash := sha256.New()
	_, _ = io.WriteString(hash, mutationAuditAPIVersion)
	_, _ = hash.Write([]byte{0})
	_, _ = io.WriteString(hash, domain)
	_, _ = hash.Write([]byte{0})
	_, _ = hash.Write(value)
	return "sha256:" + hex.EncodeToString(hash.Sum(nil))
}

func newMutationID(random io.Reader) (string, error) {
	value := make([]byte, 16)
	if _, err := io.ReadFull(random, value); err != nil {
		return "", apperrors.New(apperrors.CodeLocalIOError, "failed to generate mutation id", nil)
	}
	return hex.EncodeToString(value), nil
}

func mutationAuditRuntimeFor(f *cliFlags) *mutationAuditRuntime {
	if f != nil && f.mutationAudit != nil {
		return f.mutationAudit
	}
	return &productionMutationAuditRuntime
}

func appendMutationAuditRecord(f *cliFlags, path string, record safeAuditRecord) error {
	mutationAuditAppendMu.Lock()
	defer mutationAuditAppendMu.Unlock()

	if err := validateSafeAuditRecord(record); err != nil {
		return err
	}
	if err := verifyMutationAuditActivePath(path, true); err != nil {
		return err
	}
	runtime := mutationAuditRuntimeFor(f)
	result := audit.AppendResult{State: audit.AppendCommitNotCommitted}
	var err error
	switch {
	case runtime.appendRecordWithResult != nil:
		result, err = runtime.appendRecordWithResult(path, record, auditOptions(f))
	case runtime.appendRecord != nil:
		err = runtime.appendRecord(path, record, auditOptions(f))
		if err == nil {
			result.State = audit.AppendCommitCommitted
		}
	default:
		return apperrors.New(apperrors.CodeLocalIOError, "mutation audit append runtime is not configured", nil)
	}
	if err != nil {
		return &mutationAuditAppendError{state: result.State, cause: err}
	}
	return nil
}

func mutationAuditAppendMayExist(err error) bool {
	state, ok := mutationAuditCommitState(err)
	return ok && state != audit.AppendCommitNotCommitted
}

func mutationAuditAppendCommitted(err error) bool {
	state, ok := mutationAuditCommitState(err)
	return ok && (state == audit.AppendCommitCommitted || state == audit.AppendCommitCommittedPostCommitError)
}

func mutationAuditCommitState(err error) (audit.AppendCommitState, bool) {
	var appendErr *mutationAuditAppendError
	if !errors.As(err, &appendErr) {
		return "", false
	}
	return appendErr.state, true
}

func appendQueuedAuditRecord(f *cliFlags, path string, record safeAuditRecord) error {
	return withMutationAuditQueue(path, func(spoolPath string) error {
		if err := replayMutationAuditSpoolLocked(f, path, spoolPath); err != nil {
			return auditIncompleteError("", false)
		}
		record.Timestamp = mutationAuditRuntimeFor(f).now().UTC()
		return appendMutationAuditRecord(f, path, record)
	})
}

func validateSafeAuditRecord(record safeAuditRecord) error {
	if record.APIVersion != mutationAuditAPIVersion ||
		(record.Kind != mutationAuditKind && record.Kind != safeAuditKind) ||
		record.Ticket != "" ||
		record.Reason != "" ||
		record.Diff != "" ||
		record.Error != nil {
		return apperrors.New(apperrors.CodeValidationFailed, "unsafe audit record rejected", nil)
	}
	return nil
}

func mutationAuditSpoolPath(auditPath string) string {
	return auditPath + mutationAuditSpoolSuffix
}

func withMutationAuditQueue(
	auditPath string,
	action func(spoolPath string) error,
) (retErr error) {
	mutationAuditSpoolMu.Lock()
	defer mutationAuditSpoolMu.Unlock()

	auditDirectory := filepath.Dir(auditPath)
	if err := ensureMutationAuditDirectory(auditDirectory); err != nil {
		return err
	}
	spoolPath := mutationAuditSpoolPath(auditPath)
	if err := ensureMutationSpoolDirectory(spoolPath); err != nil {
		return err
	}
	lockBase := filepath.Join(spoolPath, mutationAuditSpoolLockBase)
	lock := lockfile.New(lockBase)
	if err := lock.Acquire(); err != nil {
		return err
	}
	if err := secureMutationSpoolFile(lockBase + ".lock"); err != nil {
		_ = lock.Release()
		return err
	}
	defer func() {
		if err := releaseMutationAuditLock(lock); err != nil && retErr == nil {
			retErr = apperrors.New(apperrors.CodeLocalIOError, "failed to release mutation audit queue lock", nil)
		}
	}()
	if err := verifyMutationSpoolDirectory(spoolPath); err != nil {
		return err
	}

	return action(spoolPath)
}

func ensureMutationAuditDirectory(path string) error {
	info, err := os.Lstat(path)
	if err == nil {
		if info.Mode()&os.ModeSymlink != 0 || !info.IsDir() {
			return apperrors.New(apperrors.CodeLocalIOError, "mutation audit directory must be a real directory", nil)
		}
		return nil
	}
	if !errors.Is(err, os.ErrNotExist) {
		return apperrors.New(apperrors.CodeLocalIOError, "failed to inspect mutation audit directory", nil)
	}
	parent := filepath.Dir(path)
	if parent == path {
		return apperrors.New(apperrors.CodeLocalIOError, "mutation audit directory has no existing ancestor", nil)
	}
	if err := ensureMutationAuditDirectory(parent); err != nil {
		return err
	}
	return createPrivateMutationAuditDirectory(path)
}

func spoolMutationAuditOutcomeEmergency(
	f *cliFlags,
	auditPath string,
	buildRecord func() safeAuditRecord,
) (retErr error) {
	mutationAuditSpoolMu.Lock()
	defer mutationAuditSpoolMu.Unlock()

	if err := ensureMutationAuditDirectory(filepath.Dir(auditPath)); err != nil {
		return err
	}
	spoolPath := mutationAuditSpoolPath(auditPath)
	if err := ensureMutationSpoolDirectory(spoolPath); err != nil {
		return err
	}
	lockBase := filepath.Join(spoolPath, mutationAuditSpoolLockBase)
	lock := lockfile.New(lockBase)
	if err := lock.Acquire(); err != nil {
		return err
	}
	if err := secureMutationSpoolFile(lockBase + ".lock"); err != nil {
		_ = lock.Release()
		return err
	}
	defer func() {
		if err := releaseMutationAuditLock(lock); err != nil && retErr == nil {
			retErr = apperrors.New(apperrors.CodeLocalIOError, "failed to release emergency mutation audit queue lock", nil)
		}
	}()
	if err := verifyMutationSpoolDirectory(spoolPath); err != nil {
		return err
	}
	return writeMutationSpoolRecord(f, spoolPath, buildRecord())
}

func releaseMutationAuditLock(lock *lockfile.Lock) error {
	var err error
	for attempt := 0; attempt < 20; attempt++ {
		err = lock.Release()
		if err == nil {
			return nil
		}
		time.Sleep(10 * time.Millisecond)
	}
	return err
}

func writeMutationSpoolRecord(_ *cliFlags, spoolPath string, record safeAuditRecord) error {
	data, err := json.Marshal(record)
	if err != nil {
		return apperrors.New(apperrors.CodeLocalIOError, "failed to encode mutation outcome spool", nil)
	}
	sequence, err := nextMutationSpoolSequence(spoolPath)
	if err != nil {
		return err
	}
	name := fmt.Sprintf("%020d-%s.json", sequence, record.MutationID)
	finalPath := filepath.Join(spoolPath, name)
	tempPath := finalPath + ".tmp"
	file, err := os.OpenFile(tempPath, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600) //nolint:gosec // Path is inside the validated owner-only spool.
	if err != nil {
		return apperrors.New(apperrors.CodeLocalIOError, "failed to create mutation outcome spool", nil)
	}
	complete := false
	defer func() {
		_ = file.Close()
		if !complete {
			_ = os.Remove(tempPath)
		}
	}()
	if err := secureMutationSpoolFile(tempPath); err != nil {
		return err
	}
	if _, err := file.Write(append(data, '\n')); err != nil {
		return apperrors.New(apperrors.CodeLocalIOError, "failed to write mutation outcome spool", nil)
	}
	if err := file.Sync(); err != nil {
		return apperrors.New(apperrors.CodeLocalIOError, "failed to sync mutation outcome spool", nil)
	}
	if err := file.Close(); err != nil {
		return apperrors.New(apperrors.CodeLocalIOError, "failed to close mutation outcome spool", nil)
	}
	if err := commitMutationSpoolFile(tempPath, finalPath); err != nil {
		return apperrors.New(apperrors.CodeLocalIOError, "failed to commit mutation outcome spool", nil)
	}
	complete = true
	return nil
}

func nextMutationSpoolSequence(spoolPath string) (uint64, error) {
	entries, err := os.ReadDir(spoolPath)
	if err != nil {
		return 0, apperrors.New(apperrors.CodeLocalIOError, "failed to list mutation outcome spool for sequencing", nil)
	}
	var maxRecord uint64
	for _, entry := range entries {
		name := entry.Name()
		if name == mutationAuditSpoolLockBase+".lock" || name == mutationAuditSpoolSequenceFile {
			continue
		}
		pendingName := strings.TrimSuffix(name, mutationAuditIndeterminateSuffix)
		if entry.IsDir() || !validMutationSpoolName(pendingName) {
			return 0, apperrors.New(apperrors.CodeLocalIOError, "mutation outcome spool contains an unexpected entry", nil)
		}
		sequence, parseErr := mutationSpoolNameSequence(pendingName)
		if parseErr != nil {
			return 0, parseErr
		}
		if sequence > maxRecord {
			maxRecord = sequence
		}
	}

	sequencePath := filepath.Join(spoolPath, mutationAuditSpoolSequenceFile)
	current, exists, err := readMutationSpoolSequence(sequencePath)
	if err != nil {
		return 0, err
	}
	if !exists {
		current = maxRecord
	} else if current < maxRecord {
		return 0, apperrors.New(apperrors.CodeLocalIOError, "mutation outcome spool sequence moved backwards", nil)
	}
	if current == ^uint64(0) {
		return 0, apperrors.New(apperrors.CodeLocalIOError, "mutation outcome spool sequence is exhausted", nil)
	}
	next := current + 1
	if err := writeMutationSpoolSequence(sequencePath, next); err != nil {
		return 0, err
	}
	return next, nil
}

func mutationSpoolNameSequence(name string) (uint64, error) {
	separator := strings.IndexByte(name, '-')
	if separator != 20 {
		return 0, apperrors.New(apperrors.CodeLocalIOError, "invalid mutation outcome spool sequence", nil)
	}
	value, err := strconv.ParseUint(name[:separator], 10, 64)
	if err != nil {
		return 0, apperrors.New(apperrors.CodeLocalIOError, "invalid mutation outcome spool sequence", nil)
	}
	return value, nil
}

func readMutationSpoolSequence(path string) (uint64, bool, error) {
	if _, err := os.Lstat(path); err != nil {
		if os.IsNotExist(err) {
			return 0, false, nil
		}
		return 0, false, apperrors.New(apperrors.CodeLocalIOError, "failed to inspect mutation outcome spool sequence", nil)
	}
	if err := verifyMutationSpoolFile(path); err != nil {
		return 0, false, err
	}
	data, err := os.ReadFile(path) //nolint:gosec // Path is the fixed sequence file inside the validated owner-only spool.
	if err != nil || len(data) > 32 {
		return 0, false, apperrors.New(apperrors.CodeLocalIOError, "failed to read mutation outcome spool sequence", nil)
	}
	value, err := strconv.ParseUint(strings.TrimSpace(string(data)), 10, 64)
	if err != nil {
		return 0, false, apperrors.New(apperrors.CodeLocalIOError, "invalid mutation outcome spool sequence", nil)
	}
	return value, true, nil
}

func writeMutationSpoolSequence(path string, sequence uint64) error {
	tempPath := path + ".tmp"
	file, err := os.OpenFile(tempPath, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600) //nolint:gosec // Path is inside the validated owner-only spool.
	if err != nil {
		return apperrors.New(apperrors.CodeLocalIOError, "failed to create mutation outcome spool sequence", nil)
	}
	complete := false
	defer func() {
		_ = file.Close()
		if !complete {
			_ = os.Remove(tempPath)
		}
	}()
	if err := secureMutationSpoolFile(tempPath); err != nil {
		return err
	}
	if _, err := fmt.Fprintf(file, "%d\n", sequence); err != nil {
		return apperrors.New(apperrors.CodeLocalIOError, "failed to write mutation outcome spool sequence", nil)
	}
	if err := file.Sync(); err != nil {
		return apperrors.New(apperrors.CodeLocalIOError, "failed to sync mutation outcome spool sequence", nil)
	}
	if err := file.Close(); err != nil {
		return apperrors.New(apperrors.CodeLocalIOError, "failed to close mutation outcome spool sequence", nil)
	}
	if err := replaceContextExportFile(tempPath, path); err != nil {
		return apperrors.New(apperrors.CodeLocalIOError, "failed to commit mutation outcome spool sequence", nil)
	}
	complete = true
	return verifyMutationSpoolFile(path)
}

func replayMutationAuditSpoolLocked(f *cliFlags, auditPath, spoolPath string) error { //nolint:gocyclo // Strict queue validation and ordered replay intentionally fail closed in one lock scope.
	if _, err := os.Lstat(spoolPath); err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil
		}
		return apperrors.New(apperrors.CodeLocalIOError, "failed to inspect mutation outcome spool", nil)
	}
	if err := verifyMutationSpoolDirectory(spoolPath); err != nil {
		return err
	}
	entries, err := os.ReadDir(spoolPath)
	if err != nil {
		return apperrors.New(apperrors.CodeLocalIOError, "failed to list mutation outcome spool", nil)
	}
	names := make([]string, 0, len(entries))
	for _, entry := range entries {
		name := entry.Name()
		if name == mutationAuditSpoolLockBase+".lock" || name == mutationAuditSpoolSequenceFile {
			continue
		}
		if strings.HasSuffix(name, mutationAuditIndeterminateSuffix) {
			pendingName := strings.TrimSuffix(name, mutationAuditIndeterminateSuffix)
			if entry.IsDir() || !validMutationSpoolName(pendingName) {
				return apperrors.New(apperrors.CodeLocalIOError, "mutation outcome spool contains an unexpected entry", nil)
			}
			return indeterminateMutationSpoolError(mutationIDFromSpoolName(pendingName))
		}
		if entry.IsDir() || !validMutationSpoolName(name) {
			return apperrors.New(apperrors.CodeLocalIOError, "mutation outcome spool contains an unexpected entry", nil)
		}
		names = append(names, name)
	}
	sort.Strings(names)
	if err := validateMutationSpoolSequence(spoolPath, names); err != nil {
		return err
	}
	for _, name := range names {
		path := filepath.Join(spoolPath, name)
		record, err := readMutationSpoolRecord(path)
		if err != nil {
			return err
		}
		if err := appendMutationAuditRecord(f, auditPath, record); err != nil {
			return handleMutationSpoolReplayAppendError(path, spoolPath, record, err)
		}
		if err := removeReplayedMutationSpool(path, spoolPath); err != nil {
			return err
		}
	}
	return nil
}

func handleMutationSpoolReplayAppendError(
	path string,
	spoolPath string,
	record safeAuditRecord,
	appendErr error,
) error {
	state, hasState := mutationAuditCommitState(appendErr)
	if hasState && state == audit.AppendCommitIndeterminate {
		if markErr := markMutationSpoolIndeterminate(path); markErr != nil {
			return auditIncompleteError(record.MutationID, true)
		}
		return indeterminateMutationSpoolError(record.MutationID)
	}
	if !mutationAuditAppendCommitted(appendErr) {
		return appendErr
	}
	if cleanupErr := removeReplayedMutationSpool(path, spoolPath); cleanupErr != nil {
		return cleanupErr
	}
	return appendErr
}

func removeReplayedMutationSpool(path, spoolPath string) error {
	if err := os.Remove(path); err != nil {
		return apperrors.New(apperrors.CodeLocalIOError, "failed to remove replayed mutation outcome spool", nil)
	}
	return syncMutationSpoolDirectory(spoolPath)
}

func markMutationSpoolIndeterminate(path string) error {
	markedPath := path + mutationAuditIndeterminateSuffix
	if err := commitMutationSpoolFile(path, markedPath); err != nil {
		return apperrors.New(apperrors.CodeLocalIOError, "failed to quarantine indeterminate mutation outcome spool", nil)
	}
	return verifyMutationSpoolFile(markedPath)
}

func validateMutationSpoolSequence(spoolPath string, names []string) error {
	var maxRecord uint64
	seen := make(map[uint64]struct{}, len(names))
	for _, name := range names {
		sequence, err := mutationSpoolNameSequence(name)
		if err != nil || sequence == 0 {
			return apperrors.New(apperrors.CodeLocalIOError, "invalid mutation outcome spool sequence", nil)
		}
		if _, duplicate := seen[sequence]; duplicate {
			return apperrors.New(apperrors.CodeLocalIOError, "duplicate mutation outcome spool sequence", nil)
		}
		seen[sequence] = struct{}{}
		if sequence > maxRecord {
			maxRecord = sequence
		}
	}
	current, exists, err := readMutationSpoolSequence(filepath.Join(spoolPath, mutationAuditSpoolSequenceFile))
	if err != nil {
		return err
	}
	if !exists {
		if len(names) == 0 {
			return nil
		}
		return apperrors.New(apperrors.CodeLocalIOError, "mutation outcome spool sequence is missing", nil)
	}
	if current < maxRecord {
		return apperrors.New(apperrors.CodeLocalIOError, "mutation outcome spool sequence moved backwards", nil)
	}
	return nil
}

func validMutationSpoolName(name string) bool {
	if !strings.HasSuffix(name, ".json") {
		return false
	}
	parts := strings.Split(strings.TrimSuffix(name, ".json"), "-")
	if len(parts) != 2 || len(parts[0]) != 20 || len(parts[1]) != 32 {
		return false
	}
	for _, digit := range parts[0] {
		if digit < '0' || digit > '9' {
			return false
		}
	}
	if parts[1] != strings.ToLower(parts[1]) {
		return false
	}
	_, err := hex.DecodeString(parts[1])
	return err == nil
}

func mutationIDFromSpoolName(name string) string {
	if !validMutationSpoolName(name) {
		return ""
	}
	stem := strings.TrimSuffix(name, ".json")
	return stem[strings.LastIndexByte(stem, '-')+1:]
}

func readMutationSpoolRecord(path string) (safeAuditRecord, error) {
	var record safeAuditRecord
	if err := verifyMutationSpoolFile(path); err != nil {
		return record, err
	}
	before, err := os.Lstat(path)
	if err != nil {
		return record, apperrors.New(apperrors.CodeLocalIOError, "failed to inspect mutation outcome spool file", nil)
	}
	file, err := os.Open(path) //nolint:gosec // Path is strictly named inside a validated owner-only spool.
	if err != nil {
		return record, apperrors.New(apperrors.CodeLocalIOError, "failed to open mutation outcome spool file", nil)
	}
	defer func() { _ = file.Close() }()
	opened, err := file.Stat()
	if err != nil || !opened.Mode().IsRegular() || !os.SameFile(before, opened) {
		return record, apperrors.New(apperrors.CodeLocalIOError, "mutation outcome spool file changed while opening", nil)
	}
	data, err := io.ReadAll(io.LimitReader(file, maxMutationSpoolRecordSize+1))
	if err != nil || len(data) > maxMutationSpoolRecordSize {
		return record, apperrors.New(apperrors.CodeLocalIOError, "failed to read mutation outcome spool file", nil)
	}
	if hasDuplicateJSONKeyFold(bytes.TrimSpace(data)) {
		return record, apperrors.New(apperrors.CodeLocalIOError, "mutation outcome spool has duplicate fields", nil)
	}
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&record); err != nil {
		return record, apperrors.New(apperrors.CodeLocalIOError, "invalid mutation outcome spool record", nil)
	}
	var extra any
	if err := decoder.Decode(&extra); !errors.Is(err, io.EOF) {
		return record, apperrors.New(apperrors.CodeLocalIOError, "mutation outcome spool contains trailing data", nil)
	}
	if err := validateMutationSpoolRecord(record, filepath.Base(path)); err != nil {
		return safeAuditRecord{}, err
	}
	return record, nil
}

func validateMutationSpoolRecord(record safeAuditRecord, name string) error {
	if err := validateSafeAuditRecord(record); err != nil {
		return apperrors.New(apperrors.CodeLocalIOError, "invalid mutation outcome spool record", nil)
	}
	if !validMutationSpoolRecordIdentity(record) {
		return apperrors.New(apperrors.CodeLocalIOError, "invalid mutation outcome spool record", nil)
	}
	if err := validateSanitizedAuditMetadata(record); err != nil {
		return apperrors.New(apperrors.CodeLocalIOError, "invalid mutation outcome spool metadata", nil)
	}
	if _, err := hex.DecodeString(record.MutationID); err != nil {
		return apperrors.New(apperrors.CodeLocalIOError, "invalid mutation outcome spool mutation id", nil)
	}
	if !validMutationSpoolName(name) ||
		!strings.HasSuffix(strings.TrimSuffix(name, ".json"), "-"+record.MutationID) {
		return apperrors.New(apperrors.CodeLocalIOError, "mutation outcome spool filename does not match its record", nil)
	}
	if record.EventType != audit.EventType(record.Action+"."+mutationAuditPhaseOutcome) ||
		record.Status != record.Outcome.Status ||
		!validMutationOutcomeStatus(record.Outcome.Status) ||
		!validMutationCompensationStatus(record.Outcome.CompensationStatus) {
		return apperrors.New(apperrors.CodeLocalIOError, "invalid mutation outcome spool semantics", nil)
	}
	if record.Metadata.Items > 0 {
		counted := record.Outcome.Succeeded +
			record.Outcome.Failed +
			record.Outcome.Skipped +
			record.Outcome.Uncertain
		if counted != record.Metadata.Items {
			return apperrors.New(apperrors.CodeLocalIOError, "mutation outcome spool counts do not match planned items", nil)
		}
	}
	_, offset := record.Timestamp.Zone()
	if offset != 0 {
		return apperrors.New(apperrors.CodeLocalIOError, "mutation outcome spool timestamp must be UTC", nil)
	}
	if err := validateMutationSpoolOutcomeStatus(*record.Outcome); err != nil {
		return err
	}
	return validateMutationSpoolCompensation(*record.Outcome)
}

func validMutationSpoolRecordIdentity(record safeAuditRecord) bool {
	return record.Kind == mutationAuditKind &&
		record.Phase == mutationAuditPhaseOutcome &&
		validMutationAuditAction(record.Action) &&
		len(record.MutationID) == 32 &&
		record.MutationID == strings.ToLower(record.MutationID) &&
		!record.Timestamp.IsZero() &&
		record.Outcome != nil
}

func validateMutationSpoolOutcomeStatus(outcome mutationAuditOutcome) error {
	switch outcome.Status {
	case audit.StatusSuccess:
		if !validSuccessfulMutationSpoolOutcome(outcome) {
			return apperrors.New(apperrors.CodeLocalIOError, "invalid successful mutation outcome spool record", nil)
		}
	case audit.StatusFailed:
		if outcome.Succeeded != 0 {
			return apperrors.New(apperrors.CodeLocalIOError, "invalid failed mutation outcome spool record", nil)
		}
	case audit.StatusPartialFailed:
		if !validPartialMutationSpoolOutcome(outcome) {
			return apperrors.New(apperrors.CodeLocalIOError, "invalid partial mutation outcome spool record", nil)
		}
	}
	return nil
}

func validSuccessfulMutationSpoolOutcome(outcome mutationAuditOutcome) bool {
	return outcome.Failed == 0 &&
		outcome.Uncertain == 0 &&
		outcome.ErrorCode == "" &&
		outcome.ErrorFingerprint == "" &&
		outcome.ErrorBytes == 0 &&
		outcome.CompensationStatus == ""
}

func validPartialMutationSpoolOutcome(outcome mutationAuditOutcome) bool {
	return outcome.Succeeded > 0 &&
		(outcome.Failed > 0 ||
			outcome.Uncertain > 0 ||
			outcome.ErrorCode != "")
}

func validateMutationSpoolCompensation(outcome mutationAuditOutcome) error {
	switch outcome.CompensationStatus {
	case "succeeded":
		if outcome.Uncertain != 0 {
			return apperrors.New(apperrors.CodeLocalIOError, "invalid compensated mutation outcome spool record", nil)
		}
	case "incomplete":
		if outcome.Uncertain == 0 {
			return apperrors.New(apperrors.CodeLocalIOError, "incomplete compensation must report uncertain credentials", nil)
		}
	case "not-safe":
		if outcome.Succeeded == 0 && outcome.Uncertain == 0 {
			return apperrors.New(apperrors.CodeLocalIOError, "unsafe compensation outcome must report committed or uncertain credentials", nil)
		}
	}
	return nil
}

func validMutationAuditAction(action string) bool {
	if action == "" || strings.TrimSpace(action) != action {
		return false
	}
	for _, char := range action {
		if (char >= 'a' && char <= 'z') ||
			(char >= '0' && char <= '9') ||
			char == '.' ||
			char == '-' ||
			char == '_' {
			continue
		}
		return false
	}
	return true
}

func validMutationOutcomeStatus(status string) bool {
	return status == audit.StatusSuccess ||
		status == audit.StatusFailed ||
		status == audit.StatusPartialFailed
}

func validMutationCompensationStatus(status string) bool {
	return status == "" ||
		status == "succeeded" ||
		status == "incomplete" ||
		status == "not-safe"
}

func auditIncompleteError(mutationID string, spoolFailed bool) error {
	message := "mutation outcome audit is incomplete"
	if spoolFailed {
		message = "mutation outcome audit is incomplete and durable spooling failed"
	}
	suggestion := "Resolve audit storage before another mutation. Definitely uncommitted outcomes replay automatically; reconcile .indeterminate entries against the audit log before handling them manually."
	if mutationID != "" {
		suggestion = fmt.Sprintf(
			"Do not retry blindly. Check mutationId %s, resolve audit storage, then run a mutation to replay durable outcomes.",
			mutationID,
		)
	}
	return apperrors.New(codeAuditIncomplete, message, nil).WithSuggestion(suggestion)
}

func indeterminateMutationSpoolError(mutationID string) error {
	suggestion := "Do not retry this spool entry automatically. Reconcile it against the authenticated audit history, then archive or restore it only through a reviewed recovery procedure."
	if mutationID != "" {
		suggestion = fmt.Sprintf("MutationId %s may already be committed. %s", mutationID, suggestion)
	}
	return apperrors.New(codeAuditIncomplete, "mutation outcome audit commit state is indeterminate", nil).
		WithSuggestion(suggestion)
}

func hasDuplicateTopLevelJSONKey(data []byte) bool {
	decoder := json.NewDecoder(bytes.NewReader(data))
	token, err := decoder.Token()
	if err != nil {
		return false
	}
	delim, ok := token.(json.Delim)
	if !ok || delim != '{' {
		return false
	}
	seen := make(map[string]struct{})
	for decoder.More() {
		token, err = decoder.Token()
		if err != nil {
			return false
		}
		key, ok := token.(string)
		if !ok {
			return false
		}
		if _, exists := seen[key]; exists {
			return true
		}
		seen[key] = struct{}{}
		var value json.RawMessage
		if err := decoder.Decode(&value); err != nil {
			return false
		}
	}
	return false
}

func hasDuplicateJSONKeyFold(data []byte) bool {
	decoder := json.NewDecoder(bytes.NewReader(data))
	duplicate, err := jsonValueHasDuplicateKeyFold(decoder)
	if err != nil {
		return false
	}
	var extra any
	return duplicate || !errors.Is(decoder.Decode(&extra), io.EOF)
}

func jsonValueHasDuplicateKeyFold(decoder *json.Decoder) (bool, error) {
	token, err := decoder.Token()
	if err != nil {
		return false, err
	}
	delim, isDelim := token.(json.Delim)
	if !isDelim {
		return false, nil
	}
	switch delim {
	case '{':
		keys := make([]string, 0)
		for decoder.More() {
			keyToken, err := decoder.Token()
			if err != nil {
				return false, err
			}
			key, ok := keyToken.(string)
			if !ok {
				return false, apperrors.New(apperrors.CodeValidationFailed, "JSON object key is not a string", nil)
			}
			for _, existing := range keys {
				if strings.EqualFold(existing, key) {
					return true, nil
				}
			}
			keys = append(keys, key)
			duplicate, err := jsonValueHasDuplicateKeyFold(decoder)
			if err != nil || duplicate {
				return duplicate, err
			}
		}
	case '[':
		for decoder.More() {
			duplicate, err := jsonValueHasDuplicateKeyFold(decoder)
			if err != nil || duplicate {
				return duplicate, err
			}
		}
	default:
		return false, apperrors.New(apperrors.CodeValidationFailed, "unexpected JSON delimiter", nil)
	}
	_, err = decoder.Token()
	return false, err
}
