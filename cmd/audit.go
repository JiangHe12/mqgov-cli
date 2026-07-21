package cmd

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"path/filepath"
	"strings"
	"time"

	"github.com/spf13/cobra"

	"github.com/JiangHe12/opskit-core/v2/apperrors"
	"github.com/JiangHe12/opskit-core/v2/audit"
	corectx "github.com/JiangHe12/opskit-core/v2/ctx"
	"github.com/JiangHe12/opskit-core/v2/lockfile"
	"github.com/JiangHe12/opskit-core/v2/safety"

	"github.com/JiangHe12/mqgov-cli/internal/mqgovctx"
)

type auditQueryOptions struct {
	since     string
	eventType string
	operator  string
	status    string
	path      string
	limit     int
}

type auditVerifyOptions struct {
	path   string
	strict bool
}

type auditPruneOptions struct {
	path           string
	before         string
	olderThanDays  int
	keepLast       int
	dryRun         bool
	dryRunExplicit bool
	confirm        bool
	expectedFiles  []string
}

type auditPruneResult struct {
	DryRun       bool     `json:"dryRun"`
	DeletedFiles []string `json:"deletedFiles"`
	Count        int      `json:"count"`
}

type auditPruneDurabilityError struct {
	cause error
}

func (err *auditPruneDurabilityError) Error() string {
	return "audit prune directory changes may not be durable"
}

func (err *auditPruneDurabilityError) Unwrap() error {
	return err.cause
}

type safeAuditQueryResult struct {
	Records          []safeAuditRecord
	MalformedEntries int
}

func newAuditCmd(f *cliFlags) *cobra.Command {
	cmd := &cobra.Command{Use: "audit", Short: "Inspect mqgov audit log", Args: requireSubcommand, RunE: runParentHelp}
	cmd.AddCommand(auditQueryCmd(f), auditVerifyCmd(f), auditPruneCmd(f))
	return cmd
}

func auditQueryCmd(f *cliFlags) *cobra.Command {
	opts := auditQueryOptions{limit: 100}
	cmd := &cobra.Command{
		Use:   "query",
		Short: "Query audit events",
		Args:  cobra.NoArgs,
		RunE: func(_ *cobra.Command, _ []string) error {
			return runAuditQuery(f, opts)
		},
	}
	cmd.Flags().StringVar(&opts.since, "since", "", "Start time: 24h or RFC3339")
	cmd.Flags().StringVar(&opts.eventType, "type", "", "Match event type exactly")
	cmd.Flags().StringVar(&opts.operator, "operator", "", "Match operator exactly")
	cmd.Flags().StringVar(&opts.status, "status", "", "Match status exactly")
	cmd.Flags().StringVar(&opts.path, "path", "", "Override audit log path")
	cmd.Flags().IntVar(&opts.limit, "limit", 100, "Maximum events (0 = unlimited)")
	return cmd
}

func runAuditQuery(f *cliFlags, opts auditQueryOptions) error {
	filter := audit.Filter{
		EventType:  opts.eventType,
		Operator:   opts.operator,
		Status:     opts.status,
		Limit:      opts.limit,
		PrivateKey: envWithDeprecatedAlias(mqgovAuditPrivateKeyEnv, deprecatedMqgovAuditPrivateKeyEnv),
	}
	if opts.since != "" {
		t, err := audit.ParseTime(opts.since, time.Now().UTC())
		if err != nil {
			return apperrors.New(apperrors.CodeUsageError, "invalid --since", err)
		}
		filter.Since = &t
	}
	path, err := auditPath(opts.path)
	if err != nil {
		return err
	}
	rawResult, err := audit.QueryRaw(path, filter)
	if err != nil {
		return err
	}
	result := sanitizeAuditQueryResult(rawResult)
	return printAuditQueryResult(f, result)
}

func sanitizeAuditQueryResult(result audit.RawResult) safeAuditQueryResult {
	safe := safeAuditQueryResult{
		Records:          make([]safeAuditRecord, 0, len(result.Records)),
		MalformedEntries: result.MalformedEntries,
	}
	for _, raw := range result.Records {
		record, err := sanitizeAuditRecord([]byte(raw.Line))
		if err != nil {
			safe.MalformedEntries++
			continue
		}
		safe.Records = append(safe.Records, record)
	}
	return safe
}

func sanitizeAuditRecord(data []byte) (safeAuditRecord, error) {
	var record safeAuditRecord
	if hasDuplicateTopLevelJSONKey(bytes.TrimSpace(data)) {
		return record, apperrors.New(apperrors.CodeValidationFailed, "audit record has duplicate fields", nil)
	}
	if err := json.Unmarshal(data, &record); err != nil {
		return record, err
	}
	if record.Timestamp.IsZero() || record.EventType == "" {
		return safeAuditRecord{}, apperrors.New(apperrors.CodeValidationFailed, "audit record is missing required fields", nil)
	}
	if record.Ticket != "" {
		record.TicketFingerprint, record.TicketBytes = sensitiveAuditFingerprint("ticket", record.Ticket)
	}
	if record.Reason != "" {
		record.ReasonFingerprint, record.ReasonBytes = sensitiveAuditFingerprint("reason", record.Reason)
	}
	if record.Diff != "" {
		record.DetailFingerprint, record.DetailBytes = sensitiveAuditFingerprint("detail", record.Diff)
	}
	if record.Error != nil {
		record.ErrorCode = record.Error.Code
		record.ErrorFingerprint, record.ErrorBytes = sensitiveAuditFingerprint("error", record.Error.Message)
	}
	record.Ticket = ""
	record.Reason = ""
	record.Diff = ""
	record.Error = nil
	record.APIVersion = mutationAuditAPIVersion
	if record.Kind != mutationAuditKind {
		record.Kind = safeAuditKind
		record.MutationID = ""
		record.Phase = ""
		record.Action = ""
		record.Outcome = nil
	}
	if err := validateSanitizedAuditMetadata(record); err != nil {
		return safeAuditRecord{}, err
	}
	return record, nil
}

func validateSanitizedAuditMetadata(record safeAuditRecord) error {
	fingerprints := []struct {
		value string
		bytes int
	}{
		{record.TicketFingerprint, record.TicketBytes},
		{record.ReasonFingerprint, record.ReasonBytes},
		{record.DetailFingerprint, record.DetailBytes},
		{record.ErrorFingerprint, record.ErrorBytes},
		{record.Metadata.PayloadFingerprint, record.Metadata.PayloadBytes},
		{record.Metadata.KeyFingerprint, record.Metadata.KeyBytes},
		{record.Metadata.BodyFingerprint, record.Metadata.BodyBytes},
	}
	if record.Outcome != nil {
		fingerprints = append(fingerprints, struct {
			value string
			bytes int
		}{record.Outcome.ErrorFingerprint, record.Outcome.ErrorBytes})
	}
	for _, fingerprint := range fingerprints {
		if fingerprint.bytes < 0 ||
			(fingerprint.value == "" && fingerprint.bytes != 0) ||
			(fingerprint.value != "" && !validAuditFingerprint(fingerprint.value)) {
			return apperrors.New(apperrors.CodeValidationFailed, "audit record has invalid fingerprint metadata", nil)
		}
	}
	counts := []int{
		record.Metadata.Items,
		record.Metadata.Creates,
		record.Metadata.Updates,
		record.Metadata.Deletes,
	}
	if record.Outcome != nil {
		counts = append(
			counts,
			record.Outcome.Succeeded,
			record.Outcome.Failed,
			record.Outcome.Skipped,
			record.Outcome.Uncertain,
		)
	}
	for _, count := range counts {
		if count < 0 {
			return apperrors.New(apperrors.CodeValidationFailed, "audit record has invalid count metadata", nil)
		}
	}
	return nil
}

func validAuditFingerprint(value string) bool {
	const prefix = "sha256:"
	if !strings.HasPrefix(value, prefix) ||
		value != strings.ToLower(value) ||
		len(value) != len(prefix)+sha256.Size*2 {
		return false
	}
	_, err := hex.DecodeString(value[len(prefix):])
	return err == nil
}

func printAuditQueryResult(f *cliFlags, result safeAuditQueryResult) error {
	p := newPrinter(f)
	switch f.Output {
	case "json":
		return p.JSONData("AuditQueryResult", map[string]any{
			"apiVersion":       auditAPIVersion,
			"events":           result.Records,
			"malformedEntries": result.MalformedEntries,
		})
	case "plain":
		for _, event := range result.Records {
			data, err := json.Marshal(event)
			if err != nil {
				return apperrors.New(apperrors.CodeLocalIOError, "failed to marshal audit event", err)
			}
			if err := p.Info(string(data)); err != nil {
				return err
			}
		}
		return nil
	default:
		rows := make([][]string, 0, len(result.Records))
		for _, event := range result.Records {
			rows = append(rows, []string{
				auditTime(event.Timestamp),
				auditDashIfEmpty(string(event.EventType)),
				auditDashIfEmpty(event.Operator),
				auditDashIfEmpty(event.Context.Name),
				truncateAuditTableValue(event.Target.Resource),
				auditDashIfEmpty(event.Status),
			})
		}
		if err := p.Table([]string{"TIMESTAMP", "TYPE", "OPERATOR", "CONTEXT", "RESOURCE", "STATUS"}, rows); err != nil {
			return err
		}
		if result.MalformedEntries > 0 {
			return p.Info(fmt.Sprintf("(skipped %d malformed audit entries)", result.MalformedEntries))
		}
		return nil
	}
}

func auditVerifyCmd(f *cliFlags) *cobra.Command {
	var opts auditVerifyOptions
	cmd := &cobra.Command{
		Use:   "verify",
		Short: "Verify audit log integrity",
		Args:  cobra.NoArgs,
		RunE: func(_ *cobra.Command, _ []string) error {
			return runAuditVerify(f, opts)
		},
	}
	cmd.Flags().StringVar(&opts.path, "path", "", "Override audit log path")
	cmd.Flags().BoolVar(&opts.strict, "strict", false, "Exit non-zero on malformed entries or invariant violations")
	return cmd
}

func runAuditVerify(f *cliFlags, opts auditVerifyOptions) error {
	path, err := auditPath(opts.path)
	if err != nil {
		return err
	}
	result, err := audit.Verify(path, audit.VerifyOptions{})
	if err != nil {
		return err
	}
	if err := printAuditVerifyResult(f, result); err != nil {
		return err
	}
	if opts.strict && auditVerifyHasProblems(result) {
		return apperrors.New(apperrors.CodeValidationFailed, "audit verification failed", nil)
	}
	return nil
}

func auditVerifyHasProblems(result audit.VerifyResult) bool {
	return result.HasProblems()
}

func printAuditVerifyResult(f *cliFlags, result audit.VerifyResult) error {
	p := newPrinter(f)
	switch f.Output {
	case "json":
		return p.JSONData("AuditVerifyResult", result)
	case "plain":
		return p.Info(fmt.Sprintf("total=%d valid=%d malformed=%d schemaErrors=%d timestampOrderViolations=%d authenticated=%d legacyUnauthenticated=%d encryptedOpaque=%d integrityErrors=%d sequenceViolations=%d checkpointViolations=%d truncationDetected=%t lockPresent=%t",
			result.Total, result.Valid, result.Malformed, result.SchemaErrors, result.TimestampOrderViolations,
			result.Authenticated, result.LegacyUnauthenticated, result.EncryptedOpaque, result.IntegrityErrors,
			result.SequenceViolations, result.CheckpointViolations, result.TruncationDetected, result.Lock.Present))
	default:
		rows := make([][]string, 0, len(result.Files))
		for _, file := range result.Files {
			rows = append(rows, []string{
				file.Path,
				fmt.Sprintf("%d", file.Total),
				fmt.Sprintf("%d", file.Valid),
				fmt.Sprintf("%d", file.Malformed),
				fmt.Sprintf("%d", file.SchemaError),
				fmt.Sprintf("%d", file.TimestampOrderViolations),
				fmt.Sprintf("%d", file.Authenticated),
				fmt.Sprintf("%d", file.LegacyUnauthenticated),
				fmt.Sprintf("%d", file.EncryptedOpaque),
				fmt.Sprintf("%d", file.IntegrityErrors),
				fmt.Sprintf("%d", file.SequenceViolations),
				auditDashIfEmpty(file.Quarantine),
				fmt.Sprintf("%t", file.Repaired),
			})
		}
		if err := p.Table([]string{"PATH", "TOTAL", "VALID", "MALFORMED", "SCHEMA_ERRORS", "TIMESTAMP_ORDER_VIOLATIONS", "AUTHENTICATED", "LEGACY_UNAUTHENTICATED", "ENCRYPTED_OPAQUE", "INTEGRITY_ERRORS", "SEQUENCE_VIOLATIONS", "QUARANTINE", "REPAIRED"}, rows); err != nil {
			return err
		}
		return p.Info(fmt.Sprintf("total=%d valid=%d malformed=%d schemaErrors=%d timestampOrderViolations=%d authenticated=%d legacyUnauthenticated=%d encryptedOpaque=%d integrityErrors=%d sequenceViolations=%d checkpointViolations=%d truncationDetected=%t lockPresent=%t",
			result.Total, result.Valid, result.Malformed, result.SchemaErrors, result.TimestampOrderViolations,
			result.Authenticated, result.LegacyUnauthenticated, result.EncryptedOpaque,
			result.IntegrityErrors, result.SequenceViolations, result.CheckpointViolations,
			result.TruncationDetected, result.Lock.Present))
	}
}

func auditPruneCmd(f *cliFlags) *cobra.Command {
	opts := auditPruneOptions{keepLast: -1, dryRun: true}
	cmd := &cobra.Command{
		Use:   "prune",
		Short: "Prune rotated audit logs",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			opts.dryRunExplicit = cmd.Flags().Changed("dry-run")
			return runAuditPrune(f, opts)
		},
	}
	cmd.Flags().StringVar(&opts.path, "path", "", "Override audit log path")
	cmd.Flags().StringVar(&opts.before, "before", "", "Prune rotated logs before this time (30d / RFC3339 / YYYY-MM-DD)")
	cmd.Flags().IntVar(&opts.olderThanDays, "older-than", 0, "Prune rotated logs older than N days")
	cmd.Flags().IntVar(&opts.keepLast, "keep-last", -1, "Keep the newest N rotated logs (0 = delete all rotated logs)")
	cmd.Flags().BoolVar(&opts.dryRun, "dry-run", true, "Preview matched rotated logs without deleting")
	cmd.Flags().BoolVar(&opts.confirm, "confirm", false, "Actually delete matched rotated logs")
	return cmd
}

func runAuditPrune(f *cliFlags, opts auditPruneOptions) error { //nolint:gocyclo // Validation and the mutually exclusive prune selectors are kept together.
	globalPlan := contextPlanOnly(f)
	if opts.dryRunExplicit && opts.dryRun && opts.confirm && !globalPlan {
		return apperrors.New(apperrors.CodeUsageError, "audit prune accepts only one of --dry-run or --confirm", nil)
	}
	confirm := opts.confirm && !globalPlan
	if countAuditPruneSelectors(opts) != 1 {
		return apperrors.New(apperrors.CodeUsageError, "audit prune requires exactly one of --before, --older-than, or --keep-last", nil)
	}
	if opts.keepLast < -1 {
		return apperrors.New(apperrors.CodeUsageError, "--keep-last must be >= 0", nil)
	}
	if opts.olderThanDays < 0 {
		return apperrors.New(apperrors.CodeUsageError, "--older-than must be >= 0", nil)
	}
	path, err := auditPath(opts.path)
	if err != nil {
		return err
	}
	path, err = normalizeAuditPruneTarget(f, path)
	if err != nil {
		return err
	}
	rotated, candidates, err := auditPrunePlan(path, opts)
	if err != nil {
		return err
	}
	opts.expectedFiles = rotated
	if !confirm {
		return printAuditPruneResult(f, auditPruneResult{DryRun: true, DeletedFiles: candidates, Count: len(candidates)})
	}
	policy, policyName, err := currentAuditPrunePolicy()
	if err != nil {
		return err
	}
	if err := authorizeForContextWithConfirmation(f, safety.R3, policy, allowAuditPrune, f.Yes, policyName); err != nil {
		return err
	}
	if len(candidates) == 0 {
		return printAuditPruneResult(f, auditPruneResult{DryRun: false, DeletedFiles: []string{}, Count: 0})
	}
	metadata := mutationValueMetadata("mq.audit.prune", candidates)
	metadata.Items = len(candidates)
	metadata.Deletes = len(candidates)
	var handle *mutationAuditHandle
	var deleted []string
	operationErr := withContextStoreLock(func(locked *corectx.Config[mqgovctx.Context]) error {
		if err := ensureAuditPrunePolicyUnchanged(locked, policy, policyName); err != nil {
			return err
		}
		if err := validateAuditPruneConfirmation(path, opts, candidates); err != nil {
			return err
		}
		var beginErr error
		handle, beginErr = beginMutationAudit(f, mutationAuditSpec{
			Action:      "mq.audit.prune",
			ContextName: policyName,
			Context:     policy,
			Target:      audit.EventTarget{ResourceType: "audit"},
			Metadata:    metadata,
			AuditPath:   auditControlPath(path),
		})
		if beginErr != nil {
			return beginErr
		}
		var deleteErr error
		deleted, _, deleteErr = deleteAuditPruneCandidates(path, opts, candidates)
		return deleteErr
	})
	if handle == nil {
		return operationErr
	}
	if err := finishAuditPrune(handle, len(candidates), len(deleted), operationErr); err != nil {
		return err
	}
	return printAuditPruneResult(f, auditPruneResult{DryRun: false, DeletedFiles: deleted, Count: len(deleted)})
}

func auditControlPath(path string) string {
	return filepath.Join(filepath.Dir(path), "."+filepath.Base(path)+"-control")
}

func finishAuditPrune(
	handle *mutationAuditHandle,
	total int,
	deleted int,
	operationErr error,
) error {
	outcome := auditPruneMutationOutcome(total, deleted, operationErr)
	return finishMutationAudit(handle, outcome, operationErr)
}

func auditPruneMutationOutcome(
	total int,
	deleted int,
	operationErr error,
) mutationAuditOutcome {
	if total < 0 {
		total = 0
	}
	if deleted < 0 {
		deleted = 0
	}
	if deleted > total {
		deleted = total
	}
	outcome := mutationAuditOutcome{
		Status:    audit.StatusSuccess,
		Succeeded: deleted,
		Skipped:   total - deleted,
		counted:   true,
	}
	if operationErr == nil {
		return outcome
	}
	outcome.Status = audit.StatusFailed
	var durabilityErr *auditPruneDurabilityError
	if errors.As(operationErr, &durabilityErr) {
		outcome.Succeeded = 0
		outcome.Uncertain = deleted
	} else if deleted > 0 {
		outcome.Status = audit.StatusPartialFailed
	}
	if deleted < total {
		outcome.Failed = 1
		outcome.Skipped = total - deleted - 1
		if outcome.Skipped < 0 {
			outcome.Skipped = 0
		}
	}
	return outcome
}

func countAuditPruneSelectors(opts auditPruneOptions) int {
	count := 0
	if opts.before != "" {
		count++
	}
	if opts.olderThanDays > 0 {
		count++
	}
	if opts.keepLast >= 0 {
		count++
	}
	return count
}

func auditPruneCandidates(path string, opts auditPruneOptions) ([]string, error) {
	_, candidates, err := auditPrunePlan(path, opts)
	return candidates, err
}

func auditPrunePlan(path string, opts auditPruneOptions) ([]string, []string, error) {
	rotated, err := strictAuditRotatedFiles(path)
	if err != nil {
		return nil, nil, err
	}
	candidates, err := selectAuditPruneCandidates(path, opts, rotated)
	if err != nil {
		return nil, nil, err
	}
	return rotated, candidates, nil
}

func selectAuditPruneCandidates(path string, opts auditPruneOptions, rotated []string) ([]string, error) {
	if opts.keepLast >= 0 {
		if opts.keepLast >= len(rotated) {
			return []string{}, nil
		}
		return append([]string{}, rotated[:len(rotated)-opts.keepLast]...), nil
	}
	cutoff, err := auditPruneCutoff(opts, time.Now().UTC())
	if err != nil {
		return nil, err
	}
	out := make([]string, 0, len(rotated))
	for _, filePath := range rotated {
		ts, _, ok := strictAuditRotatedFileOrder(path, filePath)
		if ok && ts.Before(cutoff) {
			out = append(out, filePath)
		}
	}
	return out, nil
}

func currentAuditPrunePolicy() (mqgovctx.Context, string, error) {
	cfg, err := mqgovctx.Load()
	if err != nil {
		return mqgovctx.Context{}, "", err
	}
	return auditPrunePolicyFromConfig(cfg)
}

func auditPrunePolicyFromConfig(cfg *corectx.Config[mqgovctx.Context]) (mqgovctx.Context, string, error) {
	if cfg.CurrentContext == "" {
		return mqgovctx.Context{}, "", nil
	}
	item, ok := cfg.Contexts[cfg.CurrentContext]
	if !ok {
		return mqgovctx.Context{}, "", apperrors.New(
			apperrors.CodeValidationFailed,
			fmt.Sprintf("current context %q does not exist; refusing audit prune authorization", cfg.CurrentContext),
			nil,
		)
	}
	return item, cfg.CurrentContext, nil
}

func ensureAuditPrunePolicyUnchanged(
	cfg *corectx.Config[mqgovctx.Context],
	expected mqgovctx.Context,
	expectedName string,
) error {
	actual, actualName, err := auditPrunePolicyFromConfig(cfg)
	if err != nil {
		return err
	}
	actualPolicy := contextControlPolicy{
		meta:         actual,
		source:       actualName,
		targetExists: actualName != "",
	}
	expectedPolicy := contextControlPolicy{
		meta:         expected,
		source:       expectedName,
		targetExists: expectedName != "",
	}
	if !sameContextControlPolicy(actualPolicy, expectedPolicy) {
		return apperrors.New(
			apperrors.CodeAuthorizationRequired,
			"audit prune policy changed during authorization; retry the command",
			nil,
		)
	}
	return nil
}

func withContextStoreLock(
	action func(*corectx.Config[mqgovctx.Context]) error,
) (retErr error) {
	configDir, err := corectx.ConfigDir()
	if err != nil {
		return apperrors.New(apperrors.CodeLocalIOError, "failed to resolve context config directory", nil)
	}
	lock := lockfile.New(filepath.Join(configDir, "config"))
	if err := lock.Acquire(); err != nil {
		return err
	}
	defer func() {
		if err := releaseMutationAuditLock(lock); err != nil && retErr == nil {
			retErr = apperrors.New(apperrors.CodeLocalIOError, "failed to release context policy lock", nil)
		}
	}()
	cfg, err := mqgovctx.Load()
	if err != nil {
		return err
	}
	return action(cfg)
}

func validateAuditPruneConfirmation(path string, opts auditPruneOptions, preview []string) error {
	_, err := audit.PruneRotatedFiles(path, preview, audit.PruneOptions{
		Confirm:              false,
		ExpectedRotatedFiles: opts.expectedFiles,
	})
	return err
}

func deleteAuditPruneCandidates(path string, opts auditPruneOptions, preview []string) ([]string, bool, error) {
	result, err := audit.PruneRotatedFiles(path, preview, audit.PruneOptions{
		Confirm:              true,
		ExpectedRotatedFiles: opts.expectedFiles,
	})
	if err != nil && result.Started {
		err = apperrors.New(
			apperrors.CodeLocalIOError,
			"audit prune changed durable state before failing",
			&auditPruneDurabilityError{cause: err},
		)
	}
	return result.DeletedFiles, result.Started, err
}

func auditPruneCutoff(opts auditPruneOptions, now time.Time) (time.Time, error) {
	if opts.olderThanDays > 0 {
		return now.Add(-time.Duration(opts.olderThanDays) * 24 * time.Hour), nil
	}
	return parseAuditPruneBefore(opts.before, now)
}

func parseAuditPruneBefore(value string, now time.Time) (time.Time, error) {
	if t, err := audit.ParseTime(value, now); err == nil {
		return t, nil
	}
	t, err := time.Parse("2006-01-02", value)
	if err != nil {
		return time.Time{}, apperrors.New(apperrors.CodeUsageError, "invalid --before: expected relative (30d), RFC3339, or YYYY-MM-DD", nil)
	}
	return t, nil
}

func printAuditPruneResult(f *cliFlags, result auditPruneResult) error {
	p := newPrinter(f)
	switch f.Output {
	case "json":
		return p.JSONData("AuditPruneResult", result)
	case "plain":
		for _, filePath := range result.DeletedFiles {
			if err := p.Info(filePath); err != nil {
				return err
			}
		}
		return nil
	default:
		rows := make([][]string, 0, len(result.DeletedFiles))
		action := "would-delete"
		if !result.DryRun {
			action = "deleted"
		}
		for _, filePath := range result.DeletedFiles {
			rows = append(rows, []string{action, filepath.Base(filePath), filePath})
		}
		if len(rows) == 0 {
			return p.Info("(no rotated audit logs matched)")
		}
		if err := p.Table([]string{"ACTION", "FILE", "PATH"}, rows); err != nil {
			return err
		}
		if result.DryRun {
			return p.Info(fmt.Sprintf("(dry-run: pass --confirm --yes --ticket <ticket> --allow-audit-prune to delete %d rotated audit logs)", result.Count))
		}
		return nil
	}
}

func auditPath(override string) (string, error) {
	if override != "" {
		return override, nil
	}
	path, err := audit.DefaultPath()
	if err != nil {
		return "", apperrors.New(apperrors.CodeLocalIOError, "failed to resolve default audit log path", err)
	}
	return path, nil
}

func normalizeAuditPruneTarget(f *cliFlags, path string) (string, error) {
	if err := validateContextExportTargetType(path); err != nil {
		return "", err
	}
	targetAbsolute, err := filepath.Abs(path)
	if err != nil {
		return "", apperrors.New(apperrors.CodeLocalIOError, "failed to resolve audit prune path", nil)
	}
	targetResolved, err := resolveContextExportAlias(path)
	if err != nil {
		return "", err
	}
	defaultAudit, err := audit.DefaultPath()
	if err != nil {
		return "", apperrors.New(apperrors.CodeLocalIOError, "failed to resolve mutation audit path", err)
	}
	defaultAbsolute, err := filepath.Abs(defaultAudit)
	if err != nil {
		return "", apperrors.New(apperrors.CodeLocalIOError, "failed to resolve default audit path", nil)
	}
	if contextExportPathsEqual(targetAbsolute, defaultAbsolute) {
		return defaultAudit, nil
	}
	defaultResolved, err := resolveContextExportAlias(defaultAudit)
	if err != nil {
		return "", err
	}
	if err := validateAuditPruneTargetConflicts(f, path, targetResolved, defaultAudit, defaultResolved); err != nil {
		return "", err
	}
	return targetResolved, nil
}

func validateAuditPruneTargetConflicts(
	f *cliFlags,
	path string,
	targetResolved string,
	defaultAudit string,
	defaultResolved string,
) error {
	if conflict, err := contextExportPathsConflict(path, targetResolved, defaultAudit); err != nil {
		return err
	} else if conflict {
		return apperrors.New(
			apperrors.CodeUsageError,
			"audit prune path aliases the default audit log; use the canonical default path",
			nil,
		)
	}
	protected, spoolPath, err := contextExportProtectedPaths(f)
	if err != nil {
		return err
	}
	for _, protectedPath := range protected {
		conflict, conflictErr := contextExportPathsConflict(path, targetResolved, protectedPath)
		if conflictErr != nil {
			return conflictErr
		}
		if conflict {
			return apperrors.New(apperrors.CodeUsageError, "audit prune path conflicts with governed state", nil)
		}
	}
	if contextExportConflictsWithAuditTempNamespace(targetResolved, defaultResolved) {
		return apperrors.New(apperrors.CodeUsageError, "audit prune path conflicts with temporary audit state", nil)
	}
	if _, _, rotated := strictAuditRotatedFileOrder(defaultResolved, targetResolved); rotated {
		return apperrors.New(apperrors.CodeUsageError, "audit prune path conflicts with rotated audit state", nil)
	}
	spoolResolved, err := resolveContextExportAlias(spoolPath)
	if err != nil {
		return err
	}
	if contextExportPathWithin(targetResolved, spoolResolved) {
		return apperrors.New(apperrors.CodeUsageError, "audit prune path conflicts with the mutation audit spool", nil)
	}
	return nil
}

func auditTime(t time.Time) string {
	if t.IsZero() {
		return "-"
	}
	return t.Format(time.RFC3339)
}

func auditDashIfEmpty(value string) string {
	if value == "" {
		return "-"
	}
	return value
}

func truncateAuditTableValue(value string) string {
	const maxRunes = 40
	const prefixRunes = 36
	runes := []rune(value)
	if len(runes) <= maxRunes {
		return auditDashIfEmpty(value)
	}
	return string(runes[:prefixRunes]) + "..."
}
