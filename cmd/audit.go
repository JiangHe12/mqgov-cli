package cmd

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"time"

	"github.com/spf13/cobra"

	"github.com/JiangHe12/opskit-core/apperrors"
	"github.com/JiangHe12/opskit-core/audit"
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
}

type auditPruneResult struct {
	DryRun       bool     `json:"dryRun"`
	DeletedFiles []string `json:"deletedFiles"`
	Count        int      `json:"count"`
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
		PrivateKey: os.Getenv("MQGOV_CLI_AUDIT_PRIVATE_KEY"),
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
	result, err := audit.Query(path, filter)
	if err != nil {
		return err
	}
	return printAuditQueryResult(f, result)
}

func printAuditQueryResult(f *cliFlags, result audit.Result) error {
	p := newPrinter(f)
	switch f.Output {
	case "json":
		return p.JSONData("AuditQueryResult", map[string]any{
			"apiVersion":       auditAPIVersion,
			"events":           result.Events,
			"malformedEntries": result.MalformedEntries,
		})
	case "plain":
		for _, event := range result.Events {
			data, err := json.Marshal(event)
			if err != nil {
				return apperrors.New(apperrors.CodeLocalIOError, "failed to marshal audit event", err)
			}
			p.Info(string(data))
		}
		return nil
	default:
		rows := make([][]string, 0, len(result.Events))
		for _, event := range result.Events {
			rows = append(rows, []string{
				auditTime(event.Timestamp),
				auditDashIfEmpty(string(event.EventType)),
				auditDashIfEmpty(event.Operator),
				auditDashIfEmpty(event.Context.Name),
				truncateAuditTableValue(event.Target.Resource),
				auditDashIfEmpty(event.Status),
			})
		}
		p.Table([]string{"TIMESTAMP", "TYPE", "OPERATOR", "CONTEXT", "RESOURCE", "STATUS"}, rows)
		if result.MalformedEntries > 0 {
			p.Info(fmt.Sprintf("(skipped %d malformed audit entries)", result.MalformedEntries))
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
	return result.Malformed > 0 || result.SchemaErrors > 0 || result.TimestampOrderViolations > 0
}

func printAuditVerifyResult(f *cliFlags, result audit.VerifyResult) error {
	p := newPrinter(f)
	switch f.Output {
	case "json":
		return p.JSONData("AuditVerifyResult", result)
	case "plain":
		p.Info(fmt.Sprintf("total=%d valid=%d malformed=%d schemaErrors=%d timestampOrderViolations=%d",
			result.Total, result.Valid, result.Malformed, result.SchemaErrors, result.TimestampOrderViolations))
		return nil
	default:
		rows := make([][]string, 0, len(result.Files))
		for _, file := range result.Files {
			rows = append(rows, []string{
				file.Path,
				fmt.Sprintf("%d", file.Total),
				fmt.Sprintf("%d", file.Valid),
				fmt.Sprintf("%d", file.Malformed),
				fmt.Sprintf("%d", file.SchemaError),
			})
		}
		p.Table([]string{"PATH", "TOTAL", "VALID", "MALFORMED", "SCHEMA_ERRORS"}, rows)
		p.Info(fmt.Sprintf("total=%d valid=%d malformed=%d schemaErrors=%d timestampOrderViolations=%d lockPresent=%t",
			result.Total, result.Valid, result.Malformed, result.SchemaErrors, result.TimestampOrderViolations, result.Lock.Present))
		return nil
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

func runAuditPrune(f *cliFlags, opts auditPruneOptions) error {
	confirm := opts.confirm || f.Yes
	if opts.dryRunExplicit && opts.dryRun && confirm {
		return apperrors.New(apperrors.CodeUsageError, "audit prune accepts only one of --dry-run or --yes/--confirm", nil)
	}
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
	candidates, err := auditPruneCandidates(path, opts)
	if err != nil {
		return err
	}
	if confirm {
		for _, filePath := range candidates {
			if err := os.Remove(filePath); err != nil {
				return apperrors.New(apperrors.CodeLocalIOError, "failed to delete rotated audit log", err)
			}
		}
		if len(candidates) > 0 {
			evt := audit.Event{
				EventType:  audit.EventAuditPrune,
				Operator:   currentOperator(f),
				Context:    audit.EventContext{Name: f.contextName()},
				Status:     audit.StatusSuccess,
				AuditPrune: &audit.AuditPruneDetail{DeletedFiles: candidates, Count: len(candidates)},
			}
			if err := audit.AppendWithOptions(path, evt, auditOptions(f)); err != nil {
				return err
			}
		}
	}
	return printAuditPruneResult(f, auditPruneResult{DryRun: !confirm, DeletedFiles: candidates, Count: len(candidates)})
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
	rotated, err := audit.RotatedFiles(path)
	if err != nil {
		return nil, err
	}
	sortRotatedAuditFiles(path, rotated)
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
		ts, ok := audit.RotatedFileTimestamp(path, filePath)
		if ok && ts.Before(cutoff) {
			out = append(out, filePath)
		}
	}
	return out, nil
}

func sortRotatedAuditFiles(activePath string, files []string) {
	sort.SliceStable(files, func(i, j int) bool {
		ti, iok := audit.RotatedFileTimestamp(activePath, files[i])
		tj, jok := audit.RotatedFileTimestamp(activePath, files[j])
		if iok && jok && !ti.Equal(tj) {
			return ti.Before(tj)
		}
		return files[i] < files[j]
	})
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
			p.Info(filePath)
		}
		return nil
	default:
		rows := make([][]string, 0, len(result.DeletedFiles))
		action := "would-delete"
		if !result.DryRun {
			action = "deleted"
		}
		files := append([]string{}, result.DeletedFiles...)
		sort.Strings(files)
		for _, filePath := range files {
			rows = append(rows, []string{action, filepath.Base(filePath), filePath})
		}
		if len(rows) == 0 {
			p.Info("(no rotated audit logs matched)")
			return nil
		}
		p.Table([]string{"ACTION", "FILE", "PATH"}, rows)
		if result.DryRun {
			p.Info(fmt.Sprintf("(dry-run: pass --yes or --confirm to delete %d rotated audit logs)", result.Count))
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
