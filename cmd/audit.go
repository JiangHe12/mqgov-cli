package cmd

import (
	"encoding/json"
	"fmt"
	"os"
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

func newAuditCmd(f *cliFlags) *cobra.Command {
	cmd := &cobra.Command{Use: "audit", Short: "Inspect mqgov audit log", Args: requireSubcommand, RunE: runParentHelp}
	cmd.AddCommand(auditQueryCmd(f), auditVerifyCmd(f))
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
