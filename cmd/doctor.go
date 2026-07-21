package cmd

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/spf13/cobra"

	"github.com/JiangHe12/opskit-core/v2/apperrors"
	"github.com/JiangHe12/opskit-core/v2/audit"
	"github.com/JiangHe12/opskit-core/v2/redact"
	"github.com/JiangHe12/opskit-core/v2/safety"

	"github.com/JiangHe12/mqgov-cli/internal/mqgovctx"
)

type doctorResult struct {
	Checks []doctorCheck `json:"checks"`
	OK     bool          `json:"ok"`
}

type doctorCheck struct {
	Name    string `json:"name"`
	Status  string `json:"status"`
	Message string `json:"message,omitempty"`
	Backend string `json:"backend,omitempty"`
	Context string `json:"context,omitempty"`
	Latency string `json:"latency,omitempty"`
}

func newDoctorCmd(f *cliFlags) *cobra.Command {
	return &cobra.Command{
		Use:   "doctor",
		Short: "Run read-only diagnostics",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			return runDoctor(cmd.Context(), f)
		},
	}
}

func runDoctor(ctx context.Context, f *cliFlags) error {
	result := doctorResult{OK: true}
	ctxMeta, ctxName, ctxErr := resolvedContext(f)
	if ctxErr != nil {
		result.add(doctorFailed("context", ctxErr))
	} else {
		result.add(doctorCheck{Name: "context", Status: audit.StatusSuccess, Context: ctxName, Message: redact.String("context loaded")})
		result.add(doctorWriteProbeCheck(ctxMeta, ctxName))
	}
	backend, _, backendErr := buildBroker(f)
	if backendErr != nil {
		result.add(doctorFailed("backend", backendErr))
	} else {
		defer backend.Close()
		start := time.Now()
		err := backend.Ping(ctx)
		result.add(doctorCheck{Name: "backend", Status: auditStatus(err), Message: doctorMessage("ping ok", err), Backend: backend.Describe().Backend, Context: ctxName, Latency: time.Since(start).String()})
	}
	result.add(doctorAuditCheck(f))
	appendAuditWarn(f, auditEventDiagnostic, ctxMeta, audit.EventTarget{ResourceType: "diagnostic"}, audit.StatusSuccess, fmt.Sprintf("doctor checks=%d", len(result.Checks)), nil)
	if err := printDoctorResult(f, result); err != nil {
		return err
	}
	if !result.OK {
		return apperrors.New(apperrors.CodeValidationFailed, "doctor checks failed", nil)
	}
	return nil
}

func (r *doctorResult) add(check doctorCheck) {
	if check.Status == audit.StatusFailed {
		r.OK = false
	}
	r.Checks = append(r.Checks, check)
}

func doctorWriteProbeCheck(meta mqgovctx.Context, ctxName string) doctorCheck {
	effective := safety.EffectiveRisk(safety.R1, safety.ContextMeta{Env: meta.Env, Protected: meta.Protected, TicketPattern: meta.TicketPattern, TicketValidator: meta.TicketValidator, Roles: meta.Roles})
	return doctorCheck{Name: "write-probe", Status: audit.StatusSuccess, Message: redact.String(fmt.Sprintf("governance path reachable; effectiveRisk=%v; broker mutation not attempted", effective)), Context: ctxName}
}

func doctorAuditCheck(f *cliFlags) doctorCheck {
	path, err := audit.DefaultPath()
	if err != nil {
		return doctorFailed("audit", err)
	}
	dir := filepath.Dir(path)
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return doctorFailed("audit", err)
	}
	file, err := os.OpenFile(path, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o600) //nolint:gosec // audit.DefaultPath returns the governed local audit log path.
	if err != nil {
		return doctorFailed("audit", err)
	}
	if err := file.Close(); err != nil {
		return doctorFailed("audit", err)
	}
	return doctorCheck{Name: "audit", Status: audit.StatusSuccess, Message: redact.String("audit log writable"), Context: f.contextName()}
}

func doctorFailed(name string, err error) doctorCheck {
	return doctorCheck{Name: name, Status: audit.StatusFailed, Message: redact.String(err.Error())}
}

func doctorMessage(ok string, err error) string {
	if err != nil {
		return redact.String(err.Error())
	}
	return redact.String(ok)
}

func auditStatus(err error) string {
	if err != nil {
		return audit.StatusFailed
	}
	return audit.StatusSuccess
}

func printDoctorResult(f *cliFlags, result doctorResult) error {
	if f.Output == "json" || f.Output == "plain" {
		return newPrinter(f).JSONData("DoctorResult", result)
	}
	rows := make([][]string, 0, len(result.Checks))
	for _, check := range result.Checks {
		rows = append(rows, []string{check.Name, check.Status, check.Backend, check.Context, check.Latency, check.Message})
	}
	return newPrinter(f).Table([]string{"CHECK", "STATUS", "BACKEND", "CONTEXT", "LATENCY", "MESSAGE"}, rows)
}
