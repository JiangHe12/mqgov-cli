package cmd

import (
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/JiangHe12/opskit-core/v2/apperrors"
	corectx "github.com/JiangHe12/opskit-core/v2/ctx"
	"github.com/JiangHe12/opskit-core/v2/safety"
	"gopkg.in/yaml.v3"

	"github.com/JiangHe12/mqgov-cli/internal/mqgovctx"
)

func TestMain(m *testing.M) {
	if path := os.Getenv("MQGOV_TEST_VALIDATOR_CAPTURE"); path != "" {
		data, err := io.ReadAll(os.Stdin)
		if err != nil {
			os.Exit(2)
		}
		if err := os.WriteFile(path, data, 0o600); err != nil {
			os.Exit(2)
		}
		os.Exit(0)
	}
	testHome, err := createCommandTestHome()
	if err != nil {
		_, _ = fmt.Fprintf(os.Stderr, "create isolated test home: %v\n", err)
		os.Exit(2)
	}
	oldHome, hadHome := os.LookupEnv("HOME")
	oldProfile, hadProfile := os.LookupEnv("USERPROFILE")
	oldTmpDir, hadTmpDir := os.LookupEnv("TMPDIR")
	oldTemp, hadTemp := os.LookupEnv("TEMP")
	oldTmp, hadTmp := os.LookupEnv("TMP")
	if err := os.Setenv("HOME", testHome); err != nil {
		_, _ = fmt.Fprintf(os.Stderr, "set isolated HOME: %v\n", err)
		_ = os.RemoveAll(testHome)
		os.Exit(2)
	}
	if err := os.Setenv("USERPROFILE", testHome); err != nil {
		_, _ = fmt.Fprintf(os.Stderr, "set isolated USERPROFILE: %v\n", err)
		restoreTestEnvironment("HOME", oldHome, hadHome)
		_ = os.RemoveAll(testHome)
		os.Exit(2)
	}
	if err := os.Setenv("TMPDIR", testHome); err != nil {
		_, _ = fmt.Fprintf(os.Stderr, "set isolated TMPDIR: %v\n", err)
		restoreTestEnvironment("HOME", oldHome, hadHome)
		restoreTestEnvironment("USERPROFILE", oldProfile, hadProfile)
		_ = os.RemoveAll(testHome)
		os.Exit(2)
	}
	if err := os.Setenv("TEMP", testHome); err != nil {
		_, _ = fmt.Fprintf(os.Stderr, "set isolated TEMP: %v\n", err)
		restoreTestEnvironment("HOME", oldHome, hadHome)
		restoreTestEnvironment("USERPROFILE", oldProfile, hadProfile)
		restoreTestEnvironment("TMPDIR", oldTmpDir, hadTmpDir)
		_ = os.RemoveAll(testHome)
		os.Exit(2)
	}
	if err := os.Setenv("TMP", testHome); err != nil {
		_, _ = fmt.Fprintf(os.Stderr, "set isolated TMP: %v\n", err)
		restoreTestEnvironment("HOME", oldHome, hadHome)
		restoreTestEnvironment("USERPROFILE", oldProfile, hadProfile)
		restoreTestEnvironment("TMPDIR", oldTmpDir, hadTmpDir)
		restoreTestEnvironment("TEMP", oldTemp, hadTemp)
		_ = os.RemoveAll(testHome)
		os.Exit(2)
	}
	code := m.Run()
	restoreTestEnvironment("HOME", oldHome, hadHome)
	restoreTestEnvironment("USERPROFILE", oldProfile, hadProfile)
	restoreTestEnvironment("TMPDIR", oldTmpDir, hadTmpDir)
	restoreTestEnvironment("TEMP", oldTemp, hadTemp)
	restoreTestEnvironment("TMP", oldTmp, hadTmp)
	if err := os.RemoveAll(testHome); err != nil && code == 0 {
		_, _ = fmt.Fprintf(os.Stderr, "remove isolated test home: %v\n", err)
		code = 2
	}
	os.Exit(code)
}

func restoreTestEnvironment(name, value string, existed bool) {
	if existed {
		_ = os.Setenv(name, value)
		return
	}
	_ = os.Unsetenv(name)
}

func TestTrustedOperatorIgnoresFlagAndEnvironment(t *testing.T) {
	f := newDefaultFlags()
	f.Operator = "spoofed-flag"
	t.Setenv(mqgovOperatorEnv, "spoofed-env")
	t.Setenv(deprecatedMqgovOperatorEnv, "spoofed-deprecated-env")

	operator, err := trustedOperatorIdentity(f)
	if err != nil {
		t.Fatal(err)
	}
	if operator == f.Operator || operator == "spoofed-env" || operator == "spoofed-deprecated-env" {
		t.Fatalf("trusted operator = %q, accepted caller-controlled identity", operator)
	}
	if got := currentOperator(f); got != operator {
		t.Fatalf("currentOperator() = %q, want trusted %q", got, operator)
	}
	if err := authorize(f, safety.R0, mqgovctx.Context{
		Base: corectx.Base{Roles: map[string]string{operator: safety.RoleReader}},
	}, ""); err != nil {
		t.Fatalf("trusted operator was not used for RBAC: %v", err)
	}
	if err := authorize(f, safety.R0, mqgovctx.Context{
		Base: corectx.Base{Roles: map[string]string{"spoofed-flag": safety.RoleAdmin}},
	}, ""); err == nil {
		t.Fatal("spoofed --operator bypassed RBAC")
	}
}

func TestContextControlsRequireTheirExactR3AllowFlag(t *testing.T) {
	operator, err := trustedOperatorIdentity(newDefaultFlags())
	if err != nil {
		t.Fatal(err)
	}
	tests := []struct {
		name string
		args []string
	}{
		{
			name: "set",
			args: []string{"--allow-context-delete", "--backend", "kafka", "ctx", "set", "dev", "--brokers", "127.0.0.1:9092"},
		},
		{
			name: "delete",
			args: []string{"--allow-context-change", "ctx", "delete", "dev"},
		},
		{
			name: "role",
			args: []string{"--allow-context-change", "ctx", "role", "set", "dev", "--target-operator", "alice", "--role", safety.RoleReader},
		},
		{
			name: "use",
			args: []string{"--allow-role-change", "ctx", "use", "dev"},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			configPath := filepath.Join(t.TempDir(), "config.yaml")
			mqgovctx.SetConfigPath(configPath)
			t.Cleanup(func() { mqgovctx.SetConfigPath("") })
			original := mqgovctx.Context{
				Base:    corectx.Base{Protected: true, Roles: map[string]string{operator: safety.RoleAdmin}},
				Backend: "fake",
				Cluster: "original",
			}
			if err := mqgovctx.Set("dev", original); err != nil {
				t.Fatal(err)
			}

			args := []string{"--config", configPath, "--yes", "--ticket", "OPS-1"}
			_, err := runCommandForTest(t, append(args, tt.args...)...)
			if got := apperrors.AsAppError(err).Code; got != apperrors.CodeAuthorizationRequired {
				t.Fatalf("error code = %s, want %s; err=%v", got, apperrors.CodeAuthorizationRequired, err)
			}
			cfg, loadErr := mqgovctx.Load()
			if loadErr != nil {
				t.Fatal(loadErr)
			}
			item, exists := cfg.Contexts["dev"]
			if !exists || item.Backend != original.Backend || item.Cluster != original.Cluster || item.Roles["alice"] != "" {
				t.Fatalf("denied %s mutated context: %+v", tt.name, item)
			}
			if cfg.CurrentContext != "" {
				t.Fatalf("denied %s changed current context to %q", tt.name, cfg.CurrentContext)
			}
		})
	}
}

func TestContextUseCannotSelectAWeakerPolicyWithoutOldPolicyApproval(t *testing.T) {
	operator, err := trustedOperatorIdentity(newDefaultFlags())
	if err != nil {
		t.Fatal(err)
	}
	configPath := filepath.Join(t.TempDir(), "config.yaml")
	mqgovctx.SetConfigPath(configPath)
	t.Cleanup(func() { mqgovctx.SetConfigPath("") })
	if err := mqgovctx.Set("strong", mqgovctx.Context{
		Base:    corectx.Base{Roles: map[string]string{operator: safety.RoleReader}},
		Backend: "fake",
	}); err != nil {
		t.Fatal(err)
	}
	if err := mqgovctx.Set("weak", mqgovctx.Context{Backend: "fake"}); err != nil {
		t.Fatal(err)
	}
	if err := mqgovctx.Use("strong"); err != nil {
		t.Fatal(err)
	}

	_, err = runCommandForTest(t,
		"--config", configPath,
		"--yes",
		"--ticket", "OPS-1",
		"--allow-context-change",
		"ctx", "use", "weak",
	)
	if got := apperrors.AsAppError(err).Code; got != apperrors.CodeAuthorizationRequired {
		t.Fatalf("error code = %s, want old-policy authorization failure; err=%v", got, err)
	}
	cfg, err := mqgovctx.Load()
	if err != nil {
		t.Fatal(err)
	}
	if cfg.CurrentContext != "strong" {
		t.Fatalf("denied context switch selected %q", cfg.CurrentContext)
	}
}

func TestFirstContextUseUsesTargetPolicy(t *testing.T) {
	operator, err := trustedOperatorIdentity(newDefaultFlags())
	if err != nil {
		t.Fatal(err)
	}
	configPath := filepath.Join(t.TempDir(), "config.yaml")
	mqgovctx.SetConfigPath(configPath)
	t.Cleanup(func() { mqgovctx.SetConfigPath("") })
	if err := mqgovctx.Set("guarded", mqgovctx.Context{
		Base:    corectx.Base{Roles: map[string]string{operator: safety.RoleReader}},
		Backend: "fake",
	}); err != nil {
		t.Fatal(err)
	}

	_, err = runCommandForTest(t,
		"--config", configPath,
		"--yes",
		"--ticket", "OPS-1",
		"--allow-context-change",
		"ctx", "use", "guarded",
	)
	if got := apperrors.AsAppError(err).Code; got != apperrors.CodeAuthorizationRequired {
		t.Fatalf("error code = %s, want target-policy authorization failure; err=%v", got, err)
	}
	cfg, err := mqgovctx.Load()
	if err != nil {
		t.Fatal(err)
	}
	if cfg.CurrentContext != "" {
		t.Fatalf("denied first context selection chose %q", cfg.CurrentContext)
	}
}

func TestContextReplacementAndCreationUsePreChangePolicy(t *testing.T) {
	operator, err := trustedOperatorIdentity(newDefaultFlags())
	if err != nil {
		t.Fatal(err)
	}
	configPath := filepath.Join(t.TempDir(), "config.yaml")
	mqgovctx.SetConfigPath(configPath)
	t.Cleanup(func() { mqgovctx.SetConfigPath("") })
	guard := mqgovctx.Context{
		Base:    corectx.Base{Protected: true, Roles: map[string]string{operator: safety.RoleReader}},
		Backend: "fake",
		Cluster: "guarded",
	}
	if err := mqgovctx.Set("guard", guard); err != nil {
		t.Fatal(err)
	}
	if err := mqgovctx.Use("guard"); err != nil {
		t.Fatal(err)
	}

	approval := []string{
		"--config", configPath,
		"--yes",
		"--ticket", "OPS-1",
		"--allow-context-change",
		"--backend", "kafka",
	}
	if _, err := runCommandForTest(t, append(approval, "ctx", "set", "guard", "--brokers", "127.0.0.1:9092")...); err == nil {
		t.Fatal("replacement bypassed the target's pre-change reader policy")
	}
	if _, err := runCommandForTest(t, append(approval, "ctx", "set", "new", "--brokers", "127.0.0.1:9092")...); err == nil {
		t.Fatal("new context bypassed the persisted current-context policy")
	}

	cfg, err := mqgovctx.Load()
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Contexts["guard"].Cluster != "guarded" || !cfg.Contexts["guard"].Protected {
		t.Fatalf("guard context was replaced: %+v", cfg.Contexts["guard"])
	}
	if _, exists := cfg.Contexts["new"]; exists {
		t.Fatal("denied new context was created")
	}
}

func TestContextControlValidatorUsesPreChangePolicySource(t *testing.T) {
	executable, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	payloadPath := filepath.Join(t.TempDir(), "validator-payload.json")
	t.Setenv("MQGOV_TEST_VALIDATOR_CAPTURE", payloadPath)
	f := newDefaultFlags()
	f.Yes = true
	f.Ticket = "OPS-1"
	f.AllowContextChange = true
	preChange := contextControlPolicy{
		source: "prod-current",
		meta: mqgovctx.Context{Base: corectx.Base{
			TicketValidator: executable,
		}},
	}

	if err := authorizeContextControl(f, "new-dev-target", preChange, allowContextChange); err != nil {
		t.Fatalf("authorizeContextControl() error = %v", err)
	}
	data, err := os.ReadFile(payloadPath)
	if err != nil {
		t.Fatal(err)
	}
	var payload map[string]string
	if err := json.Unmarshal(data, &payload); err != nil {
		t.Fatalf("validator payload decode error = %v; payload=%s", err, data)
	}
	if payload["context"] != "prod-current" {
		t.Fatalf("validator context = %q, want pre-change policy source; payload=%+v", payload["context"], payload)
	}
	if payload["context"] == "new-dev-target" {
		t.Fatal("validator was bound to the mutation target instead of the inherited policy source")
	}
}

func TestDanglingCurrentContextFailsClosed(t *testing.T) {
	operator, err := trustedOperatorIdentity(newDefaultFlags())
	if err != nil {
		t.Fatal(err)
	}
	configPath := filepath.Join(t.TempDir(), "config.yaml")
	mqgovctx.SetConfigPath(configPath)
	t.Cleanup(func() { mqgovctx.SetConfigPath("") })
	if err := mqgovctx.Set("target", mqgovctx.Context{
		Base:    corectx.Base{Roles: map[string]string{operator: safety.RoleAdmin}},
		Backend: "fake",
	}); err != nil {
		t.Fatal(err)
	}
	if err := mqgovctx.Update(func(cfg *corectx.Config[mqgovctx.Context]) error {
		cfg.CurrentContext = "missing-current"
		return nil
	}); err != nil {
		t.Fatal(err)
	}

	_, err = runCommandForTest(t,
		"--config", configPath,
		"--yes",
		"--ticket", "OPS-1",
		"--allow-context-change",
		"--backend", "kafka",
		"ctx", "set", "new-target",
		"--brokers", "127.0.0.1:9092",
	)
	if got := apperrors.AsAppError(err).Code; got != apperrors.CodeLocalIOError {
		t.Fatalf("ctx set error code = %s, want fail-closed local I/O error; err=%v", got, err)
	}
	_, err = runCommandForTest(t,
		"--config", configPath,
		"--yes",
		"--ticket", "OPS-1",
		"--allow-context-change",
		"ctx", "use", "target",
	)
	if got := apperrors.AsAppError(err).Code; got != apperrors.CodeLocalIOError {
		t.Fatalf("ctx use error code = %s, want fail-closed local I/O error; err=%v", got, err)
	}
	cfg, err := mqgovctx.Load()
	if err != nil {
		t.Fatal(err)
	}
	if _, exists := cfg.Contexts["new-target"]; exists {
		t.Fatal("dangling current context allowed a new context write")
	}
	if cfg.CurrentContext != "missing-current" {
		t.Fatalf("dangling current context was changed to %q", cfg.CurrentContext)
	}
}

func TestContextPlansDoNotAuthorizeOrMutate(t *testing.T) {
	operator, err := trustedOperatorIdentity(newDefaultFlags())
	if err != nil {
		t.Fatal(err)
	}
	configPath := filepath.Join(t.TempDir(), "config.yaml")
	mqgovctx.SetConfigPath(configPath)
	t.Cleanup(func() { mqgovctx.SetConfigPath("") })
	item := mqgovctx.Context{
		Base:    corectx.Base{Protected: true, Roles: map[string]string{operator: safety.RoleReader}},
		Backend: "fake",
		Cluster: "unchanged",
	}
	if err := mqgovctx.Set("dev", item); err != nil {
		t.Fatal(err)
	}
	if err := mqgovctx.Set("other", mqgovctx.Context{Backend: "fake"}); err != nil {
		t.Fatal(err)
	}
	if err := mqgovctx.Use("dev"); err != nil {
		t.Fatal(err)
	}
	t.Setenv("MQGOV_CREDENTIAL_PASSPHRASE", "test-passphrase")
	exportPath := filepath.Join(t.TempDir(), "contexts.yaml")

	commands := [][]string{
		{"--backend", "kafka", "ctx", "set", "dev", "--brokers", "127.0.0.1:9092", "--password", "new-secret", "--credential-backend", "encrypted-file"},
		{"ctx", "delete", "dev"},
		{"ctx", "role", "set", "dev", "--target-operator", "alice", "--role", safety.RoleAdmin},
		{"ctx", "use", "other"},
		{"--backend", "kafka", "ctx", "set", "new", "--brokers", "127.0.0.1:9092"},
		{"ctx", "export", "dev", "--output", exportPath},
	}
	for _, command := range commands {
		args := []string{"--config", configPath, "--plan", "--operator", "spoofed"}
		if out, err := runCommandForTest(t, append(args, command...)...); err != nil {
			t.Fatalf("plan %v error = %v; out=%s", command, err, out)
		}
	}

	cfg, err := mqgovctx.Load()
	if err != nil {
		t.Fatal(err)
	}
	got := cfg.Contexts["dev"]
	if got.Cluster != "unchanged" || got.Password != "" || got.Roles["alice"] != "" {
		t.Fatalf("plan mutated guarded context: %+v", got)
	}
	if _, exists := cfg.Contexts["new"]; exists {
		t.Fatal("plan created a new context")
	}
	if cfg.CurrentContext != "dev" {
		t.Fatalf("plan changed current context to %q", cfg.CurrentContext)
	}
	if _, err := os.Stat(exportPath); !os.IsNotExist(err) {
		t.Fatalf("plan created context export file: %v", err)
	}
}

func TestGlobalPlanShortCircuitsBrokerMutations(t *testing.T) {
	commands := [][]string{
		{"topic", "create", "new-topic", "--partitions", "2"},
		{"topic", "alter", "orders", "--partitions", "4"},
		{"topic", "delete", "orders"},
		{"group", "create", "new-group"},
		{"group", "delete", "billing"},
		{"message", "produce", "orders", "--key", "secret-key", "--body", "secret-body"},
		{"acl", "grant", "--principal", "User:svc", "--resource-type", "topic", "--resource-name", "orders", "--operation", "read", "--permission", "allow"},
		{"acl", "revoke", "--principal", "User:svc", "--resource-type", "topic", "--resource-name", "orders", "--operation", "read", "--permission", "allow"},
		{"schema", "register", "orders-value", "--schema", `{"type":"record","name":"Order"}`},
		{"schema", "delete", "orders-value", "--permanent"},
	}
	for _, planFlag := range []string{"--plan", "--dry-run"} {
		for _, command := range commands {
			name := strings.TrimPrefix(planFlag, "--") + "/" + strings.Join(command[:2], "-")
			t.Run(name, func(t *testing.T) {
				args := []string{"-o", "json", "--backend", "unsupported-test-backend", planFlag}
				out, err := runCommandForTest(t, append(args, command...)...)
				if err != nil {
					t.Fatalf("%s %v reached authorization/backend: %v; out=%s", planFlag, command, err, out)
				}
				var payload struct {
					Kind string `json:"kind"`
				}
				if err := json.Unmarshal([]byte(out), &payload); err != nil {
					t.Fatalf("plan output decode error = %v; out=%s", err, out)
				}
				if payload.Kind != "ChangePlan" {
					t.Fatalf("plan kind = %q, want ChangePlan; out=%s", payload.Kind, out)
				}
				if strings.Contains(out, "secret-key") || strings.Contains(out, "secret-body") {
					t.Fatalf("plan leaked message plaintext: %s", out)
				}
			})
		}
	}
}

func TestGlobalPlanPreventsInstallAndConfirmedAuditPrune(t *testing.T) {
	installTarget := t.TempDir()
	flags := newDefaultFlags()
	flags.Plan = true
	if err := installSkills(flags, installTarget); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(filepath.Join(installTarget, "mqgov-cli")); !os.IsNotExist(err) {
		t.Fatalf("plan created install directory: %v", err)
	}

	auditPath := filepath.Join(t.TempDir(), "audit.log")
	rotated := auditPath + ".20240101-000000.log"
	writePrivateMutationAuditTestFile(t, rotated, []byte("{}\n"))
	flags = newDefaultFlags()
	flags.Plan = true
	flags.Yes = true
	if err := runAuditPrune(flags, auditPruneOptions{path: auditPath, keepLast: 0, confirm: true}); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(rotated); err != nil {
		t.Fatalf("global plan deleted rotated audit log: %v", err)
	}
}

func TestMultiContextImportAuthorizesEveryTargetBeforeWriting(t *testing.T) {
	operator, err := trustedOperatorIdentity(newDefaultFlags())
	if err != nil {
		t.Fatal(err)
	}
	dir := t.TempDir()
	configPath := filepath.Join(dir, "config.yaml")
	mqgovctx.SetConfigPath(configPath)
	t.Cleanup(func() { mqgovctx.SetConfigPath("") })
	if err := mqgovctx.Set("current-admin", mqgovctx.Context{
		Base:    corectx.Base{Roles: map[string]string{operator: safety.RoleAdmin}},
		Backend: "fake",
	}); err != nil {
		t.Fatal(err)
	}
	if err := mqgovctx.Set("z-denied", mqgovctx.Context{
		Base:    corectx.Base{Roles: map[string]string{operator: safety.RoleReader}},
		Backend: "fake",
		Cluster: "original",
	}); err != nil {
		t.Fatal(err)
	}
	if err := mqgovctx.Use("current-admin"); err != nil {
		t.Fatal(err)
	}
	doc := contextExportDocument{
		APIVersion: ctxExportAPIVersion,
		Contexts: map[string]mqgovctx.Context{
			"a-new":    {Backend: "kafka", KafkaBrokers: []string{"127.0.0.1:9092"}},
			"z-denied": {Backend: "kafka", Cluster: "replacement", KafkaBrokers: []string{"127.0.0.1:9092"}},
		},
	}
	data, err := yaml.Marshal(doc)
	if err != nil {
		t.Fatal(err)
	}
	importPath := filepath.Join(dir, "contexts.yaml")
	if err := os.WriteFile(importPath, data, 0o600); err != nil {
		t.Fatal(err)
	}

	_, err = runCommandForTest(t,
		"--config", configPath,
		"--yes",
		"--ticket", "OPS-1",
		"--allow-context-change",
		"ctx", "import",
		"--input", importPath,
		"--overwrite",
	)
	if got := apperrors.AsAppError(err).Code; got != apperrors.CodeAuthorizationRequired {
		t.Fatalf("error code = %s, want authorization failure; err=%v", got, err)
	}
	cfg, err := mqgovctx.Load()
	if err != nil {
		t.Fatal(err)
	}
	if _, exists := cfg.Contexts["a-new"]; exists {
		t.Fatal("multi-import wrote an earlier target before all targets were authorized")
	}
	if cfg.Contexts["z-denied"].Cluster != "original" {
		t.Fatalf("denied target was overwritten: %+v", cfg.Contexts["z-denied"])
	}
}
