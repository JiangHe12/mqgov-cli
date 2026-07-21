package mqgovctx

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/JiangHe12/opskit-core/v2/apperrors"
)

func TestLoadRejectsUnknownPersistedRole(t *testing.T) {
	Configure()
	path := filepath.Join(t.TempDir(), "config.yaml")
	SetConfigPath(path)
	t.Cleanup(func() { SetConfigPath("") })
	content := strings.Join([]string{
		"apiVersion: " + SupportedContextAPIVersion,
		"current-context: guarded",
		"contexts:",
		"  guarded:",
		"    backend: fake",
		"    roles:",
		"      alice: owner",
		"",
	}, "\n")
	if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
		t.Fatal(err)
	}

	if _, err := Load(); apperrors.AsAppError(err).Code != apperrors.CodeValidationFailed {
		t.Fatalf("Load() error = %v, want VALIDATION_FAILED", err)
	}
	if _, _, err := Current(); apperrors.AsAppError(err).Code != apperrors.CodeValidationFailed {
		t.Fatalf("Current() error = %v, want VALIDATION_FAILED", err)
	}
}
