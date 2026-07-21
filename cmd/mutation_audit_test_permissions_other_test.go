//go:build !windows

package cmd

import (
	"os"
	"path/filepath"
	"testing"
)

func createCommandTestHome() (string, error) {
	tempRoot, err := filepath.EvalSymlinks(os.TempDir())
	if err != nil {
		return "", err
	}
	if err := verifyMutationSpoolParent(tempRoot); err != nil {
		return "", err
	}
	path, err := os.MkdirTemp(tempRoot, "mqgov-cli-test-home-*")
	if err != nil {
		return "", err
	}
	if err := verifyMutationSpoolDirectory(path); err != nil {
		_ = os.RemoveAll(path)
		return "", err
	}
	return path, nil
}

func secureMutationAuditTestAncestors(_ *testing.T, _ string) {}

func secureMutationAuditTestFile(t *testing.T, path string) {
	t.Helper()
	if err := os.Chmod(path, 0o600); err != nil {
		t.Fatalf("secure mutation audit test file %s: %v", path, err)
	}
}
