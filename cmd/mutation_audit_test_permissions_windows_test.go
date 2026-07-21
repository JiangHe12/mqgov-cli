//go:build windows

package cmd

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"golang.org/x/sys/windows"
)

func createCommandTestHome() (string, error) {
	tempRoot, err := filepath.EvalSymlinks(os.TempDir())
	if err != nil {
		return "", err
	}
	path, err := os.MkdirTemp(tempRoot, "mqgov-cli-test-home-*")
	if err != nil {
		return "", err
	}
	if err := setMutationSpoolACL(path, windows.SUB_CONTAINERS_AND_OBJECTS_INHERIT); err != nil {
		_ = os.RemoveAll(path)
		return "", err
	}
	if err := verifyMutationSpoolDirectory(path); err != nil {
		_ = os.RemoveAll(path)
		return "", err
	}
	return path, nil
}

func secureMutationAuditTestAncestors(t *testing.T, path string) {
	t.Helper()
	for parent := filepath.Dir(path); !strings.EqualFold(parent, os.TempDir()); parent = filepath.Dir(parent) {
		if err := setMutationSpoolACL(parent, windows.SUB_CONTAINERS_AND_OBJECTS_INHERIT); err != nil {
			t.Fatalf("setMutationSpoolACL(%s) error = %v", parent, err)
		}
	}
}

func secureMutationAuditTestFile(t *testing.T, path string) {
	t.Helper()
	if err := setMutationSpoolACL(path, windows.NO_INHERITANCE); err != nil {
		t.Fatalf("secure mutation audit test file %s: %v", path, err)
	}
	if err := verifyMutationSpoolFile(path); err != nil {
		t.Fatalf("verify mutation audit test file %s: %v", path, err)
	}
}
