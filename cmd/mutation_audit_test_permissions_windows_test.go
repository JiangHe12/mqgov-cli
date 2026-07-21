//go:build windows

package cmd

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"golang.org/x/sys/windows"
)

func secureMutationAuditTestAncestors(t *testing.T, path string) {
	t.Helper()
	for parent := filepath.Dir(path); !strings.EqualFold(parent, os.TempDir()); parent = filepath.Dir(parent) {
		if err := setMutationSpoolACL(parent, windows.SUB_CONTAINERS_AND_OBJECTS_INHERIT); err != nil {
			t.Fatalf("setMutationSpoolACL(%s) error = %v", parent, err)
		}
	}
}
