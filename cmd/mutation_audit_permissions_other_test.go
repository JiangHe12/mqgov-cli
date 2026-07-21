//go:build !windows

package cmd

import (
	"os"
	"path/filepath"
	"testing"
)

func TestMutationSpoolRejectsReplaceableAncestor(t *testing.T) {
	root := t.TempDir()
	parent := filepath.Join(root, "audit")
	if err := os.Mkdir(parent, 0o700); err != nil {
		t.Fatalf("Mkdir() error = %v", err)
	}
	if err := os.Chmod(root, 0o777); err != nil {
		t.Fatalf("Chmod(0777) error = %v", err)
	}
	t.Cleanup(func() { _ = os.Chmod(root, 0o700) })
	if err := verifyMutationSpoolParent(parent); err == nil {
		t.Fatal("verifyMutationSpoolParent() accepted a replaceable ancestor")
	}
	if err := os.Chmod(root, 0o1777); err != nil {
		t.Fatalf("Chmod(01777) error = %v", err)
	}
	if err := verifyMutationSpoolParent(parent); err != nil {
		t.Fatalf("verifyMutationSpoolParent() rejected sticky owner-controlled ancestor: %v", err)
	}
}
