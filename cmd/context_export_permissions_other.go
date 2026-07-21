//go:build !windows

package cmd

import (
	"os"
	"path/filepath"
)

func openPrivateContextExportTemp(path string) (*os.File, error) {
	return os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_RDWR, 0o600) //nolint:gosec // The random exclusive path is derived from a validated export parent.
}

func replaceContextExportFile(from, to string) error {
	if err := os.Rename(from, to); err != nil {
		return err
	}
	return syncMutationSpoolDirectory(filepath.Dir(to))
}

func verifyContextExportOwnerOnly(path string) error {
	return verifyMutationSpoolFile(path)
}

func contextExportPathIsReparse(_ string) (bool, error) {
	return false, nil
}
