//go:build !windows

package cmd

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"syscall"

	"github.com/JiangHe12/opskit-core/v2/apperrors"
)

func ensureMutationSpoolDirectory(path string) error {
	parent := filepath.Dir(path)
	if err := verifyMutationSpoolParent(parent); err != nil {
		return err
	}
	created := false
	if err := os.Mkdir(path, 0o700); err != nil {
		if !os.IsExist(err) {
			return apperrors.New(apperrors.CodeLocalIOError, "failed to create mutation outcome spool directory", nil)
		}
	} else {
		created = true
	}
	if created {
		if err := syncMutationSpoolDirectory(parent); err != nil {
			return err
		}
	}
	return verifyMutationSpoolDirectory(path)
}

func createPrivateMutationAuditDirectory(path string) error {
	return ensureMutationSpoolDirectory(path)
}

func verifyMutationSpoolParent(path string) error {
	clean, err := filepath.Abs(path)
	if err != nil {
		return apperrors.New(apperrors.CodeLocalIOError, "failed to resolve mutation outcome spool parent", nil)
	}
	current := filepath.Clean(clean)
	for {
		info, err := os.Lstat(current)
		if err != nil {
			return apperrors.New(apperrors.CodeLocalIOError, "failed to inspect mutation outcome spool ancestor", nil)
		}
		if info.Mode()&os.ModeSymlink != 0 || !info.IsDir() {
			return apperrors.New(apperrors.CodeLocalIOError, "mutation outcome spool ancestors must be real directories", nil)
		}
		stat, ok := info.Sys().(*syscall.Stat_t)
		if !ok {
			return apperrors.New(apperrors.CodeLocalIOError, "failed to identify mutation outcome spool ancestor owner", nil)
		}
		uid := uint64(stat.Uid)
		euid, err := effectiveMutationSpoolUID()
		if err != nil {
			return err
		}
		if uid != euid && uid != 0 {
			return apperrors.New(apperrors.CodeLocalIOError, "mutation outcome spool ancestor has an untrusted owner", nil)
		}
		if info.Mode().Perm()&0o022 != 0 && info.Mode()&os.ModeSticky == 0 {
			return apperrors.New(apperrors.CodeLocalIOError, "mutation outcome spool ancestor is replaceable by another user", nil)
		}
		next := filepath.Dir(current)
		if next == current {
			break
		}
		current = next
	}
	return nil
}

func verifyMutationSpoolDirectory(path string) error {
	info, err := os.Lstat(path)
	if err != nil {
		return apperrors.New(apperrors.CodeLocalIOError, "failed to inspect mutation outcome spool directory", nil)
	}
	if info.Mode()&os.ModeSymlink != 0 || !info.IsDir() {
		return apperrors.New(apperrors.CodeLocalIOError, "mutation outcome spool must be a real directory", nil)
	}
	if err := verifyMutationSpoolOwner(info, path); err != nil {
		return err
	}
	if info.Mode().Perm() != 0o700 {
		return apperrors.New(apperrors.CodeLocalIOError, "mutation outcome spool directory must have mode 0700", nil)
	}
	return nil
}

func secureMutationSpoolFile(path string) error {
	if err := os.Chmod(path, 0o600); err != nil {
		return apperrors.New(apperrors.CodeLocalIOError, "failed to secure mutation outcome spool file", nil)
	}
	return verifyMutationSpoolFile(path)
}

func verifyMutationSpoolFile(path string) error {
	info, err := os.Lstat(path)
	if err != nil {
		return apperrors.New(apperrors.CodeLocalIOError, "failed to inspect mutation outcome spool file", nil)
	}
	if info.Mode()&os.ModeSymlink != 0 || !info.Mode().IsRegular() {
		return apperrors.New(apperrors.CodeLocalIOError, "mutation outcome spool entry must be a regular file", nil)
	}
	if err := verifyMutationSpoolOwner(info, path); err != nil {
		return err
	}
	if info.Mode().Perm() != 0o600 {
		return apperrors.New(apperrors.CodeLocalIOError, "mutation outcome spool file must have mode 0600", nil)
	}
	return nil
}

func verifyMutationAuditActivePath(path string, allowMissing bool) error {
	info, err := os.Lstat(path)
	if err != nil {
		if allowMissing && errors.Is(err, os.ErrNotExist) {
			return nil
		}
		return apperrors.New(apperrors.CodeLocalIOError, "failed to inspect mutation audit log", nil)
	}
	if info.Mode()&os.ModeSymlink != 0 || !info.Mode().IsRegular() {
		return apperrors.New(apperrors.CodeLocalIOError, "mutation audit log must be a real regular file", nil)
	}
	return nil
}

func verifyMutationSpoolOwner(info os.FileInfo, path string) error {
	stat, ok := info.Sys().(*syscall.Stat_t)
	euid, err := effectiveMutationSpoolUID()
	if err != nil {
		return err
	}
	if !ok || uint64(stat.Uid) != euid {
		return apperrors.New(
			apperrors.CodeLocalIOError,
			fmt.Sprintf("mutation outcome spool path %s is not owned by the current user", path),
			nil,
		)
	}
	return nil
}

func effectiveMutationSpoolUID() (uint64, error) {
	euid := os.Geteuid()
	if euid < 0 {
		return 0, apperrors.New(apperrors.CodeLocalIOError, "failed to identify the current mutation outcome spool owner", nil)
	}
	return uint64(euid), nil //nolint:gosec // The explicit non-negative check above makes the signed-to-unsigned conversion safe.
}

func syncMutationSpoolDirectory(path string) error {
	dir, err := os.Open(path) //nolint:gosec // Path is a validated owner-only directory.
	if err != nil {
		return apperrors.New(apperrors.CodeLocalIOError, "failed to open mutation outcome spool directory", nil)
	}
	defer func() { _ = dir.Close() }()
	if err := dir.Sync(); err != nil {
		return apperrors.New(apperrors.CodeLocalIOError, "failed to sync mutation outcome spool directory", nil)
	}
	return nil
}

func commitMutationSpoolFile(from, to string) error {
	if err := os.Rename(from, to); err != nil {
		return err
	}
	return syncMutationSpoolDirectory(filepath.Dir(to))
}
