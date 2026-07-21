package cmd

import (
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/JiangHe12/opskit-core/v2/apperrors"
)

// strictAuditRotatedFiles is a compatibility guard for the published core
// version used by this CLI. Evidence deletion must accept only core's exact
// timestamp[.positive-decimal-ordinal].log naming contract.
func strictAuditRotatedFiles(path string) ([]string, error) {
	directory := filepath.Dir(path)
	entries, err := os.ReadDir(directory)
	if os.IsNotExist(err) {
		return []string{}, nil
	}
	if err != nil {
		return nil, apperrors.New(apperrors.CodeLocalIOError, "failed to list rotated audit logs", err)
	}
	activeName := filepath.Base(path)
	type rotation struct {
		path      string
		timestamp time.Time
		ordinal   uint64
	}
	rotated := make([]rotation, 0)
	for _, entry := range entries {
		name := entry.Name()
		if !strings.HasPrefix(name, activeName+".") || !strings.HasSuffix(name, ".log") {
			continue
		}
		candidate := filepath.Join(directory, name)
		timestamp, ordinal, ok := strictAuditRotatedFileOrder(path, candidate)
		if !ok {
			return nil, apperrors.New(
				apperrors.CodeValidationFailed,
				fmt.Sprintf("unexpected audit rotation filename %q; refusing prune", name),
				nil,
			)
		}
		info, err := os.Lstat(candidate)
		if err != nil {
			return nil, apperrors.New(apperrors.CodeLocalIOError, "failed to inspect rotated audit log", err)
		}
		if info.Mode()&os.ModeSymlink != 0 || !info.Mode().IsRegular() {
			return nil, apperrors.New(
				apperrors.CodeValidationFailed,
				fmt.Sprintf("rotated audit log %q must be a real regular file", name),
				nil,
			)
		}
		rotated = append(rotated, rotation{
			path:      candidate,
			timestamp: timestamp,
			ordinal:   ordinal,
		})
	}
	sort.Slice(rotated, func(i, j int) bool {
		if !rotated[i].timestamp.Equal(rotated[j].timestamp) {
			return rotated[i].timestamp.Before(rotated[j].timestamp)
		}
		if rotated[i].ordinal != rotated[j].ordinal {
			return rotated[i].ordinal < rotated[j].ordinal
		}
		return rotated[i].path < rotated[j].path
	})
	out := make([]string, len(rotated))
	for i := range rotated {
		out[i] = rotated[i].path
	}
	return out, nil
}

func strictAuditRotatedFileOrder(activePath, candidate string) (time.Time, uint64, bool) {
	if filepath.Clean(filepath.Dir(activePath)) != filepath.Clean(filepath.Dir(candidate)) {
		return time.Time{}, 0, false
	}
	activeName := filepath.Base(activePath)
	candidateName := filepath.Base(candidate)
	if !strings.HasPrefix(candidateName, activeName+".") || !strings.HasSuffix(candidateName, ".log") {
		return time.Time{}, 0, false
	}
	stem := strings.TrimSuffix(strings.TrimPrefix(candidateName, activeName+"."), ".log")
	parts := strings.Split(stem, ".")
	if len(parts) < 1 || len(parts) > 2 {
		return time.Time{}, 0, false
	}
	timestamp, err := time.Parse("20060102-150405", parts[0])
	if err != nil {
		return time.Time{}, 0, false
	}
	ordinal := uint64(0)
	if len(parts) == 2 {
		if parts[1] == "" || (len(parts[1]) > 1 && strings.HasPrefix(parts[1], "0")) {
			return time.Time{}, 0, false
		}
		ordinal, err = strconv.ParseUint(parts[1], 10, 64)
		if err != nil || ordinal == 0 {
			return time.Time{}, 0, false
		}
	}
	return timestamp.UTC(), ordinal, true
}
