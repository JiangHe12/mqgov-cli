//go:build windows

package cmd

import (
	"fmt"
	"os"
	"unsafe"

	"github.com/JiangHe12/opskit-core/v2/apperrors"
	"golang.org/x/sys/windows"
)

func openPrivateContextExportTemp(path string) (*os.File, error) {
	userSID, systemSID, adminSID, err := trustedMutationSpoolSIDs()
	if err != nil {
		return nil, err
	}
	descriptor, err := windows.SecurityDescriptorFromString(fmt.Sprintf(
		"O:%sD:P(A;;FA;;;%s)(A;;FA;;;%s)(A;;FA;;;%s)",
		userSID,
		userSID,
		systemSID,
		adminSID,
	))
	if err != nil {
		return nil, apperrors.New(apperrors.CodeLocalIOError, "failed to create context export security descriptor", nil)
	}
	pathPtr, err := windows.UTF16PtrFromString(path)
	if err != nil {
		return nil, apperrors.New(apperrors.CodeLocalIOError, "failed to encode context export temporary path", nil)
	}
	attributes := windows.SecurityAttributes{
		SecurityDescriptor: descriptor,
		Length:             uint32(unsafe.Sizeof(windows.SecurityAttributes{})),
	}
	handle, err := windows.CreateFile(
		pathPtr,
		windows.GENERIC_READ|windows.GENERIC_WRITE,
		0,
		&attributes,
		windows.CREATE_NEW,
		windows.FILE_ATTRIBUTE_NORMAL,
		0,
	)
	if err != nil {
		return nil, err
	}
	return os.NewFile(uintptr(handle), path), nil
}

func replaceContextExportFile(from, to string) error {
	fromPtr, err := windows.UTF16PtrFromString(from)
	if err != nil {
		return err
	}
	toPtr, err := windows.UTF16PtrFromString(to)
	if err != nil {
		return err
	}
	return windows.MoveFileEx(
		fromPtr,
		toPtr,
		windows.MOVEFILE_REPLACE_EXISTING|windows.MOVEFILE_WRITE_THROUGH,
	)
}

func verifyContextExportOwnerOnly(path string) error {
	return verifyMutationSpoolFile(path)
}

func contextExportPathIsReparse(path string) (bool, error) {
	pathPtr, err := windows.UTF16PtrFromString(path)
	if err != nil {
		return false, apperrors.New(apperrors.CodeLocalIOError, "failed to encode context export path", nil)
	}
	attributes, err := windows.GetFileAttributes(pathPtr)
	if err != nil {
		return false, apperrors.New(apperrors.CodeLocalIOError, "failed to inspect context export path attributes", nil)
	}
	return attributes&windows.FILE_ATTRIBUTE_REPARSE_POINT != 0, nil
}
