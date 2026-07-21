//go:build windows

package cmd

import (
	"os"
	"path/filepath"
	"testing"

	"golang.org/x/sys/windows"
)

func TestEnsureMutationSpoolDirectoryRejectsUntrustedParentWithoutChangingACL(t *testing.T) {
	root := t.TempDir()
	userSID, systemSID, adminSID, err := trustedMutationSpoolSIDs()
	if err != nil {
		t.Fatalf("trustedMutationSpoolSIDs() error = %v", err)
	}
	worldSID, err := windows.CreateWellKnownSid(windows.WinWorldSid)
	if err != nil {
		t.Fatalf("CreateWellKnownSid(World) error = %v", err)
	}
	fullControl := windows.ACCESS_MASK(
		windows.STANDARD_RIGHTS_ALL |
			windows.FILE_GENERIC_READ |
			windows.FILE_GENERIC_WRITE |
			windows.FILE_GENERIC_EXECUTE |
			windows.DELETE,
	)
	const inheritance = windows.SUB_CONTAINERS_AND_OBJECTS_INHERIT
	entries := []windows.EXPLICIT_ACCESS{
		mutationSpoolExplicitAccess(userSID, windows.TRUSTEE_IS_USER, fullControl, inheritance),
		mutationSpoolExplicitAccess(systemSID, windows.TRUSTEE_IS_WELL_KNOWN_GROUP, fullControl, inheritance),
		mutationSpoolExplicitAccess(adminSID, windows.TRUSTEE_IS_GROUP, fullControl, inheritance),
		mutationSpoolExplicitAccess(
			worldSID,
			windows.TRUSTEE_IS_WELL_KNOWN_GROUP,
			windows.FILE_GENERIC_WRITE|windows.DELETE,
			inheritance,
		),
	}
	dacl, err := windows.ACLFromEntries(entries, nil)
	if err != nil {
		t.Fatalf("ACLFromEntries() error = %v", err)
	}
	if err := windows.SetNamedSecurityInfo(
		root,
		windows.SE_FILE_OBJECT,
		windows.DACL_SECURITY_INFORMATION|windows.PROTECTED_DACL_SECURITY_INFORMATION,
		nil,
		nil,
		dacl,
		nil,
	); err != nil {
		t.Fatalf("SetNamedSecurityInfo(root) error = %v", err)
	}

	auditDirectory := filepath.Join(root, ".mqgov-cli")
	if err := os.Mkdir(auditDirectory, 0o700); err != nil {
		t.Fatalf("Mkdir(audit directory) error = %v", err)
	}
	if err := verifyMutationSpoolParent(auditDirectory); err == nil {
		t.Fatal("verifyMutationSpoolParent() accepted inherited untrusted write access")
	}
	before := mutationTestSecurityDescriptor(t, auditDirectory)

	spoolPath := filepath.Join(auditDirectory, "audit.log"+mutationAuditSpoolSuffix)
	if err := ensureMutationSpoolDirectory(spoolPath); err == nil {
		t.Fatal("ensureMutationSpoolDirectory() accepted an untrusted parent")
	}
	after := mutationTestSecurityDescriptor(t, auditDirectory)
	if after != before {
		t.Fatalf("audit directory ACL changed on rejection:\nbefore: %s\nafter:  %s", before, after)
	}
	if _, err := os.Lstat(spoolPath); !os.IsNotExist(err) {
		t.Fatalf("spool path exists after rejected parent validation: %v", err)
	}
}

func TestEnsureMutationSpoolDirectoryLeavesTrustedParentACLUnchanged(t *testing.T) {
	auditDirectory := filepath.Join(t.TempDir(), "private-audit")
	if err := createPrivateMutationAuditDirectory(auditDirectory); err != nil {
		t.Fatalf("createPrivateMutationAuditDirectory() error = %v", err)
	}
	before := mutationTestSecurityDescriptor(t, auditDirectory)
	spoolPath := filepath.Join(auditDirectory, "audit.log"+mutationAuditSpoolSuffix)
	if err := ensureMutationSpoolDirectory(spoolPath); err != nil {
		t.Fatalf("ensureMutationSpoolDirectory() error = %v", err)
	}
	after := mutationTestSecurityDescriptor(t, auditDirectory)
	if after != before {
		t.Fatalf("trusted audit directory ACL changed:\nbefore: %s\nafter:  %s", before, after)
	}
}

func mutationTestSecurityDescriptor(t *testing.T, path string) string {
	t.Helper()
	descriptor, err := windows.GetNamedSecurityInfo(
		path,
		windows.SE_FILE_OBJECT,
		windows.OWNER_SECURITY_INFORMATION|
			windows.GROUP_SECURITY_INFORMATION|
			windows.DACL_SECURITY_INFORMATION,
	)
	if err != nil {
		t.Fatalf("GetNamedSecurityInfo(%s) error = %v", path, err)
	}
	return descriptor.String()
}
