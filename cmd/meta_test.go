package cmd

import (
	"strings"
	"testing"
)

func TestVersionPlain(t *testing.T) {
	SetVersionInfo("v0.0.0-test", "deadbeef", "2026-06-29")
	t.Cleanup(func() { SetVersionInfo("dev", "", "") })

	out, err := runCommandForTest(t, "-o", "plain", "version")
	if err != nil {
		t.Fatalf("Execute() error = %v", err)
	}
	if want := "v0.0.0-test\n"; out != want {
		t.Fatalf("unexpected version plain: %q", out)
	}
}

func TestCapabilitiesPlain(t *testing.T) {
	out, err := runCommandForTest(t, "-o", "plain", "capabilities")
	if err != nil {
		t.Fatalf("Execute() error = %v", err)
	}
	want := strings.Join(capabilityPlainCommands(), "\n") + "\n"
	if out != want {
		t.Fatalf("unexpected capabilities plain:\n%s", out)
	}
	if strings.Contains(out, "{") || strings.Contains(out, "\t") {
		t.Fatalf("capabilities plain should be a command list, got %q", out)
	}
}

func TestGlobalFlagsHelp(t *testing.T) {
	out, err := runCommandForTest(t, "--help")
	if err != nil {
		t.Fatalf("Execute() error = %v", err)
	}
	for _, flag := range []string{"--debug", "--trace", "--no-color"} {
		if !strings.Contains(out, flag) {
			t.Fatalf("help missing %s:\n%s", flag, out)
		}
	}
}

func TestGlobalFlagsWithVersion(t *testing.T) {
	SetVersionInfo("v0.0.0-test", "deadbeef", "2026-06-29")
	t.Cleanup(func() { SetVersionInfo("dev", "", "") })

	out, err := runCommandForTest(t, "--debug", "--trace", "--no-color", "-o", "plain", "version")
	if err != nil {
		t.Fatalf("Execute() error = %v", err)
	}
	if want := "v0.0.0-test\n"; out != want {
		t.Fatalf("version plain = %q, want %q", out, want)
	}
}
