package cmd

import (
	"os"
	"testing"
)

func TestEnvWithDeprecatedAliasPrefersPrimary(t *testing.T) {
	t.Setenv("MQGOV_TEST_PRIMARY_ENV", "new")
	t.Setenv("MQGOV_TEST_DEPRECATED_ENV", "old")

	if got := envWithDeprecatedAlias("MQGOV_TEST_PRIMARY_ENV", "MQGOV_TEST_DEPRECATED_ENV"); got != "new" {
		t.Fatalf("envWithDeprecatedAlias() = %q, want new", got)
	}
}

func TestEnvWithDeprecatedAliasFallsBackToDeprecated(t *testing.T) {
	t.Setenv("MQGOV_TEST_PRIMARY_ENV", "")
	t.Setenv("MQGOV_TEST_DEPRECATED_ENV", "old")

	if got := envWithDeprecatedAlias("MQGOV_TEST_PRIMARY_ENV", "MQGOV_TEST_DEPRECATED_ENV"); got != "old" {
		t.Fatalf("envWithDeprecatedAlias() = %q, want old", got)
	}
}

func TestConfigureEnvWithDeprecatedAliasCopiesDeprecated(t *testing.T) {
	t.Setenv("MQGOV_TEST_PRIMARY_ENV", "")
	t.Setenv("MQGOV_TEST_DEPRECATED_ENV", "old")

	if got := configureEnvWithDeprecatedAlias("MQGOV_TEST_PRIMARY_ENV", "MQGOV_TEST_DEPRECATED_ENV"); got != "MQGOV_TEST_PRIMARY_ENV" {
		t.Fatalf("configureEnvWithDeprecatedAlias() = %q", got)
	}
	if got := os.Getenv("MQGOV_TEST_PRIMARY_ENV"); got != "old" {
		t.Fatalf("primary env = %q, want old", got)
	}
}
