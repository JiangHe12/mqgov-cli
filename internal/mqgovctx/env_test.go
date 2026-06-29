package mqgovctx

import (
	"os"
	"testing"
)

func TestConfigureEnvWithDeprecatedAliasCopiesDeprecated(t *testing.T) {
	t.Setenv("MQGOVCTX_TEST_PRIMARY_ENV", "")
	t.Setenv("MQGOVCTX_TEST_DEPRECATED_ENV", "old")

	if got := configureEnvWithDeprecatedAlias("MQGOVCTX_TEST_PRIMARY_ENV", "MQGOVCTX_TEST_DEPRECATED_ENV"); got != "MQGOVCTX_TEST_PRIMARY_ENV" {
		t.Fatalf("configureEnvWithDeprecatedAlias() = %q", got)
	}
	if got := os.Getenv("MQGOVCTX_TEST_PRIMARY_ENV"); got != "old" {
		t.Fatalf("primary env = %q, want old", got)
	}
}
