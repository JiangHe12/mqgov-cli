package cmd

import "os"

const (
	mqgovAuditPrivateKeyEnv           = "MQGOV_AUDIT_PRIVATE_KEY"
	deprecatedMqgovAuditPrivateKeyEnv = "MQGOV_CLI_AUDIT_PRIVATE_KEY"
	mqgovOperatorEnv                  = "MQGOV_OPERATOR"
	deprecatedMqgovOperatorEnv        = "MQGOV_CLI_OPERATOR"
)

func envWithDeprecatedAlias(primary, deprecatedName string) string {
	if value := os.Getenv(primary); value != "" {
		return value
	}
	return os.Getenv(deprecatedName)
}

func configureEnvWithDeprecatedAlias(primary, deprecatedName string) string {
	if os.Getenv(primary) == "" {
		if value := os.Getenv(deprecatedName); value != "" {
			_ = os.Setenv(primary, value)
		}
	}
	return primary
}
