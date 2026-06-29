package mqgovctx

import "os"

const (
	credentialPassphraseEnv           = "MQGOV_CREDENTIAL_PASSPHRASE"     // #nosec G101 -- environment variable name, not a credential.
	deprecatedCredentialPassphraseEnv = "MQGOV_CLI_CREDENTIAL_PASSPHRASE" // #nosec G101 -- environment variable name, not a credential.
)

func configureEnvWithDeprecatedAlias(primary, deprecatedName string) string {
	if os.Getenv(primary) == "" {
		if value := os.Getenv(deprecatedName); value != "" {
			_ = os.Setenv(primary, value)
		}
	}
	return primary
}
