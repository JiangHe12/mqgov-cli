package mqgovctx

import (
	"context"
	"fmt"
	"os"
	"sort"

	"github.com/JiangHe12/opskit-core/v2/apperrors"
	"github.com/JiangHe12/opskit-core/v2/credstore"
	corectx "github.com/JiangHe12/opskit-core/v2/ctx"
	"github.com/JiangHe12/opskit-core/v2/safety"
)

const SupportedContextAPIVersion = "mqgov-cli.io/context/v1"

type Context struct {
	corectx.Base                `yaml:",inline"`
	Backend                     string   `yaml:"backend"`
	Cluster                     string   `yaml:"cluster,omitempty"`
	Namespace                   string   `yaml:"namespace,omitempty"`
	Topics                      []string `yaml:"protectedTopics,omitempty"`
	KafkaBrokers                []string `yaml:"kafkaBrokers,omitempty"`
	KafkaSASLMechanism          string   `yaml:"kafkaSaslMechanism,omitempty"`
	KafkaTLS                    bool     `yaml:"kafkaTls,omitempty"`
	KafkaCACertFile             string   `yaml:"kafkaCaCertFile,omitempty"`
	KafkaClientCertFile         string   `yaml:"kafkaClientCertFile,omitempty"`
	KafkaClientKeyFile          string   `yaml:"kafkaClientKeyFile,omitempty"`
	KafkaSchemaRegistryURL      string   `yaml:"kafkaSchemaRegistryUrl,omitempty"`
	KafkaSchemaRegistryUsername string   `yaml:"kafkaSchemaRegistryUsername,omitempty"`
	KafkaSchemaRegistryPassword string   `yaml:"kafkaSchemaRegistryPassword,omitempty"`
	RabbitMQAMQPURL             string   `yaml:"rabbitmqAmqpUrl,omitempty"`
	RabbitMQManagementURL       string   `yaml:"rabbitmqManagementUrl,omitempty"`
	RabbitMQHost                string   `yaml:"rabbitmqHost,omitempty"`
	RabbitMQPort                int      `yaml:"rabbitmqPort,omitempty"`
	RabbitMQVHost               string   `yaml:"rabbitmqVhost,omitempty"`
	RabbitMQTLS                 bool     `yaml:"rabbitmqTls,omitempty"`
	RabbitMQCACertFile          string   `yaml:"rabbitmqCaCertFile,omitempty"`
	RabbitMQClientCertFile      string   `yaml:"rabbitmqClientCertFile,omitempty"`
	RabbitMQClientKeyFile       string   `yaml:"rabbitmqClientKeyFile,omitempty"`
	PulsarServiceURL            string   `yaml:"pulsarServiceUrl,omitempty"`
	PulsarAdminURL              string   `yaml:"pulsarAdminUrl,omitempty"`
	PulsarTenant                string   `yaml:"pulsarTenant,omitempty"`
	PulsarNamespace             string   `yaml:"pulsarNamespace,omitempty"`
	PulsarTLS                   bool     `yaml:"pulsarTls,omitempty"`
	PulsarCACertFile            string   `yaml:"pulsarCaCertFile,omitempty"`
	PulsarClientCertFile        string   `yaml:"pulsarClientCertFile,omitempty"`
	PulsarClientKeyFile         string   `yaml:"pulsarClientKeyFile,omitempty"`
	RocketMQNameServers         []string `yaml:"rocketmqNameServers,omitempty"`
	RocketMQBrokerAddr          string   `yaml:"rocketmqBrokerAddr,omitempty"`
	RocketMQAccessKey           string   `yaml:"rocketmqAccessKey,omitempty"`
	RocketMQTLS                 bool     `yaml:"rocketmqTls,omitempty"`
	RocketMQCACertFile          string   `yaml:"rocketmqCaCertFile,omitempty"`
	RocketMQClientCertFile      string   `yaml:"rocketmqClientCertFile,omitempty"`
	RocketMQClientKeyFile       string   `yaml:"rocketmqClientKeyFile,omitempty"`
}

func (c *Context) base() *corectx.Base { return &c.Base }

var store = corectx.NewStore(func(c *Context) *corectx.Base {
	if c == nil {
		return nil
	}
	return c.base()
})

func Configure() {
	corectx.Configure(corectx.Options{APIVersion: SupportedContextAPIVersion, ConfigDirName: ".mqgov-cli"})
	credstore.Configure(credstore.Options{
		MasterPasswordEnv:     configureEnvWithDeprecatedAlias(credentialPassphraseEnv, deprecatedCredentialPassphraseEnv),
		PromptName:            "mqgov-cli",
		ConfigDirName:         ".mqgov-cli",
		KeychainService:       "mqgov-cli",
		KeychainAccountPrefix: "mqgov-cli:",
		EncryptedFileMagic:    []byte("MQGOV001"), // #nosec G101 -- file-format magic, not a secret.
	})
}

func SetConfigPath(path string) { corectx.SetConfigPath(path) }

func Load() (*corectx.Config[Context], error) {
	cfg, err := store.Load()
	if err != nil {
		return nil, err
	}
	if err := validatePersistedRoles(cfg); err != nil {
		return nil, err
	}
	return cfg, nil
}

func Current() (*Context, string, error) {
	cfg, err := Load()
	if err != nil {
		return nil, "", err
	}
	if cfg.CurrentContext == "" {
		return nil, "", apperrors.New(apperrors.CodeUsageError, "no current context set", nil)
	}
	item, ok := cfg.Contexts[cfg.CurrentContext]
	if !ok {
		return nil, "", apperrors.New(apperrors.CodeUsageError, fmt.Sprintf("context %q not found", cfg.CurrentContext), nil)
	}
	return &item, cfg.CurrentContext, nil
}

func Set(name string, item Context) error {
	if err := validateContextRoles(name, item); err != nil {
		return err
	}
	return store.SetContext(name, item)
}

func Update(fn func(cfg *corectx.Config[Context]) error) error {
	return store.Update(func(cfg *corectx.Config[Context]) error {
		if err := validatePersistedRoles(cfg); err != nil {
			return err
		}
		if err := fn(cfg); err != nil {
			return err
		}
		return validatePersistedRoles(cfg)
	})
}

func Use(name string) error { return store.UseContext(name) }

func Delete(name string) error { return store.DeleteContext(name) }

func validatePersistedRoles(cfg *corectx.Config[Context]) error {
	names := make([]string, 0, len(cfg.Contexts))
	for name := range cfg.Contexts {
		names = append(names, name)
	}
	sort.Strings(names)
	for _, name := range names {
		if err := validateContextRoles(name, cfg.Contexts[name]); err != nil {
			return err
		}
	}
	return nil
}

func validateContextRoles(contextName string, item Context) error {
	operators := make([]string, 0, len(item.Roles))
	for operator := range item.Roles {
		operators = append(operators, operator)
	}
	sort.Strings(operators)
	for _, operator := range operators {
		role := item.Roles[operator]
		if role != safety.RoleReader && role != safety.RoleWriter && role != safety.RoleAdmin {
			return apperrors.New(
				apperrors.CodeValidationFailed,
				fmt.Sprintf("context %q has unsupported role %q for operator %q", contextName, role, operator),
				nil,
			)
		}
	}
	return nil
}

func ResolvePassword(ctx context.Context, name string, item Context) (string, error) {
	if item.Password == "" {
		if password := os.Getenv("MQGOV_PASSWORD"); password != "" {
			return password, nil
		}
	}
	return item.ResolvePasswordContext(ctx, name)
}

func ResolveKafkaSchemaRegistryPassword(ctx context.Context, name string, item Context) (string, error) {
	ref := credstore.ParseRef(item.KafkaSchemaRegistryPassword)
	if !ref.IsRef {
		return item.KafkaSchemaRegistryPassword, nil
	}
	backend, err := credstore.New(ref.BackendName)
	if err != nil {
		return "", err
	}
	password, err := backend.Get(ctx, name+"/schema-registry")
	if err != nil {
		return "", apperrors.New(apperrors.CodeCredentialStoreError, fmt.Sprintf("resolve schema registry password for context %q", name), err)
	}
	return password, nil
}

func StoreCredential(ctx context.Context, name, backendName, password string, item Context) (Context, error) {
	if backendName == "" || backendName == "plain-yaml" {
		item.Password = password
		item.CredentialBackend = backendName
		return item, nil
	}
	backend, err := credstore.New(backendName)
	if err != nil {
		return item, err
	}
	if err := backend.Put(ctx, name, password); err != nil {
		return item, fmt.Errorf("store credential: %w", err)
	}
	item.Password = credstore.EncodeRef(backendName)
	item.CredentialBackend = backendName
	return item, nil
}

func StoreKafkaSchemaRegistryCredential(ctx context.Context, name, backendName, password string, item Context) (Context, error) {
	if password == "" {
		return item, nil
	}
	if backendName == "" || backendName == "plain-yaml" {
		item.KafkaSchemaRegistryPassword = password
		return item, nil
	}
	backend, err := credstore.New(backendName)
	if err != nil {
		return item, err
	}
	if err := backend.Put(ctx, name+"/schema-registry", password); err != nil {
		return item, fmt.Errorf("store schema registry credential: %w", err)
	}
	item.KafkaSchemaRegistryPassword = credstore.EncodeRef(backendName)
	return item, nil
}
