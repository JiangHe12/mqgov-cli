package mqgovctx

import (
	"context"
	"fmt"
	"os"

	"github.com/JiangHe12/opskit-core/apperrors"
	"github.com/JiangHe12/opskit-core/credstore"
	corectx "github.com/JiangHe12/opskit-core/ctx"
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
		MasterPasswordEnv:     "MQGOV_CLI_CREDENTIAL_PASSPHRASE",
		PromptName:            "mqgov-cli",
		ConfigDirName:         ".mqgov-cli",
		KeychainService:       "mqgov-cli",
		KeychainAccountPrefix: "mqgov-cli:",
		EncryptedFileMagic:    []byte("MQGOV001"), // #nosec G101 -- file-format magic, not a secret.
	})
}

func SetConfigPath(path string) { corectx.SetConfigPath(path) }

func Load() (*corectx.Config[Context], error) { return store.Load() }

func Current() (*Context, string, error) { return store.Current() }

func Set(name string, item Context) error { return store.SetContext(name, item) }

func Use(name string) error { return store.UseContext(name) }

func Delete(name string) error { return store.DeleteContext(name) }

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
