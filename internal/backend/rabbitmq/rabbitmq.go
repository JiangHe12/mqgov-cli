package rabbitmq

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"os"
	"sort"
	"strconv"
	"strings"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"

	"github.com/JiangHe12/opskit-core/apperrors"

	"github.com/JiangHe12/mqgov-cli/internal/mqgov"
)

type Options struct {
	AMQPURL        string
	ManagementURL  string
	Host           string
	Port           int
	VHost          string
	Cluster        string
	Namespace      string
	Username       string
	Password       string
	TLS            bool
	CACertFile     string
	ClientCertFile string
	ClientKeyFile  string
	Timeout        time.Duration
}

type Broker struct {
	opts       Options
	amqpURL    string
	manageURL  string
	httpClient *http.Client
	tlsConfig  *tls.Config
}

func New(opts Options) (*Broker, error) {
	tlsConfig, err := buildTLSConfig(opts)
	if err != nil {
		return nil, err
	}
	amqpURL := buildAMQPURL(opts)
	managementURL := buildManagementURL(opts)
	httpClient := &http.Client{Timeout: timeout(opts)}
	if tlsConfig != nil {
		httpClient.Transport = &http.Transport{TLSClientConfig: tlsConfig}
	}
	return &Broker{opts: opts, amqpURL: amqpURL, manageURL: managementURL, httpClient: httpClient, tlsConfig: tlsConfig}, nil
}

func (b *Broker) Ping(ctx context.Context) error {
	conn, err := b.dial()
	if err != nil {
		return unreachable(err)
	}
	defer func() { _ = conn.Close() }()
	ch, err := conn.Channel()
	if err != nil {
		return backendErr(err)
	}
	defer func() { _ = ch.Close() }()
	return nil
}

func (b *Broker) Describe() mqgov.Description {
	return mqgov.Description{Backend: "rabbitmq", Cluster: b.opts.Cluster, Namespace: b.vhost()}
}

func (b *Broker) Capabilities() mqgov.Capabilities {
	return mqgov.Capabilities{
		Backend:            "rabbitmq",
		ResourceTypes:      []string{"topic", "message"},
		Verbs:              []string{"list", "describe", "peek", "produce", "create", "delete", "purge"},
		SupportsOffsets:    false,
		SupportsPartitions: true,
		SupportsACL:        false,
	}
}

func (b *Broker) ListTopics(ctx context.Context, opts mqgov.TopicListOptions) ([]mqgov.TopicDescription, error) {
	queues, err := b.listQueues(ctx)
	if err != nil {
		return nil, err
	}
	items := make([]mqgov.TopicDescription, 0, len(queues))
	for _, queue := range queues {
		if opts.Pattern != "" && opts.Pattern != queue.Name {
			continue
		}
		items = append(items, mqgov.TopicDescription{
			Coordinate: mqgov.TopicCoordinate{Cluster: b.opts.Cluster, Namespace: b.vhost(), Topic: queue.Name},
			Partitions: 1,
			Config:     map[string]string{"messages": strconv.Itoa(queue.Messages), "consumers": strconv.Itoa(queue.Consumers)},
		})
	}
	sort.Slice(items, func(i, j int) bool { return items[i].Coordinate.Topic < items[j].Coordinate.Topic })
	if opts.Limit > 0 && len(items) > opts.Limit {
		items = items[:opts.Limit]
	}
	return items, nil
}

func (b *Broker) DescribeTopic(ctx context.Context, coord mqgov.TopicCoordinate) (mqgov.TopicDescription, error) {
	queue, err := b.inspectQueue(ctx, coord.Topic)
	if err != nil {
		return mqgov.TopicDescription{}, err
	}
	return b.topicDescription(queue), nil
}

func (b *Broker) CreateTopic(ctx context.Context, req mqgov.TopicCreateRequest) (mqgov.TopicDescription, error) {
	if _, err := b.inspectQueue(ctx, req.Coordinate.Topic); err == nil {
		return mqgov.TopicDescription{}, apperrors.New(apperrors.CodeResourceAlreadyExists, "queue already exists", nil)
	} else if apperrors.AsAppError(err).Code != apperrors.CodeResourceNotFound {
		return mqgov.TopicDescription{}, err
	}
	queue, err := withChannel(ctx, b, func(ch *amqp.Channel) (amqp.Queue, error) {
		return ch.QueueDeclare(req.Coordinate.Topic, true, false, false, false, nil)
	})
	if err != nil {
		return mqgov.TopicDescription{}, classifyAMQPError(err)
	}
	return b.topicDescription(queue), nil
}

func (b *Broker) DeleteTopic(ctx context.Context, coord mqgov.TopicCoordinate) error {
	if _, err := b.inspectQueue(ctx, coord.Topic); err != nil {
		return err
	}
	_, err := withChannel(ctx, b, func(ch *amqp.Channel) (int, error) {
		return ch.QueueDelete(coord.Topic, false, false, false)
	})
	if err != nil {
		return classifyAMQPError(err)
	}
	return nil
}

func (b *Broker) ListGroups(context.Context, mqgov.GroupListOptions) ([]mqgov.GroupDescription, error) {
	return []mqgov.GroupDescription{}, nil
}

func (b *Broker) CreateGroup(context.Context, mqgov.GroupCoordinate) (mqgov.GroupDescription, error) {
	return mqgov.GroupDescription{}, apperrors.New(apperrors.CodeNotImplemented, "RabbitMQ does not support consumer groups", nil)
}

func (b *Broker) DeleteGroup(context.Context, mqgov.GroupCoordinate) error {
	return apperrors.New(apperrors.CodeNotImplemented, "RabbitMQ does not support consumer groups", nil)
}

func (b *Broker) Peek(ctx context.Context, req mqgov.MessagePeekRequest) (mqgov.MessagePeekResult, error) {
	count := req.Count
	if count <= 0 {
		count = 1
	}
	messages := make([]mqgov.MessageFingerprint, 0, count)
	_, err := withChannel(ctx, b, func(ch *amqp.Channel) (struct{}, error) {
		for len(messages) < count {
			msg, ok, err := ch.Get(req.Coordinate.Topic, false)
			if err != nil {
				return struct{}{}, err
			}
			if !ok {
				return struct{}{}, nil
			}
			messages = append(messages, mqgov.FingerprintMessage(0, int64(len(messages)), []byte(msg.RoutingKey), msg.Body))
			if err := msg.Nack(false, true); err != nil {
				return struct{}{}, err
			}
		}
		return struct{}{}, nil
	})
	if err != nil {
		return mqgov.MessagePeekResult{}, classifyAMQPError(err)
	}
	return mqgov.MessagePeekResult{Coordinate: req.Coordinate, Partition: 0, Offset: req.Offset, Count: len(messages), Messages: messages}, nil
}

func (b *Broker) Produce(ctx context.Context, req mqgov.MessageProduceRequest) (mqgov.MessageProduceResult, error) {
	_, err := withChannel(ctx, b, func(ch *amqp.Channel) (struct{}, error) {
		return struct{}{}, ch.PublishWithContext(ctx, "", req.Coordinate.Topic, false, false, amqp.Publishing{
			DeliveryMode: amqp.Persistent,
			Headers:      headers(req.Headers),
			Body:         req.Body,
		})
	})
	if err != nil {
		return mqgov.MessageProduceResult{}, classifyAMQPError(err)
	}
	return mqgov.MessageProduceResult{
		Coordinate:  mqgov.MessageCoordinate{TopicCoordinate: req.Coordinate, Partition: 0, Offset: 0},
		Fingerprint: mqgov.Fingerprints(req.Key, req.Body, 1),
	}, nil
}

func (b *Broker) AlterTopic(context.Context, mqgov.TopicAlterRequest) (mqgov.TopicDescription, error) {
	return mqgov.TopicDescription{}, apperrors.New(apperrors.CodeNotImplemented, "RabbitMQ queues do not support partitions", nil)
}

func (b *Broker) PurgeTopic(ctx context.Context, req mqgov.TopicPurgeRequest) (mqgov.TopicPurgeResult, error) {
	queue, err := b.inspectQueue(ctx, req.Coordinate.Topic)
	if err != nil {
		return mqgov.TopicPurgeResult{}, err
	}
	count := int64(queue.Messages)
	if !req.DryRun {
		purged, err := withChannel(ctx, b, func(ch *amqp.Channel) (int, error) {
			return ch.QueuePurge(req.Coordinate.Topic, false)
		})
		if err != nil {
			return mqgov.TopicPurgeResult{}, classifyAMQPError(err)
		}
		count = int64(purged)
	}
	return mqgov.TopicPurgeResult{
		Coordinate:  req.Coordinate,
		DLQ:         req.DLQ,
		DryRun:      req.DryRun,
		Impact:      []mqgov.PartitionImpact{{Partition: 0, Count: count}},
		Total:       count,
		Fingerprint: mqgov.ResourceFingerprints{Count: count},
	}, nil
}

func (b *Broker) dial() (*amqp.Connection, error) {
	if b.tlsConfig != nil {
		return amqp.DialTLS(b.amqpURL, b.tlsConfig)
	}
	return amqp.DialConfig(b.amqpURL, amqp.Config{Heartbeat: 10 * time.Second})
}

func withChannel[T any](ctx context.Context, b *Broker, fn func(*amqp.Channel) (T, error)) (T, error) {
	var zero T
	conn, err := b.dial()
	if err != nil {
		return zero, unreachable(err)
	}
	defer func() { _ = conn.Close() }()
	ch, err := conn.Channel()
	if err != nil {
		return zero, backendErr(err)
	}
	defer func() { _ = ch.Close() }()
	done := make(chan struct{})
	defer close(done)
	select {
	case <-ctx.Done():
		return zero, ctx.Err()
	default:
		return fn(ch)
	}
}

func (b *Broker) inspectQueue(ctx context.Context, name string) (amqp.Queue, error) {
	queue, err := withChannel(ctx, b, func(ch *amqp.Channel) (amqp.Queue, error) {
		return ch.QueueDeclarePassive(name, true, false, false, false, nil)
	})
	if err != nil {
		return amqp.Queue{}, classifyAMQPError(err)
	}
	return queue, nil
}

type managementQueue struct {
	Name      string `json:"name"`
	Messages  int    `json:"messages_ready"`
	Consumers int    `json:"consumers"`
}

func (b *Broker) listQueues(ctx context.Context) ([]managementQueue, error) {
	endpoint := strings.TrimRight(b.manageURL, "/") + "/api/queues/" + url.PathEscape(b.vhost())
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
	if err != nil {
		return nil, apperrors.New(apperrors.CodeUsageError, "invalid RabbitMQ management URL", err)
	}
	req.SetBasicAuth(b.opts.Username, b.opts.Password)
	resp, err := b.httpClient.Do(req)
	if err != nil {
		return nil, unreachable(err)
	}
	defer func() { _ = resp.Body.Close() }()
	switch resp.StatusCode {
	case http.StatusOK:
	case http.StatusUnauthorized, http.StatusForbidden:
		return nil, apperrors.New(apperrors.CodeAuthFailed, "RabbitMQ management authentication failed", nil)
	default:
		return nil, backendErr(fmt.Errorf("rabbitmq management status %d", resp.StatusCode))
	}
	var queues []managementQueue
	if err := json.NewDecoder(resp.Body).Decode(&queues); err != nil {
		return nil, backendErr(err)
	}
	return queues, nil
}

func (b *Broker) topicDescription(queue amqp.Queue) mqgov.TopicDescription {
	return mqgov.TopicDescription{
		Coordinate: mqgov.TopicCoordinate{Cluster: b.opts.Cluster, Namespace: b.vhost(), Topic: queue.Name},
		Partitions: 1,
		Config:     map[string]string{"messages": strconv.Itoa(queue.Messages), "consumers": strconv.Itoa(queue.Consumers)},
	}
}

func (b *Broker) vhost() string {
	if b.opts.VHost == "" {
		return "/"
	}
	return b.opts.VHost
}

func buildAMQPURL(opts Options) string {
	if opts.AMQPURL != "" {
		return opts.AMQPURL
	}
	host := opts.Host
	if host == "" {
		host = "127.0.0.1"
	}
	port := opts.Port
	scheme := "amqp"
	if opts.TLS {
		scheme = "amqps"
		if port == 0 {
			port = 5671
		}
	} else if port == 0 {
		port = 5672
	}
	u := url.URL{Scheme: scheme, Host: netJoin(host, port), Path: "/" + strings.TrimPrefix(defaultVHost(opts.VHost), "/")}
	if opts.Username != "" {
		u.User = url.UserPassword(opts.Username, opts.Password)
	}
	return u.String()
}

func buildManagementURL(opts Options) string {
	if opts.ManagementURL != "" {
		return opts.ManagementURL
	}
	host := opts.Host
	if host == "" {
		host = "127.0.0.1"
	}
	scheme := "http"
	port := 15672
	if opts.TLS {
		scheme = "https"
		port = 15671
	}
	u := url.URL{Scheme: scheme, Host: netJoin(host, port)}
	return u.String()
}

func buildTLSConfig(opts Options) (*tls.Config, error) {
	if !opts.TLS && opts.CACertFile == "" && opts.ClientCertFile == "" && opts.ClientKeyFile == "" {
		return nil, nil
	}
	cfg := &tls.Config{MinVersion: tls.VersionTLS12}
	if opts.CACertFile != "" {
		pool, err := loadCertPool(opts.CACertFile)
		if err != nil {
			return nil, err
		}
		cfg.RootCAs = pool
	}
	if opts.ClientCertFile != "" || opts.ClientKeyFile != "" {
		if opts.ClientCertFile == "" || opts.ClientKeyFile == "" {
			return nil, apperrors.New(apperrors.CodeUsageError, "RabbitMQ mTLS requires both client certificate and key files", nil)
		}
		cert, err := tls.LoadX509KeyPair(opts.ClientCertFile, opts.ClientKeyFile)
		if err != nil {
			return nil, apperrors.New(apperrors.CodeCredentialStoreError, "failed to load RabbitMQ client certificate", err)
		}
		cfg.Certificates = []tls.Certificate{cert}
	}
	return cfg, nil
}

func loadCertPool(path string) (*x509.CertPool, error) {
	data, err := os.ReadFile(path) //nolint:gosec // RabbitMQ CA certificate path is an operator-supplied context setting, never derived from message data.
	if err != nil {
		return nil, apperrors.New(apperrors.CodeLocalIOError, "failed to read RabbitMQ CA certificate", err)
	}
	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM(data) {
		return nil, apperrors.New(apperrors.CodeValidationFailed, "failed to parse RabbitMQ CA certificate", nil)
	}
	return pool, nil
}

func headers(in map[string][]byte) amqp.Table {
	if len(in) == 0 {
		return nil
	}
	out := make(amqp.Table, len(in))
	for key, value := range in {
		out[key] = value
	}
	return out
}

func classifyAMQPError(err error) error {
	var amqpErr *amqp.Error
	if errors.As(err, &amqpErr) {
		switch amqpErr.Code {
		case amqp.NotFound:
			return apperrors.New(apperrors.CodeResourceNotFound, "queue not found", err)
		case amqp.ResourceLocked, amqp.PreconditionFailed:
			return apperrors.New(apperrors.CodeResourceAlreadyExists, "queue already exists with incompatible properties", err)
		default:
			return backendErr(err)
		}
	}
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return apperrors.New(apperrors.CodeBackendUnreachable, "rabbitmq backend unreachable", err)
	}
	return backendErr(err)
}

func unreachable(err error) error {
	return apperrors.New(apperrors.CodeBackendUnreachable, "rabbitmq backend unreachable", err)
}

func backendErr(err error) error {
	return apperrors.New(apperrors.CodeBackendError, "rabbitmq backend error", err)
}

func timeout(opts Options) time.Duration {
	if opts.Timeout > 0 {
		return opts.Timeout
	}
	return 30 * time.Second
}

func defaultVHost(vhost string) string {
	if vhost == "" {
		return "/"
	}
	return vhost
}

func netJoin(host string, port int) string {
	if strings.Contains(host, ":") && !strings.HasPrefix(host, "[") {
		return "[" + host + "]:" + strconv.Itoa(port)
	}
	return host + ":" + strconv.Itoa(port)
}
