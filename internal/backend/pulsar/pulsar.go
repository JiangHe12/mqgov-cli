package pulsar

import (
	"bytes"
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"sort"
	"strconv"
	"strings"
	"time"

	pulsarclient "github.com/apache/pulsar-client-go/pulsar"

	"github.com/JiangHe12/opskit-core/apperrors"

	"github.com/JiangHe12/mqgov-cli/internal/mqgov"
)

type Options struct {
	ServiceURL     string
	AdminURL       string
	Tenant         string
	Namespace      string
	Cluster        string
	Token          string
	TLS            bool
	CACertFile     string
	ClientCertFile string
	ClientKeyFile  string
	Timeout        time.Duration
}

type Broker struct {
	opts       Options
	client     pulsarclient.Client
	httpClient *http.Client
	tlsConfig  *tls.Config
}

type topicStats struct {
	MsgRateIn          float64                     `json:"msgRateIn"`
	MsgThroughputIn    float64                     `json:"msgThroughputIn"`
	Subscriptions      map[string]subscriptionStat `json:"subscriptions"`
	Partitions         map[string]topicStats       `json:"partitions"`
	MsgInCounter       int64                       `json:"msgInCounter"`
	BacklogSize        int64                       `json:"backlogSize"`
	StorageSize        int64                       `json:"storageSize"`
	NumberOfEntries    int64                       `json:"numberOfEntries"`
	NumberOfPartitions int                         `json:"numberOfPartitions"`
}

type subscriptionStat struct {
	MsgBacklog     int64   `json:"msgBacklog"`
	MsgRateExpired float64 `json:"msgRateExpired"`
	Type           string  `json:"type"`
	Consumers      []any   `json:"consumers"`
}

type partitionedTopicMetadata struct {
	Partitions int `json:"partitions"`
}

func New(opts Options) (*Broker, error) {
	opts.Tenant = firstNonEmpty(opts.Tenant, "public")
	opts.Namespace = firstNonEmpty(opts.Namespace, "default")
	opts.ServiceURL = firstNonEmpty(opts.ServiceURL, "pulsar://127.0.0.1:6650")
	opts.AdminURL = firstNonEmpty(opts.AdminURL, "http://127.0.0.1:8080")
	tlsConfig, err := buildTLSConfig(opts)
	if err != nil {
		return nil, err
	}
	clientOpts := pulsarclient.ClientOptions{
		URL:                        opts.ServiceURL,
		ConnectionTimeout:          timeout(opts),
		OperationTimeout:           timeout(opts),
		TLSAllowInsecureConnection: false,
		TLSValidateHostname:        true,
		TLSConfig:                  tlsConfig,
	}
	if opts.CACertFile != "" {
		clientOpts.TLSTrustCertsFilePath = opts.CACertFile
	}
	if opts.Token != "" {
		clientOpts.Authentication = pulsarclient.NewAuthenticationToken(opts.Token)
	}
	if opts.ClientCertFile != "" || opts.ClientKeyFile != "" {
		if opts.ClientCertFile == "" || opts.ClientKeyFile == "" {
			return nil, apperrors.New(apperrors.CodeUsageError, "Pulsar mTLS requires both client certificate and key files", nil)
		}
		clientOpts.TLSCertificateFile = opts.ClientCertFile
		clientOpts.TLSKeyFilePath = opts.ClientKeyFile
	}
	client, err := pulsarclient.NewClient(clientOpts)
	if err != nil {
		return nil, unreachable(err)
	}
	httpClient := &http.Client{Timeout: timeout(opts)}
	if tlsConfig != nil {
		httpClient.Transport = &http.Transport{TLSClientConfig: tlsConfig}
	}
	return &Broker{opts: opts, client: client, httpClient: httpClient, tlsConfig: tlsConfig}, nil
}

func (b *Broker) Ping(ctx context.Context) error {
	_, err := b.adminJSON(ctx, http.MethodGet, "/admin/v2/namespaces/"+pathEscape(b.opts.Tenant)+"/"+pathEscape(b.opts.Namespace), nil)
	return err
}

func (b *Broker) Describe() mqgov.Description {
	return mqgov.Description{Backend: "pulsar", Cluster: b.opts.Cluster, Namespace: b.opts.Tenant + "/" + b.opts.Namespace}
}

func (b *Broker) Capabilities() mqgov.Capabilities {
	return mqgov.Capabilities{
		Backend:            "pulsar",
		ResourceTypes:      []string{"topic", "group", "message", "offset"},
		Verbs:              []string{"list", "describe", "lag", "peek", "produce", "create", "alter", "delete", "purge", "reset-offset"},
		SupportsOffsets:    true,
		SupportsPartitions: true,
		SupportsACL:        false,
	}
}

func (b *Broker) ListTopics(ctx context.Context, opts mqgov.TopicListOptions) ([]mqgov.TopicDescription, error) {
	topics, err := b.listTopicNames(ctx, b.topicNamespacePath())
	if err != nil {
		return nil, err
	}
	partitioned, err := b.listTopicNames(ctx, b.topicNamespacePath()+"/partitioned")
	if err != nil {
		return nil, err
	}
	topics = append(topics, partitioned...)
	items := make([]mqgov.TopicDescription, 0, len(topics))
	for _, fqn := range topics {
		short := shortTopicName(fqn)
		if opts.Pattern != "" && opts.Pattern != short {
			continue
		}
		partitions := 1
		if meta, err := b.partitionedMetadata(ctx, short); err == nil && meta.Partitions > 0 {
			partitions = meta.Partitions
		}
		desc := mqgov.TopicDescription{
			Coordinate: mqgov.TopicCoordinate{Cluster: b.opts.Cluster, Namespace: b.opts.Tenant + "/" + b.opts.Namespace, Topic: short},
			Partitions: partitions,
			Internal:   isInternalTopic(b.opts.Tenant, b.opts.Namespace, short),
		}
		items = append(items, desc)
	}
	sort.Slice(items, func(i, j int) bool { return items[i].Coordinate.Topic < items[j].Coordinate.Topic })
	if opts.Limit > 0 && len(items) > opts.Limit {
		items = items[:opts.Limit]
	}
	return items, nil
}

func (b *Broker) DescribeTopic(ctx context.Context, coord mqgov.TopicCoordinate) (mqgov.TopicDescription, error) {
	stats, err := b.stats(ctx, coord.Topic)
	if err != nil {
		return mqgov.TopicDescription{}, err
	}
	partitions := stats.NumberOfPartitions
	if partitions == 0 && len(stats.Partitions) > 0 {
		partitions = len(stats.Partitions)
	}
	if partitions == 0 {
		partitions = 1
	}
	backlog := stats.totalBacklog()
	return mqgov.TopicDescription{
		Coordinate: mqgov.TopicCoordinate{Cluster: b.opts.Cluster, Namespace: b.opts.Tenant + "/" + b.opts.Namespace, Topic: coord.Topic},
		Partitions: partitions,
		Config: map[string]string{
			"backlog":       strconv.FormatInt(backlog, 10),
			"subscriptions": strconv.Itoa(len(stats.Subscriptions)),
		},
		Internal: isInternalTopic(b.opts.Tenant, b.opts.Namespace, coord.Topic),
	}, nil
}

func (b *Broker) CreateTopic(ctx context.Context, req mqgov.TopicCreateRequest) (mqgov.TopicDescription, error) {
	path := b.topicPath(req.Coordinate.Topic)
	if req.Partitions > 1 {
		data, _ := json.Marshal(req.Partitions)
		_, err := b.adminJSON(ctx, http.MethodPut, path+"/partitions", data)
		if err != nil {
			return mqgov.TopicDescription{}, err
		}
		return b.DescribeTopic(ctx, req.Coordinate)
	}
	_, err := b.adminJSON(ctx, http.MethodPut, path, nil)
	if err != nil {
		return mqgov.TopicDescription{}, err
	}
	return b.DescribeTopic(ctx, req.Coordinate)
}

func (b *Broker) DeleteTopic(ctx context.Context, coord mqgov.TopicCoordinate) error {
	path := b.topicPath(coord.Topic)
	if meta, err := b.partitionedMetadata(ctx, coord.Topic); err == nil && meta.Partitions > 0 {
		_, err := b.adminJSON(ctx, http.MethodDelete, path+"/partitions?force=false", nil)
		return err
	}
	_, err := b.adminJSON(ctx, http.MethodDelete, path+"?force=false", nil)
	return err
}

func (b *Broker) ListGroups(context.Context, mqgov.GroupListOptions) ([]mqgov.GroupDescription, error) {
	return nil, apperrors.New(apperrors.CodeNotImplemented, "Pulsar subscriptions are per-topic; list is not supported without a topic", nil)
}

func (b *Broker) CreateGroup(ctx context.Context, coord mqgov.GroupCoordinate) (mqgov.GroupDescription, error) {
	return mqgov.GroupDescription{}, apperrors.New(apperrors.CodeNotImplemented, "Pulsar subscriptions are created on first use", nil)
}

func (b *Broker) DeleteGroup(ctx context.Context, coord mqgov.GroupCoordinate) error {
	return apperrors.New(apperrors.CodeNotImplemented, "Pulsar subscription delete requires a topic", nil)
}

func (b *Broker) Peek(ctx context.Context, req mqgov.MessagePeekRequest) (mqgov.MessagePeekResult, error) {
	count := req.Count
	if count <= 0 {
		count = 1
	}
	reader, err := b.client.CreateReader(pulsarclient.ReaderOptions{
		Topic:                   b.fqn(req.Coordinate.Topic),
		StartMessageID:          pulsarclient.EarliestMessageID(),
		StartMessageIDInclusive: true,
		ReceiverQueueSize:       count,
	})
	if err != nil {
		return mqgov.MessagePeekResult{}, backendErr(err)
	}
	defer reader.Close()
	messages := make([]mqgov.MessageFingerprint, 0, count)
	for len(messages) < count && reader.HasNext() {
		msg, err := reader.Next(ctx)
		if err != nil {
			return mqgov.MessagePeekResult{}, backendErr(err)
		}
		messages = append(messages, fingerprint(msg))
	}
	return mqgov.MessagePeekResult{Coordinate: req.Coordinate, Count: len(messages), Messages: messages}, nil
}

func (b *Broker) Produce(ctx context.Context, req mqgov.MessageProduceRequest) (mqgov.MessageProduceResult, error) {
	producer, err := b.client.CreateProducer(pulsarclient.ProducerOptions{Topic: b.fqn(req.Coordinate.Topic), DisableBatching: true})
	if err != nil {
		return mqgov.MessageProduceResult{}, backendErr(err)
	}
	defer producer.Close()
	msgID, err := producer.Send(ctx, &pulsarclient.ProducerMessage{Payload: req.Body, Key: string(req.Key), Properties: stringHeaders(req.Headers)})
	if err != nil {
		return mqgov.MessageProduceResult{}, backendErr(err)
	}
	return mqgov.MessageProduceResult{
		Coordinate:  mqgov.MessageCoordinate{TopicCoordinate: req.Coordinate, Partition: int(msgID.PartitionIdx()), Offset: msgID.EntryID()},
		Fingerprint: mqgov.Fingerprints(req.Key, req.Body, 1),
	}, nil
}

func (b *Broker) AlterTopic(ctx context.Context, req mqgov.TopicAlterRequest) (mqgov.TopicDescription, error) {
	if req.Partitions <= 0 {
		return b.DescribeTopic(ctx, req.Coordinate)
	}
	meta, err := b.partitionedMetadata(ctx, req.Coordinate.Topic)
	if err != nil {
		if apperrors.AsAppError(err).Code == apperrors.CodeResourceNotFound {
			if _, existsErr := b.adminJSON(ctx, http.MethodGet, b.topicPath(req.Coordinate.Topic)+"/stats", nil); existsErr != nil {
				return mqgov.TopicDescription{}, existsErr
			}
			return mqgov.TopicDescription{}, apperrors.New(apperrors.CodeBackendError, "cannot update partitions on a non-partitioned Pulsar topic", nil)
		}
		return mqgov.TopicDescription{}, err
	}
	if meta.Partitions <= 0 {
		return mqgov.TopicDescription{}, apperrors.New(apperrors.CodeBackendError, "cannot update partitions on a non-partitioned Pulsar topic", nil)
	}
	data, _ := json.Marshal(req.Partitions)
	_, err = b.adminJSON(ctx, http.MethodPost, b.topicPath(req.Coordinate.Topic)+"/partitions", data)
	if err != nil {
		return mqgov.TopicDescription{}, backendErr(err)
	}
	return b.DescribeTopic(ctx, req.Coordinate)
}

func (b *Broker) PurgeTopic(ctx context.Context, req mqgov.TopicPurgeRequest) (mqgov.TopicPurgeResult, error) {
	stats, err := b.stats(ctx, req.Coordinate.Topic)
	if err != nil {
		return mqgov.TopicPurgeResult{}, err
	}
	total := stats.totalBacklog()
	if !req.DryRun {
		for _, sub := range stats.subscriptionNames() {
			_, err := b.adminJSON(ctx, http.MethodPost, b.topicPath(req.Coordinate.Topic)+"/subscription/"+pathEscape(sub)+"/skip_all", nil)
			if err != nil {
				return mqgov.TopicPurgeResult{}, err
			}
		}
	}
	return mqgov.TopicPurgeResult{
		Coordinate:  req.Coordinate,
		DryRun:      req.DryRun,
		Impact:      []mqgov.PartitionImpact{{Partition: 0, Count: total}},
		Total:       total,
		Fingerprint: mqgov.ResourceFingerprints{Count: total},
	}, nil
}

func (b *Broker) Lag(ctx context.Context, group mqgov.GroupCoordinate, topic mqgov.TopicCoordinate) (mqgov.OffsetPlan, error) {
	stats, err := b.stats(ctx, topic.Topic)
	if err != nil {
		return mqgov.OffsetPlan{}, err
	}
	backlog := stats.subscriptionBacklog(group.Group)
	return mqgov.OffsetPlan{
		Group:  group,
		Topic:  topic,
		To:     "latest",
		DryRun: true,
		Impact: []mqgov.PartitionImpact{{Partition: 0, Count: backlog}},
		Total:  backlog,
	}, nil
}

func (b *Broker) PlanOffsetReset(ctx context.Context, req mqgov.OffsetPlanRequest) (mqgov.OffsetPlan, error) {
	return b.offsetPlan(ctx, req)
}

func (b *Broker) ResetOffset(ctx context.Context, req mqgov.OffsetPlanRequest) (mqgov.OffsetPlan, error) {
	plan, err := b.offsetPlan(ctx, req)
	if err != nil {
		return mqgov.OffsetPlan{}, err
	}
	if req.DryRun {
		return plan, nil
	}
	if err := b.ensureSubscriptionInactive(ctx, req.Group.Group, req.Topic.Topic); err != nil {
		return mqgov.OffsetPlan{}, err
	}
	if strings.HasPrefix(req.To, "offset:") || strings.HasPrefix(req.To, "shift:") {
		return mqgov.OffsetPlan{}, apperrors.New(apperrors.CodeUsageError, "unsupported for pulsar", nil)
	}
	path := b.topicPath(req.Topic.Topic) + "/subscription/" + pathEscape(req.Group.Group)
	switch {
	case req.To == "" || req.To == "earliest":
		_, err = b.adminJSON(ctx, http.MethodPost, path+"/resetcursor/0", nil)
	case req.To == "latest":
		_, err = b.adminJSON(ctx, http.MethodPost, path+"/skip_all", nil)
	case strings.HasPrefix(req.To, "datetime:"):
		t, perr := time.Parse(time.RFC3339, strings.TrimPrefix(req.To, "datetime:"))
		if perr != nil {
			return mqgov.OffsetPlan{}, apperrors.New(apperrors.CodeUsageError, "invalid datetime target", perr)
		}
		_, err = b.adminJSON(ctx, http.MethodPost, path+"/resetcursor/"+strconv.FormatInt(t.UnixMilli(), 10), nil)
	default:
		return mqgov.OffsetPlan{}, apperrors.New(apperrors.CodeUsageError, "unsupported for pulsar", nil)
	}
	if err != nil {
		return mqgov.OffsetPlan{}, err
	}
	return plan, nil
}

func (b *Broker) offsetPlan(ctx context.Context, req mqgov.OffsetPlanRequest) (mqgov.OffsetPlan, error) {
	if strings.HasPrefix(req.To, "offset:") || strings.HasPrefix(req.To, "shift:") {
		return mqgov.OffsetPlan{}, apperrors.New(apperrors.CodeUsageError, "unsupported for pulsar", nil)
	}
	stats, err := b.stats(ctx, req.Topic.Topic)
	if err != nil {
		return mqgov.OffsetPlan{}, err
	}
	backlog := stats.subscriptionBacklog(req.Group.Group)
	target := backlog
	if req.To == "latest" {
		target = 0
	}
	return mqgov.OffsetPlan{
		Group:  req.Group,
		Topic:  req.Topic,
		To:     req.To,
		DryRun: req.DryRun,
		Impact: []mqgov.PartitionImpact{{Partition: 0, From: backlog, To: target, Count: abs64(backlog - target)}},
		Total:  abs64(backlog - target),
	}, nil
}

func (b *Broker) stats(ctx context.Context, topic string) (topicStats, error) {
	path := b.topicPath(topic) + "/stats"
	partitions := 0
	if meta, err := b.partitionedMetadata(ctx, topic); err == nil && meta.Partitions > 0 {
		partitions = meta.Partitions
		path = b.topicPath(topic) + "/partitioned-stats?perPartition=true"
	}
	for attempts := 0; attempts < 6; attempts++ {
		body, err := b.adminJSON(ctx, http.MethodGet, path, nil)
		if err != nil {
			return topicStats{}, err
		}
		if len(bytes.TrimSpace(body)) > 0 {
			var stats topicStats
			err := json.Unmarshal(body, &stats)
			if err == nil {
				return stats, nil
			}
			if !isTruncatedJSON(err) {
				return topicStats{}, backendErr(fmt.Errorf("decode Pulsar topic stats: %w", err))
			}
		}
		if attempts < 5 {
			select {
			case <-ctx.Done():
				return topicStats{}, backendErr(ctx.Err())
			case <-time.After(200 * time.Millisecond):
			}
		}
	}
	return topicStats{NumberOfPartitions: partitions}, nil
}

func (s topicStats) totalBacklog() int64 {
	var total int64
	for _, sub := range s.Subscriptions {
		total += sub.MsgBacklog
	}
	for _, part := range s.Partitions {
		total += part.totalBacklog()
	}
	return total
}

func (s topicStats) subscriptionBacklog(name string) int64 {
	var total int64
	if sub, ok := s.Subscriptions[name]; ok {
		total += sub.MsgBacklog
	}
	for _, part := range s.Partitions {
		total += part.subscriptionBacklog(name)
	}
	return total
}

func (s topicStats) subscriptionConsumers(name string) int {
	var total int
	if sub, ok := s.Subscriptions[name]; ok {
		total += len(sub.Consumers)
	}
	for _, part := range s.Partitions {
		total += part.subscriptionConsumers(name)
	}
	return total
}

func (s topicStats) subscriptionNames() []string {
	seen := make(map[string]struct{})
	for name := range s.Subscriptions {
		seen[name] = struct{}{}
	}
	for _, part := range s.Partitions {
		for _, name := range part.subscriptionNames() {
			seen[name] = struct{}{}
		}
	}
	names := make([]string, 0, len(seen))
	for name := range seen {
		names = append(names, name)
	}
	sort.Strings(names)
	return names
}

func (b *Broker) listTopicNames(ctx context.Context, path string) ([]string, error) {
	body, err := b.adminJSON(ctx, http.MethodGet, path, nil)
	if err != nil {
		return nil, err
	}
	var topics []string
	if err := json.Unmarshal(body, &topics); err != nil {
		return nil, backendErr(fmt.Errorf("decode Pulsar topic list: %w", err))
	}
	return topics, nil
}

func (b *Broker) partitionedMetadata(ctx context.Context, topic string) (partitionedTopicMetadata, error) {
	body, err := b.adminJSON(ctx, http.MethodGet, b.topicPath(topic)+"/partitions", nil)
	if err != nil {
		return partitionedTopicMetadata{}, err
	}
	if len(bytes.TrimSpace(body)) == 0 {
		return partitionedTopicMetadata{}, apperrors.New(apperrors.CodeResourceNotFound, "topic not found", nil)
	}
	var partitions int
	if err := json.Unmarshal(body, &partitions); err == nil {
		return partitionedTopicMetadata{Partitions: partitions}, nil
	}
	var meta partitionedTopicMetadata
	if err := json.Unmarshal(body, &meta); err != nil {
		return partitionedTopicMetadata{}, backendErr(fmt.Errorf("decode Pulsar partition metadata: %w", err))
	}
	return meta, nil
}

func (b *Broker) ensureSubscriptionInactive(ctx context.Context, group, topic string) error {
	stats, err := b.stats(ctx, topic)
	if err != nil {
		return err
	}
	if stats.subscriptionConsumers(group) > 0 {
		return apperrors.New(apperrors.CodeBackendError, "subscription has active consumers", nil)
	}
	return nil
}

func (b *Broker) adminJSON(ctx context.Context, method, path string, payload []byte) ([]byte, error) {
	return b.adminJSONWithContentType(ctx, method, path, payload, "application/json")
}

func (b *Broker) adminJSONWithContentType(ctx context.Context, method, path string, payload []byte, contentType string) ([]byte, error) {
	endpoint := strings.TrimRight(b.opts.AdminURL, "/") + path
	var body io.Reader
	if payload != nil {
		body = bytes.NewReader(payload)
	}
	req, err := http.NewRequestWithContext(ctx, method, endpoint, body)
	if err != nil {
		return nil, apperrors.New(apperrors.CodeUsageError, "invalid Pulsar admin URL", err)
	}
	if payload != nil {
		req.Header.Set("Content-Type", contentType)
	}
	if b.opts.Token != "" {
		req.Header.Set("Authorization", "Bearer "+b.opts.Token)
	}
	resp, err := b.httpClient.Do(req)
	if err != nil {
		return nil, unreachable(err)
	}
	defer func() { _ = resp.Body.Close() }()
	data, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
	switch {
	case resp.StatusCode >= 200 && resp.StatusCode < 300:
		return data, nil
	case resp.StatusCode == http.StatusNotFound:
		return nil, apperrors.New(apperrors.CodeResourceNotFound, "topic not found", fmt.Errorf("pulsar admin status %d", resp.StatusCode))
	case resp.StatusCode == http.StatusConflict || resp.StatusCode == http.StatusPreconditionFailed:
		return nil, apperrors.New(apperrors.CodeResourceAlreadyExists, "topic already exists", fmt.Errorf("pulsar admin status %d", resp.StatusCode))
	default:
		return nil, backendErr(fmt.Errorf("pulsar admin status %d: %s", resp.StatusCode, string(data)))
	}
}

func (b *Broker) fqn(topic string) string {
	if strings.HasPrefix(topic, "persistent://") {
		return topic
	}
	return "persistent://" + b.opts.Tenant + "/" + b.opts.Namespace + "/" + topic
}

func (b *Broker) topicNamespacePath() string {
	return "/admin/v2/persistent/" + pathEscape(b.opts.Tenant) + "/" + pathEscape(b.opts.Namespace)
}

func (b *Broker) topicPath(topic string) string {
	return b.topicNamespacePath() + "/" + pathEscape(shortTopicName(topic))
}

func fingerprint(msg pulsarclient.Message) mqgov.MessageFingerprint {
	id := msg.ID()
	return mqgov.MessageFingerprint{
		Partition:  int(id.PartitionIdx()),
		Offset:     id.EntryID(),
		KeySHA256:  mqgov.Fingerprints([]byte(msg.Key()), nil, 0).KeySHA256,
		BodySHA256: mqgov.Fingerprints(nil, msg.Payload(), 0).BodySHA256,
		Size:       len(msg.Payload()),
	}
}

func buildTLSConfig(opts Options) (*tls.Config, error) {
	if !opts.TLS && opts.CACertFile == "" && opts.ClientCertFile == "" && opts.ClientKeyFile == "" {
		return nil, nil
	}
	cfg := &tls.Config{MinVersion: tls.VersionTLS12}
	if opts.CACertFile != "" {
		data, err := os.ReadFile(opts.CACertFile) //nolint:gosec // Pulsar CA certificate path is operator supplied.
		if err != nil {
			return nil, apperrors.New(apperrors.CodeLocalIOError, "failed to read Pulsar CA certificate", err)
		}
		pool := x509.NewCertPool()
		if !pool.AppendCertsFromPEM(data) {
			return nil, apperrors.New(apperrors.CodeValidationFailed, "failed to parse Pulsar CA certificate", nil)
		}
		cfg.RootCAs = pool
	}
	if opts.ClientCertFile != "" || opts.ClientKeyFile != "" {
		if opts.ClientCertFile == "" || opts.ClientKeyFile == "" {
			return nil, apperrors.New(apperrors.CodeUsageError, "Pulsar mTLS requires both client certificate and key files", nil)
		}
		cert, err := tls.LoadX509KeyPair(opts.ClientCertFile, opts.ClientKeyFile)
		if err != nil {
			return nil, apperrors.New(apperrors.CodeCredentialStoreError, "failed to load Pulsar client certificate", err)
		}
		cfg.Certificates = []tls.Certificate{cert}
	}
	return cfg, nil
}

func unreachable(err error) error {
	return apperrors.New(apperrors.CodeBackendUnreachable, "pulsar backend unreachable", err)
}

func backendErr(err error) error {
	if err == nil {
		return nil
	}
	var appErr *apperrors.AppError
	if errors.As(err, &appErr) {
		return err
	}
	return apperrors.New(apperrors.CodeBackendError, "pulsar backend error", err)
}

func isTruncatedJSON(err error) bool {
	return errors.Is(err, io.ErrUnexpectedEOF) || strings.Contains(err.Error(), "unexpected end of JSON input")
}

func isInternalTopic(tenant, namespace, topic string) bool {
	name := strings.ToLower(topic)
	return tenant == "pulsar" || namespace == "system" || strings.HasPrefix(name, "__") || strings.Contains(name, "__change_events") || strings.Contains(name, "__transaction")
}

func pathEscape(value string) string { return url.PathEscape(value) }

func shortTopicName(topic string) string {
	if strings.HasPrefix(topic, "persistent://") {
		parts := strings.Split(topic, "/")
		return parts[len(parts)-1]
	}
	if i := strings.LastIndex(topic, "/"); i >= 0 {
		return topic[i+1:]
	}
	return topic
}

func firstNonEmpty(values ...string) string {
	for _, value := range values {
		if value != "" {
			return value
		}
	}
	return ""
}

func stringHeaders(headers map[string][]byte) map[string]string {
	if len(headers) == 0 {
		return nil
	}
	out := make(map[string]string, len(headers))
	for key, value := range headers {
		out[key] = string(value)
	}
	return out
}

func timeout(opts Options) time.Duration {
	if opts.Timeout > 0 {
		return opts.Timeout
	}
	return 30 * time.Second
}

func abs64(value int64) int64 {
	if value < 0 {
		return -value
	}
	return value
}
