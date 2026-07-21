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
	"sync"
	"time"

	pulsarclient "github.com/apache/pulsar-client-go/pulsar"

	"github.com/JiangHe12/opskit-core/v2/apperrors"

	"github.com/JiangHe12/mqgov-cli/internal/mqgov"
	"github.com/JiangHe12/mqgov-cli/internal/tlspin"
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
	TLSPinPath     string
	Timeout        time.Duration
}

type Broker struct {
	opts       Options
	client     pulsarclient.Client
	httpClient *http.Client
	tlsConfig  *tls.Config
	close      sync.Once
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

type pulsarSchemaInfo struct {
	Version    any               `json:"version"`
	Type       string            `json:"type"`
	Timestamp  any               `json:"timestamp"`
	Data       string            `json:"data"`
	Properties map[string]string `json:"properties"`
}

type pulsarSchemaPayload struct {
	Type       string            `json:"type"`
	Schema     string            `json:"schema"`
	Properties map[string]string `json:"properties,omitempty"`
}

func New(opts Options) (*Broker, error) {
	opts, baseTLSConfig, err := prepareConnectionOptions(opts)
	if err != nil {
		return nil, err
	}
	var serviceTLSConfig *tls.Config
	if baseTLSConfig != nil && strings.HasPrefix(strings.ToLower(strings.TrimSpace(opts.ServiceURL)), "pulsar+ssl://") {
		serviceTLSConfig, err = tlspin.CloneForEndpoint(baseTLSConfig, opts.TLSPinPath, opts.ServiceURL, tlspin.NotifyStderr)
		if err != nil {
			return nil, err
		}
	}
	clientOpts := pulsarclient.ClientOptions{
		URL:                        opts.ServiceURL,
		ConnectionTimeout:          timeout(opts),
		OperationTimeout:           timeout(opts),
		TLSAllowInsecureConnection: false,
		TLSValidateHostname:        true,
		TLSConfig:                  serviceTLSConfig,
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
	if baseTLSConfig != nil && strings.HasPrefix(strings.ToLower(strings.TrimSpace(opts.AdminURL)), "https://") {
		adminTLSConfig, err := tlspin.CloneForEndpoint(baseTLSConfig, opts.TLSPinPath, opts.AdminURL, tlspin.NotifyStderr)
		if err != nil {
			client.Close()
			return nil, err
		}
		httpClient.Transport = &http.Transport{TLSClientConfig: adminTLSConfig}
	}
	return &Broker{opts: opts, client: client, httpClient: httpClient, tlsConfig: serviceTLSConfig}, nil
}

func (b *Broker) Close() {
	if b == nil {
		return
	}
	b.close.Do(func() {
		if b.client != nil {
			b.client.Close()
		}
		if b.httpClient != nil {
			b.httpClient.CloseIdleConnections()
		}
	})
}

func prepareConnectionOptions(opts Options) (Options, *tls.Config, error) {
	opts.Tenant = firstNonEmpty(opts.Tenant, "public")
	opts.Namespace = firstNonEmpty(opts.Namespace, "default")
	opts = normalizeConnectionURLs(opts)
	if err := validateTLSConnectionURLs(opts); err != nil {
		return Options{}, nil, err
	}
	baseTLSConfig, err := buildTLSConfig(opts)
	if err != nil {
		return Options{}, nil, err
	}
	return opts, baseTLSConfig, nil
}

func normalizeConnectionURLs(opts Options) Options {
	if strings.TrimSpace(opts.ServiceURL) == "" {
		opts.ServiceURL = "pulsar://127.0.0.1:6650"
		if opts.TLS {
			opts.ServiceURL = "pulsar+ssl://127.0.0.1:6651"
		}
	}
	if strings.TrimSpace(opts.AdminURL) == "" {
		opts.AdminURL = "http://127.0.0.1:8080"
		if opts.TLS {
			opts.AdminURL = "https://127.0.0.1:8443"
		}
	}
	return opts
}

func validateTLSConnectionURLs(opts Options) error {
	if !opts.TLS {
		return nil
	}
	if err := requireEndpointScheme(opts.ServiceURL, "pulsar+ssl", "Pulsar service"); err != nil {
		return err
	}
	return requireEndpointScheme(opts.AdminURL, "https", "Pulsar admin")
}

func requireEndpointScheme(rawURL, wantScheme, endpointName string) error {
	parsed, err := url.Parse(strings.TrimSpace(rawURL))
	if err != nil || parsed.Host == "" || !strings.EqualFold(parsed.Scheme, wantScheme) {
		return apperrors.New(
			apperrors.CodeUsageError,
			fmt.Sprintf("%s TLS requires a %s:// URL", endpointName, wantScheme),
			nil,
		)
	}
	return nil
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
		ResourceTypes:      []string{"topic", "group", "message", "offset", "acl", "dlq", "schema"},
		Verbs:              []string{"list", "describe", "lag", "peek", "tail", "produce", "mirror", "create", "alter", "delete", "purge", "reset-offset", "grant-acl", "revoke-acl", "check-schema", "register-schema", "delete-schema"},
		SupportsOffsets:    true,
		SupportsPartitions: true,
		SupportsACL:        true,
		SupportsDLQList:    true,
		SupportsDLQPeek:    true,
		SupportsDLQRedrive: false,
		SupportsDLQPurge:   false,
		SupportsSchema:     true,
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
	if req.Count <= 0 {
		return mqgov.MessagePeekResult{}, apperrors.New(apperrors.CodeUsageError, "peek count must be positive", nil)
	}
	if req.Partition < 0 || req.Offset < 0 {
		return mqgov.MessagePeekResult{}, apperrors.New(apperrors.CodeUsageError, "peek partition and offset must be non-negative", nil)
	}
	if req.Partition > 0 || req.Offset > 0 {
		return mqgov.MessagePeekResult{}, apperrors.New(
			apperrors.CodeNotImplemented,
			"Pulsar peek does not support partition-specific or offset-specific reads",
			nil,
		)
	}
	count := req.Count
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

func (b *Broker) Tail(ctx context.Context, req mqgov.MessageTailRequest, emit func(mqgov.MessageFingerprint) error) (mqgov.MessageTailResult, error) {
	start, inclusive, err := pulsarTailStart(req.From)
	if err != nil {
		return mqgov.MessageTailResult{}, err
	}
	reader, err := b.client.CreateReader(pulsarclient.ReaderOptions{
		Topic:                   b.fqn(req.Coordinate.Topic),
		StartMessageID:          start,
		StartMessageIDInclusive: inclusive,
		ReceiverQueueSize:       maxInt(req.MaxMessages, 1),
	})
	if err != nil {
		return mqgov.MessageTailResult{}, backendErr(err)
	}
	defer reader.Close()
	result := mqgov.MessageTailResult{Coordinate: req.Coordinate}
	impact := map[int]*mqgov.PartitionImpact{}
	for req.MaxMessages <= 0 || int(result.Count) < req.MaxMessages {
		if !req.Follow && !reader.HasNext() {
			break
		}
		msg, err := reader.Next(ctx)
		if err != nil {
			if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
				break
			}
			return result, backendErr(err)
		}
		fp := tailFingerprint(msg)
		if err := emit(fp); err != nil {
			return result, err
		}
		result.Count++
		result.TotalSize += int64(fp.Size)
		updateTailImpact(impact, fp.Partition, fp.Offset)
	}
	result.Impact = tailImpactSlice(impact)
	return result, nil
}

func (b *Broker) MirrorMessages(ctx context.Context, req mqgov.MessageMirrorRequest, emit func(mqgov.Message) error) (mqgov.MessageMirrorResult, error) {
	if req.Limit <= 0 {
		return mqgov.MessageMirrorResult{}, apperrors.New(apperrors.CodeUsageError, "mirror limit must be positive", nil)
	}
	if req.Partition >= 0 {
		return mqgov.MessageMirrorResult{}, apperrors.New(apperrors.CodeNotImplemented, "Pulsar partition-specific mirror is not supported by this backend", nil)
	}
	reader, err := b.mirrorReader(req)
	if err != nil {
		return mqgov.MessageMirrorResult{}, err
	}
	defer reader.Close()
	result := mqgov.MessageMirrorResult{Source: req.Source, Target: req.Target, DryRun: req.DryRun}
	impact := map[int]*mqgov.PartitionImpact{}
	for int(result.Count) < req.Limit && reader.HasNext() {
		msg, err := reader.Next(ctx)
		if err != nil {
			if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
				break
			}
			return result, backendErr(err)
		}
		id := msg.ID()
		out := mqgov.Message{
			Coordinate: mqgov.MessageCoordinate{TopicCoordinate: req.Source, Partition: int(id.PartitionIdx()), Offset: id.EntryID()},
			Key:        []byte(msg.Key()),
			Body:       msg.Payload(),
			Headers:    bytesHeaders(msg.Properties()),
		}
		if emit != nil {
			if err := emit(out); err != nil {
				return result, err
			}
		}
		result.Count++
		updateTailImpact(impact, out.Coordinate.Partition, out.Coordinate.Offset)
	}
	result.Impact = tailImpactSlice(impact)
	return result, nil
}

func (b *Broker) mirrorReader(req mqgov.MessageMirrorRequest) (pulsarclient.Reader, error) {
	start, inclusive, seekTime, err := pulsarMirrorStart(req.From)
	if err != nil {
		return nil, err
	}
	reader, err := b.client.CreateReader(pulsarclient.ReaderOptions{
		Topic:                   b.fqn(req.Source.Topic),
		StartMessageID:          start,
		StartMessageIDInclusive: inclusive,
		ReceiverQueueSize:       maxInt(req.Limit, 1),
	})
	if err != nil {
		return nil, backendErr(err)
	}
	if !seekTime.IsZero() {
		if err := reader.SeekByTime(seekTime); err != nil {
			reader.Close()
			return nil, backendErr(err)
		}
	}
	return reader, nil
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

func (b *Broker) ListDLQs(ctx context.Context, opts mqgov.DLQListOptions) ([]mqgov.DLQDescription, error) {
	if opts.Topic != "" && opts.Group != "" {
		dlq := pulsarDLQName(opts.Topic, opts.Group)
		desc, err := b.DescribeTopic(ctx, mqgov.TopicCoordinate{Topic: dlq})
		if err != nil {
			return nil, err
		}
		return []mqgov.DLQDescription{{
			Coordinate:    desc.Coordinate,
			SourceTopic:   opts.Topic,
			ConsumerGroup: opts.Group,
			NativeModel:   "{topic}-{subscription}-DLQ",
			Messages:      pulsarTopicMessages(desc),
		}}, nil
	}
	topics, err := b.ListTopics(ctx, mqgov.TopicListOptions{Pattern: opts.Pattern, Limit: opts.Limit})
	if err != nil {
		return nil, err
	}
	items := make([]mqgov.DLQDescription, 0)
	for _, topic := range topics {
		if !strings.HasSuffix(topic.Coordinate.Topic, "-DLQ") {
			continue
		}
		source, sub := pulsarDLQParts(topic.Coordinate.Topic)
		if opts.Topic != "" && source != opts.Topic {
			continue
		}
		if opts.Group != "" && sub != opts.Group {
			continue
		}
		items = append(items, mqgov.DLQDescription{
			Coordinate:    topic.Coordinate,
			SourceTopic:   source,
			ConsumerGroup: sub,
			NativeModel:   "{topic}-{subscription}-DLQ",
			Messages:      pulsarTopicMessages(topic),
		})
	}
	sort.Slice(items, func(i, j int) bool { return items[i].Coordinate.Topic < items[j].Coordinate.Topic })
	return items, nil
}

func (b *Broker) PeekDLQ(ctx context.Context, req mqgov.DLQPeekRequest) (mqgov.DLQPeekResult, error) {
	dlq := b.resolvePulsarDLQ(req.DLQ.Topic, req.Topic, req.Group)
	result, err := b.Peek(ctx, mqgov.MessagePeekRequest{
		Coordinate: mqgov.TopicCoordinate{Cluster: req.DLQ.Cluster, Namespace: req.DLQ.Namespace, Topic: dlq},
		Partition:  req.Partition,
		Offset:     req.Offset,
		Count:      req.Count,
	})
	if err != nil {
		return mqgov.DLQPeekResult{}, err
	}
	return mqgov.DLQPeekResult{DLQ: result.Coordinate, Count: result.Count, Messages: result.Messages}, nil
}

func (b *Broker) RedriveDLQ(context.Context, mqgov.DLQRedriveRequest) (mqgov.DLQRedriveResult, error) {
	return mqgov.DLQRedriveResult{}, apperrors.New(
		apperrors.CodeNotImplemented,
		"Pulsar DLQ redrive cannot remove exactly the messages copied by a non-destructive reader",
		nil,
	)
}

func (b *Broker) PurgeDLQ(context.Context, mqgov.DLQPurgeRequest) (mqgov.DLQPurgeResult, error) {
	return mqgov.DLQPurgeResult{}, apperrors.New(
		apperrors.CodeNotImplemented,
		"Pulsar DLQ purge cannot delete retained messages without deleting or recreating the topic",
		nil,
	)
}

func (b *Broker) ListACLs(ctx context.Context, filter mqgov.ACLFilter) ([]mqgov.ACLBinding, error) {
	target, err := b.pulsarACLTarget(filter.ResourceType, filter.ResourceName, true)
	if err != nil {
		return nil, err
	}
	if err := validatePulsarACLFilter(filter); err != nil {
		return nil, err
	}
	filter.ResourceType = target.resourceType
	filter.ResourceName = target.resourceName
	permissions, err := b.pulsarPermissions(ctx, target)
	if err != nil {
		return nil, err
	}
	out := make([]mqgov.ACLBinding, 0, len(permissions))
	for role, actions := range permissions {
		for _, action := range actions {
			binding := mqgov.ACLBinding{
				Principal:    role,
				Host:         "*",
				ResourceType: target.resourceType,
				ResourceName: target.resourceName,
				PatternType:  "literal",
				Operation:    strings.ToLower(strings.TrimSpace(action)),
				Permission:   "allow",
			}
			if pulsarACLMatches(binding, filter) {
				out = append(out, binding)
			}
		}
	}
	sort.Slice(out, func(i, j int) bool { return pulsarACLSortKey(out[i]) < pulsarACLSortKey(out[j]) })
	return out, nil
}

func (b *Broker) GrantACL(ctx context.Context, binding mqgov.ACLBinding) error {
	target, err := b.validatePulsarACLBinding(binding)
	if err != nil {
		return err
	}
	permissions, err := b.pulsarPermissions(ctx, target)
	if err != nil {
		return err
	}
	actions := pulsarMergeAction(permissions[binding.Principal], binding.Operation)
	return b.postPulsarPermissions(ctx, target, binding.Principal, actions)
}

func (b *Broker) RevokeACL(ctx context.Context, binding mqgov.ACLBinding) error {
	target, err := b.validatePulsarACLBinding(binding)
	if err != nil {
		return err
	}
	permissions, err := b.pulsarPermissions(ctx, target)
	if err != nil {
		return err
	}
	actions := pulsarRemoveAction(permissions[binding.Principal], binding.Operation)
	if len(actions) == 0 {
		_, err = b.adminJSON(ctx, http.MethodDelete, target.path+"/"+pathEscape(binding.Principal), nil)
		return err
	}
	return b.postPulsarPermissions(ctx, target, binding.Principal, actions)
}

func (b *Broker) ListSchemas(ctx context.Context, opts mqgov.SchemaListOptions) ([]mqgov.SchemaSubject, error) {
	topics, err := b.listTopicNames(ctx, b.topicNamespacePath())
	if err != nil {
		return nil, err
	}
	partitioned, err := b.listTopicNames(ctx, b.topicNamespacePath()+"/partitioned")
	if err != nil {
		return nil, err
	}
	topics = append(topics, partitioned...)
	items := make([]mqgov.SchemaSubject, 0, len(topics))
	for _, fqn := range topics {
		subject := shortTopicName(fqn)
		if opts.Subject != "" && opts.Subject != subject {
			continue
		}
		if opts.Pattern != "" && opts.Pattern != subject {
			continue
		}
		if _, err := b.pulsarSchema(ctx, subject, "latest"); err == nil {
			items = append(items, mqgov.SchemaSubject{Subject: subject})
		} else if apperrors.AsAppError(err).Code != apperrors.CodeResourceNotFound {
			return nil, err
		}
	}
	sort.Slice(items, func(i, j int) bool { return items[i].Subject < items[j].Subject })
	if opts.Limit > 0 && len(items) > opts.Limit {
		items = items[:opts.Limit]
	}
	return items, nil
}

func (b *Broker) DescribeSchema(ctx context.Context, req mqgov.SchemaDescribeRequest) (mqgov.SchemaDescription, error) {
	if strings.TrimSpace(req.Subject) == "" {
		return mqgov.SchemaDescription{}, apperrors.New(apperrors.CodeUsageError, "schema subject is required", nil)
	}
	info, err := b.pulsarSchema(ctx, req.Subject, req.Version)
	if err != nil {
		return mqgov.SchemaDescription{}, err
	}
	versions, _ := b.pulsarSchemaVersions(ctx, req.Subject)
	version := pulsarSchemaVersionString(info.Version)
	return mqgov.SchemaDescription{
		Subject:    shortTopicName(req.Subject),
		Version:    version,
		Type:       info.Type,
		Schema:     info.Data,
		SchemaHash: mqgov.SHA256Hex([]byte(info.Data)),
		Versions:   versions,
		Properties: info.Properties,
	}, nil
}

func (b *Broker) CheckCompatibility(ctx context.Context, req mqgov.SchemaCheckRequest) (mqgov.SchemaCheckResult, error) {
	if strings.TrimSpace(req.Subject) == "" {
		return mqgov.SchemaCheckResult{}, apperrors.New(apperrors.CodeUsageError, "schema subject is required", nil)
	}
	if strings.TrimSpace(req.Schema) == "" {
		return mqgov.SchemaCheckResult{}, apperrors.New(apperrors.CodeUsageError, "schema text is required", nil)
	}
	payload := pulsarSchemaPayload{Type: firstNonEmpty(strings.ToUpper(strings.TrimSpace(req.Type)), "AVRO"), Schema: req.Schema}
	data, err := json.Marshal(payload)
	if err != nil {
		return mqgov.SchemaCheckResult{}, backendErr(err)
	}
	body, err := b.adminJSON(ctx, http.MethodPost, b.schemaPath(req.Subject)+"/compatibility", data)
	if err != nil {
		return mqgov.SchemaCheckResult{}, err
	}
	compatible, message, err := pulsarCompatibility(body)
	if err != nil {
		return mqgov.SchemaCheckResult{}, backendErr(err)
	}
	return mqgov.SchemaCheckResult{Subject: shortTopicName(req.Subject), Version: firstNonEmpty(req.Version, "latest"), Compatible: compatible, SchemaHash: mqgov.SHA256Hex([]byte(req.Schema)), Message: message}, nil
}

func (b *Broker) RegisterSchema(ctx context.Context, req mqgov.SchemaRegisterRequest) (mqgov.SchemaDescription, error) {
	if strings.TrimSpace(req.Subject) == "" {
		return mqgov.SchemaDescription{}, apperrors.New(apperrors.CodeUsageError, "schema subject is required", nil)
	}
	if strings.TrimSpace(req.Schema) == "" {
		return mqgov.SchemaDescription{}, apperrors.New(apperrors.CodeUsageError, "schema text is required", nil)
	}
	payload := pulsarSchemaPayload{Type: firstNonEmpty(strings.ToUpper(strings.TrimSpace(req.Type)), "AVRO"), Schema: req.Schema}
	data, err := json.Marshal(payload)
	if err != nil {
		return mqgov.SchemaDescription{}, backendErr(err)
	}
	if _, err := b.adminJSON(ctx, http.MethodPost, b.schemaPath(req.Subject)+"/schema", data); err != nil {
		return mqgov.SchemaDescription{}, err
	}
	return b.DescribeSchema(ctx, mqgov.SchemaDescribeRequest{Subject: req.Subject, Version: "latest"})
}

func (b *Broker) DeleteSchema(ctx context.Context, req mqgov.SchemaDeleteRequest) (mqgov.SchemaDeleteResult, error) {
	if strings.TrimSpace(req.Subject) == "" {
		return mqgov.SchemaDeleteResult{}, apperrors.New(apperrors.CodeUsageError, "schema subject is required", nil)
	}
	if !req.Permanent {
		return mqgov.SchemaDeleteResult{}, apperrors.New(apperrors.CodeNotImplemented, "Pulsar schema delete is permanent only; pass --permanent", nil)
	}
	if req.Version != "" && req.Version != "latest" {
		return mqgov.SchemaDeleteResult{}, apperrors.New(apperrors.CodeNotImplemented, "Pulsar schema version delete is not supported by this backend", nil)
	}
	_, err := b.adminJSON(ctx, http.MethodDelete, b.schemaPath(req.Subject)+"/schema", nil)
	if err != nil {
		return mqgov.SchemaDeleteResult{}, err
	}
	return mqgov.SchemaDeleteResult{Subject: shortTopicName(req.Subject), Version: req.Version, Permanent: true}, nil
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
	path := b.topicPath(req.Topic.Topic) + "/subscription/" + pathEscape(req.Group.Group)
	_, err = b.adminJSON(ctx, http.MethodPost, path+"/skip_all", nil)
	if err != nil {
		return mqgov.OffsetPlan{}, err
	}
	return plan, nil
}

func (b *Broker) offsetPlan(ctx context.Context, req mqgov.OffsetPlanRequest) (mqgov.OffsetPlan, error) {
	if err := validatePulsarOffsetReset(req); err != nil {
		return mqgov.OffsetPlan{}, err
	}
	stats, err := b.stats(ctx, req.Topic.Topic)
	if err != nil {
		return mqgov.OffsetPlan{}, err
	}
	impact, total, err := pulsarLatestResetImpact(stats, req.Group.Group)
	if err != nil {
		return mqgov.OffsetPlan{}, err
	}
	return mqgov.OffsetPlan{
		Group:  req.Group,
		Topic:  req.Topic,
		To:     req.To,
		DryRun: req.DryRun,
		Impact: impact,
		Total:  total,
	}, nil
}

func validatePulsarOffsetReset(req mqgov.OffsetPlanRequest) error {
	if req.Partition >= 0 {
		return apperrors.New(apperrors.CodeNotImplemented, "Pulsar partition-specific offset reset is not supported", nil)
	}
	if strings.TrimSpace(req.To) != "latest" {
		return apperrors.New(
			apperrors.CodeNotImplemented,
			"Pulsar offset reset supports only latest because other targets do not expose a reliable affected-message count",
			nil,
		)
	}
	if strings.TrimSpace(req.Group.Group) == "" {
		return apperrors.New(apperrors.CodeUsageError, "Pulsar subscription is required", nil)
	}
	return nil
}

func pulsarLatestResetImpact(stats topicStats, subscription string) ([]mqgov.PartitionImpact, int64, error) {
	if len(stats.Partitions) == 0 {
		if stats.NumberOfPartitions > 1 {
			return nil, 0, apperrors.New(
				apperrors.CodeNotImplemented,
				"Pulsar partition stats are unavailable; cannot compute a reliable reset impact",
				nil,
			)
		}
		sub, ok := stats.Subscriptions[subscription]
		if !ok {
			return nil, 0, apperrors.New(apperrors.CodeResourceNotFound, "Pulsar subscription not found", nil)
		}
		if sub.MsgBacklog < 0 {
			return nil, 0, apperrors.New(apperrors.CodeBackendError, "Pulsar returned a negative subscription backlog", nil)
		}
		return []mqgov.PartitionImpact{{Partition: 0, Count: sub.MsgBacklog}}, sub.MsgBacklog, nil
	}

	keys := make([]string, 0, len(stats.Partitions))
	for name := range stats.Partitions {
		keys = append(keys, name)
	}
	sort.Strings(keys)
	impact := make([]mqgov.PartitionImpact, 0, len(keys))
	seen := make(map[int]struct{}, len(keys))
	foundSubscription := false
	var total int64
	for _, name := range keys {
		partition, err := pulsarPartitionIndex(name)
		if err != nil {
			return nil, 0, err
		}
		if _, duplicate := seen[partition]; duplicate {
			return nil, 0, apperrors.New(apperrors.CodeNotImplemented, "Pulsar partition stats contain duplicate partition identifiers", nil)
		}
		seen[partition] = struct{}{}
		backlog := int64(0)
		if sub, ok := stats.Partitions[name].Subscriptions[subscription]; ok {
			foundSubscription = true
			backlog = sub.MsgBacklog
		}
		if backlog < 0 {
			return nil, 0, apperrors.New(apperrors.CodeBackendError, "Pulsar returned a negative subscription backlog", nil)
		}
		impact = append(impact, mqgov.PartitionImpact{Partition: partition, Count: backlog})
		total += backlog
	}
	if !foundSubscription {
		return nil, 0, apperrors.New(apperrors.CodeResourceNotFound, "Pulsar subscription not found", nil)
	}
	sort.Slice(impact, func(i, j int) bool { return impact[i].Partition < impact[j].Partition })
	return impact, total, nil
}

func pulsarPartitionIndex(name string) (int, error) {
	const marker = "-partition-"
	index := strings.LastIndex(name, marker)
	if index < 0 {
		return 0, apperrors.New(
			apperrors.CodeNotImplemented,
			"Pulsar partition stats do not expose parseable partition identifiers",
			nil,
		)
	}
	partition, err := strconv.Atoi(name[index+len(marker):])
	if err != nil || partition < 0 {
		return 0, apperrors.New(
			apperrors.CodeNotImplemented,
			"Pulsar partition stats do not expose parseable partition identifiers",
			nil,
		)
	}
	return partition, nil
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

type pulsarACLTarget struct {
	resourceType string
	resourceName string
	path         string
}

func (b *Broker) validatePulsarACLBinding(binding mqgov.ACLBinding) (pulsarACLTarget, error) {
	if strings.TrimSpace(binding.Principal) == "" {
		return pulsarACLTarget{}, apperrors.New(apperrors.CodeUsageError, "ACL principal is required", nil)
	}
	if strings.TrimSpace(binding.ResourceName) == "" {
		return pulsarACLTarget{}, apperrors.New(apperrors.CodeUsageError, "ACL resource name is required", nil)
	}
	if !pulsarLiteralPattern(binding.PatternType) {
		return pulsarACLTarget{}, apperrors.New(apperrors.CodeUsageError, "Pulsar ACL pattern type must be literal", nil)
	}
	if !pulsarACLAction(binding.Operation) {
		return pulsarACLTarget{}, apperrors.New(apperrors.CodeUsageError, "Pulsar ACL operation must be produce, consume, functions, sources, sinks, or packages", nil)
	}
	if strings.ToLower(strings.TrimSpace(binding.Permission)) != "allow" {
		return pulsarACLTarget{}, apperrors.New(apperrors.CodeUsageError, "Pulsar ACL permission must be allow", nil)
	}
	return b.pulsarACLTarget(binding.ResourceType, binding.ResourceName, false)
}

func validatePulsarACLFilter(filter mqgov.ACLFilter) error {
	if filter.PatternType != "" && !pulsarLiteralPattern(filter.PatternType) {
		return apperrors.New(apperrors.CodeUsageError, "Pulsar ACL pattern type must be literal", nil)
	}
	if filter.Operation != "" && !pulsarACLAction(filter.Operation) {
		return apperrors.New(apperrors.CodeUsageError, "Pulsar ACL operation must be produce, consume, functions, sources, sinks, or packages", nil)
	}
	if filter.Permission != "" && strings.ToLower(strings.TrimSpace(filter.Permission)) != "allow" {
		return apperrors.New(apperrors.CodeUsageError, "Pulsar ACL permission must be allow", nil)
	}
	return nil
}

func (b *Broker) pulsarACLTarget(resourceType, resourceName string, allowDefault bool) (pulsarACLTarget, error) {
	resourceType = normalizePulsarACLValue(resourceType)
	resourceName = strings.TrimSpace(resourceName)
	if resourceType == "" && allowDefault {
		resourceType = "namespace"
	}
	switch resourceType {
	case "namespace":
		tenant, namespace, err := b.pulsarNamespaceParts(resourceName, allowDefault)
		if err != nil {
			return pulsarACLTarget{}, err
		}
		name := tenant + "/" + namespace
		return pulsarACLTarget{
			resourceType: "namespace",
			resourceName: name,
			path:         "/admin/v2/namespaces/" + pathEscape(tenant) + "/" + pathEscape(namespace) + "/permissions",
		}, nil
	case "topic":
		if resourceName == "" {
			return pulsarACLTarget{}, apperrors.New(apperrors.CodeUsageError, "Pulsar topic ACL resource name is required", nil)
		}
		topic := shortTopicName(resourceName)
		return pulsarACLTarget{
			resourceType: "topic",
			resourceName: topic,
			path:         b.topicPath(topic) + "/permissions",
		}, nil
	default:
		return pulsarACLTarget{}, apperrors.New(apperrors.CodeUsageError, "Pulsar ACL resource type must be namespace or topic", nil)
	}
}

func (b *Broker) pulsarNamespaceParts(resourceName string, allowDefault bool) (string, string, error) {
	if resourceName == "" && allowDefault {
		return b.opts.Tenant, b.opts.Namespace, nil
	}
	parts := strings.Split(resourceName, "/")
	if len(parts) != 2 || strings.TrimSpace(parts[0]) == "" || strings.TrimSpace(parts[1]) == "" {
		return "", "", apperrors.New(apperrors.CodeUsageError, "Pulsar namespace ACL resource name must be tenant/namespace", nil)
	}
	return strings.TrimSpace(parts[0]), strings.TrimSpace(parts[1]), nil
}

func (b *Broker) pulsarPermissions(ctx context.Context, target pulsarACLTarget) (map[string][]string, error) {
	body, err := b.adminJSON(ctx, http.MethodGet, target.path, nil)
	if err != nil {
		return nil, err
	}
	permissions := map[string][]string{}
	if len(bytes.TrimSpace(body)) == 0 {
		return permissions, nil
	}
	if err := json.Unmarshal(body, &permissions); err != nil {
		return nil, backendErr(fmt.Errorf("decode Pulsar permissions: %w", err))
	}
	return permissions, nil
}

func (b *Broker) postPulsarPermissions(ctx context.Context, target pulsarACLTarget, role string, actions []string) error {
	data, err := json.Marshal(actions)
	if err != nil {
		return backendErr(err)
	}
	_, err = b.adminJSON(ctx, http.MethodPost, target.path+"/"+pathEscape(role), data)
	return err
}

func pulsarMergeAction(actions []string, action string) []string {
	seen := map[string]struct{}{}
	out := make([]string, 0, len(actions)+1)
	for _, existing := range actions {
		normalized := strings.ToLower(strings.TrimSpace(existing))
		if normalized == "" {
			continue
		}
		if _, ok := seen[normalized]; ok {
			continue
		}
		seen[normalized] = struct{}{}
		out = append(out, normalized)
	}
	normalized := strings.ToLower(strings.TrimSpace(action))
	if _, ok := seen[normalized]; !ok {
		out = append(out, normalized)
	}
	sort.Strings(out)
	return out
}

func pulsarRemoveAction(actions []string, action string) []string {
	remove := strings.ToLower(strings.TrimSpace(action))
	out := make([]string, 0, len(actions))
	seen := map[string]struct{}{}
	for _, existing := range actions {
		normalized := strings.ToLower(strings.TrimSpace(existing))
		if normalized == "" || normalized == remove {
			continue
		}
		if _, ok := seen[normalized]; ok {
			continue
		}
		seen[normalized] = struct{}{}
		out = append(out, normalized)
	}
	sort.Strings(out)
	return out
}

func pulsarACLMatches(binding mqgov.ACLBinding, filter mqgov.ACLFilter) bool {
	if filter.Principal != "" && binding.Principal != filter.Principal {
		return false
	}
	if filter.ResourceType != "" && normalizePulsarACLValue(filter.ResourceType) != binding.ResourceType {
		return false
	}
	if filter.ResourceName != "" && binding.ResourceName != strings.TrimSpace(filter.ResourceName) {
		return false
	}
	if filter.PatternType != "" && !pulsarLiteralPattern(filter.PatternType) {
		return false
	}
	if filter.Operation != "" && normalizePulsarACLValue(filter.Operation) != binding.Operation {
		return false
	}
	if filter.Permission != "" && strings.ToLower(strings.TrimSpace(filter.Permission)) != "allow" {
		return false
	}
	return true
}

func pulsarACLAction(action string) bool {
	switch normalizePulsarACLValue(action) {
	case "produce", "consume", "functions", "sources", "sinks", "packages":
		return true
	default:
		return false
	}
}

func pulsarLiteralPattern(pattern string) bool {
	normalized := normalizePulsarACLValue(pattern)
	return normalized == "" || normalized == "literal"
}

func pulsarACLSortKey(binding mqgov.ACLBinding) string {
	return binding.ResourceType + "\x00" + binding.ResourceName + "\x00" + binding.Principal + "\x00" + binding.Operation
}

func normalizePulsarACLValue(value string) string {
	return strings.ToLower(strings.NewReplacer("_", "", "-", "", ".", "").Replace(strings.TrimSpace(value)))
}

func (b *Broker) pulsarSchema(ctx context.Context, subject, version string) (pulsarSchemaInfo, error) {
	path := b.schemaPath(subject) + "/schema"
	if version != "" && version != "latest" {
		path += "/" + pathEscape(version)
	}
	body, err := b.adminJSON(ctx, http.MethodGet, path, nil)
	if err != nil {
		return pulsarSchemaInfo{}, err
	}
	var info pulsarSchemaInfo
	if err := json.Unmarshal(body, &info); err != nil {
		return pulsarSchemaInfo{}, backendErr(fmt.Errorf("decode Pulsar schema: %w", err))
	}
	return info, nil
}

func (b *Broker) pulsarSchemaVersions(ctx context.Context, subject string) ([]string, error) {
	body, err := b.adminJSON(ctx, http.MethodGet, b.schemaPath(subject)+"/versions", nil)
	if err != nil {
		return nil, err
	}
	var raw []any
	if err := json.Unmarshal(body, &raw); err != nil {
		var ints []int
		if intErr := json.Unmarshal(body, &ints); intErr != nil {
			return nil, backendErr(fmt.Errorf("decode Pulsar schema versions: %w", err))
		}
		out := make([]string, 0, len(ints))
		for _, version := range ints {
			out = append(out, strconv.Itoa(version))
		}
		return out, nil
	}
	out := make([]string, 0, len(raw))
	for _, version := range raw {
		out = append(out, pulsarSchemaVersionString(version))
	}
	return out, nil
}

func pulsarCompatibility(body []byte) (bool, string, error) {
	trimmed := strings.TrimSpace(string(body))
	if trimmed == "" {
		return false, "", fmt.Errorf("pulsar compatibility response is empty")
	}
	var result struct {
		Compatible   *bool    `json:"compatible"`
		IsCompatible *bool    `json:"is_compatible"`
		Message      string   `json:"message"`
		Messages     []string `json:"messages"`
	}
	if err := json.Unmarshal(body, &result); err != nil {
		return false, "", fmt.Errorf("decode Pulsar compatibility response: %w", err)
	}
	if result.Compatible == nil && result.IsCompatible == nil {
		return false, "", fmt.Errorf("pulsar compatibility response is missing a compatibility result")
	}
	if result.Compatible != nil && result.IsCompatible != nil && *result.Compatible != *result.IsCompatible {
		return false, "", fmt.Errorf("pulsar compatibility response contains conflicting results")
	}
	compatible := result.Compatible
	if compatible == nil {
		compatible = result.IsCompatible
	}
	return *compatible, firstNonEmpty(result.Message, strings.Join(result.Messages, "; ")), nil
}

func pulsarSchemaVersionString(version any) string {
	switch v := version.(type) {
	case string:
		return v
	case float64:
		return strconv.FormatInt(int64(v), 10)
	case int:
		return strconv.Itoa(v)
	case int64:
		return strconv.FormatInt(v, 10)
	default:
		return fmt.Sprint(v)
	}
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

func (b *Broker) schemaPath(subject string) string {
	return "/admin/v2/schemas/" + pathEscape(b.opts.Tenant) + "/" + pathEscape(b.opts.Namespace) + "/" + pathEscape(shortTopicName(subject))
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

func tailFingerprint(msg pulsarclient.Message) mqgov.MessageFingerprint {
	fp := fingerprint(msg)
	if !msg.EventTime().IsZero() {
		fp.Timestamp = msg.EventTime().UTC().Format(time.RFC3339Nano)
	}
	return fp
}

func pulsarTailStart(from string) (pulsarclient.MessageID, bool, error) {
	switch {
	case from == "" || from == "earliest":
		return pulsarclient.EarliestMessageID(), true, nil
	case from == "latest":
		return pulsarclient.LatestMessageID(), false, nil
	case strings.HasPrefix(from, "offset:"):
		return nil, false, apperrors.New(apperrors.CodeUsageError, "Pulsar tail does not support offset:N start positions", nil)
	default:
		return nil, false, apperrors.New(apperrors.CodeUsageError, "unsupported tail start position", nil)
	}
}

func pulsarMirrorStart(from string) (pulsarclient.MessageID, bool, time.Time, error) {
	switch {
	case from == "" || from == "earliest":
		return pulsarclient.EarliestMessageID(), true, time.Time{}, nil
	case from == "latest":
		return pulsarclient.LatestMessageID(), false, time.Time{}, nil
	case strings.HasPrefix(from, "offset:"):
		return nil, false, time.Time{}, apperrors.New(apperrors.CodeUsageError, "Pulsar mirror does not support offset:N start positions", nil)
	case strings.HasPrefix(from, "timestamp:"), strings.HasPrefix(from, "datetime:"):
		value := strings.TrimPrefix(strings.TrimPrefix(from, "timestamp:"), "datetime:")
		t, err := time.Parse(time.RFC3339, value)
		if err != nil {
			return nil, false, time.Time{}, apperrors.New(apperrors.CodeUsageError, "invalid mirror timestamp", nil)
		}
		return pulsarclient.EarliestMessageID(), true, t, nil
	default:
		return nil, false, time.Time{}, apperrors.New(apperrors.CodeUsageError, "unsupported mirror start position", nil)
	}
}

func updateTailImpact(impact map[int]*mqgov.PartitionImpact, partition int, offset int64) {
	item, ok := impact[partition]
	if !ok {
		impact[partition] = &mqgov.PartitionImpact{Partition: partition, From: offset, To: offset + 1, Count: 1}
		return
	}
	if offset < item.From {
		item.From = offset
	}
	if offset+1 > item.To {
		item.To = offset + 1
	}
	item.Count++
}

func tailImpactSlice(impact map[int]*mqgov.PartitionImpact) []mqgov.PartitionImpact {
	out := make([]mqgov.PartitionImpact, 0, len(impact))
	for _, item := range impact {
		out = append(out, *item)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Partition < out[j].Partition })
	return out
}

func maxInt(a, b int) int {
	if a > b {
		return a
	}
	return b
}

func buildTLSConfig(opts Options) (*tls.Config, error) {
	if !pulsarUsesTLS(opts) && opts.CACertFile == "" && opts.ClientCertFile == "" && opts.ClientKeyFile == "" {
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
	if appErr := tlspin.AppError(err); appErr != nil {
		return appErr
	}
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

func bytesHeaders(headers map[string]string) map[string][]byte {
	if len(headers) == 0 {
		return nil
	}
	out := make(map[string][]byte, len(headers))
	for key, value := range headers {
		out[key] = []byte(value)
	}
	return out
}

func (b *Broker) resolvePulsarDLQ(dlq, topic, group string) string {
	if topic != "" && group != "" {
		return pulsarDLQName(topic, group)
	}
	return shortTopicName(dlq)
}

func pulsarDLQName(topic, group string) string {
	return shortTopicName(topic) + "-" + group + "-DLQ"
}

func pulsarDLQParts(dlq string) (string, string) {
	name := strings.TrimSuffix(shortTopicName(dlq), "-DLQ")
	idx := strings.LastIndex(name, "-")
	if idx <= 0 {
		return name, ""
	}
	return name[:idx], name[idx+1:]
}

func pulsarTopicMessages(desc mqgov.TopicDescription) int64 {
	if value := desc.Config["backlog"]; value != "" {
		parsed, _ := strconv.ParseInt(value, 10, 64)
		return parsed
	}
	return 0
}

func timeout(opts Options) time.Duration {
	if opts.Timeout > 0 {
		return opts.Timeout
	}
	return 30 * time.Second
}

func pulsarUsesTLS(opts Options) bool {
	return opts.TLS ||
		strings.HasPrefix(strings.ToLower(strings.TrimSpace(opts.ServiceURL)), "pulsar+ssl://") ||
		strings.HasPrefix(strings.ToLower(strings.TrimSpace(opts.AdminURL)), "https://")
}
