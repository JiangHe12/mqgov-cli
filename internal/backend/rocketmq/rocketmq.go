package rocketmq

import (
	"context"
	"errors"
	"net"
	"sort"
	"strconv"
	"strings"
	"time"

	rocketmqclient "github.com/apache/rocketmq-client-go/v2"
	rmqadmin "github.com/apache/rocketmq-client-go/v2/admin"
	rmqerrors "github.com/apache/rocketmq-client-go/v2/errors"
	"github.com/apache/rocketmq-client-go/v2/primitive"
	rmqproducer "github.com/apache/rocketmq-client-go/v2/producer"

	"github.com/JiangHe12/opskit-core/v2/apperrors"

	"github.com/JiangHe12/mqgov-cli/internal/mqgov"
)

type Options struct {
	NameServers    []string
	BrokerAddr     string
	Cluster        string
	Namespace      string
	AccessKey      string
	SecretKey      string
	TLS            bool
	CACertFile     string
	ClientCertFile string
	ClientKeyFile  string
	Timeout        time.Duration
}

type Broker struct {
	opts                       Options
	producerFactory            func() (rocketmqclient.Producer, error)
	adminFactory               func() (topicAdmin, error)
	adminFactoryForNameServers func([]string) (topicAdmin, error)
	topicPropagationInterval   time.Duration
}

const (
	defaultTopicPropagationTimeout  = 10 * time.Second
	defaultTopicPropagationInterval = 200 * time.Millisecond
)

type topicAdmin interface {
	CreateTopic(ctx context.Context, opts ...rmqadmin.OptionCreate) error
	DeleteTopic(ctx context.Context, opts ...rmqadmin.OptionDelete) error
	FetchAllTopicList(ctx context.Context) (*rmqadmin.TopicList, error)
	FetchPublishMessageQueues(ctx context.Context, topic string) ([]*primitive.MessageQueue, error)
	Close() error
}

func New(opts Options) (*Broker, error) {
	opts.NameServers = cleanedNameServers(opts.NameServers)
	opts.Namespace = strings.TrimSpace(opts.Namespace)
	if len(opts.NameServers) == 0 {
		return nil, apperrors.New(apperrors.CodeUsageError, "RocketMQ name server addresses not specified", nil)
	}
	if opts.TLS || opts.CACertFile != "" || opts.ClientCertFile != "" || opts.ClientKeyFile != "" {
		return nil, apperrors.New(apperrors.CodeNotImplemented, "RocketMQ v2 client TLS is not supported by this backend", nil)
	}
	if (opts.AccessKey == "") != (opts.SecretKey == "") {
		return nil, apperrors.New(apperrors.CodeUsageError, "RocketMQ ACL requires both access key and secret key", nil)
	}
	if opts.Namespace != "" {
		return nil, apperrors.New(apperrors.CodeNotImplemented, "RocketMQ namespace is not supported consistently by the v2 admin client", nil)
	}
	return &Broker{opts: opts}, nil
}

func (*Broker) Close() {}

func (b *Broker) Ping(ctx context.Context) error {
	admin, err := b.newAdmin()
	if err != nil {
		return err
	}
	defer func() { _ = admin.Close() }()
	if _, err := admin.FetchAllTopicList(ctx); err != nil {
		return classifyErr(err)
	}
	return nil
}

func (b *Broker) Describe() mqgov.Description {
	return mqgov.Description{Backend: "rocketmq", Cluster: b.opts.Cluster, Namespace: b.opts.Namespace}
}

func (b *Broker) Capabilities() mqgov.Capabilities {
	// RocketMQ broker ACL management stays unsupported until rocketmq-client-go/v2
	// exposes a public, cgo-free admin API for plain_acl. The public admin.Admin
	// interface has no ACL config methods; continuing would require Go internal
	// packages or hand-rolled remoting commands, both of which are forbidden here.
	return mqgov.Capabilities{
		Backend:            "rocketmq",
		ResourceTypes:      []string{"topic", "message", "dlq"},
		Verbs:              []string{"list", "describe", "produce", "create"},
		SupportsOffsets:    false,
		SupportsPartitions: false,
		SupportsACL:        false,
		SupportsDLQList:    true,
		SupportsDLQPeek:    false,
		SupportsDLQRedrive: false,
		SupportsDLQPurge:   false,
		SupportsSchema:     false,
	}
}

func (b *Broker) ListTopics(ctx context.Context, opts mqgov.TopicListOptions) ([]mqgov.TopicDescription, error) {
	admin, err := b.newAdmin()
	if err != nil {
		return nil, err
	}
	defer func() { _ = admin.Close() }()
	list, err := admin.FetchAllTopicList(ctx)
	if err != nil {
		return nil, classifyErr(err)
	}
	items := make([]mqgov.TopicDescription, 0, len(list.TopicList))
	for _, topic := range list.TopicList {
		if opts.Pattern != "" && opts.Pattern != topic {
			continue
		}
		items = append(items, mqgov.TopicDescription{
			Coordinate: mqgov.TopicCoordinate{Cluster: b.opts.Cluster, Namespace: b.opts.Namespace, Topic: topic},
			Internal:   isInternalTopic(topic),
		})
	}
	sort.Slice(items, func(i, j int) bool { return items[i].Coordinate.Topic < items[j].Coordinate.Topic })
	if opts.Limit > 0 && len(items) > opts.Limit {
		items = items[:opts.Limit]
	}
	return items, nil
}

func (b *Broker) DescribeTopic(ctx context.Context, coord mqgov.TopicCoordinate) (mqgov.TopicDescription, error) {
	queues, err := b.topicQueues(ctx, coord.Topic)
	if err != nil {
		return mqgov.TopicDescription{}, err
	}
	return mqgov.TopicDescription{
		Coordinate: mqgov.TopicCoordinate{Cluster: b.opts.Cluster, Namespace: b.opts.Namespace, Topic: coord.Topic},
		Partitions: len(queues),
		Internal:   isInternalTopic(coord.Topic),
		Config: map[string]string{
			"queues": strconv.Itoa(len(queues)),
		},
	}, nil
}

func (b *Broker) CreateTopic(ctx context.Context, req mqgov.TopicCreateRequest) (mqgov.TopicDescription, error) {
	if b.opts.BrokerAddr == "" {
		return mqgov.TopicDescription{}, apperrors.New(apperrors.CodeUsageError, "RocketMQ broker address is required to create topics", nil)
	}
	if err := b.ensureTopicAbsentBeforeCreate(ctx, req.Coordinate); err != nil {
		return mqgov.TopicDescription{}, err
	}
	if err := ctx.Err(); err != nil {
		return mqgov.TopicDescription{}, err
	}
	admin, err := b.newAdmin()
	if err != nil {
		return mqgov.TopicDescription{}, err
	}
	defer func() { _ = admin.Close() }()
	if err := ctx.Err(); err != nil {
		return mqgov.TopicDescription{}, err
	}
	queues := req.Partitions
	if queues <= 0 {
		queues = 8
	}
	err = admin.CreateTopic(ctx,
		rmqadmin.WithTopicCreate(req.Coordinate.Topic),
		rmqadmin.WithBrokerAddrCreate(b.opts.BrokerAddr),
		rmqadmin.WithReadQueueNums(queues),
		rmqadmin.WithWriteQueueNums(queues),
	)
	if err != nil {
		return mqgov.TopicDescription{}, apperrors.New(
			apperrors.CodePartialFailure,
			"RocketMQ topic create request outcome is uncertain",
			classifyErr(err),
		)
	}
	description, err := b.waitForTopicPropagation(ctx, admin, req.Coordinate, queues)
	if err != nil {
		return mqgov.TopicDescription{}, apperrors.New(
			apperrors.CodePartialFailure,
			"RocketMQ topic create request returned without a client-reported error but confirmation failed",
			err,
		)
	}
	return description, nil
}

func (b *Broker) ensureTopicAbsentBeforeCreate(ctx context.Context, coord mqgov.TopicCoordinate) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if b.adminFactory != nil {
		return ensureRocketMQTopicAbsent(ctx, b, coord)
	}
	if len(b.opts.NameServers) <= 1 {
		return b.ensureTopicAbsentOnNameServer(ctx, b.opts.NameServers, coord)
	}
	for _, nameServer := range b.opts.NameServers {
		if err := ctx.Err(); err != nil {
			return err
		}
		if err := b.ensureTopicAbsentOnNameServer(ctx, []string{nameServer}, coord); err != nil {
			return err
		}
	}
	return nil
}

func (b *Broker) ensureTopicAbsentOnNameServer(ctx context.Context, nameServers []string, coord mqgov.TopicCoordinate) error {
	admin, err := b.newAdminForNameServers(nameServers)
	if err != nil {
		return err
	}
	defer func() { _ = admin.Close() }()
	absent, err := rocketMQTopicAbsent(ctx, admin, coord.Topic)
	if err != nil {
		return err
	}
	if !absent {
		return apperrors.New(apperrors.CodeResourceAlreadyExists, "topic already exists", nil)
	}
	return nil
}

func ensureRocketMQTopicAbsent(ctx context.Context, backend *Broker, coord mqgov.TopicCoordinate) error {
	if _, err := backend.DescribeTopic(ctx, coord); err == nil {
		return apperrors.New(apperrors.CodeResourceAlreadyExists, "topic already exists", nil)
	} else if apperrors.AsAppError(err).Code != apperrors.CodeResourceNotFound {
		return err
	}
	return nil
}

func (b *Broker) waitForTopicPropagation(
	ctx context.Context,
	admin topicAdmin,
	coord mqgov.TopicCoordinate,
	queues int,
) (mqgov.TopicDescription, error) {
	timeout := b.opts.Timeout
	if timeout <= 0 {
		timeout = defaultTopicPropagationTimeout
	}
	interval := b.topicPropagationInterval
	if interval <= 0 {
		interval = defaultTopicPropagationInterval
	}
	waitCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	for {
		description, confirmed, err := b.confirmTopicOnEveryNameServer(waitCtx, admin, coord, queues)
		if err == nil && confirmed {
			return description, nil
		}
		if waitCtx.Err() != nil {
			if ctx.Err() != nil {
				return mqgov.TopicDescription{}, ctx.Err()
			}
			return mqgov.TopicDescription{}, apperrors.New(
				apperrors.CodeResourceNotFound,
				"RocketMQ topic was not visible before the confirmation timeout",
				waitCtx.Err(),
			)
		}
		if err != nil {
			return mqgov.TopicDescription{}, err
		}

		timer := time.NewTimer(interval)
		select {
		case <-waitCtx.Done():
			timer.Stop()
			if ctx.Err() != nil {
				return mqgov.TopicDescription{}, ctx.Err()
			}
			return mqgov.TopicDescription{}, apperrors.New(
				apperrors.CodeResourceNotFound,
				"RocketMQ topic was not visible before the confirmation timeout",
				waitCtx.Err(),
			)
		case <-timer.C:
		}
	}
}

func (b *Broker) confirmTopicOnEveryNameServer(
	ctx context.Context,
	fallbackAdmin topicAdmin,
	coord mqgov.TopicCoordinate,
	expectedQueues int,
) (mqgov.TopicDescription, bool, error) {
	if b.adminFactory != nil {
		return b.confirmTopicWithAdmin(ctx, fallbackAdmin, coord, expectedQueues)
	}
	var confirmed mqgov.TopicDescription
	for _, nameServer := range b.opts.NameServers {
		if err := ctx.Err(); err != nil {
			return mqgov.TopicDescription{}, false, err
		}
		admin, err := b.newAdminForNameServers([]string{nameServer})
		if err != nil {
			return mqgov.TopicDescription{}, false, err
		}
		description, visible, confirmErr := b.confirmTopicWithAdmin(ctx, admin, coord, expectedQueues)
		_ = admin.Close()
		if confirmErr != nil || !visible {
			return mqgov.TopicDescription{}, false, confirmErr
		}
		confirmed = description
	}
	if len(b.opts.NameServers) == 0 {
		return mqgov.TopicDescription{}, false, apperrors.New(apperrors.CodeUsageError, "RocketMQ name server addresses not specified", nil)
	}
	return confirmed, true, nil
}

func (b *Broker) confirmTopicWithAdmin(
	ctx context.Context,
	admin topicAdmin,
	coord mqgov.TopicCoordinate,
	expectedQueues int,
) (mqgov.TopicDescription, bool, error) {
	description, err := b.describeTopicWithAdmin(ctx, admin, coord)
	if err != nil {
		if apperrors.AsAppError(err).Code == apperrors.CodeResourceNotFound {
			return mqgov.TopicDescription{}, false, nil
		}
		return mqgov.TopicDescription{}, false, err
	}
	if description.Partitions != expectedQueues {
		return mqgov.TopicDescription{}, false, apperrors.New(
			apperrors.CodeConflict,
			"RocketMQ topic queue count does not match the requested count",
			nil,
		)
	}
	return description, true, nil
}

func (*Broker) DeleteTopic(context.Context, mqgov.TopicCoordinate) error {
	return notImplemented("RocketMQ topic delete is disabled because the v2 admin client cannot prove broker-side deletion")
}

func rocketMQTopicAbsent(ctx context.Context, admin topicAdmin, topic string) (bool, error) {
	queues, err := admin.FetchPublishMessageQueues(ctx, topic)
	if ctxErr := ctx.Err(); ctxErr != nil {
		return false, ctxErr
	}
	if err == nil {
		if len(queues) == 0 {
			return false, apperrors.New(apperrors.CodeBackendError, "RocketMQ topic route response is empty", nil)
		}
		return false, nil
	}
	classified := classifyErr(err)
	if apperrors.AsAppError(classified).Code == apperrors.CodeResourceNotFound {
		return true, nil
	}
	return false, classified
}

func (b *Broker) ListGroups(context.Context, mqgov.GroupListOptions) ([]mqgov.GroupDescription, error) {
	return nil, notImplemented("RocketMQ consumer group listing is not supported")
}

func (b *Broker) CreateGroup(context.Context, mqgov.GroupCoordinate) (mqgov.GroupDescription, error) {
	return mqgov.GroupDescription{}, notImplemented("RocketMQ consumer groups are created by consumers")
}

func (b *Broker) DeleteGroup(context.Context, mqgov.GroupCoordinate) error {
	return notImplemented("RocketMQ consumer group delete is not supported")
}

func (b *Broker) Peek(ctx context.Context, req mqgov.MessagePeekRequest) (mqgov.MessagePeekResult, error) {
	return mqgov.MessagePeekResult{}, notImplemented("RocketMQ v2 PullConsumer cannot provide a guaranteed non-destructive peek")
}

func (b *Broker) Produce(ctx context.Context, req mqgov.MessageProduceRequest) (mqgov.MessageProduceResult, error) {
	producer, err := b.newProducer()
	if err != nil {
		return mqgov.MessageProduceResult{}, err
	}
	defer func() { _ = producer.Shutdown() }()
	if err := producer.Start(); err != nil {
		return mqgov.MessageProduceResult{}, classifyErr(err)
	}
	msg := primitive.NewMessage(req.Coordinate.Topic, req.Body)
	if len(req.Key) > 0 {
		msg.WithKeys([]string{string(req.Key)})
	}
	for key, value := range req.Headers {
		msg.WithProperty(key, string(value))
	}
	result, err := producer.SendSync(ctx, msg)
	if err != nil {
		return mqgov.MessageProduceResult{}, classifyErr(err)
	}
	partition := 0
	if result.MessageQueue != nil {
		partition = result.MessageQueue.QueueId
	}
	return mqgov.MessageProduceResult{
		Coordinate:  mqgov.MessageCoordinate{TopicCoordinate: req.Coordinate, Partition: partition, Offset: result.QueueOffset},
		Fingerprint: mqgov.Fingerprints(req.Key, req.Body, 1),
	}, nil
}

func (b *Broker) ListDLQs(ctx context.Context, opts mqgov.DLQListOptions) ([]mqgov.DLQDescription, error) {
	topics, err := b.ListTopics(ctx, mqgov.TopicListOptions{Pattern: opts.Pattern, Limit: opts.Limit})
	if err != nil {
		return nil, err
	}
	items := make([]mqgov.DLQDescription, 0)
	for _, topic := range topics {
		if !strings.HasPrefix(topic.Coordinate.Topic, "%DLQ%") {
			continue
		}
		group := strings.TrimPrefix(topic.Coordinate.Topic, "%DLQ%")
		if opts.Group != "" && opts.Group != group {
			continue
		}
		items = append(items, mqgov.DLQDescription{
			Coordinate:    topic.Coordinate,
			ConsumerGroup: group,
			NativeModel:   "%DLQ%{consumerGroup}",
		})
	}
	return items, nil
}

func (b *Broker) PeekDLQ(context.Context, mqgov.DLQPeekRequest) (mqgov.DLQPeekResult, error) {
	return mqgov.DLQPeekResult{}, notImplemented("RocketMQ v2 client cannot provide a guaranteed non-destructive DLQ peek")
}

func (b *Broker) RedriveDLQ(context.Context, mqgov.DLQRedriveRequest) (mqgov.DLQRedriveResult, error) {
	return mqgov.DLQRedriveResult{}, notImplemented("RocketMQ DLQ redrive requires non-destructive DLQ reads not supported by this backend")
}

func (b *Broker) PurgeDLQ(context.Context, mqgov.DLQPurgeRequest) (mqgov.DLQPurgeResult, error) {
	return mqgov.DLQPurgeResult{}, notImplemented("RocketMQ DLQ purge is not supported by this backend")
}

func (b *Broker) topicQueues(ctx context.Context, topic string) ([]*primitive.MessageQueue, error) {
	admin, err := b.newAdmin()
	if err != nil {
		return nil, err
	}
	defer func() { _ = admin.Close() }()
	return topicQueuesWithAdmin(ctx, admin, topic)
}

func topicQueuesWithAdmin(ctx context.Context, admin topicAdmin, topic string) ([]*primitive.MessageQueue, error) {
	queues, err := admin.FetchPublishMessageQueues(ctx, topic)
	if ctxErr := ctx.Err(); ctxErr != nil {
		return nil, ctxErr
	}
	if err != nil {
		return nil, classifyErr(err)
	}
	if len(queues) == 0 {
		return nil, apperrors.New(apperrors.CodeBackendError, "RocketMQ topic route response is empty", nil)
	}
	sort.Slice(queues, func(i, j int) bool {
		if queues[i].BrokerName == queues[j].BrokerName {
			return queues[i].QueueId < queues[j].QueueId
		}
		return queues[i].BrokerName < queues[j].BrokerName
	})
	return queues, nil
}

func (b *Broker) describeTopicWithAdmin(ctx context.Context, admin topicAdmin, coord mqgov.TopicCoordinate) (mqgov.TopicDescription, error) {
	queues, err := topicQueuesWithAdmin(ctx, admin, coord.Topic)
	if err != nil {
		return mqgov.TopicDescription{}, err
	}
	return mqgov.TopicDescription{
		Coordinate: mqgov.TopicCoordinate{Cluster: b.opts.Cluster, Namespace: b.opts.Namespace, Topic: coord.Topic},
		Partitions: len(queues),
		Internal:   isInternalTopic(coord.Topic),
		Config:     map[string]string{"queues": strconv.Itoa(len(queues))},
	}, nil
}

func (b *Broker) newAdmin() (topicAdmin, error) {
	if b.adminFactory != nil {
		return b.adminFactory()
	}
	return b.newAdminForNameServers(b.opts.NameServers)
}

func (b *Broker) newAdminForNameServers(nameServers []string) (topicAdmin, error) {
	if b.adminFactoryForNameServers != nil {
		return b.adminFactoryForNameServers(append([]string(nil), nameServers...))
	}
	opts := []rmqadmin.AdminOption{rmqadmin.WithResolver(primitive.NewPassthroughResolver(nameServers))}
	if creds := b.credentials(); !creds.IsEmpty() {
		opts = append(opts, rmqadmin.WithCredentials(creds))
	}
	admin, err := rmqadmin.NewAdmin(opts...)
	if err != nil {
		return nil, classifyErr(err)
	}
	return admin, nil
}

func (b *Broker) newProducer() (rocketmqclient.Producer, error) {
	if b.producerFactory != nil {
		return b.producerFactory()
	}
	nameServers, err := primitive.NewNamesrvAddr(b.opts.NameServers...)
	if err != nil {
		return nil, apperrors.New(apperrors.CodeUsageError, "invalid RocketMQ name server address", err)
	}
	opts := []rmqproducer.Option{rmqproducer.WithNameServer(nameServers)}
	if creds := b.credentials(); !creds.IsEmpty() {
		opts = append(opts, rmqproducer.WithCredentials(creds))
	}
	if b.opts.Namespace != "" {
		opts = append(opts, rmqproducer.WithNamespace(b.opts.Namespace))
	}
	producer, err := rocketmqclient.NewProducer(opts...)
	if err != nil {
		return nil, classifyErr(err)
	}
	return producer, nil
}

func (b *Broker) credentials() primitive.Credentials {
	return primitive.Credentials{AccessKey: b.opts.AccessKey, SecretKey: b.opts.SecretKey}
}

func notImplemented(message string) error {
	return apperrors.New(apperrors.CodeNotImplemented, message, nil)
}

func classifyErr(err error) error {
	if err == nil {
		return nil
	}
	var appErr *apperrors.AppError
	if errors.As(err, &appErr) {
		return err
	}
	if errors.Is(err, rmqerrors.ErrTopicNotExist) || errors.Is(err, rmqerrors.ErrNotExisted) {
		return apperrors.New(apperrors.CodeResourceNotFound, "topic not found", err)
	}
	if isNetworkErr(err) {
		return apperrors.New(apperrors.CodeBackendUnreachable, "rocketmq backend unreachable", err)
	}
	return apperrors.New(apperrors.CodeBackendError, "rocketmq backend error", err)
}

func isNetworkErr(err error) bool {
	var netErr net.Error
	return errors.As(err, &netErr) || strings.Contains(strings.ToLower(err.Error()), "connectex") || strings.Contains(strings.ToLower(err.Error()), "connection refused")
}

func isInternalTopic(topic string) bool {
	upper := strings.ToUpper(topic)
	return strings.HasPrefix(upper, "RMQ_SYS_") || strings.HasPrefix(upper, "%RETRY%") || strings.HasPrefix(upper, "%DLQ%") || strings.HasPrefix(upper, "SCHEDULE_TOPIC_") || strings.HasPrefix(upper, "TBW102")
}

func cleanedNameServers(in []string) []string {
	out := make([]string, 0, len(in))
	for _, value := range in {
		for _, part := range strings.Split(value, ",") {
			if trimmed := strings.TrimSpace(part); trimmed != "" {
				out = append(out, trimmed)
			}
		}
	}
	return out
}
