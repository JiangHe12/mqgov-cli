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

	"github.com/JiangHe12/opskit-core/apperrors"

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
	opts Options
}

type topicAdmin interface {
	CreateTopic(ctx context.Context, opts ...rmqadmin.OptionCreate) error
	DeleteTopic(ctx context.Context, opts ...rmqadmin.OptionDelete) error
	FetchAllTopicList(ctx context.Context) (*rmqadmin.TopicList, error)
	FetchPublishMessageQueues(ctx context.Context, topic string) ([]*primitive.MessageQueue, error)
	Close() error
}

func New(opts Options) (*Broker, error) {
	opts.NameServers = cleanedNameServers(opts.NameServers)
	if len(opts.NameServers) == 0 {
		return nil, apperrors.New(apperrors.CodeUsageError, "RocketMQ name server addresses not specified", nil)
	}
	if opts.TLS || opts.CACertFile != "" || opts.ClientCertFile != "" || opts.ClientKeyFile != "" {
		return nil, apperrors.New(apperrors.CodeNotImplemented, "RocketMQ v2 client TLS is not supported by this backend", nil)
	}
	if (opts.AccessKey == "") != (opts.SecretKey == "") {
		return nil, apperrors.New(apperrors.CodeUsageError, "RocketMQ ACL requires both access key and secret key", nil)
	}
	return &Broker{opts: opts}, nil
}

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
		Verbs:              []string{"list", "describe", "produce", "create", "delete"},
		SupportsOffsets:    false,
		SupportsPartitions: false,
		SupportsACL:        false,
		SupportsDLQList:    true,
		SupportsDLQPeek:    false,
		SupportsDLQRedrive: false,
		SupportsDLQPurge:   false,
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
	if _, err := b.DescribeTopic(ctx, req.Coordinate); err == nil {
		return mqgov.TopicDescription{}, apperrors.New(apperrors.CodeResourceAlreadyExists, "topic already exists", nil)
	} else if apperrors.AsAppError(err).Code != apperrors.CodeResourceNotFound {
		return mqgov.TopicDescription{}, err
	}
	admin, err := b.newAdmin()
	if err != nil {
		return mqgov.TopicDescription{}, err
	}
	defer func() { _ = admin.Close() }()
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
		return mqgov.TopicDescription{}, classifyErr(err)
	}
	return b.DescribeTopic(ctx, req.Coordinate)
}

func (b *Broker) DeleteTopic(ctx context.Context, coord mqgov.TopicCoordinate) error {
	if _, err := b.DescribeTopic(ctx, coord); err != nil {
		return err
	}
	admin, err := b.newAdmin()
	if err != nil {
		return err
	}
	defer func() { _ = admin.Close() }()
	opts := []rmqadmin.OptionDelete{rmqadmin.WithTopicDelete(coord.Topic), rmqadmin.WithNameSrvAddr(b.opts.NameServers)}
	if b.opts.BrokerAddr != "" {
		opts = append(opts, rmqadmin.WithBrokerAddrDelete(b.opts.BrokerAddr))
	}
	if err := admin.DeleteTopic(ctx, opts...); err != nil {
		return classifyErr(err)
	}
	return nil
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
	if err := producer.Start(); err != nil {
		return mqgov.MessageProduceResult{}, classifyErr(err)
	}
	defer func() { _ = producer.Shutdown() }()
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
	queues, err := admin.FetchPublishMessageQueues(ctx, topic)
	if err != nil {
		return nil, classifyErr(err)
	}
	sort.Slice(queues, func(i, j int) bool {
		if queues[i].BrokerName == queues[j].BrokerName {
			return queues[i].QueueId < queues[j].QueueId
		}
		return queues[i].BrokerName < queues[j].BrokerName
	})
	return queues, nil
}

func (b *Broker) newAdmin() (topicAdmin, error) {
	opts := []rmqadmin.AdminOption{rmqadmin.WithResolver(primitive.NewPassthroughResolver(b.opts.NameServers))}
	if creds := b.credentials(); !creds.IsEmpty() {
		opts = append(opts, rmqadmin.WithCredentials(creds))
	}
	if b.opts.Namespace != "" {
		opts = append(opts, rmqadmin.WithNamespace(b.opts.Namespace))
	}
	admin, err := rmqadmin.NewAdmin(opts...)
	if err != nil {
		return nil, classifyErr(err)
	}
	return admin, nil
}

func (b *Broker) newProducer() (rocketmqclient.Producer, error) {
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
	if errors.Is(err, rmqerrors.ErrTopicNotExist) || errors.Is(err, rmqerrors.ErrNotExisted) || strings.Contains(strings.ToLower(err.Error()), "topic not exist") {
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
