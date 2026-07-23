package rabbitmq

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

	amqp "github.com/rabbitmq/amqp091-go"

	"github.com/JiangHe12/opskit-core/v2/apperrors"

	"github.com/JiangHe12/mqgov-cli/internal/mqgov"
	"github.com/JiangHe12/mqgov-cli/internal/tlspin"
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
	TLSPinPath     string
	TLSNotify      tlspin.NotifyFunc
	Timeout        time.Duration
}

type Broker struct {
	opts       Options
	amqpURL    string
	manageURL  string
	httpClient *http.Client
	tlsConfig  *tls.Config
	close      sync.Once
}

func New(opts Options) (*Broker, error) {
	if err := validateTLSConnectionURLs(opts); err != nil {
		return nil, err
	}
	opts = normalizeConnectionCredentials(opts)
	baseTLSConfig, err := buildTLSConfig(opts)
	if err != nil {
		return nil, err
	}
	amqpURL := buildAMQPURL(opts)
	managementURL := buildManagementURL(opts)
	var amqpTLSConfig *tls.Config
	if baseTLSConfig != nil && rabbitMQAMQPUsesTLS(opts, amqpURL) {
		amqpTLSConfig, err = tlspin.CloneForEndpoint(baseTLSConfig, opts.TLSPinPath, amqpURL, rabbitMQTLSNotify(opts))
		if err != nil {
			return nil, err
		}
	}
	httpClient := &http.Client{Timeout: timeout(opts)}
	if baseTLSConfig != nil && strings.HasPrefix(strings.ToLower(strings.TrimSpace(managementURL)), "https://") {
		managementTLSConfig, err := tlspin.CloneForEndpoint(baseTLSConfig, opts.TLSPinPath, managementURL, rabbitMQTLSNotify(opts))
		if err != nil {
			return nil, err
		}
		httpClient.Transport = &http.Transport{TLSClientConfig: managementTLSConfig}
	}
	return &Broker{opts: opts, amqpURL: amqpURL, manageURL: managementURL, httpClient: httpClient, tlsConfig: amqpTLSConfig}, nil
}

func rabbitMQTLSNotify(opts Options) tlspin.NotifyFunc {
	if opts.TLSNotify != nil {
		return opts.TLSNotify
	}
	return tlspin.NotifyDiscard
}

func (b *Broker) Close() {
	if b == nil {
		return
	}
	b.close.Do(func() {
		if b.httpClient != nil {
			b.httpClient.CloseIdleConnections()
		}
	})
}

func validateTLSConnectionURLs(opts Options) error {
	if !opts.TLS {
		return nil
	}
	if err := requireEndpointScheme(opts.AMQPURL, "amqps", "RabbitMQ AMQP"); err != nil {
		return err
	}
	return requireEndpointScheme(opts.ManagementURL, "https", "RabbitMQ management")
}

func requireEndpointScheme(rawURL, wantScheme, endpointName string) error {
	rawURL = strings.TrimSpace(rawURL)
	if rawURL == "" {
		return nil
	}
	parsed, err := url.Parse(rawURL)
	if err != nil || parsed.Host == "" || !strings.EqualFold(parsed.Scheme, wantScheme) {
		return apperrors.New(
			apperrors.CodeUsageError,
			fmt.Sprintf("%s TLS requires a %s:// URL", endpointName, wantScheme),
			nil,
		)
	}
	return nil
}

func normalizeConnectionCredentials(opts Options) Options {
	urlUsername, urlPassword := amqpURLUserInfo(opts.AMQPURL)
	if opts.Username == "" {
		opts.Username = urlUsername
	}
	if opts.Password == "" {
		opts.Password = urlPassword
	}
	if opts.Username == "" {
		opts.Username = "guest"
	}
	if opts.Password == "" {
		opts.Password = "guest"
	}
	return opts
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
		ResourceTypes:      []string{"topic", "message", "acl", "dlq"},
		Verbs:              []string{"list", "describe", "produce", "create", "delete", "purge", "grant-acl", "revoke-acl", "redrive"},
		SupportsOffsets:    false,
		SupportsPartitions: true,
		SupportsACL:        true,
		SupportsDLQList:    true,
		SupportsDLQPeek:    false,
		SupportsDLQRedrive: true,
		SupportsDLQPurge:   true,
		SupportsSchema:     false,
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

func (*Broker) Peek(context.Context, mqgov.MessagePeekRequest) (mqgov.MessagePeekResult, error) {
	return mqgov.MessagePeekResult{}, apperrors.New(
		apperrors.CodeNotImplemented,
		"RabbitMQ does not provide a non-consuming peek operation",
		nil,
	)
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

func (b *Broker) ListDLQs(ctx context.Context, opts mqgov.DLQListOptions) ([]mqgov.DLQDescription, error) {
	queues, err := b.listQueues(ctx)
	if err != nil {
		return nil, err
	}
	sources := rabbitMQDLQSources(queues)
	items := make([]mqgov.DLQDescription, 0)
	for _, queue := range queues {
		if opts.Pattern != "" && opts.Pattern != queue.Name {
			continue
		}
		if opts.Topic != "" && opts.Topic != queue.Name {
			continue
		}
		source := sources[queue.Name]
		if source == "" && rabbitMQDLQByName(queue.Name) {
			source = rabbitMQSourceQueue(queue.Name)
		}
		if source == "" {
			continue
		}
		items = append(items, mqgov.DLQDescription{
			Coordinate:  mqgov.TopicCoordinate{Cluster: b.opts.Cluster, Namespace: b.vhost(), Topic: queue.Name},
			SourceTopic: source,
			NativeModel: "dead-letter-exchange queue",
			Messages:    int64(queue.Messages),
		})
	}
	sort.Slice(items, func(i, j int) bool { return items[i].Coordinate.Topic < items[j].Coordinate.Topic })
	return items, nil
}

func (*Broker) PeekDLQ(context.Context, mqgov.DLQPeekRequest) (mqgov.DLQPeekResult, error) {
	return mqgov.DLQPeekResult{}, apperrors.New(
		apperrors.CodeNotImplemented,
		"RabbitMQ does not provide a non-consuming DLQ peek operation",
		nil,
	)
}

func (b *Broker) RedriveDLQ(ctx context.Context, req mqgov.DLQRedriveRequest) (mqgov.DLQRedriveResult, error) {
	count := req.Count
	if count <= 0 {
		count = 100
	}
	if count > mqgov.MaxMessageBatchSize {
		return mqgov.DLQRedriveResult{}, apperrors.New(apperrors.CodeUsageError, "redrive count exceeds the safe batch limit", nil)
	}
	if req.DLQ.Topic == req.Target.Topic {
		return mqgov.DLQRedriveResult{}, apperrors.New(apperrors.CodeUsageError, "RabbitMQ redrive target must differ from the DLQ", nil)
	}
	if req.DryRun {
		queue, err := b.inspectQueue(ctx, req.DLQ.Topic)
		if err != nil {
			return mqgov.DLQRedriveResult{}, err
		}
		if _, err := b.inspectQueue(ctx, req.Target.Topic); err != nil {
			return mqgov.DLQRedriveResult{}, err
		}
		total := int64(queue.Messages)
		if total > int64(count) {
			total = int64(count)
		}
		return mqgov.DLQRedriveResult{DLQ: req.DLQ, Target: req.Target, DryRun: true, Impact: []mqgov.PartitionImpact{{Partition: 0, Count: total}}, Total: total, Fingerprint: mqgov.ResourceFingerprints{Count: total}}, nil
	}
	batch, err := withChannel(ctx, b, func(ch *amqp.Channel) (rabbitMQRedriveBatchResult, error) {
		return rabbitMQRedriveBatch(ctx, ch, req, count)
	})
	result := mqgov.DLQRedriveResult{
		BatchOutcome: batch.outcome,
		DLQ:          req.DLQ,
		Target:       req.Target,
		DryRun:       false,
		Impact:       []mqgov.PartitionImpact{{Partition: 0, Count: batch.total}},
		Total:        batch.total,
		Fingerprint:  mqgov.ResourceFingerprints{Count: batch.total},
	}
	if err != nil {
		return result, rabbitMQRedriveFailure(batch.outcome, err)
	}
	return result, nil
}

type rabbitMQRedriveBatchResult struct {
	total   int64
	outcome mqgov.BatchOutcome
}

func rabbitMQRedriveBatch(ctx context.Context, ch *amqp.Channel, req mqgov.DLQRedriveRequest, count int) (rabbitMQRedriveBatchResult, error) {
	var result rabbitMQRedriveBatchResult
	if err := ch.Confirm(false); err != nil {
		return result, err
	}
	returns := ch.NotifyReturn(make(chan amqp.Return, 1))
	for int(result.total) < count {
		msg, ok, err := ch.Get(req.DLQ.Topic, false)
		if err != nil {
			result.outcome.Failed++
			return result, err
		}
		if !ok {
			return result, nil
		}
		err = publishConfirmed(ctx, ch, returns, req.Target.Topic, amqp.Publishing{
			DeliveryMode: amqp.Persistent,
			Headers:      msg.Headers,
			Body:         msg.Body,
		})
		if err != nil {
			nackErr := msg.Nack(false, true)
			if nackErr != nil || rabbitMQRedrivePublishIsUncertain(err) {
				result.outcome.Uncertain++
			} else {
				result.outcome.Failed++
			}
			if nackErr != nil {
				return result, fmt.Errorf("RabbitMQ redrive publish failed and source requeue was not confirmed: %w", errors.Join(err, nackErr))
			}
			return result, err
		}
		if err := msg.Ack(false); err != nil {
			result.outcome.Uncertain++
			return result, err
		}
		result.total++
		result.outcome.Succeeded++
	}
	return result, nil
}

func rabbitMQRedrivePublishIsUncertain(err error) bool {
	var appErr *apperrors.AppError
	if !errors.As(err, &appErr) {
		return true
	}
	return appErr.Code == apperrors.CodeBackendUnreachable || appErr.Code == apperrors.CodeNetworkError
}

func rabbitMQRedriveFailure(outcome mqgov.BatchOutcome, cause error) error {
	var appErr *apperrors.AppError
	if !errors.As(cause, &appErr) {
		cause = classifyAMQPError(cause)
	}
	if outcome.Succeeded == 0 && outcome.Uncertain == 0 {
		return cause
	}
	return apperrors.New(
		apperrors.CodePartialFailure,
		fmt.Sprintf(
			"RabbitMQ redrive did not complete atomically (succeeded=%d failed=%d uncertain=%d)",
			outcome.Succeeded,
			outcome.Failed,
			outcome.Uncertain,
		),
		cause,
	)
}

func (b *Broker) PurgeDLQ(ctx context.Context, req mqgov.DLQPurgeRequest) (mqgov.DLQPurgeResult, error) {
	result, err := b.PurgeTopic(ctx, mqgov.TopicPurgeRequest{Coordinate: req.DLQ, DLQ: true, DryRun: req.DryRun})
	if err != nil {
		return mqgov.DLQPurgeResult{}, err
	}
	return mqgov.DLQPurgeResult{DLQ: req.DLQ, DryRun: result.DryRun, Impact: result.Impact, Total: result.Total, Fingerprint: result.Fingerprint}, nil
}

func (b *Broker) dial() (*amqp.Connection, error) {
	if b.tlsConfig != nil && rabbitMQAMQPUsesTLS(b.opts, b.amqpURL) {
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

func publishConfirmed(ctx context.Context, ch *amqp.Channel, returns <-chan amqp.Return, target string, publishing amqp.Publishing) error {
	drainReturns(returns)
	confirm, err := ch.PublishWithDeferredConfirmWithContext(ctx, "", target, true, false, publishing)
	if err != nil {
		return err
	}
	if confirm == nil {
		return apperrors.New(apperrors.CodeBackendError, "RabbitMQ publisher confirm was not enabled", nil)
	}
	for {
		select {
		case ret := <-returns:
			return unroutableRedriveErr(ret)
		case <-confirm.Done():
			if !confirm.Acked() {
				return apperrors.New(apperrors.CodeBackendError, "RabbitMQ redrive publish was not confirmed", nil)
			}
			select {
			case ret := <-returns:
				return unroutableRedriveErr(ret)
			default:
				return nil
			}
		case <-ctx.Done():
			return apperrors.New(apperrors.CodeBackendUnreachable, "RabbitMQ redrive publish confirmation timed out", ctx.Err())
		}
	}
}

func drainReturns(returns <-chan amqp.Return) {
	for {
		select {
		case <-returns:
		default:
			return
		}
	}
}

func unroutableRedriveErr(ret amqp.Return) error {
	if ret.Exchange == "" {
		return apperrors.New(apperrors.CodeResourceNotFound, "RabbitMQ redrive target queue not found", fmt.Errorf("rabbitmq publish returned: %s", ret.ReplyText))
	}
	return apperrors.New(apperrors.CodeBackendError, "RabbitMQ redrive publish was returned unroutable", fmt.Errorf("rabbitmq publish returned: %s", ret.ReplyText))
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
	Name      string         `json:"name"`
	Messages  int            `json:"messages_ready"`
	Consumers int            `json:"consumers"`
	Arguments map[string]any `json:"arguments"`
}

type rabbitMQPermission struct {
	User      string `json:"user"`
	Vhost     string `json:"vhost"`
	Configure string `json:"configure"`
	Write     string `json:"write"`
	Read      string `json:"read"`
}

func (b *Broker) listQueues(ctx context.Context) ([]managementQueue, error) {
	endpoint := strings.TrimRight(b.manageURL, "/") + "/api/queues/" + url.PathEscape(b.vhost())
	resp, err := b.managementRequest(ctx, http.MethodGet, endpoint, nil)
	if err != nil {
		return nil, err
	}
	defer func() { _ = resp.Body.Close() }()
	if err := rabbitMQManagementStatus(resp, http.StatusOK, http.StatusOK); err != nil {
		return nil, err
	}
	var queues []managementQueue
	if err := json.NewDecoder(resp.Body).Decode(&queues); err != nil {
		return nil, backendErr(err)
	}
	return queues, nil
}

func (b *Broker) ListACLs(ctx context.Context, filter mqgov.ACLFilter) ([]mqgov.ACLBinding, error) {
	if err := validateRabbitMQACLFilter(filter); err != nil {
		return nil, err
	}
	endpoint := strings.TrimRight(b.manageURL, "/") + "/api/permissions"
	resp, err := b.managementRequest(ctx, http.MethodGet, endpoint, nil)
	if err != nil {
		return nil, err
	}
	defer func() { _ = resp.Body.Close() }()
	if err := rabbitMQManagementStatus(resp, http.StatusOK, http.StatusOK); err != nil {
		return nil, err
	}
	var permissions []rabbitMQPermission
	if err := json.NewDecoder(resp.Body).Decode(&permissions); err != nil {
		return nil, backendErr(err)
	}
	out := make([]mqgov.ACLBinding, 0, len(permissions)*3)
	for _, permission := range permissions {
		out = append(out, rabbitMQACLBindings(permission, filter)...)
	}
	sort.Slice(out, func(i, j int) bool { return aclSortKey(out[i]) < aclSortKey(out[j]) })
	return out, nil
}

func (b *Broker) GrantACL(ctx context.Context, binding mqgov.ACLBinding) error {
	if err := validateRabbitMQACLBinding(binding); err != nil {
		return err
	}
	permission, err := b.getRabbitMQPermission(ctx, binding.Vhost, binding.Principal)
	if err != nil {
		return err
	}
	rabbitMQSetPermissionScope(&permission, binding.Operation, binding.ResourceName)
	return b.putRabbitMQPermission(ctx, permission)
}

func (b *Broker) RevokeACL(ctx context.Context, binding mqgov.ACLBinding) error {
	if err := validateRabbitMQACLBinding(binding); err != nil {
		return err
	}
	permission, err := b.getRabbitMQPermission(ctx, binding.Vhost, binding.Principal)
	if err != nil {
		return err
	}
	rabbitMQSetPermissionScope(&permission, binding.Operation, "")
	if permission.Configure == "" && permission.Write == "" && permission.Read == "" {
		return b.deleteRabbitMQPermission(ctx, permission.Vhost, permission.User)
	}
	return b.putRabbitMQPermission(ctx, permission)
}

func (b *Broker) getRabbitMQPermission(ctx context.Context, vhost, user string) (rabbitMQPermission, error) {
	permission := rabbitMQPermission{User: strings.TrimSpace(user), Vhost: rabbitMQVhost(vhost)}
	endpoint := b.rabbitMQPermissionEndpoint(permission.Vhost, permission.User)
	resp, err := b.managementRequest(ctx, http.MethodGet, endpoint, nil)
	if err != nil {
		return permission, err
	}
	defer func() { _ = resp.Body.Close() }()
	switch resp.StatusCode {
	case http.StatusOK:
		if err := json.NewDecoder(resp.Body).Decode(&permission); err != nil {
			return permission, backendErr(err)
		}
		if permission.Vhost == "" {
			permission.Vhost = rabbitMQVhost(vhost)
		}
		if permission.User == "" {
			permission.User = strings.TrimSpace(user)
		}
		return permission, nil
	case http.StatusNotFound:
		return permission, nil
	case http.StatusUnauthorized, http.StatusForbidden:
		return permission, apperrors.New(apperrors.CodeAuthFailed, "RabbitMQ management authentication failed", nil)
	default:
		return permission, backendErr(fmt.Errorf("rabbitmq management status %d", resp.StatusCode))
	}
}

func (b *Broker) putRabbitMQPermission(ctx context.Context, permission rabbitMQPermission) error {
	body, err := json.Marshal(struct {
		Configure string `json:"configure"`
		Write     string `json:"write"`
		Read      string `json:"read"`
	}{Configure: permission.Configure, Write: permission.Write, Read: permission.Read})
	if err != nil {
		return backendErr(err)
	}
	resp, err := b.managementRequest(ctx, http.MethodPut, b.rabbitMQPermissionEndpoint(permission.Vhost, permission.User), bytes.NewReader(body))
	if err != nil {
		return err
	}
	defer func() { _ = resp.Body.Close() }()
	return rabbitMQManagementStatus(resp, http.StatusNoContent, http.StatusCreated)
}

func (b *Broker) deleteRabbitMQPermission(ctx context.Context, vhost, user string) error {
	resp, err := b.managementRequest(ctx, http.MethodDelete, b.rabbitMQPermissionEndpoint(vhost, user), nil)
	if err != nil {
		return err
	}
	defer func() { _ = resp.Body.Close() }()
	return rabbitMQManagementStatus(resp, http.StatusNoContent, http.StatusNotFound)
}

func (b *Broker) rabbitMQPermissionEndpoint(vhost, user string) string {
	return strings.TrimRight(b.manageURL, "/") + "/api/permissions/" + url.PathEscape(rabbitMQVhost(vhost)) + "/" + url.PathEscape(strings.TrimSpace(user))
}

func (b *Broker) managementRequest(ctx context.Context, method, endpoint string, body io.Reader) (*http.Response, error) {
	req, err := http.NewRequestWithContext(ctx, method, endpoint, body)
	if err != nil {
		return nil, apperrors.New(apperrors.CodeUsageError, "invalid RabbitMQ management URL", err)
	}
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	req.SetBasicAuth(b.opts.Username, b.opts.Password)
	resp, err := b.httpClient.Do(req)
	if err != nil {
		return nil, unreachable(err)
	}
	return resp, nil
}

func rabbitMQManagementStatus(resp *http.Response, wantA, wantB int) error {
	if resp.StatusCode == wantA || resp.StatusCode == wantB {
		return nil
	}
	switch resp.StatusCode {
	case http.StatusUnauthorized, http.StatusForbidden:
		return apperrors.New(apperrors.CodeAuthFailed, "RabbitMQ management authentication failed", nil)
	default:
		return backendErr(fmt.Errorf("rabbitmq management status %d", resp.StatusCode))
	}
}

func rabbitMQACLBindings(permission rabbitMQPermission, filter mqgov.ACLFilter) []mqgov.ACLBinding {
	scopes := []struct {
		operation string
		pattern   string
	}{
		{operation: "configure", pattern: permission.Configure},
		{operation: "write", pattern: permission.Write},
		{operation: "read", pattern: permission.Read},
	}
	out := make([]mqgov.ACLBinding, 0, 3)
	for _, scope := range scopes {
		if scope.pattern == "" {
			continue
		}
		binding := mqgov.ACLBinding{
			Principal:    permission.User,
			Host:         "*",
			Vhost:        permission.Vhost,
			ResourceType: "vhost",
			ResourceName: scope.pattern,
			PatternType:  "regex",
			Operation:    scope.operation,
			Permission:   "allow",
		}
		if rabbitMQACLMatches(binding, filter) {
			out = append(out, binding)
		}
	}
	return out
}

func rabbitMQDLQSources(queues []managementQueue) map[string]string {
	out := make(map[string]string)
	for _, queue := range queues {
		if queue.Arguments == nil {
			continue
		}
		routingKey, _ := queue.Arguments["x-dead-letter-routing-key"].(string)
		if routingKey != "" {
			out[routingKey] = queue.Name
		}
	}
	return out
}

func rabbitMQDLQByName(name string) bool {
	lower := strings.ToLower(name)
	return strings.HasSuffix(lower, ".dlq") || strings.HasSuffix(lower, "-dlq") || strings.HasSuffix(lower, "_dlq")
}

func rabbitMQSourceQueue(name string) string {
	lower := strings.ToLower(name)
	for _, suffix := range []string{".dlq", "-dlq", "_dlq"} {
		if strings.HasSuffix(lower, suffix) {
			return name[:len(name)-len(suffix)]
		}
	}
	return ""
}

func rabbitMQACLMatches(binding mqgov.ACLBinding, filter mqgov.ACLFilter) bool {
	if filter.Principal != "" && binding.Principal != filter.Principal {
		return false
	}
	if filter.Vhost != "" && binding.Vhost != filter.Vhost {
		return false
	}
	if filter.ResourceType != "" && normalizeRabbitMQACLValue(filter.ResourceType) != "vhost" {
		return false
	}
	if filter.ResourceName != "" && binding.ResourceName != filter.ResourceName {
		return false
	}
	if filter.PatternType != "" && normalizeRabbitMQACLValue(filter.PatternType) != "regex" {
		return false
	}
	if filter.Operation != "" && normalizeRabbitMQACLValue(binding.Operation) != normalizeRabbitMQACLValue(filter.Operation) {
		return false
	}
	if filter.Permission != "" && strings.ToLower(strings.TrimSpace(filter.Permission)) != "allow" {
		return false
	}
	return true
}

func validateRabbitMQACLFilter(filter mqgov.ACLFilter) error {
	if filter.PatternType != "" && normalizeRabbitMQACLValue(filter.PatternType) != "regex" {
		return apperrors.New(apperrors.CodeUsageError, "RabbitMQ uses regex permission patterns", nil)
	}
	if filter.ResourceType != "" && normalizeRabbitMQACLValue(filter.ResourceType) != "vhost" {
		return apperrors.New(apperrors.CodeUsageError, "RabbitMQ ACL resource type must be vhost", nil)
	}
	if filter.Permission != "" && strings.ToLower(strings.TrimSpace(filter.Permission)) != "allow" {
		return apperrors.New(apperrors.CodeUsageError, "RabbitMQ ACL permission must be allow", nil)
	}
	if filter.Operation != "" && !rabbitMQACLOperation(filter.Operation) {
		return apperrors.New(apperrors.CodeUsageError, "RabbitMQ ACL operation must be configure, write, or read", nil)
	}
	return nil
}

func validateRabbitMQACLBinding(binding mqgov.ACLBinding) error {
	if strings.TrimSpace(binding.Principal) == "" {
		return apperrors.New(apperrors.CodeUsageError, "ACL principal is required", nil)
	}
	if strings.TrimSpace(binding.ResourceName) == "" {
		return apperrors.New(apperrors.CodeUsageError, "ACL resource name is required", nil)
	}
	if normalizeRabbitMQACLValue(binding.PatternType) != "regex" {
		return apperrors.New(apperrors.CodeUsageError, "RabbitMQ uses regex permission patterns", nil)
	}
	if binding.ResourceType != "" && normalizeRabbitMQACLValue(binding.ResourceType) != "vhost" {
		return apperrors.New(apperrors.CodeUsageError, "RabbitMQ ACL resource type must be vhost", nil)
	}
	if !rabbitMQACLOperation(binding.Operation) {
		return apperrors.New(apperrors.CodeUsageError, "RabbitMQ ACL operation must be configure, write, or read", nil)
	}
	if strings.ToLower(strings.TrimSpace(binding.Permission)) != "allow" {
		return apperrors.New(apperrors.CodeUsageError, "RabbitMQ ACL permission must be allow", nil)
	}
	return nil
}

func rabbitMQSetPermissionScope(permission *rabbitMQPermission, operation, pattern string) {
	switch normalizeRabbitMQACLValue(operation) {
	case "configure":
		permission.Configure = pattern
	case "write":
		permission.Write = pattern
	case "read":
		permission.Read = pattern
	}
}

func rabbitMQACLOperation(operation string) bool {
	switch normalizeRabbitMQACLValue(operation) {
	case "configure", "write", "read":
		return true
	default:
		return false
	}
}

func rabbitMQVhost(vhost string) string {
	if strings.TrimSpace(vhost) == "" {
		return "/"
	}
	return strings.TrimSpace(vhost)
}

func aclSortKey(binding mqgov.ACLBinding) string {
	return binding.Vhost + "\x00" + binding.Principal + "\x00" + binding.Operation + "\x00" + binding.ResourceName
}

func normalizeRabbitMQACLValue(value string) string {
	return strings.ToLower(strings.NewReplacer("_", "", "-", "", ".", "").Replace(strings.TrimSpace(value)))
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
		return amqpURLWithUserInfo(opts.AMQPURL, opts.Username, opts.Password)
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

func amqpURLUserInfo(rawURL string) (string, string) {
	if strings.TrimSpace(rawURL) == "" {
		return "", ""
	}
	parsed, err := url.Parse(rawURL)
	if err != nil || parsed.User == nil {
		return "", ""
	}
	password, _ := parsed.User.Password()
	return parsed.User.Username(), password
}

func amqpURLWithUserInfo(rawURL, username, password string) string {
	if strings.TrimSpace(rawURL) == "" || username == "" {
		return rawURL
	}
	parsed, err := url.Parse(rawURL)
	if err != nil {
		return rawURL
	}
	parsed.User = url.UserPassword(username, password)
	return parsed.String()
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
	if !rabbitMQUsesTLS(opts) && opts.CACertFile == "" && opts.ClientCertFile == "" && opts.ClientKeyFile == "" {
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
	if appErr := tlspin.AppError(err); appErr != nil {
		return appErr
	}
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

func rabbitMQUsesTLS(opts Options) bool {
	return rabbitMQAMQPUsesTLS(opts, opts.AMQPURL) ||
		strings.HasPrefix(strings.ToLower(strings.TrimSpace(opts.ManagementURL)), "https://")
}

func rabbitMQAMQPUsesTLS(opts Options, amqpURL string) bool {
	return opts.TLS || strings.HasPrefix(strings.ToLower(strings.TrimSpace(amqpURL)), "amqps://")
}
