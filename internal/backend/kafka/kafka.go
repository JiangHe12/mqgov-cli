package kafka

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

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/sasl"
	"github.com/twmb/franz-go/pkg/sasl/plain"
	"github.com/twmb/franz-go/pkg/sasl/scram"

	"github.com/JiangHe12/opskit-core/apperrors"

	"github.com/JiangHe12/mqgov-cli/internal/mqgov"
)

type Options struct {
	Brokers                []string
	Cluster                string
	Namespace              string
	Username               string
	Password               string
	SASLMechanism          string
	TLS                    bool
	CACertFile             string
	ClientCertFile         string
	ClientKeyFile          string
	SchemaRegistryURL      string
	SchemaRegistryUsername string
	SchemaRegistryPassword string
	Timeout                time.Duration
}

type Broker struct {
	opts   Options
	client *kgo.Client
	admin  *kadm.Client
	srHTTP *http.Client
}

type schemaRegistryVersion struct {
	Subject    string `json:"subject"`
	ID         int    `json:"id"`
	Version    int    `json:"version"`
	Schema     string `json:"schema"`
	SchemaType string `json:"schemaType"`
}

type schemaRegistryCompatibility struct {
	Compatible bool     `json:"is_compatible"`
	Messages   []string `json:"messages"`
}

type schemaRegistryRegisterResult struct {
	ID int `json:"id"`
}

func New(opts Options) (*Broker, error) {
	kopts, err := kgoOptions(opts)
	if err != nil {
		return nil, err
	}
	client, err := kgo.NewClient(kopts...)
	if err != nil {
		return nil, unreachable(err)
	}
	httpClient, err := schemaRegistryHTTPClient(opts)
	if err != nil {
		client.Close()
		return nil, err
	}
	return &Broker{opts: opts, client: client, admin: kadm.NewClient(client), srHTTP: httpClient}, nil
}

func (b *Broker) Ping(ctx context.Context) error {
	if err := b.client.Ping(ctx); err != nil {
		return unreachable(err)
	}
	return nil
}

func (b *Broker) Describe() mqgov.Description {
	return mqgov.Description{Backend: "kafka", Cluster: b.opts.Cluster, Namespace: b.opts.Namespace}
}

func (b *Broker) Capabilities() mqgov.Capabilities {
	return mqgov.Capabilities{
		Backend:            "kafka",
		ResourceTypes:      []string{"topic", "group", "message", "offset", "acl", "dlq", "schema"},
		Verbs:              []string{"list", "describe", "lag", "peek", "tail", "produce", "mirror", "create", "alter", "delete", "purge", "reset-offset", "grant-acl", "revoke-acl", "redrive", "check-schema", "register-schema", "delete-schema"},
		SupportsOffsets:    true,
		SupportsPartitions: true,
		SupportsACL:        true,
		SupportsDLQList:    false,
		SupportsDLQPeek:    true,
		SupportsDLQRedrive: true,
		SupportsDLQPurge:   true,
		SupportsSchema:     b.opts.SchemaRegistryURL != "",
	}
}

func (b *Broker) ListTopics(ctx context.Context, opts mqgov.TopicListOptions) ([]mqgov.TopicDescription, error) {
	details, err := b.admin.ListTopicsWithInternal(ctx)
	if err != nil {
		return nil, backendErr(err)
	}
	items := make([]mqgov.TopicDescription, 0, len(details))
	for name, detail := range details {
		if detail.Err != nil {
			return nil, backendErr(detail.Err)
		}
		if opts.Pattern != "" && opts.Pattern != name {
			continue
		}
		items = append(items, topicDescription(b, detail))
	}
	sort.Slice(items, func(i, j int) bool { return items[i].Coordinate.Topic < items[j].Coordinate.Topic })
	if opts.Limit > 0 && len(items) > opts.Limit {
		items = items[:opts.Limit]
	}
	return items, nil
}

func (b *Broker) DescribeTopic(ctx context.Context, coord mqgov.TopicCoordinate) (mqgov.TopicDescription, error) {
	details, err := b.admin.ListTopicsWithInternal(ctx, coord.Topic)
	if err != nil {
		return mqgov.TopicDescription{}, backendErr(err)
	}
	detail, ok := details[coord.Topic]
	if !ok {
		return mqgov.TopicDescription{}, apperrors.New(apperrors.CodeResourceNotFound, "topic not found", nil)
	}
	if detail.Err != nil {
		return mqgov.TopicDescription{}, topicNotFoundErr(detail.Err)
	}
	return topicDescription(b, detail), nil
}

func (b *Broker) CreateTopic(ctx context.Context, req mqgov.TopicCreateRequest) (mqgov.TopicDescription, error) {
	partitions := int32(-1)
	if req.Partitions > 0 {
		value, err := safeInt32(req.Partitions, "partitions")
		if err != nil {
			return mqgov.TopicDescription{}, err
		}
		partitions = value
	}
	if req.Partitions <= 0 {
		partitions = -1
	}
	configs := make(map[string]*string, len(req.Config))
	for key, value := range req.Config {
		configs[key] = kadm.StringPtr(value)
	}
	resp, err := b.admin.CreateTopic(ctx, partitions, -1, configs, req.Coordinate.Topic)
	if err != nil {
		return mqgov.TopicDescription{}, createTopicErr(err)
	}
	if resp.Err != nil {
		return mqgov.TopicDescription{}, createTopicErr(resp.Err)
	}
	return b.DescribeTopic(ctx, req.Coordinate)
}

func (b *Broker) DeleteTopic(ctx context.Context, coord mqgov.TopicCoordinate) error {
	resp, err := b.admin.DeleteTopic(ctx, coord.Topic)
	if err != nil {
		return backendErr(err)
	}
	if resp.Err != nil {
		return topicNotFoundErr(resp.Err)
	}
	return nil
}

func (b *Broker) ListGroups(ctx context.Context, opts mqgov.GroupListOptions) ([]mqgov.GroupDescription, error) {
	groups, err := b.admin.ListGroups(ctx)
	if err != nil {
		return nil, backendErr(err)
	}
	items := make([]mqgov.GroupDescription, 0, len(groups))
	for name, group := range groups {
		if opts.Pattern != "" && opts.Pattern != name {
			continue
		}
		items = append(items, mqgov.GroupDescription{
			Coordinate: mqgov.GroupCoordinate{Cluster: b.opts.Cluster, Namespace: b.opts.Namespace, Group: name},
			State:      group.State,
		})
	}
	sort.Slice(items, func(i, j int) bool { return items[i].Coordinate.Group < items[j].Coordinate.Group })
	if opts.Limit > 0 && len(items) > opts.Limit {
		items = items[:opts.Limit]
	}
	return items, nil
}

func (b *Broker) CreateGroup(context.Context, mqgov.GroupCoordinate) (mqgov.GroupDescription, error) {
	return mqgov.GroupDescription{}, apperrors.New(apperrors.CodeNotImplemented, "Kafka groups are created by consumers", nil)
}

func (b *Broker) DeleteGroup(ctx context.Context, coord mqgov.GroupCoordinate) error {
	resp, err := b.admin.DeleteGroup(ctx, coord.Group)
	if err != nil {
		return backendErr(err)
	}
	if resp.Err != nil {
		return backendErr(resp.Err)
	}
	return nil
}

func (b *Broker) Peek(ctx context.Context, req mqgov.MessagePeekRequest) (mqgov.MessagePeekResult, error) {
	count := req.Count
	if count <= 0 {
		count = 1
	}
	client, err := b.peekClient(req)
	if err != nil {
		return mqgov.MessagePeekResult{}, err
	}
	defer client.Close()
	deadlineCtx, cancel := context.WithTimeout(ctx, b.timeout())
	defer cancel()
	messages := make([]mqgov.MessageFingerprint, 0, count)
	for len(messages) < count {
		fetches := client.PollFetches(deadlineCtx)
		if err := fetches.Err(); err != nil {
			if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled) {
				break
			}
			return mqgov.MessagePeekResult{}, backendErr(err)
		}
		if fetches.Empty() {
			break
		}
		iter := fetches.RecordIter()
		for !iter.Done() && len(messages) < count {
			record := iter.Next()
			if record == nil {
				continue
			}
			messages = append(messages, mqgov.FingerprintMessage(int(record.Partition), record.Offset, record.Key, record.Value))
		}
	}
	return mqgov.MessagePeekResult{Coordinate: req.Coordinate, Partition: req.Partition, Offset: req.Offset, Count: len(messages), Messages: messages}, nil
}

func (b *Broker) Produce(ctx context.Context, req mqgov.MessageProduceRequest) (mqgov.MessageProduceResult, error) {
	record := &kgo.Record{
		Topic:   req.Coordinate.Topic,
		Key:     req.Key,
		Value:   req.Body,
		Headers: headers(req.Headers),
	}
	produced, err := b.client.ProduceSync(ctx, record).First()
	if err != nil {
		return mqgov.MessageProduceResult{}, backendErr(err)
	}
	return mqgov.MessageProduceResult{
		Coordinate:  mqgov.MessageCoordinate{TopicCoordinate: req.Coordinate, Partition: int(produced.Partition), Offset: produced.Offset},
		Fingerprint: mqgov.Fingerprints(req.Key, req.Body, 1),
	}, nil
}

func (b *Broker) ListACLs(ctx context.Context, filter mqgov.ACLFilter) ([]mqgov.ACLBinding, error) {
	builder, err := aclFilterBuilder(filter)
	if err != nil {
		return nil, err
	}
	results, err := b.admin.DescribeACLs(ctx, builder)
	if err != nil {
		return nil, backendErr(err)
	}
	out := make([]mqgov.ACLBinding, 0)
	for _, result := range results {
		if result.Err != nil {
			return nil, backendErr(result.Err)
		}
		for _, acl := range result.Described {
			out = append(out, mqgov.ACLBinding{
				Principal:    acl.Principal,
				Host:         acl.Host,
				ResourceType: strings.ToLower(acl.Type.String()),
				ResourceName: acl.Name,
				PatternType:  strings.ToLower(acl.Pattern.String()),
				Operation:    strings.ToLower(acl.Operation.String()),
				Permission:   strings.ToLower(acl.Permission.String()),
			})
		}
	}
	sort.Slice(out, func(i, j int) bool { return aclSortKey(out[i]) < aclSortKey(out[j]) })
	return out, nil
}

func (b *Broker) GrantACL(ctx context.Context, binding mqgov.ACLBinding) error {
	builder, err := aclBindingBuilder(binding, false)
	if err != nil {
		return err
	}
	results, err := b.admin.CreateACLs(ctx, builder)
	if err != nil {
		return backendErr(err)
	}
	for _, result := range results {
		if result.Err != nil {
			return backendErr(result.Err)
		}
	}
	return nil
}

func (b *Broker) RevokeACL(ctx context.Context, binding mqgov.ACLBinding) error {
	builder, err := aclBindingBuilder(binding, true)
	if err != nil {
		return err
	}
	results, err := b.admin.DeleteACLs(ctx, builder)
	if err != nil {
		return backendErr(err)
	}
	for _, result := range results {
		if result.Err != nil {
			return backendErr(result.Err)
		}
		for _, deleted := range result.Deleted {
			if deleted.Err != nil {
				return backendErr(deleted.Err)
			}
		}
	}
	return nil
}

func (b *Broker) Tail(ctx context.Context, req mqgov.MessageTailRequest, emit func(mqgov.MessageFingerprint) error) (mqgov.MessageTailResult, error) {
	starts, ends, err := b.tailOffsets(ctx, req)
	if err != nil {
		return mqgov.MessageTailResult{}, err
	}
	client, err := b.tailClient(starts)
	if err != nil {
		return mqgov.MessageTailResult{}, err
	}
	defer client.Close()
	result := mqgov.MessageTailResult{Coordinate: req.Coordinate}
	impact := map[int]*mqgov.PartitionImpact{}
	done := tailDonePartitions(starts, ends)
	for !tailLimitReached(req, result) {
		if !req.Follow && tailAllDone(done, starts[req.Coordinate.Topic]) {
			break
		}
		fetches := client.PollFetches(ctx)
		if err := fetches.Err(); err != nil {
			if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
				break
			}
			return result, backendErr(err)
		}
		if fetches.Empty() {
			if !req.Follow {
				break
			}
			continue
		}
		if err := processKafkaTailFetches(fetches, req, ends, done, impact, &result, emit); err != nil {
			return result, err
		}
	}
	result.Impact = tailImpactSlice(impact)
	return result, nil
}

func (b *Broker) MirrorMessages(ctx context.Context, req mqgov.MessageMirrorRequest, emit func(mqgov.Message) error) (mqgov.MessageMirrorResult, error) {
	if req.Limit <= 0 {
		return mqgov.MessageMirrorResult{}, apperrors.New(apperrors.CodeUsageError, "mirror limit must be positive", nil)
	}
	starts, ends, err := b.mirrorOffsets(ctx, req)
	if err != nil {
		return mqgov.MessageMirrorResult{}, err
	}
	client, err := b.tailClient(starts)
	if err != nil {
		return mqgov.MessageMirrorResult{}, err
	}
	defer client.Close()
	result := mqgov.MessageMirrorResult{Source: req.Source, Target: req.Target, DryRun: req.DryRun}
	impact := map[int]*mqgov.PartitionImpact{}
	done := tailDonePartitions(starts, ends)
	for int(result.Count) < req.Limit {
		if tailAllDone(done, starts[req.Source.Topic]) {
			break
		}
		fetches := client.PollFetches(ctx)
		if err := fetches.Err(); err != nil {
			if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
				break
			}
			return result, backendErr(err)
		}
		if fetches.Empty() {
			break
		}
		if err := processKafkaMirrorFetches(fetches, req, ends, done, impact, &result, emit); err != nil {
			return result, err
		}
	}
	result.Impact = tailImpactSlice(impact)
	return result, nil
}

func processKafkaMirrorFetches(fetches kgo.Fetches, req mqgov.MessageMirrorRequest, ends kadm.ListedOffsets, done map[int32]bool, impact map[int]*mqgov.PartitionImpact, result *mqgov.MessageMirrorResult, emit func(mqgov.Message) error) error {
	iter := fetches.RecordIter()
	for !iter.Done() && int(result.Count) < req.Limit {
		record := iter.Next()
		if record == nil {
			continue
		}
		if kafkaTailPastEnd(record, ends) {
			done[record.Partition] = true
			continue
		}
		msg := mqgov.Message{
			Coordinate: mqgov.MessageCoordinate{TopicCoordinate: req.Source, Partition: int(record.Partition), Offset: record.Offset},
			Key:        record.Key,
			Body:       record.Value,
			Headers:    recordHeaders(record.Headers),
		}
		if emit != nil {
			if err := emit(msg); err != nil {
				return err
			}
		}
		result.Count++
		updateTailImpact(impact, int(record.Partition), record.Offset)
		if kafkaTailReachedEnd(record, ends) {
			done[record.Partition] = true
		}
	}
	return nil
}

func processKafkaTailFetches(fetches kgo.Fetches, req mqgov.MessageTailRequest, ends kadm.ListedOffsets, done map[int32]bool, impact map[int]*mqgov.PartitionImpact, result *mqgov.MessageTailResult, emit func(mqgov.MessageFingerprint) error) error {
	iter := fetches.RecordIter()
	for !iter.Done() && !tailLimitReached(req, *result) {
		record := iter.Next()
		if record == nil {
			continue
		}
		if !req.Follow && kafkaTailPastEnd(record, ends) {
			done[record.Partition] = true
			continue
		}
		fp := mqgov.FingerprintMessageAt(int(record.Partition), record.Offset, record.Key, record.Value, record.Timestamp)
		if err := emit(fp); err != nil {
			return err
		}
		result.Count++
		result.TotalSize += int64(fp.Size)
		updateTailImpact(impact, int(record.Partition), record.Offset)
		if !req.Follow && kafkaTailReachedEnd(record, ends) {
			done[record.Partition] = true
		}
	}
	return nil
}

func (b *Broker) mirrorOffsets(ctx context.Context, req mqgov.MessageMirrorRequest) (map[string]map[int32]kgo.Offset, kadm.ListedOffsets, error) {
	end, err := b.admin.ListEndOffsets(ctx, req.Source.Topic)
	if err != nil {
		return nil, nil, backendErr(err)
	}
	if err := end.Error(); err != nil {
		return nil, nil, topicNotFoundErr(err)
	}
	start, err := b.mirrorStartOffsets(ctx, req)
	if err != nil {
		return nil, nil, err
	}
	partitions := make(map[int32]kgo.Offset)
	for partition, listed := range start[req.Source.Topic] {
		if req.Partition >= 0 && int(partition) != req.Partition {
			continue
		}
		if listed.Err != nil {
			return nil, nil, backendErr(listed.Err)
		}
		partitions[partition] = kgo.NewOffset().At(listed.Offset)
	}
	if len(partitions) == 0 {
		return nil, nil, apperrors.New(apperrors.CodeResourceNotFound, "partition not found", nil)
	}
	return map[string]map[int32]kgo.Offset{req.Source.Topic: partitions}, end, nil
}

func (b *Broker) mirrorStartOffsets(ctx context.Context, req mqgov.MessageMirrorRequest) (kadm.ListedOffsets, error) {
	from := strings.TrimSpace(req.From)
	switch {
	case from == "" || from == "earliest":
		offsets, err := b.admin.ListStartOffsets(ctx, req.Source.Topic)
		return offsets, wrapListedOffsetsErr(err)
	case from == "latest":
		offsets, err := b.admin.ListEndOffsets(ctx, req.Source.Topic)
		return offsets, wrapListedOffsetsErr(err)
	case strings.HasPrefix(from, "offset:"):
		value, err := strconv.ParseInt(strings.TrimPrefix(from, "offset:"), 10, 64)
		if err != nil || value < 0 {
			return nil, apperrors.New(apperrors.CodeUsageError, "invalid mirror offset", nil)
		}
		return b.fixedOffsets(ctx, req.Source.Topic, value)
	case strings.HasPrefix(from, "timestamp:"), strings.HasPrefix(from, "datetime:"):
		value := strings.TrimPrefix(strings.TrimPrefix(from, "timestamp:"), "datetime:")
		t, err := time.Parse(time.RFC3339, value)
		if err != nil {
			return nil, apperrors.New(apperrors.CodeUsageError, "invalid mirror timestamp", nil)
		}
		offsets, err := b.admin.ListOffsetsAfterMilli(ctx, t.UnixMilli(), req.Source.Topic)
		return offsets, wrapListedOffsetsErr(err)
	default:
		return nil, apperrors.New(apperrors.CodeUsageError, "unsupported mirror start position", nil)
	}
}

func (b *Broker) AlterTopic(ctx context.Context, req mqgov.TopicAlterRequest) (mqgov.TopicDescription, error) {
	if req.Partitions > 0 {
		resp, err := b.admin.UpdatePartitions(ctx, req.Partitions, req.Coordinate.Topic)
		if err != nil {
			return mqgov.TopicDescription{}, backendErr(err)
		}
		if r, ok := resp[req.Coordinate.Topic]; ok && r.Err != nil {
			return mqgov.TopicDescription{}, backendErr(r.Err)
		}
	}
	if len(req.Config) > 0 {
		configs := make([]kadm.AlterConfig, 0, len(req.Config))
		for key, value := range req.Config {
			configs = append(configs, kadm.AlterConfig{Op: kadm.SetConfig, Name: key, Value: kadm.StringPtr(value)})
		}
		responses, err := b.admin.AlterTopicConfigs(ctx, configs, req.Coordinate.Topic)
		if err != nil {
			return mqgov.TopicDescription{}, backendErr(err)
		}
		if err := alterResponsesErr(responses); err != nil {
			return mqgov.TopicDescription{}, backendErr(err)
		}
	}
	return b.DescribeTopic(ctx, req.Coordinate)
}

func (b *Broker) PurgeTopic(ctx context.Context, req mqgov.TopicPurgeRequest) (mqgov.TopicPurgeResult, error) {
	start, end, err := b.startEndOffsets(ctx, req.Coordinate.Topic)
	if err != nil {
		return mqgov.TopicPurgeResult{}, err
	}
	impact, total := purgeImpact(start, end)
	if !req.DryRun {
		if err := b.deleteRecords(ctx, end); err != nil {
			return mqgov.TopicPurgeResult{}, err
		}
	}
	return mqgov.TopicPurgeResult{
		Coordinate:  req.Coordinate,
		DLQ:         req.DLQ,
		DryRun:      req.DryRun,
		Impact:      impact,
		Total:       total,
		Fingerprint: mqgov.ResourceFingerprints{Count: total},
	}, nil
}

func (b *Broker) ListDLQs(context.Context, mqgov.DLQListOptions) ([]mqgov.DLQDescription, error) {
	return nil, apperrors.New(apperrors.CodeNotImplemented, "Kafka has no native DLQ discovery; specify a DLQ topic explicitly", nil)
}

func (b *Broker) PeekDLQ(ctx context.Context, req mqgov.DLQPeekRequest) (mqgov.DLQPeekResult, error) {
	result, err := b.Peek(ctx, mqgov.MessagePeekRequest{Coordinate: req.DLQ, Partition: req.Partition, Offset: req.Offset, Count: req.Count})
	if err != nil {
		return mqgov.DLQPeekResult{}, err
	}
	return mqgov.DLQPeekResult{DLQ: req.DLQ, Partition: result.Partition, Offset: result.Offset, Count: result.Count, Messages: result.Messages}, nil
}

func (b *Broker) RedriveDLQ(ctx context.Context, req mqgov.DLQRedriveRequest) (mqgov.DLQRedriveResult, error) {
	count := req.Count
	if count <= 0 {
		count = 100
	}
	impact, total, err := b.kafkaDLQImpact(ctx, req.DLQ.Topic, int64(count))
	if err != nil {
		return mqgov.DLQRedriveResult{}, err
	}
	if !req.DryRun && total > 0 {
		messages, err := b.readDLQMessages(ctx, req.DLQ, int(total))
		if err != nil {
			return mqgov.DLQRedriveResult{}, err
		}
		for _, msg := range messages {
			if _, err := b.Produce(ctx, mqgov.MessageProduceRequest{Coordinate: req.Target, Key: msg.Key, Body: msg.Body, Headers: msg.Headers}); err != nil {
				return mqgov.DLQRedriveResult{}, err
			}
		}
	}
	return mqgov.DLQRedriveResult{
		DLQ:         req.DLQ,
		Target:      req.Target,
		DryRun:      req.DryRun,
		Impact:      impact,
		Total:       total,
		Fingerprint: mqgov.ResourceFingerprints{Count: total},
	}, nil
}

func (b *Broker) PurgeDLQ(ctx context.Context, req mqgov.DLQPurgeRequest) (mqgov.DLQPurgeResult, error) {
	result, err := b.PurgeTopic(ctx, mqgov.TopicPurgeRequest{Coordinate: req.DLQ, DLQ: true, DryRun: req.DryRun})
	if err != nil {
		return mqgov.DLQPurgeResult{}, err
	}
	return mqgov.DLQPurgeResult{DLQ: req.DLQ, DryRun: result.DryRun, Impact: result.Impact, Total: result.Total, Fingerprint: result.Fingerprint}, nil
}

func (b *Broker) ListSchemas(ctx context.Context, opts mqgov.SchemaListOptions) ([]mqgov.SchemaSubject, error) {
	if b.opts.SchemaRegistryURL == "" {
		return nil, apperrors.New(apperrors.CodeNotImplemented, "Kafka Schema Registry URL is not configured", nil)
	}
	body, err := b.schemaRegistryJSON(ctx, http.MethodGet, "/subjects", nil)
	if err != nil {
		return nil, err
	}
	var subjects []string
	if err := json.Unmarshal(body, &subjects); err != nil {
		return nil, backendErr(fmt.Errorf("decode schema registry subjects: %w", err))
	}
	items := make([]mqgov.SchemaSubject, 0, len(subjects))
	for _, subject := range subjects {
		if opts.Subject != "" && opts.Subject != subject {
			continue
		}
		if opts.Pattern != "" && opts.Pattern != subject {
			continue
		}
		items = append(items, mqgov.SchemaSubject{Subject: subject})
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
	version := firstNonEmpty(req.Version, "latest")
	versions, err := b.schemaVersions(ctx, req.Subject)
	if err != nil {
		return mqgov.SchemaDescription{}, err
	}
	body, err := b.schemaRegistryJSON(ctx, http.MethodGet, "/subjects/"+url.PathEscape(req.Subject)+"/versions/"+url.PathEscape(version), nil)
	if err != nil {
		return mqgov.SchemaDescription{}, err
	}
	var sr schemaRegistryVersion
	if err := json.Unmarshal(body, &sr); err != nil {
		return mqgov.SchemaDescription{}, backendErr(fmt.Errorf("decode schema registry version: %w", err))
	}
	return mqgov.SchemaDescription{
		Subject:    sr.Subject,
		Version:    strconv.Itoa(sr.Version),
		ID:         sr.ID,
		Type:       firstNonEmpty(sr.SchemaType, "AVRO"),
		Schema:     sr.Schema,
		SchemaHash: mqgov.SHA256Hex([]byte(sr.Schema)),
		Versions:   versions,
	}, nil
}

func (b *Broker) CheckCompatibility(ctx context.Context, req mqgov.SchemaCheckRequest) (mqgov.SchemaCheckResult, error) {
	if strings.TrimSpace(req.Subject) == "" {
		return mqgov.SchemaCheckResult{}, apperrors.New(apperrors.CodeUsageError, "schema subject is required", nil)
	}
	if strings.TrimSpace(req.Schema) == "" {
		return mqgov.SchemaCheckResult{}, apperrors.New(apperrors.CodeUsageError, "schema text is required", nil)
	}
	version := firstNonEmpty(req.Version, "latest")
	payload := map[string]string{"schema": req.Schema}
	if req.Type != "" {
		payload["schemaType"] = strings.ToUpper(strings.TrimSpace(req.Type))
	}
	data, err := json.Marshal(payload)
	if err != nil {
		return mqgov.SchemaCheckResult{}, backendErr(err)
	}
	body, err := b.schemaRegistryJSON(ctx, http.MethodPost, "/compatibility/subjects/"+url.PathEscape(req.Subject)+"/versions/"+url.PathEscape(version), data)
	if err != nil {
		return mqgov.SchemaCheckResult{}, err
	}
	var compat schemaRegistryCompatibility
	if err := json.Unmarshal(body, &compat); err != nil {
		return mqgov.SchemaCheckResult{}, backendErr(fmt.Errorf("decode schema registry compatibility: %w", err))
	}
	return mqgov.SchemaCheckResult{Subject: req.Subject, Version: version, Compatible: compat.Compatible, SchemaHash: mqgov.SHA256Hex([]byte(req.Schema)), Message: strings.Join(compat.Messages, "; ")}, nil
}

func (b *Broker) RegisterSchema(ctx context.Context, req mqgov.SchemaRegisterRequest) (mqgov.SchemaDescription, error) {
	if strings.TrimSpace(req.Subject) == "" {
		return mqgov.SchemaDescription{}, apperrors.New(apperrors.CodeUsageError, "schema subject is required", nil)
	}
	if strings.TrimSpace(req.Schema) == "" {
		return mqgov.SchemaDescription{}, apperrors.New(apperrors.CodeUsageError, "schema text is required", nil)
	}
	payload := map[string]string{"schema": req.Schema}
	if req.Type != "" {
		payload["schemaType"] = strings.ToUpper(strings.TrimSpace(req.Type))
	}
	data, err := json.Marshal(payload)
	if err != nil {
		return mqgov.SchemaDescription{}, backendErr(err)
	}
	body, err := b.schemaRegistryJSON(ctx, http.MethodPost, "/subjects/"+url.PathEscape(req.Subject)+"/versions", data)
	if err != nil {
		return mqgov.SchemaDescription{}, err
	}
	var registered schemaRegistryRegisterResult
	if err := json.Unmarshal(body, &registered); err != nil {
		return mqgov.SchemaDescription{}, backendErr(fmt.Errorf("decode schema registry registration: %w", err))
	}
	desc, err := b.DescribeSchema(ctx, mqgov.SchemaDescribeRequest{Subject: req.Subject, Version: "latest"})
	if err != nil {
		return mqgov.SchemaDescription{}, err
	}
	if desc.ID == 0 {
		desc.ID = registered.ID
	}
	return desc, nil
}

func (b *Broker) DeleteSchema(ctx context.Context, req mqgov.SchemaDeleteRequest) (mqgov.SchemaDeleteResult, error) {
	if strings.TrimSpace(req.Subject) == "" {
		return mqgov.SchemaDeleteResult{}, apperrors.New(apperrors.CodeUsageError, "schema subject is required", nil)
	}
	path := "/subjects/" + url.PathEscape(req.Subject)
	if req.Version != "" {
		path += "/versions/" + url.PathEscape(req.Version)
	}
	if req.Permanent {
		path += "?permanent=true"
	}
	body, err := b.schemaRegistryJSON(ctx, http.MethodDelete, path, nil)
	if err != nil {
		return mqgov.SchemaDeleteResult{}, err
	}
	result := mqgov.SchemaDeleteResult{Subject: req.Subject, Version: req.Version, Permanent: req.Permanent}
	if req.Version != "" {
		var version int
		if err := json.Unmarshal(body, &version); err == nil && version > 0 {
			result.Version = strconv.Itoa(version)
		}
		return result, nil
	}
	var versions []int
	if err := json.Unmarshal(body, &versions); err == nil {
		result.Versions = make([]string, 0, len(versions))
		for _, version := range versions {
			result.Versions = append(result.Versions, strconv.Itoa(version))
		}
	}
	return result, nil
}

func (b *Broker) Lag(ctx context.Context, group mqgov.GroupCoordinate, topic mqgov.TopicCoordinate) (mqgov.OffsetPlan, error) {
	plan, err := b.offsetPlan(ctx, mqgov.OffsetPlanRequest{Group: group, Topic: topic, To: "latest", DryRun: true, Partition: -1})
	if err != nil {
		return mqgov.OffsetPlan{}, err
	}
	return plan, nil
}

func (b *Broker) PlanOffsetReset(ctx context.Context, req mqgov.OffsetPlanRequest) (mqgov.OffsetPlan, error) {
	return b.offsetPlan(ctx, req)
}

func (b *Broker) ResetOffset(ctx context.Context, req mqgov.OffsetPlanRequest) (mqgov.OffsetPlan, error) {
	if err := b.ensureGroupInactive(ctx, req.Group.Group); err != nil {
		return mqgov.OffsetPlan{}, err
	}
	plan, err := b.offsetPlan(ctx, req)
	if err != nil {
		return mqgov.OffsetPlan{}, err
	}
	if !req.DryRun {
		offsets := make(kadm.Offsets)
		offsets[req.Topic.Topic] = make(map[int32]kadm.Offset)
		for _, item := range plan.Impact {
			partition, err := safeInt32(item.Partition, "partition")
			if err != nil {
				return mqgov.OffsetPlan{}, err
			}
			offsets[req.Topic.Topic][partition] = kadm.Offset{Topic: req.Topic.Topic, Partition: partition, At: item.To}
		}
		responses, err := b.admin.CommitOffsets(ctx, req.Group.Group, offsets)
		if err != nil {
			return mqgov.OffsetPlan{}, backendErr(err)
		}
		if err := responses.Error(); err != nil {
			return mqgov.OffsetPlan{}, backendErr(err)
		}
	}
	return plan, nil
}

func (b *Broker) offsetPlan(ctx context.Context, req mqgov.OffsetPlanRequest) (mqgov.OffsetPlan, error) {
	committed, err := b.admin.FetchOffsetsForTopics(ctx, req.Group.Group, req.Topic.Topic)
	if err != nil {
		return mqgov.OffsetPlan{}, backendErr(err)
	}
	targets, err := b.targetOffsets(ctx, req)
	if err != nil {
		return mqgov.OffsetPlan{}, err
	}
	impact := make([]mqgov.PartitionImpact, 0, len(targets[req.Topic.Topic]))
	var total int64
	for partition, target := range targets[req.Topic.Topic] {
		if !partitionMatches(req.Partition, partition) {
			continue
		}
		from, err := committedOffset(committed, req.Topic.Topic, partition)
		if err != nil {
			return mqgov.OffsetPlan{}, err
		}
		to := target.Offset
		if to < 0 {
			to = from
		}
		count := abs64(to - from)
		total += count
		impact = append(impact, mqgov.PartitionImpact{Partition: int(partition), From: from, To: to, Count: count})
	}
	sort.Slice(impact, func(i, j int) bool { return impact[i].Partition < impact[j].Partition })
	return mqgov.OffsetPlan{Group: req.Group, Topic: req.Topic, To: req.To, DryRun: req.DryRun, Impact: impact, Total: total}, nil
}

func (b *Broker) targetOffsets(ctx context.Context, req mqgov.OffsetPlanRequest) (kadm.ListedOffsets, error) {
	target := strings.TrimSpace(req.To)
	switch {
	case target == "" || target == "earliest":
		offsets, err := b.admin.ListStartOffsets(ctx, req.Topic.Topic)
		return offsets, wrapListedOffsetsErr(err)
	case target == "latest":
		offsets, err := b.admin.ListEndOffsets(ctx, req.Topic.Topic)
		return offsets, wrapListedOffsetsErr(err)
	case strings.HasPrefix(target, "offset:"):
		value, err := strconv.ParseInt(strings.TrimPrefix(target, "offset:"), 10, 64)
		if err != nil || value < 0 {
			return nil, apperrors.New(apperrors.CodeUsageError, "invalid offset target", nil)
		}
		return b.fixedOffsets(ctx, req.Topic.Topic, value)
	case strings.HasPrefix(target, "datetime:"):
		t, err := time.Parse(time.RFC3339, strings.TrimPrefix(target, "datetime:"))
		if err != nil {
			return nil, apperrors.New(apperrors.CodeUsageError, "invalid datetime target", nil)
		}
		offsets, err := b.admin.ListOffsetsAfterMilli(ctx, t.UnixMilli(), req.Topic.Topic)
		return offsets, wrapListedOffsetsErr(err)
	case strings.HasPrefix(target, "shift:"):
		shift, err := strconv.ParseInt(strings.TrimPrefix(target, "shift:"), 10, 64)
		if err != nil {
			return nil, apperrors.New(apperrors.CodeUsageError, "invalid shift target", nil)
		}
		return b.shiftOffsets(ctx, req, shift)
	default:
		return nil, apperrors.New(apperrors.CodeUsageError, "unsupported offset target", nil)
	}
}

func (b *Broker) fixedOffsets(ctx context.Context, topic string, offset int64) (kadm.ListedOffsets, error) {
	end, err := b.admin.ListEndOffsets(ctx, topic)
	if err != nil {
		return nil, backendErr(err)
	}
	out := make(kadm.ListedOffsets)
	out[topic] = make(map[int32]kadm.ListedOffset)
	for partition := range end[topic] {
		out[topic][partition] = kadm.ListedOffset{Topic: topic, Partition: partition, Offset: offset}
	}
	return out, nil
}

func (b *Broker) shiftOffsets(ctx context.Context, req mqgov.OffsetPlanRequest, shift int64) (kadm.ListedOffsets, error) {
	committed, err := b.admin.FetchOffsetsForTopics(ctx, req.Group.Group, req.Topic.Topic)
	if err != nil {
		return nil, backendErr(err)
	}
	end, err := b.admin.ListEndOffsets(ctx, req.Topic.Topic)
	if err != nil {
		return nil, backendErr(err)
	}
	out := make(kadm.ListedOffsets)
	out[req.Topic.Topic] = make(map[int32]kadm.ListedOffset)
	for partition := range end[req.Topic.Topic] {
		from := int64(0)
		if response, ok := committed[req.Topic.Topic][partition]; ok {
			if response.Err != nil {
				return nil, backendErr(response.Err)
			}
			from = response.At
		}
		to := from + shift
		if to < 0 {
			to = 0
		}
		out[req.Topic.Topic][partition] = kadm.ListedOffset{Topic: req.Topic.Topic, Partition: partition, Offset: to}
	}
	return out, nil
}

func (b *Broker) ensureGroupInactive(ctx context.Context, group string) error {
	groups, err := b.admin.DescribeGroups(ctx, group)
	if err != nil {
		return backendErr(err)
	}
	if described, ok := groups[group]; ok {
		if described.Err != nil {
			return backendErr(described.Err)
		}
		if len(described.Members) > 0 {
			return apperrors.New(apperrors.CodeBackendError, "group has active members", nil)
		}
	}
	return nil
}

func (b *Broker) startEndOffsets(ctx context.Context, topic string) (kadm.ListedOffsets, kadm.ListedOffsets, error) {
	start, err := b.admin.ListStartOffsets(ctx, topic)
	if err != nil {
		return nil, nil, backendErr(err)
	}
	end, err := b.admin.ListEndOffsets(ctx, topic)
	if err != nil {
		return nil, nil, backendErr(err)
	}
	return start, end, nil
}

func (b *Broker) deleteRecords(ctx context.Context, offsets kadm.ListedOffsets) error {
	req := make(kadm.Offsets)
	for topic, partitions := range offsets {
		req[topic] = make(map[int32]kadm.Offset)
		for partition, offset := range partitions {
			req[topic][partition] = kadm.Offset{Topic: topic, Partition: partition, At: offset.Offset}
		}
	}
	responses, err := b.admin.DeleteRecords(ctx, req)
	if err != nil {
		return backendErr(err)
	}
	if err := responses.Error(); err != nil {
		return backendErr(err)
	}
	return nil
}

func (b *Broker) kafkaDLQImpact(ctx context.Context, topic string, limit int64) ([]mqgov.PartitionImpact, int64, error) {
	start, end, err := b.startEndOffsets(ctx, topic)
	if err != nil {
		return nil, 0, err
	}
	impact := make([]mqgov.PartitionImpact, 0)
	var total int64
	for _, item := range purgeImpactLimited(start, end, limit) {
		impact = append(impact, item)
		total += item.Count
	}
	return impact, total, nil
}

func (b *Broker) readDLQMessages(ctx context.Context, coord mqgov.TopicCoordinate, count int) ([]mqgov.Message, error) {
	start, end, err := b.startEndOffsets(ctx, coord.Topic)
	if err != nil {
		return nil, err
	}
	partitions := make(map[int32]kgo.Offset)
	for partition, listed := range start[coord.Topic] {
		if listed.Err != nil {
			return nil, backendErr(listed.Err)
		}
		partitions[partition] = kgo.NewOffset().At(listed.Offset)
	}
	client, err := b.tailClient(map[string]map[int32]kgo.Offset{coord.Topic: partitions})
	if err != nil {
		return nil, err
	}
	defer client.Close()
	out := make([]mqgov.Message, 0, count)
	done := tailDonePartitions(map[string]map[int32]kgo.Offset{coord.Topic: partitions}, end)
	for len(out) < count && !tailAllDone(done, partitions) {
		fetches := client.PollFetches(ctx)
		if err := fetches.Err(); err != nil {
			if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
				break
			}
			return nil, backendErr(err)
		}
		if fetches.Empty() {
			break
		}
		appendDLQFetches(fetches, coord, end, done, count, &out)
	}
	return out, nil
}

func appendDLQFetches(fetches kgo.Fetches, coord mqgov.TopicCoordinate, end kadm.ListedOffsets, done map[int32]bool, count int, out *[]mqgov.Message) {
	iter := fetches.RecordIter()
	for !iter.Done() && len(*out) < count {
		record := iter.Next()
		if record == nil {
			continue
		}
		if kafkaTailPastEnd(record, end) {
			done[record.Partition] = true
			continue
		}
		*out = append(*out, mqgov.Message{
			Coordinate: mqgov.MessageCoordinate{TopicCoordinate: coord, Partition: int(record.Partition), Offset: record.Offset},
			Key:        record.Key,
			Body:       record.Value,
			Headers:    recordHeaders(record.Headers),
		})
		if kafkaTailReachedEnd(record, end) {
			done[record.Partition] = true
		}
	}
}

func (b *Broker) peekClient(req mqgov.MessagePeekRequest) (*kgo.Client, error) {
	kopts, err := kgoOptions(b.opts)
	if err != nil {
		return nil, err
	}
	partition, err := safeInt32(req.Partition, "partition")
	if err != nil {
		return nil, err
	}
	partitions := map[string]map[int32]kgo.Offset{
		req.Coordinate.Topic: {partition: kgo.NewOffset().At(req.Offset)},
	}
	kopts = append(kopts, kgo.ConsumePartitions(partitions))
	client, err := kgo.NewClient(kopts...)
	if err != nil {
		return nil, unreachable(err)
	}
	return client, nil
}

func (b *Broker) tailOffsets(ctx context.Context, req mqgov.MessageTailRequest) (map[string]map[int32]kgo.Offset, kadm.ListedOffsets, error) {
	start, err := b.admin.ListStartOffsets(ctx, req.Coordinate.Topic)
	if err != nil {
		return nil, nil, backendErr(err)
	}
	end, err := b.admin.ListEndOffsets(ctx, req.Coordinate.Topic)
	if err != nil {
		return nil, nil, backendErr(err)
	}
	if err := start.Error(); err != nil {
		return nil, nil, topicNotFoundErr(err)
	}
	if err := end.Error(); err != nil {
		return nil, nil, topicNotFoundErr(err)
	}
	partitions := make(map[int32]kgo.Offset)
	for partition, listed := range end[req.Coordinate.Topic] {
		if req.Partition >= 0 && int(partition) != req.Partition {
			continue
		}
		offset, err := kafkaTailStartOffset(req.From, start[req.Coordinate.Topic][partition].Offset, listed.Offset)
		if err != nil {
			return nil, nil, err
		}
		partitions[partition] = kgo.NewOffset().At(offset)
	}
	if len(partitions) == 0 {
		return nil, nil, apperrors.New(apperrors.CodeResourceNotFound, "partition not found", nil)
	}
	return map[string]map[int32]kgo.Offset{req.Coordinate.Topic: partitions}, end, nil
}

func (b *Broker) tailClient(partitions map[string]map[int32]kgo.Offset) (*kgo.Client, error) {
	kopts, err := kgoOptions(b.opts)
	if err != nil {
		return nil, err
	}
	kopts = append(kopts, kgo.ConsumePartitions(partitions))
	client, err := kgo.NewClient(kopts...)
	if err != nil {
		return nil, unreachable(err)
	}
	return client, nil
}

func kafkaTailStartOffset(from string, start, end int64) (int64, error) {
	switch {
	case from == "" || from == "earliest":
		return start, nil
	case from == "latest":
		return end, nil
	case strings.HasPrefix(from, "offset:"):
		value, err := strconv.ParseInt(strings.TrimPrefix(from, "offset:"), 10, 64)
		if err != nil || value < 0 {
			return 0, apperrors.New(apperrors.CodeUsageError, "invalid tail offset", err)
		}
		return value, nil
	default:
		return 0, apperrors.New(apperrors.CodeUsageError, "unsupported tail start position", nil)
	}
}

func tailDonePartitions(starts map[string]map[int32]kgo.Offset, ends kadm.ListedOffsets) map[int32]bool {
	done := map[int32]bool{}
	for topic, partitions := range starts {
		for partition, offset := range partitions {
			end := ends[topic][partition].Offset
			if end >= 0 && offset.EpochOffset().Offset >= end {
				done[partition] = true
			}
		}
	}
	return done
}

func tailLimitReached(req mqgov.MessageTailRequest, result mqgov.MessageTailResult) bool {
	return req.MaxMessages > 0 && int(result.Count) >= req.MaxMessages
}

func tailAllDone(done map[int32]bool, partitions map[int32]kgo.Offset) bool {
	return len(done) == len(partitions)
}

func kafkaTailPastEnd(record *kgo.Record, ends kadm.ListedOffsets) bool {
	end := ends[record.Topic][record.Partition].Offset
	return end >= 0 && record.Offset >= end
}

func kafkaTailReachedEnd(record *kgo.Record, ends kadm.ListedOffsets) bool {
	end := ends[record.Topic][record.Partition].Offset
	return end >= 0 && record.Offset+1 >= end
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

func (b *Broker) timeout() time.Duration {
	if b.opts.Timeout > 0 {
		return b.opts.Timeout
	}
	return 30 * time.Second
}

func topicDescription(b *Broker, detail kadm.TopicDetail) mqgov.TopicDescription {
	return mqgov.TopicDescription{
		Coordinate: mqgov.TopicCoordinate{Cluster: b.opts.Cluster, Namespace: b.opts.Namespace, Topic: detail.Topic},
		Partitions: len(detail.Partitions),
		Internal:   detail.IsInternal || strings.HasPrefix(detail.Topic, "__"),
	}
}

func purgeImpact(start, end kadm.ListedOffsets) ([]mqgov.PartitionImpact, int64) {
	impact := make([]mqgov.PartitionImpact, 0)
	var total int64
	for topic, partitions := range end {
		for partition, endOffset := range partitions {
			from := int64(0)
			if s, ok := start[topic][partition]; ok && s.Err == nil && s.Offset >= 0 {
				from = s.Offset
			}
			to := endOffset.Offset
			if to < 0 {
				to = from
			}
			count := abs64(to - from)
			total += count
			impact = append(impact, mqgov.PartitionImpact{Partition: int(partition), From: from, To: to, Count: count})
		}
	}
	sort.Slice(impact, func(i, j int) bool { return impact[i].Partition < impact[j].Partition })
	return impact, total
}

func purgeImpactLimited(start, end kadm.ListedOffsets, limit int64) []mqgov.PartitionImpact {
	impact := make([]mqgov.PartitionImpact, 0)
	remaining := limit
	for topic, partitions := range end {
		keys := make([]int, 0, len(partitions))
		for partition := range partitions {
			keys = append(keys, int(partition))
		}
		sort.Ints(keys)
		for _, key := range keys {
			if remaining <= 0 {
				return impact
			}
			partition, err := safeInt32(key, "partition")
			if err != nil {
				return impact
			}
			endOffset := partitions[partition]
			from := int64(0)
			if s, ok := start[topic][partition]; ok && s.Err == nil && s.Offset >= 0 {
				from = s.Offset
			}
			to := endOffset.Offset
			if to < 0 {
				to = from
			}
			count := abs64(to - from)
			if count > remaining {
				count = remaining
				to = from + count
			}
			if count > 0 {
				impact = append(impact, mqgov.PartitionImpact{Partition: int(partition), From: from, To: to, Count: count})
				remaining -= count
			}
		}
	}
	return impact
}

func kgoOptions(opts Options) ([]kgo.Opt, error) {
	brokers := cleanedBrokers(opts.Brokers)
	if len(brokers) == 0 {
		return nil, apperrors.New(apperrors.CodeUsageError, "kafka bootstrap brokers not specified", nil)
	}
	kopts := []kgo.Opt{
		kgo.SeedBrokers(brokers...),
		kgo.ClientID("mqgov-cli"),
	}
	if opts.Timeout > 0 {
		kopts = append(kopts, kgo.DialTimeout(opts.Timeout), kgo.RequestTimeoutOverhead(opts.Timeout))
	}
	tlsConfig, err := buildTLSConfig(opts)
	if err != nil {
		return nil, err
	}
	if tlsConfig != nil {
		kopts = append(kopts, kgo.DialTLSConfig(tlsConfig))
	}
	mechanism, err := saslMechanism(opts)
	if err != nil {
		return nil, err
	}
	if mechanism != nil {
		if tlsConfig == nil {
			return nil, apperrors.New(apperrors.CodeUsageError, "Kafka SASL requires TLS", nil)
		}
		kopts = append(kopts, kgo.SASL(mechanism))
	}
	return kopts, nil
}

func schemaRegistryHTTPClient(opts Options) (*http.Client, error) {
	if opts.SchemaRegistryURL == "" {
		return &http.Client{Timeout: timeout(opts)}, nil
	}
	parsed, err := url.Parse(opts.SchemaRegistryURL)
	if err != nil || parsed.Scheme == "" || parsed.Host == "" {
		return nil, apperrors.New(apperrors.CodeUsageError, "invalid Kafka Schema Registry URL", err)
	}
	if schemaRegistryCredentialsConfigured(opts) && parsed.Scheme != "https" {
		return nil, apperrors.New(apperrors.CodeUsageError, "Kafka Schema Registry credentials require https", nil)
	}
	client := &http.Client{Timeout: timeout(opts)}
	if parsed.Scheme == "https" {
		tlsConfig, err := buildTLSConfig(opts)
		if err != nil {
			return nil, err
		}
		if tlsConfig == nil {
			tlsConfig = &tls.Config{MinVersion: tls.VersionTLS12}
		}
		client.Transport = &http.Transport{TLSClientConfig: tlsConfig}
	}
	return client, nil
}

func (b *Broker) schemaVersions(ctx context.Context, subject string) ([]string, error) {
	body, err := b.schemaRegistryJSON(ctx, http.MethodGet, "/subjects/"+url.PathEscape(subject)+"/versions", nil)
	if err != nil {
		return nil, err
	}
	var versions []int
	if err := json.Unmarshal(body, &versions); err != nil {
		return nil, backendErr(fmt.Errorf("decode schema registry versions: %w", err))
	}
	out := make([]string, 0, len(versions))
	for _, version := range versions {
		out = append(out, strconv.Itoa(version))
	}
	return out, nil
}

func (b *Broker) schemaRegistryJSON(ctx context.Context, method, path string, payload []byte) ([]byte, error) {
	endpoint, err := schemaRegistryEndpoint(b.opts, path)
	if err != nil {
		return nil, err
	}
	var body io.Reader
	if payload != nil {
		body = bytes.NewReader(payload)
	}
	req, err := http.NewRequestWithContext(ctx, method, endpoint, body)
	if err != nil {
		return nil, apperrors.New(apperrors.CodeUsageError, "invalid Kafka Schema Registry request", err)
	}
	req.Header.Set("Accept", "application/vnd.schemaregistry.v1+json, application/json")
	if payload != nil {
		req.Header.Set("Content-Type", "application/vnd.schemaregistry.v1+json")
	}
	if b.opts.SchemaRegistryUsername != "" || b.opts.SchemaRegistryPassword != "" {
		if b.opts.SchemaRegistryUsername == "" || b.opts.SchemaRegistryPassword == "" {
			return nil, apperrors.New(apperrors.CodeUsageError, "Kafka Schema Registry basic auth requires username and password", nil)
		}
		req.SetBasicAuth(b.opts.SchemaRegistryUsername, b.opts.SchemaRegistryPassword)
	}
	resp, err := b.srHTTP.Do(req)
	if err != nil {
		return nil, unreachable(err)
	}
	defer func() { _ = resp.Body.Close() }()
	data, _ := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	switch {
	case resp.StatusCode >= 200 && resp.StatusCode < 300:
		return data, nil
	case resp.StatusCode == http.StatusNotFound:
		return nil, apperrors.New(apperrors.CodeResourceNotFound, "schema subject or version not found", fmt.Errorf("schema registry status %d", resp.StatusCode))
	default:
		return nil, backendErr(fmt.Errorf("schema registry status %d: %s", resp.StatusCode, string(data)))
	}
}

func schemaRegistryEndpoint(opts Options, path string) (string, error) {
	if opts.SchemaRegistryURL == "" {
		return "", apperrors.New(apperrors.CodeNotImplemented, "Kafka Schema Registry URL is not configured", nil)
	}
	parsed, err := url.Parse(opts.SchemaRegistryURL)
	if err != nil || parsed.Scheme == "" || parsed.Host == "" {
		return "", apperrors.New(apperrors.CodeUsageError, "invalid Kafka Schema Registry URL", err)
	}
	if schemaRegistryCredentialsConfigured(opts) && parsed.Scheme != "https" {
		return "", apperrors.New(apperrors.CodeUsageError, "Kafka Schema Registry credentials require https", nil)
	}
	return strings.TrimRight(opts.SchemaRegistryURL, "/") + path, nil
}

func schemaRegistryCredentialsConfigured(opts Options) bool {
	return opts.SchemaRegistryUsername != "" || opts.SchemaRegistryPassword != ""
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
			return nil, apperrors.New(apperrors.CodeUsageError, "Kafka mTLS requires both client certificate and key files", nil)
		}
		cert, err := tls.LoadX509KeyPair(opts.ClientCertFile, opts.ClientKeyFile)
		if err != nil {
			return nil, apperrors.New(apperrors.CodeCredentialStoreError, "failed to load Kafka client certificate", nil)
		}
		cfg.Certificates = []tls.Certificate{cert}
	}
	return cfg, nil
}

func loadCertPool(path string) (*x509.CertPool, error) {
	data, err := os.ReadFile(path) //nolint:gosec // Kafka CA certificate path is an operator-supplied context setting, never derived from message data.
	if err != nil {
		return nil, apperrors.New(apperrors.CodeLocalIOError, "failed to read Kafka CA certificate", nil)
	}
	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM(data) {
		return nil, apperrors.New(apperrors.CodeValidationFailed, "failed to parse Kafka CA certificate", nil)
	}
	return pool, nil
}

func saslMechanism(opts Options) (sasl.Mechanism, error) {
	mechanism := strings.ToUpper(strings.TrimSpace(opts.SASLMechanism))
	if mechanism == "" {
		return nil, nil
	}
	if opts.Username == "" || opts.Password == "" {
		return nil, apperrors.New(apperrors.CodeUsageError, "Kafka SASL requires username and password", nil)
	}
	authFn := func(context.Context) (plain.Auth, error) {
		return plain.Auth{User: opts.Username, Pass: opts.Password}, nil
	}
	scramAuthFn := func(context.Context) (scram.Auth, error) {
		return scram.Auth{User: opts.Username, Pass: opts.Password}, nil
	}
	switch mechanism {
	case "PLAIN":
		return plain.Plain(authFn), nil
	case "SCRAM-SHA-256", "SCRAM_SHA_256":
		return scram.Sha256(scramAuthFn), nil
	case "SCRAM-SHA-512", "SCRAM_SHA_512":
		return scram.Sha512(scramAuthFn), nil
	default:
		return nil, apperrors.New(apperrors.CodeUsageError, "unsupported Kafka SASL mechanism", nil)
	}
}

func cleanedBrokers(in []string) []string {
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

func headers(in map[string][]byte) []kgo.RecordHeader {
	out := make([]kgo.RecordHeader, 0, len(in))
	for key, value := range in {
		out = append(out, kgo.RecordHeader{Key: key, Value: value})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Key < out[j].Key })
	return out
}

func recordHeaders(in []kgo.RecordHeader) map[string][]byte {
	if len(in) == 0 {
		return nil
	}
	out := make(map[string][]byte, len(in))
	for _, header := range in {
		out[header.Key] = header.Value
	}
	return out
}

func wrapListedOffsetsErr(err error) error {
	if err != nil {
		return backendErr(err)
	}
	return nil
}

func alterResponsesErr(responses kadm.AlterConfigsResponses) error {
	for _, response := range responses {
		if response.Err != nil {
			return response.Err
		}
	}
	return nil
}

func abs64(v int64) int64 {
	if v < 0 {
		return -v
	}
	return v
}

func timeout(opts Options) time.Duration {
	if opts.Timeout > 0 {
		return opts.Timeout
	}
	return 30 * time.Second
}

func firstNonEmpty(values ...string) string {
	for _, value := range values {
		if value != "" {
			return value
		}
	}
	return ""
}

func safeInt32(value int, field string) (int32, error) {
	const maxInt32 = int64(1<<31 - 1)
	if value < 0 || int64(value) > maxInt32 {
		return 0, apperrors.New(apperrors.CodeUsageError, field+" is out of range", nil)
	}
	return int32(value), nil
}

func partitionMatches(requested int, actual int32) bool {
	if requested < 0 {
		return true
	}
	return int64(requested) == int64(actual)
}

func committedOffset(committed kadm.OffsetResponses, topic string, partition int32) (int64, error) {
	topicOffsets, ok := committed[topic]
	if !ok {
		return 0, nil
	}
	response, ok := topicOffsets[partition]
	if !ok {
		return 0, nil
	}
	if response.Err != nil {
		return 0, backendErr(response.Err)
	}
	if response.At < 0 {
		return 0, nil
	}
	return response.At, nil
}

func aclFilterBuilder(filter mqgov.ACLFilter) (*kadm.ACLBuilder, error) {
	builder := kadm.NewACLs()
	if err := applyACLResource(builder, filter.ResourceType, filter.ResourceName); err != nil {
		return nil, err
	}
	if err := applyACLPattern(builder, filter.PatternType, true); err != nil {
		return nil, err
	}
	if err := applyACLOperation(builder, filter.Operation, true); err != nil {
		return nil, err
	}
	applyACLPrincipal(builder, filter.Principal, filter.Host, filter.Permission, true)
	return builder, nil
}

func aclBindingBuilder(binding mqgov.ACLBinding, filter bool) (*kadm.ACLBuilder, error) {
	if err := validateACLBinding(binding); err != nil {
		return nil, err
	}
	builder := kadm.NewACLs()
	if err := applyACLResource(builder, binding.ResourceType, binding.ResourceName); err != nil {
		return nil, err
	}
	if err := applyACLPattern(builder, binding.PatternType, filter); err != nil {
		return nil, err
	}
	if err := applyACLOperation(builder, binding.Operation, filter); err != nil {
		return nil, err
	}
	applyACLPrincipal(builder, binding.Principal, binding.Host, binding.Permission, filter)
	return builder, nil
}

func validateACLBinding(binding mqgov.ACLBinding) error {
	if strings.TrimSpace(binding.Principal) == "" {
		return apperrors.New(apperrors.CodeUsageError, "ACL principal is required", nil)
	}
	if strings.TrimSpace(binding.ResourceType) == "" {
		return apperrors.New(apperrors.CodeUsageError, "ACL resource type is required", nil)
	}
	if strings.TrimSpace(binding.ResourceName) == "" {
		return apperrors.New(apperrors.CodeUsageError, "ACL resource name is required", nil)
	}
	if strings.TrimSpace(binding.Operation) == "" {
		return apperrors.New(apperrors.CodeUsageError, "ACL operation is required", nil)
	}
	permission := strings.ToLower(strings.TrimSpace(binding.Permission))
	if permission != "allow" && permission != "deny" {
		return apperrors.New(apperrors.CodeUsageError, "ACL permission must be allow or deny", nil)
	}
	return nil
}

func applyACLResource(builder *kadm.ACLBuilder, resourceType, resourceName string) error {
	resourceType = strings.TrimSpace(resourceType)
	resourceName = strings.TrimSpace(resourceName)
	if resourceType == "" {
		applyACLNames(resourceName, builder.AnyResource)
		return nil
	}
	switch normalizeACLValue(resourceType) {
	case "any":
		applyACLNames(resourceName, builder.AnyResource)
	case "topic":
		applyACLNames(resourceName, builder.Topics)
	case "group":
		applyACLNames(resourceName, builder.Groups)
	case "cluster":
		builder.Clusters()
	case "transactionalid":
		applyACLNames(resourceName, builder.TransactionalIDs)
	case "delegationtoken":
		applyACLNames(resourceName, builder.DelegationTokens)
	default:
		return apperrors.New(apperrors.CodeUsageError, "unsupported ACL resource type", nil)
	}
	return nil
}

func applyACLNames(resourceName string, apply func(...string) *kadm.ACLBuilder) {
	if resourceName == "" {
		apply()
		return
	}
	apply(resourceName)
}

func applyACLPattern(builder *kadm.ACLBuilder, pattern string, filter bool) error {
	pattern = strings.TrimSpace(pattern)
	if pattern == "" {
		if filter {
			builder.ResourcePatternType(kadm.ACLPatternAny)
		} else {
			builder.ResourcePatternType(kadm.ACLPatternLiteral)
		}
		return nil
	}
	switch normalizeACLValue(pattern) {
	case "literal":
		builder.ResourcePatternType(kadm.ACLPatternLiteral)
	case "prefixed":
		builder.ResourcePatternType(kadm.ACLPatternPrefixed)
	case "any":
		builder.ResourcePatternType(kadm.ACLPatternAny)
	case "match":
		builder.ResourcePatternType(kadm.ACLPatternMatch)
	case "regex":
		return apperrors.New(apperrors.CodeUsageError, "Kafka ACL pattern type must be literal or prefixed", nil)
	default:
		return apperrors.New(apperrors.CodeUsageError, "invalid ACL pattern type", nil)
	}
	return nil
}

func applyACLOperation(builder *kadm.ACLBuilder, operation string, filter bool) error {
	operation = strings.TrimSpace(operation)
	if operation == "" {
		if filter {
			builder.Operations()
			return nil
		}
		return apperrors.New(apperrors.CodeUsageError, "ACL operation is required", nil)
	}
	parsed, ok := parseACLOperation(operation)
	if !ok {
		return apperrors.New(apperrors.CodeUsageError, "invalid ACL operation", nil)
	}
	builder.Operations(parsed)
	return nil
}

func parseACLOperation(operation string) (kadm.ACLOperation, bool) {
	switch normalizeACLValue(operation) {
	case "any":
		return kadm.OpAny, true
	case "all":
		return kadm.OpAll, true
	case "read":
		return kadm.OpRead, true
	case "write":
		return kadm.OpWrite, true
	case "create":
		return kadm.OpCreate, true
	case "delete":
		return kadm.OpDelete, true
	case "alter":
		return kadm.OpAlter, true
	case "describe":
		return kadm.OpDescribe, true
	case "clusteraction":
		return kadm.OpClusterAction, true
	case "describeconfigs":
		return kadm.OpDescribeConfigs, true
	case "alterconfigs":
		return kadm.OpAlterConfigs, true
	case "idempotentwrite":
		return kadm.OpIdempotentWrite, true
	default:
		return kadm.OpUnknown, false
	}
}

func normalizeACLValue(value string) string {
	replacer := strings.NewReplacer("_", "", "-", "", ".", "")
	return strings.ToLower(replacer.Replace(strings.TrimSpace(value)))
}

func applyACLPrincipal(builder *kadm.ACLBuilder, principal, host, permission string, filter bool) {
	principal = strings.TrimSpace(principal)
	host = strings.TrimSpace(host)
	permission = strings.ToLower(strings.TrimSpace(permission))
	if host == "" && !filter {
		host = "*"
	}
	switch permission {
	case "allow":
		builder.Allow(principalOrAny(principal)...).AllowHosts(hostOrAny(host)...)
	case "deny":
		builder.Deny(principalOrAny(principal)...).DenyHosts(hostOrAny(host)...)
	default:
		builder.Allow(principalOrAny(principal)...).AllowHosts(hostOrAny(host)...)
		if filter {
			builder.Deny(principalOrAny(principal)...).DenyHosts(hostOrAny(host)...)
		}
	}
}

func principalOrAny(value string) []string {
	if value == "" {
		return nil
	}
	return []string{value}
}

func hostOrAny(value string) []string {
	if value == "" {
		return nil
	}
	return []string{value}
}

func aclSortKey(binding mqgov.ACLBinding) string {
	return binding.Principal + "\x00" + binding.Host + "\x00" + binding.ResourceType + "\x00" + binding.ResourceName + "\x00" + binding.PatternType + "\x00" + binding.Operation + "\x00" + binding.Permission
}

func unreachable(causes ...error) error {
	return apperrors.New(apperrors.CodeBackendUnreachable, "kafka backend unreachable", firstCause(causes))
}

func backendErr(causes ...error) error {
	return apperrors.New(apperrors.CodeBackendError, "kafka backend error", firstCause(causes))
}

func createTopicErr(err error) error {
	if errors.Is(err, kerr.TopicAlreadyExists) {
		return apperrors.New(apperrors.CodeResourceAlreadyExists, "topic already exists", err)
	}
	return backendErr(err)
}

func topicNotFoundErr(err error) error {
	if err == nil || errors.Is(err, kerr.UnknownTopicOrPartition) || errors.Is(err, kerr.UnknownTopicID) {
		return apperrors.New(apperrors.CodeResourceNotFound, "topic not found", err)
	}
	return backendErr(err)
}

func firstCause(causes []error) error {
	for _, cause := range causes {
		if cause != nil {
			return cause
		}
	}
	return nil
}

func (b *Broker) String() string {
	return fmt.Sprintf("kafka(%s)", b.opts.Cluster)
}
