package fake

import (
	"context"
	"sort"
	"sync"

	"github.com/JiangHe12/opskit-core/apperrors"

	"github.com/JiangHe12/mqgov-cli/internal/mqgov"
)

type Broker struct {
	mu        sync.Mutex
	cluster   string
	namespace string
	topics    map[string]*topicState
	groups    map[string]mqgov.GroupDescription
	lag       map[string]map[int]int64
}

type topicState struct {
	desc     mqgov.TopicDescription
	messages []mqgov.Message
}

func New(cluster, namespace string) *Broker {
	if cluster == "" {
		cluster = "fake"
	}
	b := &Broker{
		cluster:   cluster,
		namespace: namespace,
		topics:    make(map[string]*topicState),
		groups:    make(map[string]mqgov.GroupDescription),
		lag:       make(map[string]map[int]int64),
	}
	b.topics["orders"] = &topicState{desc: mqgov.TopicDescription{
		Coordinate: mqgov.TopicCoordinate{Cluster: cluster, Namespace: namespace, Topic: "orders"},
		Partitions: 3,
		Config:     map[string]string{"retention.ms": "60000"},
	}}
	b.topics["orders.dlq"] = &topicState{
		desc: mqgov.TopicDescription{
			Coordinate: mqgov.TopicCoordinate{Cluster: cluster, Namespace: namespace, Topic: "orders.dlq"},
			Partitions: 1,
		},
		messages: []mqgov.Message{{
			Coordinate: mqgov.MessageCoordinate{TopicCoordinate: mqgov.TopicCoordinate{Cluster: cluster, Namespace: namespace, Topic: "orders.dlq"}},
			Key:        []byte("dlq-key"),
			Body:       []byte("dlq-body"),
		}},
	}
	b.topics["payments"] = &topicState{desc: mqgov.TopicDescription{
		Coordinate: mqgov.TopicCoordinate{Cluster: cluster, Namespace: namespace, Topic: "payments"},
		Partitions: 2,
		Protected:  true,
	}}
	b.topics["__consumer_offsets"] = &topicState{desc: mqgov.TopicDescription{
		Coordinate: mqgov.TopicCoordinate{Cluster: cluster, Namespace: namespace, Topic: "__consumer_offsets"},
		Partitions: 2,
		Internal:   true,
		Protected:  true,
	}}
	b.groups["billing"] = mqgov.GroupDescription{Coordinate: mqgov.GroupCoordinate{Cluster: cluster, Namespace: namespace, Group: "billing"}, Members: 1, State: "stable"}
	b.lag["billing:orders"] = map[int]int64{0: 12, 1: 0, 2: 7}
	return b
}

func (b *Broker) Ping(context.Context) error { return nil }

func (b *Broker) Describe() mqgov.Description {
	return mqgov.Description{Backend: "fake", Cluster: b.cluster, Namespace: b.namespace}
}

func (b *Broker) Capabilities() mqgov.Capabilities {
	return mqgov.Capabilities{
		Backend:            "fake",
		ResourceTypes:      []string{"topic", "group", "message", "offset", "dlq"},
		Verbs:              []string{"list", "describe", "lag", "peek", "produce", "create", "alter", "delete", "purge", "reset-offset", "redrive"},
		SupportsOffsets:    true,
		SupportsPartitions: true,
		SupportsACL:        false,
		SupportsDLQList:    true,
		SupportsDLQPeek:    true,
		SupportsDLQRedrive: true,
		SupportsDLQPurge:   true,
	}
}

func (b *Broker) ListTopics(_ context.Context, opts mqgov.TopicListOptions) ([]mqgov.TopicDescription, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	items := make([]mqgov.TopicDescription, 0, len(b.topics))
	for _, state := range b.topics {
		if opts.Pattern != "" && opts.Pattern != state.desc.Coordinate.Topic {
			continue
		}
		items = append(items, state.desc)
	}
	sort.Slice(items, func(i, j int) bool { return items[i].Coordinate.Topic < items[j].Coordinate.Topic })
	if opts.Limit > 0 && len(items) > opts.Limit {
		items = items[:opts.Limit]
	}
	return items, nil
}

func (b *Broker) DescribeTopic(_ context.Context, coord mqgov.TopicCoordinate) (mqgov.TopicDescription, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	state, ok := b.topics[coord.Topic]
	if !ok {
		return mqgov.TopicDescription{}, apperrors.New(apperrors.CodeResourceNotFound, "topic not found", nil)
	}
	return state.desc, nil
}

func (b *Broker) CreateTopic(_ context.Context, req mqgov.TopicCreateRequest) (mqgov.TopicDescription, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if _, ok := b.topics[req.Coordinate.Topic]; ok {
		return mqgov.TopicDescription{}, apperrors.New(apperrors.CodeResourceAlreadyExists, "topic already exists", nil)
	}
	if req.Partitions <= 0 {
		req.Partitions = 1
	}
	desc := mqgov.TopicDescription{Coordinate: req.Coordinate, Partitions: req.Partitions, Config: req.Config, Protected: req.Protected}
	b.topics[req.Coordinate.Topic] = &topicState{desc: desc}
	return desc, nil
}

func (b *Broker) DeleteTopic(_ context.Context, coord mqgov.TopicCoordinate) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	if _, ok := b.topics[coord.Topic]; !ok {
		return apperrors.New(apperrors.CodeResourceNotFound, "topic not found", nil)
	}
	delete(b.topics, coord.Topic)
	return nil
}

func (b *Broker) ListGroups(_ context.Context, opts mqgov.GroupListOptions) ([]mqgov.GroupDescription, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	items := make([]mqgov.GroupDescription, 0, len(b.groups))
	for _, group := range b.groups {
		if opts.Pattern != "" && opts.Pattern != group.Coordinate.Group {
			continue
		}
		items = append(items, group)
	}
	sort.Slice(items, func(i, j int) bool { return items[i].Coordinate.Group < items[j].Coordinate.Group })
	return items, nil
}

func (b *Broker) CreateGroup(_ context.Context, coord mqgov.GroupCoordinate) (mqgov.GroupDescription, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if _, ok := b.groups[coord.Group]; ok {
		return mqgov.GroupDescription{}, apperrors.New(apperrors.CodeResourceAlreadyExists, "group already exists", nil)
	}
	desc := mqgov.GroupDescription{Coordinate: coord, State: "empty"}
	b.groups[coord.Group] = desc
	return desc, nil
}

func (b *Broker) DeleteGroup(_ context.Context, coord mqgov.GroupCoordinate) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	if _, ok := b.groups[coord.Group]; !ok {
		return apperrors.New(apperrors.CodeResourceNotFound, "group not found", nil)
	}
	delete(b.groups, coord.Group)
	return nil
}

func (b *Broker) Peek(_ context.Context, req mqgov.MessagePeekRequest) (mqgov.MessagePeekResult, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	state, ok := b.topics[req.Coordinate.Topic]
	if !ok {
		return mqgov.MessagePeekResult{}, apperrors.New(apperrors.CodeResourceNotFound, "topic not found", nil)
	}
	count := req.Count
	if count <= 0 {
		count = 1
	}
	msgs := make([]mqgov.MessageFingerprint, 0, count)
	for i, msg := range state.messages {
		if len(msgs) >= count {
			break
		}
		if msg.Coordinate.Partition == req.Partition && msg.Coordinate.Offset >= req.Offset {
			msgs = append(msgs, mqgov.FingerprintMessage(req.Partition, int64(i), msg.Key, msg.Body))
		}
	}
	return mqgov.MessagePeekResult{Coordinate: req.Coordinate, Partition: req.Partition, Offset: req.Offset, Count: len(msgs), Messages: msgs}, nil
}

func (b *Broker) Produce(_ context.Context, req mqgov.MessageProduceRequest) (mqgov.MessageProduceResult, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	state, ok := b.topics[req.Coordinate.Topic]
	if !ok {
		return mqgov.MessageProduceResult{}, apperrors.New(apperrors.CodeResourceNotFound, "topic not found", nil)
	}
	offset := int64(len(state.messages))
	msg := mqgov.Message{Coordinate: mqgov.MessageCoordinate{TopicCoordinate: req.Coordinate, Offset: offset}, Key: req.Key, Body: req.Body, Headers: req.Headers}
	state.messages = append(state.messages, msg)
	return mqgov.MessageProduceResult{
		Coordinate:  msg.Coordinate,
		Fingerprint: mqgov.Fingerprints(req.Key, req.Body, 1),
	}, nil
}

func (b *Broker) AlterTopic(_ context.Context, req mqgov.TopicAlterRequest) (mqgov.TopicDescription, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	state, ok := b.topics[req.Coordinate.Topic]
	if !ok {
		return mqgov.TopicDescription{}, apperrors.New(apperrors.CodeResourceNotFound, "topic not found", nil)
	}
	if req.Partitions > state.desc.Partitions {
		state.desc.Partitions = req.Partitions
	}
	for key, value := range req.Config {
		if state.desc.Config == nil {
			state.desc.Config = make(map[string]string)
		}
		state.desc.Config[key] = value
	}
	return state.desc, nil
}

func (b *Broker) PurgeTopic(_ context.Context, req mqgov.TopicPurgeRequest) (mqgov.TopicPurgeResult, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	state, ok := b.topics[req.Coordinate.Topic]
	if !ok {
		return mqgov.TopicPurgeResult{}, apperrors.New(apperrors.CodeResourceNotFound, "topic not found", nil)
	}
	total := int64(len(state.messages))
	impact := []mqgov.PartitionImpact{{Partition: 0, Count: total}}
	if !req.DryRun {
		state.messages = nil
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

func (b *Broker) ListDLQs(_ context.Context, opts mqgov.DLQListOptions) ([]mqgov.DLQDescription, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	items := make([]mqgov.DLQDescription, 0)
	for name, state := range b.topics {
		if opts.Pattern != "" && opts.Pattern != name {
			continue
		}
		if opts.Topic != "" && opts.Topic+".dlq" != name && opts.Topic != name {
			continue
		}
		if name == "orders.dlq" {
			items = append(items, mqgov.DLQDescription{
				Coordinate:  state.desc.Coordinate,
				SourceTopic: "orders",
				NativeModel: "fake-topic",
				Messages:    int64(len(state.messages)),
			})
		}
	}
	sort.Slice(items, func(i, j int) bool { return items[i].Coordinate.Topic < items[j].Coordinate.Topic })
	return items, nil
}

func (b *Broker) PeekDLQ(_ context.Context, req mqgov.DLQPeekRequest) (mqgov.DLQPeekResult, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	state, ok := b.topics[req.DLQ.Topic]
	if !ok {
		return mqgov.DLQPeekResult{}, apperrors.New(apperrors.CodeResourceNotFound, "DLQ not found", nil)
	}
	count := req.Count
	if count <= 0 {
		count = 1
	}
	msgs := make([]mqgov.MessageFingerprint, 0, count)
	for i, msg := range state.messages {
		if len(msgs) >= count {
			break
		}
		msgs = append(msgs, mqgov.FingerprintMessage(0, int64(i), msg.Key, msg.Body))
	}
	return mqgov.DLQPeekResult{DLQ: req.DLQ, Count: len(msgs), Messages: msgs}, nil
}

func (b *Broker) RedriveDLQ(_ context.Context, req mqgov.DLQRedriveRequest) (mqgov.DLQRedriveResult, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	dlq, ok := b.topics[req.DLQ.Topic]
	if !ok {
		return mqgov.DLQRedriveResult{}, apperrors.New(apperrors.CodeResourceNotFound, "DLQ not found", nil)
	}
	target, ok := b.topics[req.Target.Topic]
	if !ok {
		return mqgov.DLQRedriveResult{}, apperrors.New(apperrors.CodeResourceNotFound, "target topic not found", nil)
	}
	limit := req.Count
	if limit <= 0 || limit > len(dlq.messages) {
		limit = len(dlq.messages)
	}
	if !req.DryRun {
		for _, msg := range dlq.messages[:limit] {
			offset := int64(len(target.messages))
			target.messages = append(target.messages, mqgov.Message{Coordinate: mqgov.MessageCoordinate{TopicCoordinate: req.Target, Offset: offset}, Key: msg.Key, Body: msg.Body, Headers: msg.Headers})
		}
		dlq.messages = dlq.messages[limit:]
	}
	total := int64(limit)
	return mqgov.DLQRedriveResult{
		DLQ:         req.DLQ,
		Target:      req.Target,
		DryRun:      req.DryRun,
		Impact:      []mqgov.PartitionImpact{{Partition: 0, Count: total}},
		Total:       total,
		Fingerprint: mqgov.ResourceFingerprints{Count: total},
	}, nil
}

func (b *Broker) PurgeDLQ(_ context.Context, req mqgov.DLQPurgeRequest) (mqgov.DLQPurgeResult, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	state, ok := b.topics[req.DLQ.Topic]
	if !ok {
		return mqgov.DLQPurgeResult{}, apperrors.New(apperrors.CodeResourceNotFound, "DLQ not found", nil)
	}
	total := int64(len(state.messages))
	if !req.DryRun {
		state.messages = nil
	}
	return mqgov.DLQPurgeResult{
		DLQ:         req.DLQ,
		DryRun:      req.DryRun,
		Impact:      []mqgov.PartitionImpact{{Partition: 0, Count: total}},
		Total:       total,
		Fingerprint: mqgov.ResourceFingerprints{Count: total},
	}, nil
}

func (b *Broker) PlanOffsetReset(_ context.Context, req mqgov.OffsetPlanRequest) (mqgov.OffsetPlan, error) {
	return b.offsetPlan(req, true), nil
}

func (b *Broker) ResetOffset(_ context.Context, req mqgov.OffsetPlanRequest) (mqgov.OffsetPlan, error) {
	return b.offsetPlan(req, req.DryRun), nil
}

func (b *Broker) Lag(_ context.Context, group mqgov.GroupCoordinate, topic mqgov.TopicCoordinate) (mqgov.OffsetPlan, error) {
	return b.offsetPlan(mqgov.OffsetPlanRequest{Group: group, Topic: topic, To: "latest", DryRun: true}, true), nil
}

func (b *Broker) offsetPlan(req mqgov.OffsetPlanRequest, dryRun bool) mqgov.OffsetPlan {
	b.mu.Lock()
	defer b.mu.Unlock()
	key := req.Group.Group + ":" + req.Topic.Topic
	partitions := b.lag[key]
	impact := make([]mqgov.PartitionImpact, 0, len(partitions))
	var total int64
	for partition, count := range partitions {
		if req.Partition >= 0 && partition != req.Partition {
			continue
		}
		impact = append(impact, mqgov.PartitionImpact{Partition: partition, From: count, To: 0, Count: count})
		total += count
	}
	sort.Slice(impact, func(i, j int) bool { return impact[i].Partition < impact[j].Partition })
	return mqgov.OffsetPlan{Group: req.Group, Topic: req.Topic, To: req.To, DryRun: dryRun, Impact: impact, Total: total}
}
