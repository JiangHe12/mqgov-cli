package mqgov

import "context"

type TopicCoordinate struct {
	Cluster   string `json:"cluster,omitempty"`
	Namespace string `json:"namespace,omitempty"`
	Topic     string `json:"topic"`
}

type GroupCoordinate struct {
	Cluster   string `json:"cluster,omitempty"`
	Namespace string `json:"namespace,omitempty"`
	Group     string `json:"group"`
}

type MessageCoordinate struct {
	TopicCoordinate
	Partition int   `json:"partition,omitempty"`
	Offset    int64 `json:"offset,omitempty"`
}

type TopicListOptions struct {
	Namespace string `json:"namespace,omitempty"`
	Pattern   string `json:"pattern,omitempty"`
	Limit     int    `json:"limit,omitempty"`
}

type TopicDescription struct {
	Coordinate TopicCoordinate   `json:"coordinate"`
	Partitions int               `json:"partitions"`
	Config     map[string]string `json:"config,omitempty"`
	Protected  bool              `json:"protected,omitempty"`
	Internal   bool              `json:"internal,omitempty"`
}

type TopicCreateRequest struct {
	Coordinate TopicCoordinate
	Partitions int
	Config     map[string]string
	Protected  bool
}

type TopicAlterRequest struct {
	Coordinate TopicCoordinate
	Partitions int
	Config     map[string]string
}

type TopicPurgeRequest struct {
	Coordinate TopicCoordinate
	DLQ        bool
	DryRun     bool
}

type TopicPurgeResult struct {
	Coordinate  TopicCoordinate      `json:"coordinate"`
	DLQ         bool                 `json:"dlq,omitempty"`
	DryRun      bool                 `json:"dryRun"`
	Impact      []PartitionImpact    `json:"impact"`
	Total       int64                `json:"total"`
	Fingerprint ResourceFingerprints `json:"fingerprint"`
}

type DLQListOptions struct {
	Namespace string `json:"namespace,omitempty"`
	Topic     string `json:"topic,omitempty"`
	Group     string `json:"group,omitempty"`
	Pattern   string `json:"pattern,omitempty"`
	Limit     int    `json:"limit,omitempty"`
}

type DLQDescription struct {
	Coordinate    TopicCoordinate `json:"coordinate"`
	SourceTopic   string          `json:"sourceTopic,omitempty"`
	ConsumerGroup string          `json:"consumerGroup,omitempty"`
	NativeModel   string          `json:"nativeModel"`
	Messages      int64           `json:"messages,omitempty"`
}

type DLQPeekRequest struct {
	DLQ       TopicCoordinate
	Topic     string
	Group     string
	Partition int
	Offset    int64
	Count     int
}

type DLQPeekResult struct {
	DLQ       TopicCoordinate      `json:"dlq"`
	Partition int                  `json:"partition,omitempty"`
	Offset    int64                `json:"offset,omitempty"`
	Count     int                  `json:"count"`
	Messages  []MessageFingerprint `json:"messages"`
}

type DLQRedriveRequest struct {
	DLQ    TopicCoordinate
	Target TopicCoordinate
	Topic  string
	Group  string
	Count  int
	DryRun bool
}

type DLQRedriveResult struct {
	DLQ         TopicCoordinate      `json:"dlq"`
	Target      TopicCoordinate      `json:"target"`
	DryRun      bool                 `json:"dryRun"`
	Impact      []PartitionImpact    `json:"impact"`
	Total       int64                `json:"total"`
	Fingerprint ResourceFingerprints `json:"fingerprint"`
}

type DLQPurgeRequest struct {
	DLQ    TopicCoordinate
	Topic  string
	Group  string
	DryRun bool
}

type DLQPurgeResult struct {
	DLQ         TopicCoordinate      `json:"dlq"`
	DryRun      bool                 `json:"dryRun"`
	Impact      []PartitionImpact    `json:"impact"`
	Total       int64                `json:"total"`
	Fingerprint ResourceFingerprints `json:"fingerprint"`
}

type SchemaListOptions struct {
	Subject string `json:"subject,omitempty"`
	Pattern string `json:"pattern,omitempty"`
	Limit   int    `json:"limit,omitempty"`
}

type SchemaSubject struct {
	Subject string `json:"subject"`
}

type SchemaDescription struct {
	Subject    string            `json:"subject"`
	Version    string            `json:"version"`
	ID         int               `json:"id,omitempty"`
	Type       string            `json:"type,omitempty"`
	Schema     string            `json:"schema,omitempty"`
	SchemaHash string            `json:"schemaSha256,omitempty"`
	Versions   []string          `json:"versions,omitempty"`
	Properties map[string]string `json:"properties,omitempty"`
}

type SchemaDescribeRequest struct {
	Subject string
	Version string
}

type SchemaCheckRequest struct {
	Subject string
	Version string
	Type    string
	Schema  string
}

type SchemaCheckResult struct {
	Subject    string `json:"subject"`
	Version    string `json:"version"`
	Compatible bool   `json:"compatible"`
	SchemaHash string `json:"schemaSha256,omitempty"`
	Message    string `json:"message,omitempty"`
}

type GroupListOptions struct {
	Namespace string `json:"namespace,omitempty"`
	Pattern   string `json:"pattern,omitempty"`
	Limit     int    `json:"limit,omitempty"`
}

type GroupDescription struct {
	Coordinate GroupCoordinate `json:"coordinate"`
	Members    int             `json:"members"`
	State      string          `json:"state,omitempty"`
}

type Message struct {
	Coordinate MessageCoordinate `json:"coordinate"`
	Key        []byte            `json:"-"`
	Body       []byte            `json:"-"`
	Headers    map[string][]byte `json:"-"`
}

type MessageProduceRequest struct {
	Coordinate TopicCoordinate
	Key        []byte
	Body       []byte
	Headers    map[string][]byte
}

type MessageProduceResult struct {
	Coordinate  MessageCoordinate    `json:"coordinate"`
	Fingerprint ResourceFingerprints `json:"fingerprint"`
}

type MessagePeekRequest struct {
	Coordinate TopicCoordinate
	Partition  int
	Offset     int64
	Count      int
}

type MessageTailRequest struct {
	Coordinate  TopicCoordinate
	Partition   int
	From        string
	Follow      bool
	MaxMessages int
}

type MessagePeekResult struct {
	Coordinate TopicCoordinate      `json:"coordinate"`
	Partition  int                  `json:"partition"`
	Offset     int64                `json:"offset"`
	Count      int                  `json:"count"`
	Messages   []MessageFingerprint `json:"messages"`
}

type MessageTailResult struct {
	Coordinate TopicCoordinate   `json:"coordinate"`
	Count      int64             `json:"count"`
	TotalSize  int64             `json:"totalSize"`
	Impact     []PartitionImpact `json:"impact,omitempty"`
}

type OffsetPlanRequest struct {
	Group     GroupCoordinate
	Topic     TopicCoordinate
	To        string
	DryRun    bool
	Partition int
}

type OffsetPlan struct {
	Group  GroupCoordinate   `json:"group"`
	Topic  TopicCoordinate   `json:"topic"`
	To     string            `json:"to"`
	DryRun bool              `json:"dryRun"`
	Impact []PartitionImpact `json:"impact"`
	Total  int64             `json:"total"`
}

type PartitionImpact struct {
	Partition int   `json:"partition"`
	From      int64 `json:"from,omitempty"`
	To        int64 `json:"to,omitempty"`
	Count     int64 `json:"count"`
}

type MessageFingerprint struct {
	Partition  int    `json:"partition,omitempty"`
	Offset     int64  `json:"offset,omitempty"`
	KeySHA256  string `json:"keySha256,omitempty"`
	BodySHA256 string `json:"bodySha256,omitempty"`
	Size       int    `json:"size"`
	Timestamp  string `json:"timestamp,omitempty"`
}

type ResourceFingerprints struct {
	KeySHA256  string `json:"keySha256,omitempty"`
	BodySHA256 string `json:"bodySha256,omitempty"`
	Count      int64  `json:"count,omitempty"`
	Size       int    `json:"size,omitempty"`
}

type Description struct {
	Backend   string `json:"backend"`
	Cluster   string `json:"cluster,omitempty"`
	Namespace string `json:"namespace,omitempty"`
}

type Capabilities struct {
	Backend            string   `json:"backend"`
	ResourceTypes      []string `json:"resourceTypes"`
	Verbs              []string `json:"verbs"`
	SupportsOffsets    bool     `json:"supportsOffsets"`
	SupportsPartitions bool     `json:"supportsPartitions"`
	SupportsACL        bool     `json:"supportsAcl"`
	SupportsDLQList    bool     `json:"supportsDlqList"`
	SupportsDLQPeek    bool     `json:"supportsDlqPeek"`
	SupportsDLQRedrive bool     `json:"supportsDlqRedrive"`
	SupportsDLQPurge   bool     `json:"supportsDlqPurge"`
	SupportsSchema     bool     `json:"supportsSchema"`
}

type ACLBinding struct {
	Principal    string `json:"principal"`
	Host         string `json:"host"`
	Vhost        string `json:"vhost,omitempty"`
	ResourceType string `json:"resourceType"`
	ResourceName string `json:"resourceName"`
	PatternType  string `json:"patternType"`
	Operation    string `json:"operation"`
	Permission   string `json:"permission"`
}

type ACLFilter struct {
	Principal    string `json:"principal,omitempty"`
	Host         string `json:"host,omitempty"`
	Vhost        string `json:"vhost,omitempty"`
	ResourceType string `json:"resourceType,omitempty"`
	ResourceName string `json:"resourceName,omitempty"`
	PatternType  string `json:"patternType,omitempty"`
	Operation    string `json:"operation,omitempty"`
	Permission   string `json:"permission,omitempty"`
}

type Broker interface {
	Ping(ctx context.Context) error
	Describe() Description
	Capabilities() Capabilities
	ListTopics(ctx context.Context, opts TopicListOptions) ([]TopicDescription, error)
	DescribeTopic(ctx context.Context, coord TopicCoordinate) (TopicDescription, error)
	CreateTopic(ctx context.Context, req TopicCreateRequest) (TopicDescription, error)
	DeleteTopic(ctx context.Context, coord TopicCoordinate) error
	ListGroups(ctx context.Context, opts GroupListOptions) ([]GroupDescription, error)
	CreateGroup(ctx context.Context, coord GroupCoordinate) (GroupDescription, error)
	DeleteGroup(ctx context.Context, coord GroupCoordinate) error
	Peek(ctx context.Context, req MessagePeekRequest) (MessagePeekResult, error)
	Produce(ctx context.Context, req MessageProduceRequest) (MessageProduceResult, error)
}

type OffsetManager interface {
	PlanOffsetReset(ctx context.Context, req OffsetPlanRequest) (OffsetPlan, error)
	ResetOffset(ctx context.Context, req OffsetPlanRequest) (OffsetPlan, error)
	Lag(ctx context.Context, group GroupCoordinate, topic TopicCoordinate) (OffsetPlan, error)
}

type PartitionManager interface {
	AlterTopic(ctx context.Context, req TopicAlterRequest) (TopicDescription, error)
	PurgeTopic(ctx context.Context, req TopicPurgeRequest) (TopicPurgeResult, error)
}

type ACLManager interface {
	ListACLs(ctx context.Context, filter ACLFilter) ([]ACLBinding, error)
	GrantACL(ctx context.Context, binding ACLBinding) error
	RevokeACL(ctx context.Context, binding ACLBinding) error
}

type Tailer interface {
	Tail(ctx context.Context, req MessageTailRequest, emit func(MessageFingerprint) error) (MessageTailResult, error)
}

type DLQManager interface {
	ListDLQs(ctx context.Context, opts DLQListOptions) ([]DLQDescription, error)
	PeekDLQ(ctx context.Context, req DLQPeekRequest) (DLQPeekResult, error)
	RedriveDLQ(ctx context.Context, req DLQRedriveRequest) (DLQRedriveResult, error)
	PurgeDLQ(ctx context.Context, req DLQPurgeRequest) (DLQPurgeResult, error)
}

type SchemaManager interface {
	ListSchemas(ctx context.Context, opts SchemaListOptions) ([]SchemaSubject, error)
	DescribeSchema(ctx context.Context, req SchemaDescribeRequest) (SchemaDescription, error)
	CheckCompatibility(ctx context.Context, req SchemaCheckRequest) (SchemaCheckResult, error)
}

func SupportsOffsets(b Broker) (OffsetManager, bool) {
	manager, ok := b.(OffsetManager)
	return manager, ok
}

func SupportsPartitions(b Broker) (PartitionManager, bool) {
	manager, ok := b.(PartitionManager)
	return manager, ok
}

func SupportsACL(b Broker) (ACLManager, bool) {
	manager, ok := b.(ACLManager)
	return manager, ok
}

func SupportsTail(b Broker) (Tailer, bool) {
	tailer, ok := b.(Tailer)
	return tailer, ok
}

func SupportsDLQ(b Broker) (DLQManager, bool) {
	manager, ok := b.(DLQManager)
	return manager, ok
}

func SupportsSchema(b Broker) (SchemaManager, bool) {
	manager, ok := b.(SchemaManager)
	return manager, ok
}
