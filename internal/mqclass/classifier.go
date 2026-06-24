package mqclass

import (
	"strings"

	"github.com/JiangHe12/opskit-core/safety"
)

type Operation string

const (
	OperationList           Operation = "list"
	OperationDescribe       Operation = "describe"
	OperationLag            Operation = "lag"
	OperationPeek           Operation = "peek"
	OperationTail           Operation = "tail"
	OperationListDLQ        Operation = "list-dlq"
	OperationPeekDLQ        Operation = "peek-dlq"
	OperationRedriveDLQ     Operation = "redrive-dlq"
	OperationClusterInfo    Operation = "cluster-info"
	OperationProduce        Operation = "produce"
	OperationCreateTopic    Operation = "create-topic"
	OperationAlterTopic     Operation = "alter-topic"
	OperationCreateGroup    Operation = "create-group"
	OperationDeleteGroup    Operation = "delete-group"
	OperationResetOffset    Operation = "reset-offset"
	OperationSeekOffset     Operation = "seek-offset"
	OperationPurgeTopic     Operation = "purge-topic"
	OperationPurgeDLQ       Operation = "purge-dlq"
	OperationDeleteTopic    Operation = "delete-topic"
	OperationListACL        Operation = "list-acl"
	OperationGrantACL       Operation = "grant-acl"
	OperationRevokeACL      Operation = "revoke-acl"
	OperationListSchema     Operation = "list-schema"
	OperationDescribeSchema Operation = "describe-schema"
	OperationCheckSchema    Operation = "check-schema"
)

type Target struct {
	Topic          string
	Group          string
	ProtectedTopic bool
	InternalTopic  bool
	ACL            ACLTarget
	Plan           bool
}

type ACLTarget struct {
	Principal    string
	ResourceType string
	ResourceName string
	PatternType  string
	Operation    string
	Permission   string
	Unknown      bool
}

type Result struct {
	Risk   safety.Risk `json:"risk"`
	Reason string      `json:"reason"`
}

func Classify(op Operation, target Target) Result {
	base := classifyBase(op)
	if target.Plan && isReadOnlyPlan(op) {
		base = Result{Risk: safety.R0, Reason: "read-only impact preview"}
	}
	result := base
	result = applyTargetEscalation(result, op, target)
	result = applyDestructivePins(result, op, target)
	return result
}

func applyTargetEscalation(result Result, op Operation, target Target) Result {
	if hasPattern(target.Topic) || hasPattern(target.Group) {
		result = escalate(result, "wildcard target expands blast radius")
	}
	if target.ProtectedTopic && !target.Plan {
		result = escalate(result, "protected topic")
	}
	if target.InternalTopic && op != OperationProduce && !target.Plan {
		result = escalate(result, "internal/system topic")
	}
	return result
}

func applyDestructivePins(result Result, op Operation, target Target) Result {
	if isOffsetChange(op) && !target.Plan {
		result = pinR3(result, "offset changes are destructive")
	}
	if isPurgeOrDelete(op) && !target.Plan {
		result = pinR3(result, "purge/delete operations are destructive")
	}
	if op == OperationProduce && (target.InternalTopic || isInternalTopic(target.Topic)) {
		result = pinR3(result, "produce to internal/system topic is destructive")
	}
	result = applyRedrivePins(result, op, target)
	if op == OperationRevokeACL {
		result = pinR3(result, "ACL revoke is destructive")
	}
	if op == OperationGrantACL && broadACLGrant(target.ACL) {
		result = pinR3(result, "broad ACL grant")
	}
	return result
}

func applyRedrivePins(result Result, op Operation, target Target) Result {
	if op != OperationRedriveDLQ || target.Plan {
		return result
	}
	if hasPattern(target.Topic) || isAllTarget(target.Topic) {
		result = pinR3(result, "redrive wildcard/all target expands blast radius")
	}
	return pinR3(result, "DLQ redrive uses internal produce")
}

func classifyBase(op Operation) Result {
	switch op {
	case OperationList, OperationDescribe, OperationLag, OperationPeek, OperationTail, OperationListDLQ, OperationPeekDLQ, OperationClusterInfo, OperationListACL, OperationListSchema, OperationDescribeSchema, OperationCheckSchema:
		return Result{Risk: safety.R0, Reason: "read-only broker operation"}
	case OperationProduce, OperationCreateTopic:
		return Result{Risk: safety.R1, Reason: "non-protected topic write"}
	case OperationAlterTopic, OperationCreateGroup, OperationDeleteGroup, OperationRedriveDLQ, OperationGrantACL:
		return Result{Risk: safety.R2, Reason: "elevated topic/group mutation"}
	case OperationResetOffset, OperationSeekOffset, OperationPurgeTopic, OperationPurgeDLQ, OperationDeleteTopic, OperationRevokeACL:
		return Result{Risk: safety.R3, Reason: "destructive broker operation"}
	default:
		return Result{Risk: safety.R3, Reason: "unknown broker operation"}
	}
}

func escalate(in Result, reason string) Result {
	if in.Risk < safety.R3 {
		in.Risk++
	}
	if in.Reason == "" {
		in.Reason = reason
		return in
	}
	in.Reason += "; " + reason
	return in
}

func pinR3(in Result, reason string) Result {
	if in.Risk < safety.R3 {
		in.Risk = safety.R3
	}
	if in.Reason == "" {
		in.Reason = reason
		return in
	}
	if !strings.Contains(in.Reason, reason) {
		in.Reason += "; " + reason
	}
	return in
}

func isOffsetChange(op Operation) bool {
	return op == OperationResetOffset || op == OperationSeekOffset
}

func isPurgeOrDelete(op Operation) bool {
	return op == OperationPurgeTopic || op == OperationPurgeDLQ || op == OperationDeleteTopic
}

func isReadOnlyPlan(op Operation) bool {
	return op == OperationResetOffset || op == OperationSeekOffset || op == OperationPurgeTopic || op == OperationPurgeDLQ || op == OperationRedriveDLQ
}

func hasPattern(value string) bool {
	return strings.ContainsAny(value, "*?[")
}

func isAllTarget(value string) bool {
	normalized := strings.ToLower(strings.TrimSpace(value))
	return normalized == "all" || normalized == "any"
}

func isInternalTopic(topic string) bool {
	name := strings.ToLower(strings.TrimSpace(topic))
	return strings.HasPrefix(name, "__") || strings.HasPrefix(name, "_system") || strings.Contains(name, "consumer_offsets")
}

func broadACLGrant(target ACLTarget) bool {
	if target.Unknown {
		return true
	}
	if broadACLPatternType(target.PatternType) {
		return true
	}
	principal := strings.ToLower(strings.TrimSpace(target.Principal))
	if principal == "" || principal == "*" || principal == "user:*" {
		return true
	}
	resourceType := strings.ToLower(strings.TrimSpace(target.ResourceType))
	if resourceType == "" || resourceType == "cluster" {
		return true
	}
	if !knownACLResourceType(resourceType) {
		return true
	}
	if broadACLResourceName(target.ResourceName, target.PatternType) {
		return true
	}
	if broadACLOperation(target.Operation) {
		return true
	}
	permission := strings.ToLower(strings.TrimSpace(target.Permission))
	return permission == "" || (permission != "allow" && permission != "deny")
}

func knownACLResourceType(resourceType string) bool {
	switch normalizeACLToken(resourceType) {
	case "any", "topic", "group", "cluster", "transactionalid", "delegationtoken", "user", "vhost", "namespace":
		return true
	default:
		return false
	}
}

func broadACLPatternType(patternType string) bool {
	patternType = normalizeACLToken(patternType)
	return patternType != "literal" && patternType != "regex"
}

func broadACLOperation(operation string) bool {
	normalized := normalizeACLToken(operation)
	switch normalized {
	case "", "all", "alter", "clusteraction", "alterconfigs", "idempotentwrite", "functions", "sources", "sinks", "packages":
		return true
	default:
		return !knownACLOperation(normalized)
	}
}

func broadACLResourceName(resourceName, patternType string) bool {
	resourceName = strings.TrimSpace(resourceName)
	if normalizeACLToken(patternType) == "regex" {
		return broadACLRegex(resourceName)
	}
	return resourceName == "" || resourceName == "*" || hasPattern(resourceName)
}

func broadACLRegex(pattern string) bool {
	pattern = strings.TrimSpace(pattern)
	switch pattern {
	case "", ".", ".*", ".+":
		return true
	}
	if strings.Contains(pattern, ".*") || strings.Contains(pattern, ".+") {
		return true
	}
	if strings.HasPrefix(pattern, "^") && strings.HasSuffix(pattern, "$") {
		return aclRegexHasMeta(pattern[1 : len(pattern)-1])
	}
	return aclRegexHasMeta(pattern)
}

func aclRegexHasMeta(pattern string) bool {
	return strings.ContainsAny(pattern, `\.+*?()|[]{}^$`)
}

func normalizeACLToken(value string) string {
	return strings.ToLower(strings.NewReplacer("_", "", "-", "", ".", "").Replace(strings.TrimSpace(value)))
}

func knownACLOperation(operation string) bool {
	switch operation {
	case "any", "all", "read", "write", "configure", "produce", "consume", "functions", "sources", "sinks", "packages", "create", "delete", "alter", "describe", "clusteraction", "describeconfigs", "alterconfigs", "idempotentwrite", "createtokens", "describetokens":
		return true
	default:
		return false
	}
}
