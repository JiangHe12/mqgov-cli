package mqclass

import (
	"strings"

	"github.com/JiangHe12/opskit-core/safety"
)

type Operation string

const (
	OperationList        Operation = "list"
	OperationDescribe    Operation = "describe"
	OperationLag         Operation = "lag"
	OperationPeek        Operation = "peek"
	OperationClusterInfo Operation = "cluster-info"
	OperationProduce     Operation = "produce"
	OperationCreateTopic Operation = "create-topic"
	OperationAlterTopic  Operation = "alter-topic"
	OperationCreateGroup Operation = "create-group"
	OperationDeleteGroup Operation = "delete-group"
	OperationResetOffset Operation = "reset-offset"
	OperationSeekOffset  Operation = "seek-offset"
	OperationPurgeTopic  Operation = "purge-topic"
	OperationPurgeDLQ    Operation = "purge-dlq"
	OperationDeleteTopic Operation = "delete-topic"
	OperationDeleteACL   Operation = "delete-acl"
)

type Target struct {
	Topic          string
	Group          string
	ProtectedTopic bool
	InternalTopic  bool
	DestructiveACL bool
	Plan           bool
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
	if op == OperationDeleteACL && target.DestructiveACL {
		result = pinR3(result, "destructive ACL change")
	}
	return result
}

func classifyBase(op Operation) Result {
	switch op {
	case OperationList, OperationDescribe, OperationLag, OperationPeek, OperationClusterInfo:
		return Result{Risk: safety.R0, Reason: "read-only broker operation"}
	case OperationProduce, OperationCreateTopic:
		return Result{Risk: safety.R1, Reason: "non-protected topic write"}
	case OperationAlterTopic, OperationCreateGroup, OperationDeleteGroup:
		return Result{Risk: safety.R2, Reason: "elevated topic/group mutation"}
	case OperationResetOffset, OperationSeekOffset, OperationPurgeTopic, OperationPurgeDLQ, OperationDeleteTopic, OperationDeleteACL:
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
	return op == OperationResetOffset || op == OperationSeekOffset || op == OperationPurgeTopic || op == OperationPurgeDLQ
}

func hasPattern(value string) bool {
	return strings.ContainsAny(value, "*?[")
}

func isInternalTopic(topic string) bool {
	name := strings.ToLower(strings.TrimSpace(topic))
	return strings.HasPrefix(name, "__") || strings.HasPrefix(name, "_system") || strings.Contains(name, "consumer_offsets")
}
