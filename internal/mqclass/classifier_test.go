package mqclass

import (
	"testing"

	"github.com/JiangHe12/opskit-core/safety"
)

func TestClassifyGovernanceInvariants(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name   string
		op     Operation
		target Target
		want   safety.Risk
	}{
		{name: "list R0", op: OperationList, want: safety.R0},
		{name: "describe R0", op: OperationDescribe, want: safety.R0},
		{name: "lag R0", op: OperationLag, want: safety.R0},
		{name: "peek R0", op: OperationPeek, want: safety.R0},
		{name: "cluster info R0", op: OperationClusterInfo, want: safety.R0},
		{name: "produce R1", op: OperationProduce, target: Target{Topic: "orders"}, want: safety.R1},
		{name: "create topic R1", op: OperationCreateTopic, target: Target{Topic: "orders"}, want: safety.R1},
		{name: "alter topic R2", op: OperationAlterTopic, target: Target{Topic: "orders"}, want: safety.R2},
		{name: "produce protected R2", op: OperationProduce, target: Target{Topic: "orders", ProtectedTopic: true}, want: safety.R2},
		{name: "create protected R2", op: OperationCreateTopic, target: Target{Topic: "orders", ProtectedTopic: true}, want: safety.R2},
		{name: "group create R2", op: OperationCreateGroup, target: Target{Group: "billing"}, want: safety.R2},
		{name: "offset reset R3", op: OperationResetOffset, target: Target{Topic: "orders"}, want: safety.R3},
		{name: "offset seek R3", op: OperationSeekOffset, target: Target{Topic: "orders"}, want: safety.R3},
		{name: "offset reset plan R0", op: OperationResetOffset, target: Target{Topic: "orders", Plan: true}, want: safety.R0},
		{name: "purge R3", op: OperationPurgeTopic, target: Target{Topic: "orders"}, want: safety.R3},
		{name: "purge plan R0", op: OperationPurgeTopic, target: Target{Topic: "orders", Plan: true}, want: safety.R0},
		{name: "delete topic R3", op: OperationDeleteTopic, target: Target{Topic: "orders"}, want: safety.R3},
		{name: "destructive ACL R3", op: OperationDeleteACL, target: Target{DestructiveACL: true}, want: safety.R3},
		{name: "unknown R3", op: Operation("teleport"), want: safety.R3},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			if got := Classify(tt.op, tt.target); got.Risk != tt.want {
				t.Fatalf("Classify() risk = %v, want %v (%s)", got.Risk, tt.want, got.Reason)
			}
		})
	}
}

func TestClassifyAdversarialEscalation(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name   string
		op     Operation
		target Target
		want   safety.Risk
	}{
		{name: "topic glob upgrades read", op: OperationDescribe, target: Target{Topic: "prod-*"}, want: safety.R1},
		{name: "group glob upgrades R2 to R3", op: OperationDeleteGroup, target: Target{Group: "prod-*"}, want: safety.R3},
		{name: "produce internal topic pins R3", op: OperationProduce, target: Target{Topic: "__consumer_offsets"}, want: safety.R3},
		{name: "protected internal produce becomes R3", op: OperationProduce, target: Target{Topic: "__consumer_offsets", ProtectedTopic: true}, want: safety.R3},
		{name: "offset pattern remains pinned R3", op: OperationResetOffset, target: Target{Topic: "prod-*"}, want: safety.R3},
		{name: "offset pattern plan still upgrades wildcard", op: OperationResetOffset, target: Target{Topic: "prod-*", Plan: true}, want: safety.R1},
		{name: "delete protected remains R3", op: OperationDeleteTopic, target: Target{Topic: "orders", ProtectedTopic: true}, want: safety.R3},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			if got := Classify(tt.op, tt.target); got.Risk != tt.want {
				t.Fatalf("Classify() risk = %v, want %v (%s)", got.Risk, tt.want, got.Reason)
			}
		})
	}
}
