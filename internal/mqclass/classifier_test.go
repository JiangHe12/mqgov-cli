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
		{name: "tail R0", op: OperationTail, want: safety.R0},
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
		{name: "list ACL R0", op: OperationListACL, want: safety.R0},
		{name: "grant ACL R2", op: OperationGrantACL, target: Target{ACL: ACLTarget{Principal: "User:svc", ResourceType: "topic", ResourceName: "orders", PatternType: "literal", Operation: "read", Permission: "allow"}}, want: safety.R2},
		{name: "revoke ACL R3", op: OperationRevokeACL, target: Target{ACL: ACLTarget{Principal: "User:svc", ResourceType: "topic", ResourceName: "orders", Operation: "read", Permission: "allow"}}, want: safety.R3},
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

func TestClassifyACLGovernance(t *testing.T) {
	t.Parallel()
	base := ACLTarget{Principal: "User:svc", ResourceType: "topic", ResourceName: "orders", PatternType: "literal", Operation: "read", Permission: "allow"}
	tests := []struct {
		name   string
		op     Operation
		target ACLTarget
		want   safety.Risk
	}{
		{name: "list is R0", op: OperationListACL, target: base, want: safety.R0},
		{name: "normal allow grant is R2", op: OperationGrantACL, target: base, want: safety.R2},
		{name: "normal deny grant is R2", op: OperationGrantACL, target: ACLTarget{Principal: "User:svc", ResourceType: "topic", ResourceName: "orders", PatternType: "literal", Operation: "read", Permission: "deny"}, want: safety.R2},
		{name: "prefixed pattern grant is R3", op: OperationGrantACL, target: ACLTarget{Principal: "User:svc", ResourceType: "topic", ResourceName: "prod", PatternType: "prefixed", Operation: "read", Permission: "allow"}, want: safety.R3},
		{name: "empty pattern type grant is R3", op: OperationGrantACL, target: ACLTarget{Principal: "User:svc", ResourceType: "topic", ResourceName: "orders", Operation: "read", Permission: "allow"}, want: safety.R3},
		{name: "unknown pattern type grant is R3", op: OperationGrantACL, target: ACLTarget{Principal: "User:svc", ResourceType: "topic", ResourceName: "orders", PatternType: "weird", Operation: "read", Permission: "allow"}, want: safety.R3},
		{name: "wildcard principal grant is R3", op: OperationGrantACL, target: ACLTarget{Principal: "*", ResourceType: "topic", ResourceName: "orders", Operation: "read", Permission: "allow"}, want: safety.R3},
		{name: "User wildcard principal grant is R3", op: OperationGrantACL, target: ACLTarget{Principal: "User:*", ResourceType: "topic", ResourceName: "orders", Operation: "read", Permission: "allow"}, want: safety.R3},
		{name: "wildcard resource grant is R3", op: OperationGrantACL, target: ACLTarget{Principal: "User:svc", ResourceType: "topic", ResourceName: "*", Operation: "read", Permission: "allow"}, want: safety.R3},
		{name: "glob resource grant is R3", op: OperationGrantACL, target: ACLTarget{Principal: "User:svc", ResourceType: "topic", ResourceName: "prod-*", Operation: "read", Permission: "allow"}, want: safety.R3},
		{name: "cluster resource grant is R3", op: OperationGrantACL, target: ACLTarget{Principal: "User:svc", ResourceType: "cluster", ResourceName: "kafka-cluster", Operation: "describe", Permission: "allow"}, want: safety.R3},
		{name: "all operation grant is R3", op: OperationGrantACL, target: ACLTarget{Principal: "User:svc", ResourceType: "topic", ResourceName: "orders", Operation: "all", Permission: "allow"}, want: safety.R3},
		{name: "alter operation grant is R3", op: OperationGrantACL, target: ACLTarget{Principal: "User:svc", ResourceType: "topic", ResourceName: "orders", Operation: "alter", Permission: "allow"}, want: safety.R3},
		{name: "cluster action operation grant is R3", op: OperationGrantACL, target: ACLTarget{Principal: "User:svc", ResourceType: "topic", ResourceName: "orders", Operation: "cluster-action", Permission: "allow"}, want: safety.R3},
		{name: "unknown resource type grant is R3", op: OperationGrantACL, target: ACLTarget{Principal: "User:svc", ResourceType: "wat", ResourceName: "orders", Operation: "read", Permission: "allow"}, want: safety.R3},
		{name: "unknown operation grant is R3", op: OperationGrantACL, target: ACLTarget{Principal: "User:svc", ResourceType: "topic", ResourceName: "orders", Operation: "bogus", Permission: "allow"}, want: safety.R3},
		{name: "unknown permission grant is R3", op: OperationGrantACL, target: ACLTarget{Principal: "User:svc", ResourceType: "topic", ResourceName: "orders", Operation: "read", Permission: "maybe"}, want: safety.R3},
		{name: "unknown shape grant is R3", op: OperationGrantACL, target: ACLTarget{Unknown: true}, want: safety.R3},
		{name: "rabbitmq anchored regex grant is R2", op: OperationGrantACL, target: ACLTarget{Principal: "svc", ResourceType: "vhost", ResourceName: "^orders$", PatternType: "regex", Operation: "read", Permission: "allow"}, want: safety.R2},
		{name: "rabbitmq plain regex grant is R2", op: OperationGrantACL, target: ACLTarget{Principal: "svc", ResourceType: "vhost", ResourceName: "orders", PatternType: "regex", Operation: "write", Permission: "allow"}, want: safety.R2},
		{name: "rabbitmq configure operation is known", op: OperationGrantACL, target: ACLTarget{Principal: "svc", ResourceType: "vhost", ResourceName: "^orders$", PatternType: "regex", Operation: "configure", Permission: "allow"}, want: safety.R2},
		{name: "rabbitmq wildcard regex grant is R3", op: OperationGrantACL, target: ACLTarget{Principal: "svc", ResourceType: "vhost", ResourceName: ".*", PatternType: "regex", Operation: "read", Permission: "allow"}, want: safety.R3},
		{name: "rabbitmq plus regex grant is R3", op: OperationGrantACL, target: ACLTarget{Principal: "svc", ResourceType: "vhost", ResourceName: ".+", PatternType: "regex", Operation: "read", Permission: "allow"}, want: safety.R3},
		{name: "rabbitmq dot regex grant is R3", op: OperationGrantACL, target: ACLTarget{Principal: "svc", ResourceType: "vhost", ResourceName: ".", PatternType: "regex", Operation: "read", Permission: "allow"}, want: safety.R3},
		{name: "rabbitmq unanchored wildcard regex grant is R3", op: OperationGrantACL, target: ACLTarget{Principal: "svc", ResourceType: "vhost", ResourceName: "orders.*", PatternType: "regex", Operation: "read", Permission: "allow"}, want: safety.R3},
		{name: "rabbitmq character class regex grant is R3", op: OperationGrantACL, target: ACLTarget{Principal: "svc", ResourceType: "vhost", ResourceName: "orders[0-9]", PatternType: "regex", Operation: "read", Permission: "allow"}, want: safety.R3},
		{name: "rabbitmq escaped regex grant is R3", op: OperationGrantACL, target: ACLTarget{Principal: "svc", ResourceType: "vhost", ResourceName: `orders\.prod`, PatternType: "regex", Operation: "read", Permission: "allow"}, want: safety.R3},
		{name: "pulsar namespace produce grant is R2", op: OperationGrantACL, target: ACLTarget{Principal: "role-a", ResourceType: "namespace", ResourceName: "public/default", PatternType: "literal", Operation: "produce", Permission: "allow"}, want: safety.R2},
		{name: "pulsar topic consume grant is R2", op: OperationGrantACL, target: ACLTarget{Principal: "role-a", ResourceType: "topic", ResourceName: "orders", PatternType: "literal", Operation: "consume", Permission: "allow"}, want: safety.R2},
		{name: "pulsar functions grant is R3", op: OperationGrantACL, target: ACLTarget{Principal: "role-a", ResourceType: "namespace", ResourceName: "public/default", PatternType: "literal", Operation: "functions", Permission: "allow"}, want: safety.R3},
		{name: "pulsar sources grant is R3", op: OperationGrantACL, target: ACLTarget{Principal: "role-a", ResourceType: "namespace", ResourceName: "public/default", PatternType: "literal", Operation: "sources", Permission: "allow"}, want: safety.R3},
		{name: "pulsar sinks grant is R3", op: OperationGrantACL, target: ACLTarget{Principal: "role-a", ResourceType: "namespace", ResourceName: "public/default", PatternType: "literal", Operation: "sinks", Permission: "allow"}, want: safety.R3},
		{name: "pulsar packages grant is R3", op: OperationGrantACL, target: ACLTarget{Principal: "role-a", ResourceType: "namespace", ResourceName: "public/default", PatternType: "literal", Operation: "packages", Permission: "allow"}, want: safety.R3},
		{name: "pulsar wildcard role grant is R3", op: OperationGrantACL, target: ACLTarget{Principal: "*", ResourceType: "namespace", ResourceName: "public/default", PatternType: "literal", Operation: "produce", Permission: "allow"}, want: safety.R3},
		{name: "pulsar wildcard resource grant is R3", op: OperationGrantACL, target: ACLTarget{Principal: "role-a", ResourceType: "namespace", ResourceName: "*", PatternType: "literal", Operation: "produce", Permission: "allow"}, want: safety.R3},
		{name: "revoke allow is R3", op: OperationRevokeACL, target: base, want: safety.R3},
		{name: "revoke deny is R3", op: OperationRevokeACL, target: ACLTarget{Principal: "User:svc", ResourceType: "topic", ResourceName: "orders", Operation: "read", Permission: "deny"}, want: safety.R3},
		{name: "unknown ACL op is R3", op: Operation("explode-acl"), target: base, want: safety.R3},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			if got := Classify(tt.op, Target{ACL: tt.target}); got.Risk != tt.want {
				t.Fatalf("Classify(%s) risk = %v, want %v (%s)", tt.op, got.Risk, tt.want, got.Reason)
			}
		})
	}
}

func TestClassifyTailEscalation(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name   string
		target Target
		want   safety.Risk
	}{
		{name: "base R0", target: Target{Topic: "orders"}, want: safety.R0},
		{name: "protected R1", target: Target{Topic: "orders", ProtectedTopic: true}, want: safety.R1},
		{name: "internal R1", target: Target{Topic: "__consumer_offsets", InternalTopic: true}, want: safety.R1},
		{name: "wildcard R1", target: Target{Topic: "orders-*"}, want: safety.R1},
		{name: "protected internal wildcard escalates to R3 by target escalation", target: Target{Topic: "__consumer_*", ProtectedTopic: true, InternalTopic: true}, want: safety.R3},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			if got := Classify(OperationTail, tt.target); got.Risk != tt.want {
				t.Fatalf("Classify(tail) risk = %v, want %v (%s)", got.Risk, tt.want, got.Reason)
			}
		})
	}
}

func TestClassifyTailMatchesPeek(t *testing.T) {
	t.Parallel()
	targets := []Target{
		{Topic: "orders"},
		{Topic: "orders", ProtectedTopic: true},
		{Topic: "__consumer_offsets", InternalTopic: true},
		{Topic: "orders-*"},
		{Topic: "__consumer_*", ProtectedTopic: true, InternalTopic: true},
	}
	for _, target := range targets {
		t.Run(target.Topic, func(t *testing.T) {
			t.Parallel()
			tail := Classify(OperationTail, target)
			peek := Classify(OperationPeek, target)
			if tail.Risk != peek.Risk {
				t.Fatalf("tail risk = %v, peek risk = %v for target %+v", tail.Risk, peek.Risk, target)
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
