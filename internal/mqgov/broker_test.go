package mqgov

import (
	"encoding/json"
	"testing"
)

func TestBatchOutcomeJSONIsFlatAndOptional(t *testing.T) {
	t.Parallel()
	withOutcome, err := json.Marshal(OffsetPlan{
		BatchOutcome: BatchOutcome{Succeeded: 2, Failed: 1, Uncertain: 1},
		Topic:        TopicCoordinate{Topic: "orders"},
	})
	if err != nil {
		t.Fatalf("json.Marshal(with outcome) error = %v", err)
	}
	var decoded map[string]any
	if err := json.Unmarshal(withOutcome, &decoded); err != nil {
		t.Fatalf("json.Unmarshal(with outcome) error = %v", err)
	}
	for key, want := range map[string]float64{"succeeded": 2, "failed": 1, "uncertain": 1} {
		if decoded[key] != want {
			t.Fatalf("%s = %#v, want %v; JSON=%s", key, decoded[key], want, withOutcome)
		}
	}

	withoutOutcome, err := json.Marshal(OffsetPlan{Topic: TopicCoordinate{Topic: "orders"}})
	if err != nil {
		t.Fatalf("json.Marshal(without outcome) error = %v", err)
	}
	decoded = nil
	if err := json.Unmarshal(withoutOutcome, &decoded); err != nil {
		t.Fatalf("json.Unmarshal(without outcome) error = %v", err)
	}
	for _, key := range []string{"succeeded", "failed", "uncertain"} {
		if _, exists := decoded[key]; exists {
			t.Fatalf("%s unexpectedly present in backward-compatible JSON: %s", key, withoutOutcome)
		}
	}
}
