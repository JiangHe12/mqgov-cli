package mqgov

import (
	"crypto/sha256"
	"encoding/hex"
	"time"
)

func FingerprintMessage(partition int, offset int64, key, body []byte) MessageFingerprint {
	return MessageFingerprint{
		Partition:  partition,
		Offset:     offset,
		KeySHA256:  sha256Hex(key),
		BodySHA256: sha256Hex(body),
		Size:       len(body),
	}
}

func FingerprintMessageAt(partition int, offset int64, key, body []byte, timestamp time.Time) MessageFingerprint {
	fp := FingerprintMessage(partition, offset, key, body)
	if !timestamp.IsZero() {
		fp.Timestamp = timestamp.UTC().Format(time.RFC3339Nano)
	}
	return fp
}

func Fingerprints(key, body []byte, count int64) ResourceFingerprints {
	return ResourceFingerprints{KeySHA256: sha256Hex(key), BodySHA256: sha256Hex(body), Count: count, Size: len(body)}
}

func SHA256Hex(data []byte) string {
	return sha256Hex(data)
}

func sha256Hex(data []byte) string {
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:])
}
