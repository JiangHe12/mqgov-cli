package mqgov

import (
	"crypto/sha256"
	"encoding/hex"
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

func Fingerprints(key, body []byte, count int64) ResourceFingerprints {
	return ResourceFingerprints{KeySHA256: sha256Hex(key), BodySHA256: sha256Hex(body), Count: count, Size: len(body)}
}

func sha256Hex(data []byte) string {
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:])
}
