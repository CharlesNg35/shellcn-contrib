package prometheus

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
)

func stableID(parts ...any) string {
	h := sha256.New()
	for _, part := range parts {
		data, _ := json.Marshal(part)
		_, _ = h.Write(data)
		_, _ = h.Write([]byte{0})
	}
	sum := h.Sum(nil)
	return hex.EncodeToString(sum[:12])
}
