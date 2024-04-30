package net_conn_nats_proxy

import (
	"crypto/rand"
	"fmt"
)

// _UUIDFromCryptoRand generates a random UUID using cryptographic randomness.
// It returns a string representation of the generated UUID and any error that occurred during the generation process.
func _UUIDFromCryptoRand() (string, error) {
	b := make([]byte, 16)
	if _, err := rand.Read(b); err != nil {
		return "", err
	}
	return fmt.Sprintf("%X-%X-%X-%X-%X", b[0:4], b[4:6], b[6:8], b[8:10], b[10:]), nil
}
