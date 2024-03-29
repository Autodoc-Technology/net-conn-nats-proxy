package redis_nats_proxy

import (
	"bytes"
	"encoding/gob"
	"time"
)

// SerializeTimeToString serializes time.Time to a string using encoding/gob
func SerializeTimeToString(t time.Time) (string, error) {
	var b bytes.Buffer
	enc := gob.NewEncoder(&b)
	err := enc.Encode(t)
	return b.String(), err
}

// DeserializeTimeFromString deserializes a string back to time.Time using encoding/gob
func DeserializeTimeFromString(data string) (time.Time, error) {
	var t time.Time
	dec := gob.NewDecoder(bytes.NewReader([]byte(data)))
	err := dec.Decode(&t)
	return t, err
}
