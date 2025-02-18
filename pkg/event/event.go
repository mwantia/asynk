package event

import (
	"crypto/rand"
	"encoding/json"
	"fmt"
	"math/big"
	"time"
)

type Event interface {
	GetID() string

	Marshal() (json.RawMessage, error)

	Unmarshal(json.RawMessage) error
}

func UUIDv7() string {
	var buf [16]byte
	_, err := rand.Read(buf[:])
	if err != nil {
		return ""
	}

	ts := big.NewInt(time.Now().UnixMilli())
	ts.FillBytes(buf[0:6])

	buf[6] = (buf[6] & 0x0F) | 0x70
	buf[8] = (buf[8] & 0x3F) | 0x80

	return fmt.Sprintf("%x", buf)
}
