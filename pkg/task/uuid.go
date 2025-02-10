package task

import (
	"crypto/rand"
	"fmt"
	"math/big"
	"time"
)

func GenUUIDv7() (string, error) {
	var buf [16]byte
	_, err := rand.Read(buf[:])
	if err != nil {
		return "", err
	}

	ts := big.NewInt(time.Now().UnixMilli())
	ts.FillBytes(buf[0:6])

	buf[6] = (buf[6] & 0x0F) | 0x70
	buf[8] = (buf[8] & 0x3F) | 0x80

	return fmt.Sprintf("%x", buf), nil
}
