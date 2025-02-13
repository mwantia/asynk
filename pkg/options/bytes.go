package options

import (
	"fmt"
	"strconv"
	"strings"
)

var sizeMap = map[string]int64{
	"b":  1,
	"kb": 1024,
	"mb": 1024 * 1024,
	"gb": 1024 * 1024 * 1024,
	"tb": 1024 * 1024 * 1024 * 1024,
}

func parseBytes(str string) (int64, error) {
	if strings.TrimSpace(str) == "" {
		return -1, nil
	}
	str = strings.ToLower(strings.TrimSpace(str))
	for unit, multiplier := range sizeMap {
		if strings.HasSuffix(str, unit) {
			numPart := strings.TrimSuffix(str, unit)
			num, err := strconv.ParseFloat(numPart, 64)
			if err != nil {
				return 0, err
			}

			return int64(num * float64(multiplier)), nil
		}
	}
	return 0, fmt.Errorf("invalid size format")
}
