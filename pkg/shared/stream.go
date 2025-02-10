package shared

import "encoding/json"

type Stream struct {
	Task   string          `json:"task"`
	Status Status          `json:"status"`
	Result json.RawMessage `json:"result,omitempty"`
}
