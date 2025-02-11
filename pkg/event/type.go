package event

type EventType string

const (
	TypeTask EventType = "task"
)

func (s EventType) String() string {
	return string(s)
}
