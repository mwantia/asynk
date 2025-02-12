package event

type EventType string

const (
	TypeSubmit EventType = "submit"
)

func (s EventType) String() string {
	return string(s)
}
