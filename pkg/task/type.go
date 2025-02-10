package task

type Type string

const (
	TypeTask Type = "task"
)

func (s Type) String() string {
	return string(s)
}
