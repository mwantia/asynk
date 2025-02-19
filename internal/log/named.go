package log

import "github.com/mwantia/asynk/pkg/log"

type NamedLogger struct {
	base log.LogBase
	name string
}

func NewNamed(base log.LogBase, name string) log.LogWrapper {
	return &NamedLogger{
		base: base,
		name: name,
	}
}

func (l *NamedLogger) Base() log.LogBase {
	return l.base
}

func (l *NamedLogger) Debug(msg string, args ...interface{}) {
	l.base.Log(log.Debug, msg, l.name, args...)
}

func (l *NamedLogger) Info(msg string, args ...interface{}) {
	l.base.Log(log.Info, msg, l.name, args...)
}

func (l *NamedLogger) Warn(msg string, args ...interface{}) {
	l.base.Log(log.Warn, msg, l.name, args...)
}

func (l *NamedLogger) Error(msg string, args ...interface{}) {
	l.base.Log(log.Error, msg, l.name, args...)
}

func (l *NamedLogger) Fatal(msg string, args ...interface{}) {
	l.base.Log(log.Fatal, msg, l.name, args...)
}

func (l *NamedLogger) Named(name string) log.LogWrapper {
	return &NamedLogger{
		base: l.base,
		name: name,
	}
}
