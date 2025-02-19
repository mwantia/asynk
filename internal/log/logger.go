package log

import (
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/mwantia/asynk/pkg/log"
)

type BasicLogger struct {
	name  string
	level log.LogLevel
}

func NewLogger(lvl string) log.Logger {
	return &BasicLogger{
		name:  "asynk",
		level: parseLevel(lvl),
	}
}

func parseLevel(lvl string) log.LogLevel {
	switch strings.ToUpper(lvl) {
	case "DEBUG":
		return log.Debug
	case "INFO":
		return log.Info
	case "WARN":
		return log.Warn
	case "ERROR":
		return log.Error
	case "FATAL":
		return log.Fatal
	default:
		return log.Info
	}
}

func (l *BasicLogger) logInternal(lvl log.LogLevel, msg string, args ...interface{}) {
	if l.level < lvl {
		return
	}

	timestamp := time.Now().Format("2006-01-02 15:04:05")
	prefix := fmt.Sprintf("[%s] %-5s", timestamp, lvl)

	if l.name != "" {
		prefix = fmt.Sprintf("%s [%s]", prefix, l.name)
	}

	formattedMsg := fmt.Sprintf(msg, args...)
	fmt.Fprintf(os.Stdout, "%s %s\n", prefix, formattedMsg)

	if lvl == log.Fatal {
		os.Exit(1)
	}
}

func (l *BasicLogger) Debug(msg string, args ...interface{}) {
	l.logInternal(log.Debug, msg, args...)
}

func (l *BasicLogger) Info(msg string, args ...interface{}) {
	l.logInternal(log.Info, msg, args...)
}

func (l *BasicLogger) Warn(msg string, args ...interface{}) {
	l.logInternal(log.Warn, msg, args...)
}

func (l *BasicLogger) Error(msg string, args ...interface{}) {
	l.logInternal(log.Error, msg, args...)
}

func (l *BasicLogger) Fatal(msg string, args ...interface{}) {
	l.logInternal(log.Fatal, msg, args...)
}

func (l *BasicLogger) Named(name string) log.Logger {
	return &BasicLogger{
		name:  name,
		level: l.level,
	}
}
