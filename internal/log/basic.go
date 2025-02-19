package log

import (
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/mwantia/asynk/pkg/log"
)

type BasicLogger struct {
	Level log.LogLevel
}

func NewBasic(lvl string) log.LogWrapper {
	return NewNamed(&BasicLogger{
		Level: parseLevel(lvl),
	}, "asynk")
}

func (l *BasicLogger) Log(level log.LogLevel, msg string, name string, args ...interface{}) {
	if l.Level < level {
		return
	}

	timestamp := time.Now().Format("2006-01-02 15:04:05")
	prefix := fmt.Sprintf("[%s] %-5s", timestamp, level)

	if name != "" {
		prefix = fmt.Sprintf("%s [%s]", prefix, name)
	}

	formattedMsg := fmt.Sprintf(msg, args...)
	fmt.Fprintf(os.Stdout, "%s%s %s%s\n", Color(level), prefix, formattedMsg, ColorReset)

	if level == log.Fatal {
		os.Exit(1)
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
