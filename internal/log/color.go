package log

import "github.com/mwantia/asynk/pkg/log"

func Color(l log.LogLevel) string {
	switch l {
	case log.Debug:
		return "\033[34m"
	case log.Info:
		return "\033[32m"
	case log.Warn:
		return "\033[33m"
	case log.Error:
		return "\033[31m"
	case log.Fatal:
		return "\033[35m"
	default:
		return "\033[0m"
	}
}
