package log

import "github.com/mwantia/asynk/pkg/log"

const (
	ColorReset  = "\033[0m"
	ColorRed    = "\033[31m"
	ColorGreen  = "\033[32m"
	ColorYellow = "\033[33m"
	ColorBlue   = "\033[34m"
	ColorPurple = "\033[35m"
)

func Color(l log.LogLevel) string {
	switch l {
	case log.Debug:
		return ColorBlue
	case log.Info:
		return ColorGreen
	case log.Warn:
		return ColorYellow
	case log.Error:
		return ColorRed
	case log.Fatal:
		return ColorPurple
	default:
		return ColorReset
	}
}
