package pkg

import (
	"fmt"
	"log/slog"
)

func Warnf(msg string, args ...any) {
	formattedMsg := fmt.Sprintf(msg, args...)
	slog.Warn(formattedMsg)
}

func Errorf(msg string, args ...any) {
	formattedMsg := fmt.Sprintf(msg, args...)
	slog.Error(formattedMsg)
}

func Infof(msg string, args ...any) {
	formattedMsg := fmt.Sprintf(msg, args...)
	slog.Info(formattedMsg)
}
