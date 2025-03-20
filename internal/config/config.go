package config

import (
	"log/slog"
	"os"
	"strings"
)

var GlobalConfig AppConfig

type AppConfig struct {
	NoColor  bool
	LogLevel slog.Level
}

func LoadConfig() {
	// Set NoColor from environment variable
	GlobalConfig.NoColor = strings.ToUpper(os.Getenv("F2F_NO_COLOR")) == "TRUE"

	// Set log level from environment variable
	switch strings.ToUpper(os.Getenv("F2F_LOG_LEVEL")) {
	case "DEBUG":
		GlobalConfig.LogLevel = slog.LevelDebug
	case "WARN":
		GlobalConfig.LogLevel = slog.LevelWarn
	case "ERROR":
		GlobalConfig.LogLevel = slog.LevelError
	default: // Default to Info
		GlobalConfig.LogLevel = slog.LevelInfo
	}
}

func NoColor() bool {
	return GlobalConfig.NoColor
}

func LogLevel() slog.Level {
	return GlobalConfig.LogLevel
}
