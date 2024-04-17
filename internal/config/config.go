package config

import (
	"os"

	log "github.com/sirupsen/logrus"
)

var GlobalConfig AppConfig

type AppConfig struct {
	LogLevel log.Level
}

func LoadConfig() {
	// Load configurations into GlobalConfig

	if os.Getenv("FLOWS2FIM_LOG_LEVEL") != "" {
		lvl, err := log.ParseLevel(os.Getenv("FLOWS2FIM_LOG_LEVEL"))
		if err != nil {
			log.Warnf("Invalid LOG_LEVEL set: %s, defaulting to INFO", os.Getenv("FLOWS2FIM_LOG_LEVEL"))
			lvl = log.InfoLevel
		}
		GlobalConfig.LogLevel = lvl
	}
}
