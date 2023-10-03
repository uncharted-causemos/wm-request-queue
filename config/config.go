package config

import (
	"go.uber.org/zap"
)

// Config defines cross-cutting concerns.
type Config struct {
	Logger      *zap.SugaredLogger
	Environment *Environment
}
