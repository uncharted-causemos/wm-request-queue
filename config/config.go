package config

import (
	"gitlab.uncharted.software/WM/wm-request-queue/api/queue"
	"go.uber.org/zap"
)

// Config defines cross-cutting concerns.
type Config struct {
	Logger       *zap.SugaredLogger
	Environment  *Environment
	RequestQueue queue.RequestQueue
}
