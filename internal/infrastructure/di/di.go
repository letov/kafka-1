package di

import (
	"kafka-1/internal/infrastructure/config"
	"kafka-1/internal/infrastructure/logger"
	"kafka-1/internal/infrastructure/queue"

	"go.uber.org/fx"
)

func GetConstructors() []interface{} {
	return []interface{}{
		config.NewConfig,
		logger.NewLogger,
		queue.NewOrderSender,
		queue.NewOrderReceiver,
	}
}

func InjectApp() fx.Option {
	return fx.Provide(
		GetConstructors()...,
	)
}
