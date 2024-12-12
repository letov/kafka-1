package di

import (
	"kafka-1/internal/application/app"
	"kafka-1/internal/infrastructure/config"
	"kafka-1/internal/infrastructure/logger"
	"kafka-1/internal/infrastructure/queue"
	"kafka-1/internal/infrastructure/storage"

	"go.uber.org/fx"
)

func GetBaseConstructors() []interface{} {
	return []interface{}{
		logger.NewLogger,
		queue.NewOrderSender,
		queue.NewOrderReceiverFactory,
		storage.NewStorage,
		queue.NewSchema,
		config.NewConfig,
	}
}

func GetAppConstructors() []interface{} {
	return []interface{}{
		app.Start,
		queue.NewBootstrapServers,
		fx.Annotate(func(q *queue.BootstrapServersProvider) *queue.BootstrapServersProvider {
			return q
		}, fx.As(new(queue.IBootstrapServersProvider))),
	}
}

func InjectApp() fx.Option {
	return fx.Provide(
		append(
			GetBaseConstructors(),
			GetAppConstructors()...,
		)...,
	)
}
