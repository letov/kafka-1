package main

import (
	"kafka-1/internal/infrastructure/config"
	"kafka-1/internal/infrastructure/di"
	"kafka-1/internal/infrastructure/queue"
	"os"

	"go.uber.org/fx"
	"go.uber.org/zap"
)

func main() {
	fx.New(
		di.InjectApp(),
		fx.Invoke(func(
			qos *queue.OrderSender,
			orf *queue.OrderReceiverFactory,
			conf *config.Config,
			l *zap.SugaredLogger,
		) {
			l.Info("That's all")
			os.Exit(0)
		}),
	).Run()
}
