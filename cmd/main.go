package main

import (
	"kafka-1/internal/application/app"
	"kafka-1/internal/infrastructure/di"

	"go.uber.org/fx"
)

func main() {
	fx.New(
		di.InjectApp(),
		fx.Invoke(app.Start),
	).Run()
}
