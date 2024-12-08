// main.go
package main

import (
	"kafka-1/internal/infrastructure/di"

	"go.uber.org/fx"
)

func main() {
	fx.New(
		di.InjectApp(),
		fx.Invoke(func() {}),
	).Run()
}
