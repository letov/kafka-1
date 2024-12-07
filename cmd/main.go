// main.go
package main

import (
	"kafka-1/internal/infrastructure/di"

	"go.uber.org/fx"
)

// ничего не делает, задание в файле теста
func main() {
	fx.New(
		di.InjectApp(),
		fx.Invoke(func() {}),
	).Run()
}
