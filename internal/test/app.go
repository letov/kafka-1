package test

import (
	"kafka-1/internal/infrastructure/di"
	"testing"

	"go.uber.org/fx"
	"go.uber.org/fx/fxtest"
)

func InjectApp() fx.Option {
	cs := di.GetConstructors()

	return fx.Provide(
		cs...,
	)
}

func initTest(t *testing.T, r interface{}) {
	t.Setenv("IS_TEST_ENV", "true")
	app := fxtest.New(t, InjectApp(), fx.Invoke(r))
	defer app.RequireStop()
	app.RequireStart()
}
