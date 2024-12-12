package test

import (
	"context"
	"kafka-1/internal/infrastructure/di"
	"kafka-1/internal/infrastructure/queue"
	"testing"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"go.uber.org/fx"
	"go.uber.org/fx/fxtest"
	"go.uber.org/zap"
)

type BootstrapServersProvider struct {
	mc *kafka.MockCluster
}

func (b BootstrapServersProvider) Provide() string {
	return b.mc.BootstrapServers()
}

func NewTestBootstrapServersProvider(lc fx.Lifecycle, l *zap.SugaredLogger) *BootstrapServersProvider {
	mockCluster, err := kafka.NewMockCluster(3)

	if err != nil {
		l.Fatal("Failed to create MockCluster")
	}

	lc.Append(fx.Hook{
		OnStop: func(ctx context.Context) error {
			mockCluster.Close()
			return nil
		},
	})

	return &BootstrapServersProvider{mc: mockCluster}
}

func GetAppTestConstructors() []interface{} {
	return []interface{}{
		NewTestBootstrapServersProvider,
		fx.Annotate(func(q *BootstrapServersProvider) *BootstrapServersProvider {
			return q
		}, fx.As(new(queue.IBootstrapServersProvider))),
	}
}

func InjectApp() fx.Option {
	return fx.Provide(
		append(
			di.GetBaseConstructors(),
			GetAppTestConstructors()...,
		)...,
	)
}

func initTest(t *testing.T, r interface{}) {
	t.Setenv("IS_TEST_ENV", "true")
	app := fxtest.New(t, InjectApp(), fx.Invoke(r))
	defer app.RequireStop()
	app.RequireStart()
}
