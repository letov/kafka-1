package queue

import "kafka-1/internal/infrastructure/config"

type IBootstrapServersProvider interface {
	Provide() string
}

type BootstrapServersProvider struct {
	conf *config.Config
}

func (b BootstrapServersProvider) Provide() string {
	return b.conf.KafkaBootstrapServers
}

func NewBootstrapServers(conf *config.Config) *BootstrapServersProvider {
	return &BootstrapServersProvider{conf: conf}
}
