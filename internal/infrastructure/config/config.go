package config

type Config struct {
	BootstrapServers      string
	OrdersTopic           string
	ConsumerPullTimeoutMs int
	ConsumerPushTimeoutMs int
}

func NewConfig() *Config {
	return &Config{
		BootstrapServers:      "127.0.0.1:9094",
		OrdersTopic:           "orders",
		ConsumerPullTimeoutMs: 1000,
		ConsumerPushTimeoutMs: 0,
	}
}
