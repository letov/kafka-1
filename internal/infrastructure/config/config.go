package config

import (
	"os"
	"strconv"

	"github.com/joho/godotenv"
)

type Config struct {
	BootstrapServers      string
	OrdersTopic           string
	ConsumerPullTimeoutMs int
	ConsumerPushTimeoutMs int
}

func NewConfig() *Config {
	var err error
	if os.Getenv("IS_TEST_ENV") == "true" {
		err = godotenv.Load("../../.env")
	} else {
		err = godotenv.Load(".env")
	}

	if err != nil {
		panic(err)
	}

	return &Config{
		BootstrapServers:      getEnv("BOOTSTRAP_SERVERS", ""),
		OrdersTopic:           getEnv("ORDERS_TOPIC", ""),
		ConsumerPullTimeoutMs: getEnvInt("CONSUMER_PULL_TIMEOUT_MS", 0),
		ConsumerPushTimeoutMs: getEnvInt("CONSUMER_PUSH_TIMEOUT_MS", 0),
	}
}

func getEnvInt(key string, def int) int {
	v, e := strconv.Atoi(getEnv(key, strconv.Itoa(def)))
	if e != nil {
		return def
	} else {
		return v
	}
}

func getEnv(key string, def string) string {
	if value, exists := os.LookupEnv(key); exists {
		return value
	}
	return def
}
