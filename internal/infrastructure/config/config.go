package config

import (
	"os"
	"strconv"

	"github.com/joho/godotenv"
)

type Config struct {
	KafkaBootstrapServers      string
	OrdersTopic                string
	KafkaCustomerGroup1        string
	KafkaCustomerGroup2        string
	KafkaSessionTimeoutMs      int
	KafkaAutoOffsetReset       string
	KafkaConsumerPullTimeoutMs int
	KafkaConsumerPushTimeoutMs int
	KafkaAcks                  string
	KafkaFlushTimeoutMs        int
	SchemaregistryUrl          string
	RedisOpt                   string
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
		KafkaBootstrapServers:      getEnv("KAFKA_BOOTSTRAP_SERVERS", ""),
		OrdersTopic:                getEnv("ORDERS_TOPIC", ""),
		KafkaCustomerGroup1:        getEnv("KAFKA_CUSTOMER_GROUP_1", ""),
		KafkaCustomerGroup2:        getEnv("KAFKA_CUSTOMER_GROUP_2", ""),
		KafkaSessionTimeoutMs:      getEnvInt("KAFKA_SESSION_TIMEOUT_MS", 0),
		KafkaAutoOffsetReset:       getEnv("KAFKA_AUTO_OFFSET_RESET", ""),
		KafkaConsumerPullTimeoutMs: getEnvInt("KAFKA_CONSUMER_PULL_TIMEOUT_MS", 0),
		KafkaConsumerPushTimeoutMs: getEnvInt("KAFKA_CONSUMER_PUSH_TIMEOUT_MS", 0),
		KafkaAcks:                  getEnv("KAFKA_ACKS", ""),
		KafkaFlushTimeoutMs:        getEnvInt("KAFKA_FLUSH_TIMEOUT_MS", 0),
		SchemaregistryUrl:          getEnv("SCHEMA_REGISTRY_URL", ""),
		RedisOpt:                   getEnv("REDIS_OPT", ""),
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
