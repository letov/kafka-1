package queue

import (
	"context"
	"kafka-1/internal/infrastructure/config"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"go.uber.org/fx"
	"go.uber.org/zap"
)

type OrderSender struct {
	p            *kafka.Producer
	c            *config.Config
	deliveryChan chan kafka.Event
	l            *zap.SugaredLogger
}

func (os OrderSender) SendMessage(payload []byte) error {
	err := os.p.Produce(&kafka.Message{
		TopicPartition: kafka.TopicPartition{Topic: &os.c.OrdersTopic, Partition: kafka.PartitionAny},
		Value:          payload,
		Headers:        []kafka.Header{{Key: "TEST_HEADER", Value: []byte("TEST_HEADER_VAL")}},
	}, os.deliveryChan)
	if err != nil {
		return err
	}

	e := <-os.deliveryChan
	m := e.(*kafka.Message)

	if m.TopicPartition.Error != nil {
		return m.TopicPartition.Error
	} else {
		os.l.Info("Message successfully sent")
	}

	return nil
}

func NewOrderSender(lc fx.Lifecycle, c *config.Config, l *zap.SugaredLogger) *OrderSender {
	p, err := kafka.NewProducer(&kafka.ConfigMap{
		"bootstrap.servers": c.KafkaBootstrapServers,
		"acks":              c.KafkaAcks,
	})
	if err != nil {
		l.Error("Can't create producer: ", err.Error())
	}
	l.Info("Producer successfully created")

	deliveryChan := make(chan kafka.Event)

	lc.Append(fx.Hook{
		OnStop: func(ctx context.Context) error {
			close(deliveryChan)
			p.Close()
			return nil
		},
	})

	return &OrderSender{p, c, deliveryChan, l}
}
