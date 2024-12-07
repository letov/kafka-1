package queue

import (
	"context"
	"fmt"
	"kafka-1/internal/infrastructure/config"
	"log"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"go.uber.org/fx"
)

type OrderSender struct {
	p            *kafka.Producer
	c            *config.Config
	deliveryChan chan kafka.Event
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
		fmt.Printf("Сообщение отправлено в топик %s [%d] офсет %v\n",
			*m.TopicPartition.Topic, m.TopicPartition.Partition, m.TopicPartition.Offset)
	}

	return nil
}

func NewOrderSender(lc fx.Lifecycle, c *config.Config) *OrderSender {
	// Настройте подтверждение доставки. Для продюсера используйте гарантию At Least Once («Как минимум один раз»)
	p, err := kafka.NewProducer(&kafka.ConfigMap{
		"bootstrap.servers": c.BootstrapServers,
		"acks":              "all",
	})
	if err != nil {
		log.Fatalf("Невозможно создать продюсера: %s\n", err)
	}
	log.Printf("Продюсер создан %v\n", p)

	deliveryChan := make(chan kafka.Event)

	lc.Append(fx.Hook{
		OnStop: func(ctx context.Context) error {
			close(deliveryChan)
			p.Close()
			return nil
		},
	})

	return &OrderSender{p, c, deliveryChan}
}
