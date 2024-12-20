package queue

import (
	"context"
	"kafka-1/internal/infrastructure/config"
	"kafka-1/internal/infrastructure/storage"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"go.uber.org/fx"
	"go.uber.org/zap"
)

type OrderSender struct {
	p   *kafka.Producer
	c   *config.Config
	l   *zap.SugaredLogger
	s   *storage.Redis
	sch *Schema
}

type KeyMsg struct {
	Key string
	Msg interface{}
}

func (osr OrderSender) SendMessages(
	doneCh chan struct{},
	inCh chan KeyMsg,
) {
	ctx := context.TODO()

	go func() {
		for {
			select {
			case <-doneCh:
				return
			case km := <-inCh:
				val, err := osr.sch.Serialize(osr.c.OrdersTopic, km.Msg)
				if err != nil {
					osr.l.Fatal("Error Serialize ", err)
				}
				err = osr.p.Produce(&kafka.Message{
					TopicPartition: kafka.TopicPartition{Topic: &osr.c.OrdersTopic, Partition: kafka.PartitionAny},
					Key:            []byte(km.Key),
					Value:          val,
					Headers:        []kafka.Header{{Key: "TEST_HEADER", Value: []byte("TEST_HEADER_VAL")}},
				}, nil)
				if err != nil {
					osr.l.Fatal("Error delivered ", err)
				}
			}
		}
	}()

	go func() {
		for {
			select {
			case <-doneCh:
				return
			case e := <-osr.p.Events():
				switch ev := e.(type) {
				case *kafka.Message:
					m := ev
					if m.TopicPartition.Error != nil {
						_ = osr.s.SaveError(ctx, string(m.Key), m.Value)
						osr.l.Warn("Delivery failed ", m.TopicPartition.Error)
					} else {
						_ = osr.s.SaveSuccess(ctx, string(m.Key), m.Value)
						osr.l.Info("Delivered message ", *m.TopicPartition.Topic, m.TopicPartition.Partition, m.TopicPartition.Offset)
					}
				case kafka.Error:
					osr.l.Fatal("Error delivered ", ev)
				default:
					osr.l.Info("Ignored event ", ev)
				}
			}
		}
	}()
}

func NewOrderSender(
	lc fx.Lifecycle,
	c *config.Config,
	l *zap.SugaredLogger,
	s *storage.Redis,
	sch *Schema,
	bsp IBootstrapServersProvider,
) *OrderSender {
	p, err := kafka.NewProducer(&kafka.ConfigMap{
		"bootstrap.servers": bsp.Provide(),
		"acks":              c.KafkaAcks,
	})
	if err != nil {
		l.Fatal("Can't create producer: ", err.Error())
	}
	l.Info("Producer successfully created")

	lc.Append(fx.Hook{
		OnStop: func(ctx context.Context) error {
			if !p.IsClosed() {
				p.Flush(c.KafkaFlushTimeoutMs)
				p.Close()
			}
			return nil
		},
	})

	return &OrderSender{p, c, l, s, sch}
}
