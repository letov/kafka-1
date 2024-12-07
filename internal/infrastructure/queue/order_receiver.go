package queue

import (
	"context"
	"fmt"
	"kafka-1/internal/infrastructure/config"
	"log"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"go.uber.org/fx"
)

type OrderReceiver struct {
	pullCons *kafka.Consumer
	pushCons *kafka.Consumer
	conf     *config.Config
}

// "enable.auto.commit": "false",
func (or OrderReceiver) ReceivePullMessage(
	doneCh chan struct{},
	outCh chan interface{},
	deserialize func(data []byte) (interface{}, error),
) {
	for {
		select {
		case <-doneCh:
			return
		default:
			ev := or.pullCons.Poll(or.conf.ConsumerPullTimeoutMs)
			if ev == nil {
				continue
			}
			switch e := ev.(type) {
			case *kafka.Message:
				fmt.Printf("Получено pull сообщение из топика %s\n", e.TopicPartition)
				data, err := deserialize(e.Value)
				if err != nil {
					fmt.Printf(err.Error())
				} else {
					_, _ = or.pullCons.Commit()
					outCh <- data
				}
			case kafka.Error:
				fmt.Printf("Error: %v\n", e)
			default:
				fmt.Printf("Другие события %v\n", e)
			}
		}
	}
}

func (or OrderReceiver) ReceivePushMessage(doneCh chan struct{}, outCh chan []byte) {
	for {
		select {
		case <-doneCh:
			return
		default:
			ev := or.pushCons.Poll(or.conf.ConsumerPushTimeoutMs)
			if ev == nil {
				continue
			}
			switch e := ev.(type) {
			case *kafka.Message:
				fmt.Printf("Получено push сообщение из топика %s\n", e.TopicPartition)
				outCh <- e.Value
			case kafka.Error:
				fmt.Printf("Error: %v\n", e)
			default:
				fmt.Printf("Другие события %v\n", e)
			}
		}
	}
}

func NewOrderReceiver(lc fx.Lifecycle, conf *config.Config) *OrderReceiver {
	//  консьюмере с pull-моделью настройте вручную управляемый коммит.
	pullCons := newConsumer(conf, &kafka.ConfigMap{
		"bootstrap.servers":  conf.BootstrapServers,
		"group.id":           "consumer_group_1",
		"session.timeout.ms": 6000,
		"auto.offset.reset":  "earliest",
		"enable.auto.commit": "false",
	})

	// В консьюмере с push-моделью настройте автокоммит смещений.
	pushCons := newConsumer(conf, &kafka.ConfigMap{
		"bootstrap.servers":  conf.BootstrapServers,
		"group.id":           "consumer_group_1",
		"session.timeout.ms": 6000,
		"auto.offset.reset":  "earliest",
		"enable.auto.commit": "true",
	})

	lc.Append(fx.Hook{
		OnStop: func(ctx context.Context) error {
			_ = pullCons.Close()
			_ = pushCons.Close()
			return nil
		},
	})

	return &OrderReceiver{pullCons, pushCons, conf}
}

func newConsumer(conf *config.Config, consConf *kafka.ConfigMap) *kafka.Consumer {
	cons, err := kafka.NewConsumer(consConf)

	if err != nil {
		log.Fatalf("Невозможно создать консьюмера: %s\n", err)
	}

	fmt.Printf("Консьюмер создан %v\n", cons)

	err = cons.SubscribeTopics([]string{conf.OrdersTopic}, nil)

	if err != nil {
		log.Fatalf("Невозможно подписаться на топик: %s\n", err)
	}

	return cons
}
