package queue

import (
	"context"
	"kafka-1/internal/infrastructure/config"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"go.uber.org/fx"
	"go.uber.org/zap"
)

type OrderReceiver struct {
	pullCons *kafka.Consumer
	pushCons *kafka.Consumer
	conf     *config.Config
	l        *zap.SugaredLogger
}

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
			ev := or.pullCons.Poll(or.conf.KafkaConsumerPullTimeoutMs)
			if ev == nil {
				continue
			}
			switch e := ev.(type) {
			case *kafka.Message:
				or.l.Info("Get pull message")
				data, err := deserialize(e.Value)
				if err != nil {
					or.l.Warn(err.Error())
				} else {
					_, _ = or.pullCons.Commit()
					outCh <- data
				}
			case kafka.Error:
				or.l.Warn("Error: ", e)
			default:
				or.l.Warn("Some event: ", e)
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
			ev := or.pushCons.Poll(or.conf.KafkaConsumerPushTimeoutMs)
			if ev == nil {
				continue
			}
			switch e := ev.(type) {
			case *kafka.Message:
				or.l.Info("Get push message")
				outCh <- e.Value
			case kafka.Error:
				or.l.Warn("Error: ", e)
			default:
				or.l.Warn("Some event: ", e)
			}
		}
	}
}

func NewOrderReceiver(lc fx.Lifecycle, conf *config.Config, l *zap.SugaredLogger) *OrderReceiver {
	pullCons := newConsumer(conf, &kafka.ConfigMap{
		"bootstrap.servers":  conf.KafkaBootstrapServers,
		"group.id":           conf.KafkaCustomerGroup1,
		"session.timeout.ms": conf.KafkaSessionTimeoutMs,
		"auto.offset.reset":  conf.KafkaAutoOffsetReset,
		"enable.auto.commit": "false",
	}, l)

	pushCons := newConsumer(conf, &kafka.ConfigMap{
		"bootstrap.servers":  conf.KafkaBootstrapServers,
		"group.id":           conf.KafkaCustomerGroup2,
		"session.timeout.ms": conf.KafkaSessionTimeoutMs,
		"auto.offset.reset":  conf.KafkaAutoOffsetReset,
		"enable.auto.commit": "true",
	}, l)

	lc.Append(fx.Hook{
		OnStop: func(ctx context.Context) error {
			_ = pullCons.Close()
			_ = pushCons.Close()
			return nil
		},
	})

	return &OrderReceiver{pullCons, pushCons, conf, l}
}

func newConsumer(conf *config.Config, consConf *kafka.ConfigMap, l *zap.SugaredLogger) *kafka.Consumer {
	cons, err := kafka.NewConsumer(consConf)

	if err != nil {
		l.Error("Error creating consumer: ", err)
	}

	l.Info("Consumer created")

	err = cons.SubscribeTopics([]string{conf.OrdersTopic}, nil)

	if err != nil {
		l.Error("Subscribe error:", err)
	}

	return cons
}
