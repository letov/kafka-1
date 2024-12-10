package queue

import (
	"context"
	"kafka-1/internal/infrastructure/config"
	"os"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"go.uber.org/fx"
	"go.uber.org/zap"
)

type OrderReceiver struct {
	cons       *kafka.Consumer
	conf       *config.Config
	l          *zap.SugaredLogger
	sch        *Schema
	autoCommit bool
}

type OrderReceiverFactory struct {
	cs   []*kafka.Consumer
	conf *config.Config
	l    *zap.SugaredLogger
	sch  *Schema
}

func (or OrderReceiver) ReceiveMessages(
	doneCh chan struct{},
	outCh chan interface{},
	pullTimeoutMs int,
) {
	go func() {
		for {
			select {
			case <-doneCh:
				close(outCh)
				return
			default:
				ev := or.cons.Poll(pullTimeoutMs)
				if ev == nil {
					continue
				}
				switch e := ev.(type) {
				case *kafka.Message:
					or.l.Info("Get message")
					var data interface{}
					err := or.sch.DeserializeInto(or.conf.OrdersTopic, e.Value, data)
					if err != nil {
						or.l.Warn(err.Error())
					} else {
						if !or.autoCommit {
							_, _ = or.cons.Commit()
						}
						outCh <- data
					}
				case kafka.Error:
					or.l.Warn("Error: ", e)
				default:
					or.l.Warn("Some event: ", e)
				}
			}
		}
	}()
}

func NewOrderReceiverFactory(
	lc fx.Lifecycle,
	conf *config.Config,
	l *zap.SugaredLogger,
	sch *Schema,
) *OrderReceiverFactory {
	cs := make([]*kafka.Consumer, 0)

	lc.Append(fx.Hook{
		OnStop: func(ctx context.Context) error {
			for _, cons := range cs {
				_ = cons.Close()
			}
			return nil
		},
	})

	return &OrderReceiverFactory{cs, conf, l, sch}
}

func (orf *OrderReceiverFactory) NewOrderReceiver(groupId string, autoCommit bool) *OrderReceiver {
	var autoCommitStr string
	if autoCommit {
		autoCommitStr = "true"
	} else {
		autoCommitStr = "false"
	}
	cons, err := kafka.NewConsumer(&kafka.ConfigMap{
		"bootstrap.servers":  orf.conf.KafkaBootstrapServers,
		"group.id":           groupId,
		"session.timeout.ms": orf.conf.KafkaSessionTimeoutMs,
		"auto.offset.reset":  orf.conf.KafkaAutoOffsetReset,
		"enable.auto.commit": autoCommitStr,
		"acks":               orf.conf.KafkaAcks,
	})
	if err != nil {
		orf.l.Error("Error creating consumer: ", err)
		os.Exit(1)
	}

	orf.l.Info("Consumer created")

	err = cons.SubscribeTopics([]string{orf.conf.OrdersTopic}, nil)
	if err != nil {
		orf.l.Error("Subscribe error:", err)
		os.Exit(1)
	}

	orf.cs = append(orf.cs, cons)
	return &OrderReceiver{cons, orf.conf, orf.l, orf.sch, autoCommit}
}
