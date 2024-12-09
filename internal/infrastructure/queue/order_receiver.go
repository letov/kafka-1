package queue

import (
	"context"
	"kafka-1/internal/infrastructure/config"
	"time"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"go.uber.org/fx"
	"go.uber.org/zap"
)

type OrderReceiver struct {
	cons       *kafka.Consumer
	l          *zap.SugaredLogger
	autoCommit bool
}

type OrderReceiverFactory struct {
	recs []*OrderReceiver
	conf *config.Config
	l    *zap.SugaredLogger
}

func (or OrderReceiver) ReceiveMessage(
	doneCh chan struct{},
	outCh chan interface{},
	pullTimeoutMs int,
	deserialize func(data []byte) (interface{}, error),
) {
	for {
		select {
		case <-doneCh:
			return
		default:
			ev := or.cons.Poll(pullTimeoutMs)
			if ev == nil {
				continue
			}
			switch e := ev.(type) {
			case *kafka.Message:
				or.l.Info("Get message")
				data, err := deserialize(e.Value)
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
}

func NewOrderReceiverFactory(
	lc fx.Lifecycle,
	conf *config.Config,
	l *zap.SugaredLogger,
) *OrderReceiverFactory {
	recs := make([]*OrderReceiver, 0)
	factory := OrderReceiverFactory{recs, conf, l}

	lc.Append(fx.Hook{
		OnStop: func(ctx context.Context) error {
			time.Sleep(1 * time.Second)
			for _, rec := range factory.recs {
				if !rec.cons.IsClosed() {
					_ = rec.cons.Close()
				}
			}
			return nil
		},
	})

	return &factory
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
	})
	if err != nil {
		orf.l.Error("Error creating consumer: ", err)
	}

	orf.l.Info("Consumer created")

	err = cons.SubscribeTopics([]string{orf.conf.OrdersTopic}, nil)
	if err != nil {
		orf.l.Error("Subscribe error:", err)
	}

	rec := &OrderReceiver{cons, orf.l, autoCommit}
	orf.recs = append(orf.recs, rec)
	return rec
}
