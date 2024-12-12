package app

import (
	"kafka-1/internal/infrastructure/config"
	"kafka-1/internal/infrastructure/dto"
	"kafka-1/internal/infrastructure/queue"
	"os"
	"strconv"

	"go.uber.org/zap"
)

func Start(
	osr *queue.OrderSender,
	orf *queue.OrderReceiverFactory,
	conf *config.Config,
	l *zap.SugaredLogger,
) {
	order := dto.Order{
		OrderID: "0001",
		UserID:  "00001",
		Items: []dto.Item{
			{ProductID: "535", Quantity: 1, Price: 300},
			{ProductID: "125", Quantity: 2, Price: 100},
		},
		TotalPrice: 500.00,
	}

	msgCnt := 1000

	doneCh := make(chan struct{})
	inCh := make(chan queue.KeyMsg)

	osr.SendMessages(doneCh, inCh)

	for i := 0; i < msgCnt; i++ {
		key := strconv.Itoa(i)
		inCh <- queue.KeyMsg{Key: key, Msg: order}
	}

	outCh1 := make(chan interface{})
	outCh2 := make(chan interface{})

	r1 := orf.NewOrderReceiver(conf.KafkaCustomerGroup1, false)
	r1.ReceiveMessages(doneCh, outCh1, conf.KafkaConsumerPushTimeoutMs)

	r2 := orf.NewOrderReceiver(conf.KafkaCustomerGroup2, true)
	r2.ReceiveMessages(doneCh, outCh2, conf.KafkaConsumerPullTimeoutMs)

	cnt1 := 0
	cnt2 := 0

loop:
	for {
		select {
		case m1 := <-outCh1:
			l.Info("ch1: ", m1)
			cnt1++
		case m2 := <-outCh2:
			l.Info("ch2: ", m2)
			cnt2++
		default:
			if cnt1 >= msgCnt && cnt2 >= msgCnt {
				break loop
			}
		}
	}

	doneCh <- struct{}{}
	close(doneCh)
	close(inCh)

	os.Exit(1)
}
