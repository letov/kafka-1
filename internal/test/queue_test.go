package test

import (
	"encoding/json"
	"kafka-1/internal/infrastructure/config"
	"kafka-1/internal/infrastructure/dto"
	"kafka-1/internal/infrastructure/queue"
	"sync"
	"testing"

	"github.com/magiconair/properties/assert"
)

func Test_Kafka(t *testing.T) {
	type args struct {
		order dto.Order
	}

	tests := []struct {
		name string
		args args
	}{
		{
			name: "test #1",
			args: args{
				order: dto.Order{
					OrderID: "0001",
					UserID:  "00001",
					Items: []dto.Item{
						{ProductID: "535", Quantity: 1, Price: 300},
						{ProductID: "125", Quantity: 2, Price: 100},
					},
					TotalPrice: 500.00,
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			initTest(t, func(
				os *queue.OrderSender,
				orf *queue.OrderReceiverFactory,
				conf *config.Config,
			) {
				const msgCnt = 10

				smp, _ := json.Marshal(tt.args.order)

				var wg sync.WaitGroup
				for i := 0; i < msgCnt; i++ {
					wg.Add(1)
					go func(wg *sync.WaitGroup) {
						_ = os.SendMessage(smp)
						wg.Done()
					}(&wg)
				}
				wg.Wait()

				doneCh := make(chan struct{})
				outCh1 := make(chan interface{})
				outCh2 := make(chan interface{})

				go orf.NewOrderReceiver(conf.KafkaCustomerGroup1, false).
					ReceiveMessage(doneCh, outCh1, conf.KafkaConsumerPushTimeoutMs, func(data []byte) (interface{}, error) {
						var o dto.Order
						err := json.Unmarshal(data, &o)
						if err != nil {
							return nil, err
						}
						return o, nil
					})

				go orf.NewOrderReceiver(conf.KafkaCustomerGroup2, true).
					ReceiveMessage(doneCh, outCh2, conf.KafkaConsumerPullTimeoutMs, func(data []byte) (interface{}, error) {
						var o dto.Order
						err := json.Unmarshal(data, &o)
						if err != nil {
							return nil, err
						}
						return o, nil
					})

				pullCnt := 0
				pushCnt := 0

			loop:
				for {
					select {
					case <-outCh1:
						pullCnt++
					case <-outCh2:
						pushCnt++
					default:
						if pullCnt >= msgCnt && pushCnt >= msgCnt {
							doneCh <- struct{}{}
							break loop
						}
					}
				}

				assert.Equal(t, pushCnt, msgCnt)
				assert.Equal(t, pullCnt, msgCnt)
			})
		})
	}
}
