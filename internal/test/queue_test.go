package test

import (
	"encoding/json"
	"fmt"
	"kafka-1/internal/infrastructure/dto"
	"kafka-1/internal/infrastructure/queue"
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
				or *queue.OrderReceiver,
			) {
				const msgCnt = 1000

				smp, _ := json.Marshal(tt.args.order)

				for i := 0; i < msgCnt; i++ {
					go func() {
						_ = os.SendMessage(smp)
					}()
				}

				doneCh := make(chan struct{})
				outCh1 := make(chan interface{})
				outCh2 := make(chan []byte)

				go or.ReceivePullMessage(doneCh, outCh1, func(data []byte) (interface{}, error) {
					var o dto.Order
					err := json.Unmarshal(data, &o)
					if err != nil {
						return nil, err
					}
					return o, nil
				})

				go or.ReceivePushMessage(doneCh, outCh2)

				pollCnt := 0
				pushCnt := 0

				for i := 0; i < msgCnt; i++ {
					select {
					case <-outCh1:
						pollCnt++
					case data := <-outCh2:
						var o dto.Order
						err := json.Unmarshal(data, &o)
						if err != nil {
							fmt.Println(err)
						}
						pushCnt++
					}
				}

				doneCh <- struct{}{}

				assert.Equal(t, pollCnt+pushCnt, msgCnt)
				assert.Equal(t, pollCnt > 0, true)
				assert.Equal(t, pushCnt > 0, true)
			})
		})
	}
}
