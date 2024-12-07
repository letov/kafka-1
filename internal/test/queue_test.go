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
				const msgCnt = 10_000

				// Создайте класс сообщения и сериализуйте его в выбранном формате перед отправкой в Kafka. Выведите отправляемое сообщение на консоль.
				smp, _ := json.Marshal(tt.args.order)

				// отправляем сообщение в кафку
				for i := 0; i < msgCnt; i++ {
					go func() {
						_ = os.SendMessage(smp)
					}()
				}

				doneCh := make(chan struct{})
				outCh1 := make(chan interface{})
				outCh2 := make(chan []byte)

				// Один консьюмер должен использовать pull-модель для регулярной проверки сообщений
				// "enable.auto.commit": "false",
				go or.ReceivePullMessage(doneCh, outCh1, func(data []byte) (interface{}, error) {
					// Реализуйте десериализацию сообщений в консьюмерах.
					var o dto.Order
					err := json.Unmarshal(data, &o)
					if err != nil {
						// В случае, если возникли проблемы с сериализацией или десериализацией в каждом из классов, выведите сообщение на консоль.
						fmt.Println(err)
						return nil, err
					}
					return o, nil
				})

				// Второй консьюмер должен реагировать на сообщения сразу после их поступления и считывать их.
				go or.ReceivePushMessage(doneCh, outCh2)

				pollCnt := 0
				pushCnt := 0

				// Консьюмеры должны уметь работать параллельно и считывать одни и те же сообщения, отправленные продюсером.
				for i := 0; i < msgCnt; i++ {
					select {
					case <-outCh1:
						pollCnt++
					case data := <-outCh2:
						// Реализуйте десериализацию сообщений в консьюмерах.
						var o dto.Order
						err := json.Unmarshal(data, &o)
						if err != nil {
							// В случае, если возникли проблемы с сериализацией или десериализацией в каждом из классов, выведите сообщение на консоль.
							fmt.Println(err)
						}
						pushCnt++
					}
				}

				doneCh <- struct{}{}

				// все отправленные собщения получены 2 консъюмерами
				assert.Equal(t, pollCnt+pushCnt, msgCnt)
				assert.Equal(t, pollCnt > 0, true)
				assert.Equal(t, pushCnt > 0, true)
			})
		})
	}
}
