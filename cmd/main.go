// main.go
package main

import (
	"kafka-1/internal/infrastructure/di"

	"go.uber.org/fx"
)

//
//// Item – структура, описывающая продукт в заказе
//type Item struct {
//	ProductID string  `json:"product_id"`
//	Quantity  int     `json:"quantity"`
//	Price     float64 `json:"price"`
//}
//
//// Order – структура, описывающая заказ с продуктами
//type Order struct {
//	Offset     int     `json:"offset"`
//	OrderID    string  `json:"order_id"`
//	UserID     string  `json:"user_id"`
//	Items      []Item  `json:"items"`
//	TotalPrice float64 `json:"total_price"`
//}
//
//const bootstrapServers = "localhost:9094"
//const group = "consumer_group"
//const topic = "orders_many_parts"
//
//// Таймаут для запроса
//const timeoutMs = 100
//const topicWaitingTime = 60 * time.Second
//
//func sendMsg() {
//	// Создаём продюсера
//	p, err := kafka.NewProducer(&kafka.ConfigMap{"bootstrap.servers": bootstrapServers})
//	if err != nil {
//		log.Fatalf("Невозможно создать продюсера: %s\n", err)
//	}
//
//	log.Printf("Продюсер создан %v\n", p)
//
//	// Канал доставки событий (информации об отправленном сообщении)
//	deliveryChan := make(chan kafka.Event)
//
//	// Создаём заказ
//	value := &Order{
//		OrderID: "0001",
//		UserID:  "00001",
//		Items: []Item{
//			{ProductID: "535", Quantity: 1, Price: 300},
//			{ProductID: "125", Quantity: 2, Price: 100},
//		},
//		TotalPrice: 500.00,
//	}
//
//	// Сериализуем заказ в массив
//	payload, err := json.Marshal(value)
//	if err != nil {
//		log.Fatalf("Невозможно сериализовать заказ: %s\n", err)
//	}
//
//	// Отправляем сообщение в брокер
//	t := topic
//	err = p.Produce(&kafka.Message{
//		TopicPartition: kafka.TopicPartition{Topic: &t, Partition: kafka.PartitionAny},
//		Value:          payload,
//		Headers:        []kafka.Header{{Key: "myTestHeader", Value: []byte("header values are binary")}},
//	}, deliveryChan)
//	if err != nil {
//		log.Fatalf("Ошибка при отправке сообщения: %v\n", err)
//	}
//
//	// Ждём информацию об отправленном сообщении. (https://docs.confluent.io/kafka-clients/go/current/overview.html#synchronous-writes)
//	e := <-deliveryChan
//
//	// Приводим Events к типу *kafka.Message
//	m := e.(*kafka.Message)
//
//	// Если возникла ошибка доставки сообщения
//	if m.TopicPartition.Error != nil {
//		fmt.Printf("Ошибка доставки сообщения: %v\n", m.TopicPartition.Error)
//	} else {
//		fmt.Printf("Сообщение отправлено в топик %s [%d] офсет %v\n",
//			*m.TopicPartition.Topic, m.TopicPartition.Partition, m.TopicPartition.Offset)
//	}
//	// Закрываем продюсера
//	p.Close()
//	// Закрываем канал доставки событий
//	close(deliveryChan)
//}
//
//func getMsg() {
//	// Перехватываем сигналы syscall.SIGINT и syscall.SIGTERM для graceful shutdown
//	sigchan := make(chan os.Signal, 1)
//	signal.Notify(sigchan, syscall.SIGINT, syscall.SIGTERM)
//
//	// Создаём консьюмера
//	c, err := kafka.NewConsumer(&kafka.ConfigMap{
//		"bootstrap.servers":  bootstrapServers,
//		"group.id":           group,       // уникальный идентификатор группы
//		"session.timeout.ms": 6000,        // время, в течение которого Kafka ожидает активности от консьюмера до того, как объявит его «мёртвым» и начнёт ребалансировку
//		"enable.auto.commit": true,        // автоматически сохранять смещения (offsets) после получения сообщений
//		"auto.offset.reset":  "earliest"}) // начинать чтение с самого старого доступного сообщения.
//
//	if err != nil {
//		log.Fatalf("Невозможно создать консьюмера: %s\n", err)
//	}
//
//	fmt.Printf("Консьюмер создан %v\n", c)
//
//	// Подписываемся на топики, в примере он один
//	err = c.SubscribeTopics([]string{topic}, nil)
//
//	if err != nil {
//		log.Fatalf("Невозможно подписаться на топик: %s\n", err)
//	}
//
//	run := true
//	// Запускаем бесконечный цикл
//	for run {
//		select {
//		// Для выхода нажмите ctrl+C
//		case sig := <-sigchan:
//			fmt.Printf("Передан сигнал %v: приложение останавливается\n", sig)
//			run = false
//		default:
//
//			// Делаем запрос на считывание сообщения из брокера
//			ev := c.Poll(timeoutMs)
//			if ev == nil {
//				continue
//			}
//
//			//     Приводим Events к
//			switch e := ev.(type) {
//			// типу *kafka.Message,
//			case *kafka.Message:
//				value := Order{}
//				err := json.Unmarshal(e.Value, &value)
//				if err != nil {
//					fmt.Printf("Ошибка десериализации: %s\n", err)
//				} else {
//					fmt.Printf("%% Получено сообщение в топик %s:\n%+v\n", e.TopicPartition, value)
//				}
//				if e.Headers != nil {
//					fmt.Printf("%% Заголовки: %v\n", e.Headers)
//				}
//			// типу Ошибки брокера
//			case kafka.Error:
//				// Ошибки обычно следует считать
//				// информационными, клиент попытается
//				// автоматически их восстановить
//				fmt.Fprintf(os.Stderr, "%% Error: %v: %v\n", e.Code(), e)
//			default:
//				fmt.Printf("Другие события %v\n", e)
//			}
//		}
//	}
//	// Закрываем потребителя
//	_ = c.Close()
//}
//
//func createPartitions() {
//	numParts := 5
//	replicationFactor := 2
//
//	// Cоздаём новый админский клиент.
//	a, err := kafka.NewAdminClient(&kafka.ConfigMap{"bootstrap.servers": bootstrapServers})
//	if err != nil {
//		log.Fatalf("Ошибка создания админского клиента: %s\n", err)
//	}
//
//	results, err := a.CreateTopics(
//		context.Background(),
//		// Можно одновременно создать несколько топиков, указав несколько структур TopicSpecification
//		[]kafka.TopicSpecification{{
//			Topic:             topic,
//			NumPartitions:     numParts,
//			ReplicationFactor: replicationFactor}},
//		// Устанавливаем опцию админского клиента — ждать не более topicWaitingTime
//		kafka.SetAdminOperationTimeout(topicWaitingTime))
//	if err != nil {
//		log.Fatalf("Ошибка создания топика: %s\n", err)
//	}
//
//	// Результат (тип TopicResult) содержит информацию о результате операции создания
//	//топика. Если бы мы создавали несколько топиков, то получили бы информацию по каждому из них.
//	for _, result := range results {
//		fmt.Printf("%s\n", result)
//	}
//	a.Close()
//}

func main() {
	fx.New(
		di.InjectApp(),
		fx.Invoke(func() {}),
	).Run()
}
