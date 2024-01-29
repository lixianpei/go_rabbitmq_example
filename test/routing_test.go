package test

import (
	"fmt"
	"github.com/streadway/amqp"
	"rabbitmq"
	"rabbitmq/consumer"
	"rabbitmq/pool"
	"rabbitmq/producer"
	"testing"
	"time"
)

func TestRouting(t *testing.T) {
	//初始化连接
	err := pool.InitConnectionPool(rabbitmq.URL, 2)
	fmt.Println(err)

	go consumeMessageRouting()

	go sendMessageRouting()

	// 阻塞进程
	forever := make(chan bool)
	<-forever
}

func messageHandlerRouting() consumer.MessageConsumerHandler {
	return func(message amqp.Delivery) error {
		fmt.Println("路由模式消息处理..............", string(message.Body))
		return nil
	}
}

func sendMessageRouting() {
	produce, err := producer.NewProducerRouting("Routing_Exchange_001", "Route_A_001")
	if err != nil {
		fmt.Println("sendMessageRouting err", err.Error())
	}

	err = produce.SendMessageRouting("路由模式消息——————————" + time.Now().Format(time.DateTime))
	if err != nil {
		fmt.Println("消息发送失败：" + err.Error())
	} else {
		fmt.Println("消息发送成功")
	}
}

func consumeMessageRouting() {
	consume := consumer.NewConsumerRouting("Routing_Exchange_001", "Route_A_001", messageHandlerRouting())
	consume.ConsumeMessageRoutingRun()
}
