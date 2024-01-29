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

// TestSimple 简单模式的消息测试
func TestWork(t *testing.T) {

	//初始化连接
	err := pool.InitConnectionPool(rabbitmq.URL, 3)
	fmt.Println(err)

	// 同时开多个消费者
	go startConsumerWork1()
	go startConsumerWork2()

	// 一个生产者
	go startProducerWork()

	// 阻塞进程
	forever := make(chan bool)
	<-forever
}

func startConsumerWork1() {
	consume := consumer.NewConsumerWork("BuyVip004", "consumer_001", messageHandlerWork())
	consume.ConsumeMessageSimpleOrWorkRun()
}
func startConsumerWork2() {
	consume := consumer.NewConsumerWork("BuyVip004", "consumer_002", messageHandlerWork())
	consume.ConsumeMessageSimpleOrWorkRun()
}

func messageHandlerWork() consumer.MessageConsumerHandler {
	return func(message amqp.Delivery) error {
		fmt.Println("work模式消息消费处理器..............", string(message.Body))
		return nil
	}
}

func startProducerWork() {

	produce, err := producer.NewProducerSimpleOrWork("BuyVip004")
	if err != nil {
		fmt.Println(err, produce)
	}

	index := 0
	for {
		msg := fmt.Sprintf("这是一条测试消息work%d......%s", index, time.Now().Format(time.DateTime))
		err = produce.SendMessageSimpleOrWork(msg)
		if err != nil {
			fmt.Println("消息发送失败：" + err.Error())
		} else {
			fmt.Println("消息发送成功")
		}
	}
}
