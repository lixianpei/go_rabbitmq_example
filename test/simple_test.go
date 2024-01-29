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

/**
 * Simple 模式
 */

// TestSimple 简单模式的消息测试
func TestSimple(t *testing.T) {

	//初始化连接
	err := pool.InitConnectionPool(rabbitmq.URL, 2)
	fmt.Println(err)

	//go startConsumer()

	go startProducer()

	// 阻塞进程
	forever := make(chan bool)
	<-forever
}

func startConsumer() {
	// 初始化消费者实例
	consume := consumer.NewConsumerSimple("BuyVip003", messageHandler())
	consume.SetRetryConnSleepTime(10)

	// 启动协程进行阻塞消费
	consume.ConsumeMessageSimpleOrWorkRun()
}

func messageHandler() consumer.MessageConsumerHandler {
	return func(message amqp.Delivery) error {
		fmt.Println("消息处理..............", string(message.Body))
		return nil
	}
}

func startProducer() {
	// 初始化生产者
	produce, err := producer.NewProducerSimpleOrWork("BuyVip003")
	if err != nil {
		fmt.Println(err, produce)
	}

	// 发送消息
	msg := fmt.Sprintf("这是一条测试消息BuyVip003...." + time.Now().Format(time.DateTime))
	err = produce.SendMessageSimpleOrWork(msg)
	if err != nil {
		fmt.Println("消息发送失败：" + err.Error())
	} else {
		fmt.Println("消息发送成功")
	}
}
