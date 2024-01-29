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

// TestSubscribe 发布订阅模式  go test -run="TestSubscribe"
func TestSubscribe(t *testing.T) {

	//初始化连接
	err := pool.InitConnectionPool(rabbitmq.URL, 5)
	fmt.Println(err)

	// 启动两个消费者订阅消息
	go consumerSubscribe()
	//go consumerSubscribe()

	// 开启一个消息发布
	//go producerSubscribe()

	// 阻塞进程
	forever := make(chan bool)
	<-forever
}

func consumerSubscribe() {
	// 初始化消费者实例
	consume := consumer.NewConsumerSubscribe("exchangeV_001", messageHandlerSubscribe())
	consume.SetRetryConnSleepTime(10)

	// 启动协程进行阻塞消费
	consume.ConsumeMessageSubscribeRun()
}

func messageHandlerSubscribe() consumer.MessageConsumerHandler {
	return func(message amqp.Delivery) error {
		fmt.Println("subscribe 消息处理..............", string(message.Body))
		return nil
	}
}

func producerSubscribe() {
	// 初始化生产者
	produce, err := producer.NewProducerSubscribe("exchangeV_001")
	if err != nil {
		fmt.Println(err, produce)
	}

	// 发送消息
	msg := fmt.Sprintf("这是一条测试消息...." + time.Now().Format(time.DateTime))
	err = produce.SendMessageSubscribe(msg)
	if err != nil {
		fmt.Println("消息发布失败：" + err.Error() + "   " + msg)
	} else {
		fmt.Println("消息发布成功：" + msg)
	}
}
