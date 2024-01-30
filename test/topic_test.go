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

//要注意key,规则
//其中“*”用于匹配一个单词，“#”用于匹配多个单词（可以是零个）
//匹配 kuteng.* 表示匹配 kuteng.hello, kuteng.hello.one需要用kuteng.#才能匹配到
//所以，当你发送消息时，根据你想要的匹配方式，你可以填写不同的 routing key。
//例如，如果你想让消费者匹配所有关于白色兔子的消息，你可以发送消息时使用 routing key："animal.rabbit.white"。
//如果你希望消息被所有关于兔子的消费者接收，不管它们是什么颜色的，你可以使用 routing key："animal.rabbit.#"。

//先启动A：  go test -run="TestTopicConsumeMessageA" key=order.#
//再启动B：  go test -run="TestTopicConsumeMessageB" key=order.create.*
//最后启动消息发送：  go test -run="TestTopicSendMessage" key=order.create.confirm  用这个key去匹配每一个绑定相同交换机的key规则，规则匹配成功，则消息推入对应的队列

const exchangeName = "exchangeOrderTopic"

func TestTopicSendMessage(t *testing.T) {
	//初始化连接
	err := pool.InitConnectionPool(rabbitmq.URL, 1)
	fmt.Println(err)

	//go topicConsumeMessageA()
	//go topicConsumeMessageB()

	// 先等待消费者启动服务
	time.Sleep(2 * time.Second)

	go topicSendMessage()

	// 阻塞进程
	forever := make(chan bool)
	<-forever
}

func TestTopicConsumeMessageA(t *testing.T) {
	//初始化连接
	err := pool.InitConnectionPool(rabbitmq.URL, 1)
	fmt.Println(err)

	go topicConsumeMessageA()

	// 阻塞进程
	forever := make(chan bool)
	<-forever
}

func TestTopicConsumeMessageB(t *testing.T) {
	//初始化连接
	err := pool.InitConnectionPool(rabbitmq.URL, 1)
	fmt.Println(err)

	go topicConsumeMessageB()

	// 阻塞进程
	forever := make(chan bool)
	<-forever
}

func topicSendMessage() {
	produce, err := producer.NewProducerTopic(exchangeName, "order.create.confirm")
	if err != nil {
		fmt.Println(err.Error())
	}

	index := 0
	for {
		index++
		msg := fmt.Sprintf("这是一条topice测试消息______%d", index)

		err = produce.SendMessageTopic(msg)
		if err != nil {
			fmt.Println("消息发送失败：" + err.Error() + "   " + msg)
		} else {
			fmt.Println("消息发送成功: " + msg)
		}
		if index >= 100 {
			break
		}
	}

}

func topicConsumeMessageA() {
	consume := consumer.NewConsumerTopic(exchangeName, "order.#", messageHandlerTopic())
	consume.ConsumeMessageTopicRun()
}

func topicConsumeMessageB() {
	consume := consumer.NewConsumerTopic(exchangeName, "order.create.*", messageHandlerTopic())
	consume.ConsumeMessageTopicRun()
}

func messageHandlerTopic() consumer.MessageConsumerHandler {
	return func(message amqp.Delivery) error {
		fmt.Println("topic模式消息消费处理器..............", string(message.Body))
		return nil
	}
}
