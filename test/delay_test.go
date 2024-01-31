package test

import (
	"fmt"
	"github.com/google/uuid"
	"github.com/streadway/amqp"
	"rabbitmq"
	"rabbitmq/consumer"
	"rabbitmq/pool"
	"rabbitmq/producer"
	"strconv"
	"testing"
	"time"
)

const DelayExchangeName = "exchange_B_1"
const DelayQueueName = "delay_B_1"

func TestDelay(t *testing.T) {
	//初始化连接
	err := pool.InitConnectionPool(rabbitmq.URL, 2)
	fmt.Println(err)

	go consumerDelay()

	go producerDelay()

	// 阻塞进程
	<-(make(chan bool))
	fmt.Println("TestDelay finished")
}

func consumerDelay() {
	consume := consumer.NewConsumerDelay(DelayExchangeName, DelayQueueName, messageHandlerDelay())
	consume.ConsumeMessageDelayRun()
}

func producerDelay() {
	produce, err := producer.NewProducerDelay(DelayExchangeName, DelayQueueName)
	fmt.Println("NewProducerDelay：", err)

	index := 0
	for {
		index++
		msg := "这是一条测试的延迟消息-" + strconv.Itoa(index) + "，标记时间：" + time.Now().Format(time.DateTime)
		err = produce.SendMessageDelay(msg, index)
		if err != nil {
			fmt.Println("消息发送失败：" + err.Error())
		} else {
			fmt.Println("消息发送成功")
		}
		if index > 10 {
			break
		}
	}

}

func messageHandlerDelay() consumer.MessageConsumerHandler {
	return func(message amqp.Delivery) error {
		fmt.Println("Delay模式消息消费处理器..............", string(message.Body))
		return nil
	}
}

// TestDeadA | TestDeadB 为配套的延迟消息模拟
func TestDeadA(t *testing.T) {
	//初始化连接
	conn, err := amqp.Dial(rabbitmq.URL)
	fmt.Println("get connection：", err)

	ch, err := conn.Channel()
	fmt.Println("get channel：", err)

	//【消息消费者消费时只需要监听过期的队列】声明消息过期后进入的交换机、队列，并且将队列和交换机进行绑定
	err = ch.ExchangeDeclare("exchange.dlx", "direct", true, false, false, false, nil)
	fmt.Println("ExchangeDeclare exchange.dlx：", err)

	_, err = ch.QueueDeclare("queue.dlx", true, false, false, false, nil)
	fmt.Println("QueueDeclare queue.dlx：", err)

	err = ch.QueueBind("queue.dlx", "", "exchange.dlx", false, nil)
	fmt.Println("QueueBind queue.dlx：", err)

	//【消息生产者只需要把消息放入普通的队列，并且需要声明消息过期后需要把消息放入的交换机名称】声明一个普通的交换机、队列，并且将队列和交换机进行绑定
	err = ch.ExchangeDeclare("exchange.normal", "direct", true, false, false, false, nil)
	fmt.Println("ExchangeDeclare exchange.normal：", err)

	_, err = ch.QueueDeclare("queue.normal", true, false, false, false, amqp.Table{
		//"x-message-ttl":          10000, //消息过期时间设置，与消息发送时设置的值功能一致，二者同时存在时取其中最小的过期时间
		"x-dead-letter-exchange": "exchange.dlx", //声明消息过期后需要把消息再次放入的交换机名称
	})
	fmt.Println("QueueDeclare queue.normal：", err)

	err = ch.QueueBind("queue.normal", "", "exchange.normal", false, nil)
	fmt.Println("QueueBind queue.normal：", err)

	// 发布消息
	err = ch.Publish("exchange.normal", "", false, false, amqp.Publishing{
		DeliveryMode: amqp.Persistent, //消息持久化
		ContentType:  "text/plain",
		Body:         []byte("test.....延迟消息：" + time.Now().Format(time.DateTime)),
		MessageId:    uuid.New().String(),
		Expiration:   strconv.Itoa(15 * 1000), // 设置TTL（毫秒），ttlSecond=传参数（单位秒）
	})
	if err != nil {
		fmt.Println("消息发布失败：" + err.Error())
	} else {
		fmt.Println("消息发布成功")
	}

	// 阻塞进程
	forever := make(chan bool)
	<-forever
}

func TestDeadB(t *testing.T) {
	//初始化连接
	err := pool.InitConnectionPool(rabbitmq.URL, 1)
	fmt.Println(err)

	conn, _ := pool.ConnectionPool.GetConnection()
	ch, _ := conn.Channel()

	//【消息消费者消费时只需要监听过期的队列】声明消息过期后进入的交换机、队列，并且将队列和交换机进行绑定
	err = ch.ExchangeDeclare("exchange.dlx", "direct", true, false, false, false, nil)
	fmt.Println("ExchangeDeclare exchange.dlx：", err)

	_, err = ch.QueueDeclare("queue.dlx", true, false, false, false, nil)
	fmt.Println("QueueDeclare queue.dlx：", err)

	err = ch.QueueBind("queue.dlx", "", "exchange.dlx", false, nil)
	fmt.Println("QueueBind queue.dlx：", err)

	//监听queue.dlx队列
	msgs, _ := ch.Consume("queue.dlx", "", true, false, false, false, nil)

	for d := range msgs {
		fmt.Printf("receive: %s\n", d.Body) // 收到消息，业务处理
		fmt.Println("接收到消息的时间：", time.Now().Format(time.DateTime))
	}
}
