package consumer

import (
	"fmt"
	"github.com/streadway/amqp"
	"rabbitmq/pool"
	"time"
)

type Consumer struct {
	QueueName          string //队列名称
	Exchange           string //交换机名称
	Key                string //路由名称key
	ConsumerName       string //工作模式下区分不同的消费者，当选择工作模式后，初始化的消费者必须填写对应的消费者名称
	conn               *amqp.Connection
	channel            *amqp.Channel
	retryConnSleepTime int                    //断开连接后重连等待时长，默认5秒
	messageHandler     MessageConsumerHandler // 消费消息的处理方法
}

// MessageConsumerHandler 消费消息的处理方法
type MessageConsumerHandler func(message amqp.Delivery) error

// SetRetryConnSleepTime 设置重连的等待时长
func (c *Consumer) SetRetryConnSleepTime(t int) {
	if t > 0 {
		c.retryConnSleepTime = t
	}
}

// ================================= Simple模式 ================================= //

// NewConsumerSimple 创建一个simple的消费者实例
func NewConsumerSimple(queueName string, messageHandler MessageConsumerHandler) *Consumer {
	return &Consumer{
		QueueName:          queueName,
		Exchange:           "",
		Key:                "",
		ConsumerName:       "",
		retryConnSleepTime: 5,
		messageHandler:     messageHandler,
	}
}

func (c *Consumer) consumerSimpleLog(logContent string) {
	fmt.Printf("消费者【%s】=>【%s】消费队列：%s \r\n", c.ConsumerName, c.QueueName, logContent)
}

// ConsumeMessageSimpleOrWorkRun 启动simple\work模式下的消费进程
func (c *Consumer) ConsumeMessageSimpleOrWorkRun() {
	for {
		c.consumerSimpleLog("开始启动消费")

		err := c.handleConsumerMessageSimpleOrWork()
		if err != nil {
			c.consumerSimpleLog(fmt.Sprintf("异常退出，%d 秒后重新连接，错误信息：%s", c.retryConnSleepTime, err.Error()))

			// 睡眠一下，提高重连的成功率
			time.Sleep(time.Duration(c.retryConnSleepTime) * time.Second)
		}
	}
}

// consumerMessageSimpleOrWorkHandle 执行消费的逻辑，当遇到错误或断开连接后就会异常退出
func (c *Consumer) handleConsumerMessageSimpleOrWork() (err error) {
	// 连接
	c.conn, err = pool.ConnectionPool.GetConnection()
	if err != nil {
		return fmt.Errorf("获取连接异常：%s", err.Error())
	}

	// 创建channel通信信道
	c.channel, err = c.conn.Channel()
	if err != nil {
		return fmt.Errorf("消费队列获取channel异常：%s", err.Error())
	}

	// 队列申明，不存在则会创建队列
	_, err = c.channel.QueueDeclare(
		c.QueueName, //队列，名称
		true,        //是否持久化
		false,       //是否自动删除
		false,       //是否具有排他性
		false,       //是否阻塞处理
		nil,         //额外的属性
	)
	if err != nil {
		return fmt.Errorf("消费队列QueueDeclare异常：%s", err.Error())
	}

	// 接收消息
	messages, err := c.channel.Consume(
		c.QueueName,
		c.ConsumerName, //简单模式下无需设置，工作模式下用例区分不同的消费者
		false,          //是否自动确认消息
		false,          // 如果一个消费者被声明为 "exclusive"，那么它是队列的唯一消费者。这意味着没有其他消费者可以同时订阅相同的队列。这通常用于确保一个队列只有一个消费者来处理消息，防止消息被多个消费者同时处理。
		false,
		false, //队列是否阻塞
		nil,
	)
	if err != nil {
		c.consumerSimpleLog(fmt.Sprintf("消费队列Consume异常：%s", err.Error()))
		return fmt.Errorf("消费队列Consume异常：%s", err.Error())
	}

	c.consumerSimpleLog(fmt.Sprintf("正在等待消息..."))

	// 处理消息，会一直阻塞等待消息
	for message := range messages {
		c.consumerSimpleLog(fmt.Sprintf("获取到消息id=%s,content=%s \r\n", message.MessageId, message.Body))
		err := c.messageHandler(message)
		var ack bool
		var errMsg string
		if err != nil {
			//消息处理失败：确认失败
			errMsg = fmt.Sprintf("处理失败：%s", err.Error())
			ack = false
		} else {
			errMsg = fmt.Sprintf("处理成功")
			ack = true
		}

		//确认消息：成功或失败
		err = message.Ack(ack)
		if err != nil {
			//消息处理成功：确认成功
			c.consumerSimpleLog(fmt.Sprintf("消息id【%s】%s，确认失败：%s", message.MessageId, errMsg, err.Error()))
		} else {
			//消息处理成功：确认成功
			c.consumerSimpleLog(fmt.Sprintf("消息id【%s】%s，确认成功", message.MessageId, errMsg))
		}

	}

	//当消费队列退出后继续重连
	return fmt.Errorf("消费队列异常退出")
}

// ================================= Work模式 ================================= //

// NewConsumerWork 创建一个Work的消费者实例
func NewConsumerWork(queueName string, consumerName string, messageHandler MessageConsumerHandler) *Consumer {
	return &Consumer{
		QueueName:          queueName,
		Exchange:           "",
		Key:                "",
		ConsumerName:       consumerName,
		retryConnSleepTime: 5,
		messageHandler:     messageHandler,
	}
}

// ================================= Subscribe 订阅模式 ================================= //

// NewConsumerSubscribe 实例化订阅模式的消费者
func NewConsumerSubscribe(exchangeName string, messageHandler MessageConsumerHandler) *Consumer {
	return &Consumer{
		QueueName:          "",
		Exchange:           exchangeName,
		Key:                "",
		ConsumerName:       "",
		retryConnSleepTime: 5,
		messageHandler:     messageHandler,
	}
}

// ConsumeMessageSubscribeRun 启动订阅模式的消费者消息消费
func (c *Consumer) ConsumeMessageSubscribeRun() {
	for {
		fmt.Printf("发布订阅模式的交换机【%s】开始启动处理消息...\r\n", c.Exchange)

		err := c.handleConsumerMessageSubscribe()
		if err != nil {
			fmt.Printf("异常退出，%d 秒后重新连接，错误信息：%s \r\n", c.retryConnSleepTime, err.Error())

			// 睡眠一下，提高重连的成功率
			time.Sleep(time.Duration(c.retryConnSleepTime) * time.Second)
		}
	}
}

// handleConsumerMessageSubscribe 订阅模式的消息处理
func (c *Consumer) handleConsumerMessageSubscribe() (err error) {
	// 获取连接
	c.conn, err = pool.ConnectionPool.GetConnection()
	if err != nil {
		return fmt.Errorf("获取连接异常：%s", err.Error())
	}

	// 创建channel通信信道
	c.channel, err = c.conn.Channel()
	if err != nil {
		return fmt.Errorf("消费队列获取channel异常：%s", err.Error())
	}

	// 尝试创建交换机
	err = c.channel.ExchangeDeclare(c.Exchange, "fanout", true, false, false, false, nil)
	if err != nil {
		return fmt.Errorf("创建交换机失败：%s", err.Error())
	}

	// 尝试创建消费队列
	// queueName=""+autoDelete=true：队列名称随机生成，消费者退出消费后，再次启动队列就会删除，每次消费都是最新的消息（推荐的方式）
	// queueName="指定队列名称"+autoDelete=false：当消费者重启后还能消费在断开连接期间未消费的消息，
	// queueName 若队列名称使用了其他模式，比如simple或work模式创建的队列名，那么当subscribe订阅模式的消费者启动后就会把当前交换机exchange与队列进行了绑定，subscribe消费者依然能够消费simple或work模式下已经存在的消息
	queueName := "" // uuid.New().String()
	queue, err := c.channel.QueueDeclare(queueName, true, false, false, false, nil)
	if err != nil {
		return fmt.Errorf("创建消费队列失败：%s", err.Error())
	}

	// 绑定队列到交换机中：pub/sub订阅模式下，key必须为空
	err = c.channel.QueueBind(queue.Name, "", c.Exchange, false, nil)
	if err != nil {
		return fmt.Errorf("绑定队列交换机发生错误：%s", err.Error())
	}

	// 消费消息，由于是发布订阅模式，consumer为空
	messages, err := c.channel.Consume(queue.Name, "", false, false, false, false, nil)

	fmt.Printf("正在等待消息... \r\n")

	// 处理消息，会一直阻塞等待消息
	for message := range messages {

		fmt.Printf("获取到消息id=%s,content=%s \r\n", message.MessageId, message.Body)

		err = c.messageHandler(message)
		var ack bool
		var errMsg string
		if err != nil {
			//消息处理失败：确认失败
			errMsg = fmt.Sprintf("处理失败：%s", err.Error())
			ack = false
		} else {
			errMsg = fmt.Sprintf("处理成功")
			ack = true
		}

		//确认消息：成功或失败
		err = message.Ack(ack)
		if err != nil {
			//消息处理成功：确认成功
			fmt.Printf("消息id【%s】%s，确认失败：%s \r\n", message.MessageId, errMsg, err.Error())
		} else {
			//消息处理成功：确认成功
			fmt.Printf("消息id【%s】%s，确认成功 \r\n", message.MessageId, errMsg)
		}

	}

	//当消费队列退出后继续重连
	return fmt.Errorf("消费队列异常退出")
}

// ================================= Routing 路由模式 ================================= //

// NewConsumerRouting 实例化路由模式的消费者
func NewConsumerRouting(exchangeName string, routingKey string, messageHandler MessageConsumerHandler) *Consumer {
	return &Consumer{
		QueueName:          "",
		Exchange:           exchangeName,
		Key:                routingKey,
		ConsumerName:       "",
		retryConnSleepTime: 5,
		messageHandler:     messageHandler,
	}
}

// ConsumeMessageRoutingRun 启动路由模式的消费者消息消费
func (c *Consumer) ConsumeMessageRoutingRun() {
	for {
		fmt.Printf("路由模式的交换机【%s】开始启动处理消息...\r\n", c.Exchange)

		err := c.handleConsumerMessageRouting()
		if err != nil {
			fmt.Printf("异常退出，%d 秒后重新连接，错误信息：%s \r\n", c.retryConnSleepTime, err.Error())

			// 睡眠一下，提高重连的成功率
			time.Sleep(time.Duration(c.retryConnSleepTime) * time.Second)
		}
	}
}

// handleConsumerMessageRouting 路由模式的消息处理
func (c *Consumer) handleConsumerMessageRouting() (err error) {
	// 获取连接
	c.conn, err = pool.ConnectionPool.GetConnection()
	if err != nil {
		return fmt.Errorf("获取连接异常：%s", err.Error())
	}

	// 创建channel通信信道
	c.channel, err = c.conn.Channel()
	if err != nil {
		return fmt.Errorf("消费队列获取channel异常：%s", err.Error())
	}

	// 尝试创建交换机
	err = c.channel.ExchangeDeclare(c.Exchange, "direct", true, false, false, false, nil)
	if err != nil {
		return fmt.Errorf("创建交换机失败：%s", err.Error())
	}

	// 尝试创建消费队列
	// queueName=""+autoDelete=true：队列名称随机生成，消费者退出消费后，再次启动队列就会删除，每次消费都是最新的消息（推荐的方式）
	// queueName="指定队列名称"+autoDelete=false：当消费者重启后还能消费在断开连接期间未消费的消息，
	// queueName 若队列名称使用了其他模式，比如simple或work模式创建的队列名，那么当subscribe订阅模式的消费者启动后就会把当前交换机exchange与队列进行了绑定，subscribe消费者依然能够消费simple或work模式下已经存在的消息
	queueName := "" // uuid.New().String()
	queue, err := c.channel.QueueDeclare(queueName, true, false, false, false, nil)
	if err != nil {
		return fmt.Errorf("创建消费队列失败：%s", err.Error())
	}

	// 绑定队列到交换机中：routing路由模式下，key必须填写，
	err = c.channel.QueueBind(queue.Name, c.Key, c.Exchange, false, nil)
	if err != nil {
		return fmt.Errorf("绑定队列交换机发生错误：%s", err.Error())
	}

	// 消费消息，consumer为空
	messages, err := c.channel.Consume(queue.Name, "", false, false, false, false, nil)

	fmt.Printf("正在等待消息... \r\n")

	// 处理消息，会一直阻塞等待消息
	for message := range messages {

		fmt.Printf("获取到消息id=%s,content=%s \r\n", message.MessageId, message.Body)

		err = c.messageHandler(message)
		var ack bool
		var errMsg string
		if err != nil {
			//消息处理失败：确认失败
			errMsg = fmt.Sprintf("处理失败：%s", err.Error())
			ack = false
		} else {
			errMsg = fmt.Sprintf("处理成功")
			ack = true
		}

		//确认消息：成功或失败
		err = message.Ack(ack)
		if err != nil {
			//消息处理成功：确认成功
			fmt.Printf("消息id【%s】%s，确认失败：%s \r\n", message.MessageId, errMsg, err.Error())
		} else {
			//消息处理成功：确认成功
			fmt.Printf("消息id【%s】%s，确认成功 \r\n", message.MessageId, errMsg)
		}

	}

	//当消费队列退出后继续重连
	return fmt.Errorf("消费队列异常退出")
}
