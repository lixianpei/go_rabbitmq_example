package producer

import (
	"fmt"
	"github.com/google/uuid"
	"github.com/streadway/amqp"
	"rabbitmq/pool"
	"strconv"
	"time"
)

// 参考文档：介绍rabbitmq的几种模式 https://www.topgoer.com/%E6%95%B0%E6%8D%AE%E5%BA%93%E6%93%8D%E4%BD%9C/go%E6%93%8D%E4%BD%9CRabbitMQ/RabbitMQ%E4%BB%8B%E7%BB%8D.html

// Producer 生产者
type Producer struct {
	conn                 *amqp.Connection
	channel              *amqp.Channel
	retryConnSleepTime   int                         //断开连接后重连等待时长，默认2秒，发送消息可设置较短的重连间隔
	QueueName            string                      //队列名称
	Exchange             string                      //交换机名称
	Key                  string                      //路由名称key
	NotifyPublishHandler MessageNotifyPublishHandler //设置监听器来处理交换机返回的消息推送状态：可以确认是否已经将消息推送到交换机
	NotifyReturnHandler  MessageNotifyReturnHandler  //设置监听器来处理无法被正确路由的消息：消息未进入消息队列

}

// MessageNotifyPublishHandler 设置监听器来处理交换机返回的消息推送状态：可以确认是否已经将消息推送到交换机
type MessageNotifyPublishHandler func(confirm amqp.Confirmation) error

// MessageNotifyReturnHandler 设置监听器来处理无法被正确路由的消息：消息未进入消息队列
type MessageNotifyReturnHandler func(message amqp.Return) error

// ================================= 生产者通用方法 ================================= //

// CheckOrGetNewConnection 检测或者获取新的连接
func (p *Producer) CheckOrGetNewConnection() (err error) {
	if pool.IsConnHealthy(p.conn) == true {
		return nil
	}
	fmt.Printf("无效的connection，%d 秒后获取新的连接 \r\n", p.retryConnSleepTime)

	// 睡眠一下，提高重连的成功率
	time.Sleep(time.Duration(p.retryConnSleepTime) * time.Second)

	p.conn, err = pool.ConnectionPool.GetConnection()
	if err != nil {
		return fmt.Errorf("获取新的connection失败：%s", err.Error())
	}

	return nil
}

// SetNotifyPublishHandler 设置监听器来处理交换机返回的消息推送状态：可以确认是否已经将消息推送到交换机
func (p *Producer) SetNotifyPublishHandler(fun MessageNotifyPublishHandler) {
	p.NotifyPublishHandler = fun
}

// SetNotifyReturnHandler 设置监听器来处理无法被正确路由的消息，无法进入消息队列
func (p *Producer) SetNotifyReturnHandler(fun MessageNotifyReturnHandler) {
	p.NotifyReturnHandler = fun
}

// SetRetryConnSleepTime 设置重新连接Mq服务器的等待时长
func (p *Producer) SetRetryConnSleepTime(t int) {
	if t > 0 {
		p.retryConnSleepTime = t
	}
}

// CloseChannel 关闭通信信道
func (p *Producer) CloseChannel() {
	if p.channel != nil {
		err := p.channel.Close()
		if err != nil {
			fmt.Printf("channel关闭失败：%s", err.Error())
		}
	}
}

// notifyPublishListen RabbitMQ 支持 Publisher Confirm 机制，可以通过该机制来确认消息是否已经被正确地发送到消息队列中。你可以在创建通道时启用 Publisher Confirm，并在消息发送后监听 Confirm 消息，以确定消息是否已经被确认。如果确认消息中包含了未被确认的消息序列号，则表示该消息未能成功地被发送到队列中
func (p *Producer) notifyPublishListen() {
	// 设置监听器来处理交换机返回的消息推送状态：可以确认是否已经将消息推送到交换机
	// 开启确认等待 TODO 可以做成配置项
	err := p.channel.Confirm(false)
	if err != nil {
		fmt.Printf("设置监听器来处理已经正确被推送到交换机异常：" + err.Error())
		return
	}

	confirms := p.channel.NotifyPublish(make(chan amqp.Confirmation))
	go func() {
		for confirm := range confirms {

			// 处理无法被正确路由的消息
			if p.NotifyReturnHandler == nil {
				//默认接收消息确认状态处理
				_ = p.defaultNotifyPublishHandler(confirm)
			} else {
				//设置监听器来处理交换机返回的消息推送状态：可以确认是否已经将消息推送到交换机
				_ = p.NotifyPublishHandler(confirm)
			}
		}
	}()
}

// defaultNotifyPublishHandler 默认消息发布到交换机确认处理器
func (p *Producer) defaultNotifyPublishHandler(confirm amqp.Confirmation) error {
	fmt.Println("defaultNotifyPublishHandler>>>>ack result: ", confirm.Ack)
	return nil
}

// notifyReturnMessageHandle 当 队列mandatory参数设为true时，交换器无法根据自身的类型和路由键找到一个符合条件的队列，那么 RabbitMQ 会调用 Basic.Return 命令将消息返回给生产者，NotifyReturn 功能只对直接交换器（Direct Exchange）和主题交换器（Topic Exchange）有效
func (p *Producer) notifyReturnListen() {
	// 设置监听器来处理无法被正确路由的消息
	notifyMessages := p.channel.NotifyReturn(make(chan amqp.Return))

	// 开启协程监听返回的数据
	go func() {
		for notifyMessage := range notifyMessages {
			// 处理无法被正确路由的消息
			if p.NotifyReturnHandler == nil {
				//默认消息异常处理
				_ = p.defaultNotifyReturnHandler(notifyMessage)
			} else {
				//发送消息时声明的处理器
				_ = p.NotifyReturnHandler(notifyMessage)
			}
		}
	}()
}

// defaultNotifyReturnHandler 默认异常处理器
func (p *Producer) defaultNotifyReturnHandler(message amqp.Return) error {
	fmt.Println("notifyReturnHandler>>>>: ", message.MessageId, string(message.Body))
	return nil
}

// ================================= Simple、Work 模式 ================================= //

// NewProducerSimpleOrWork simple或work模式创建RabbitMQ实例
func NewProducerSimpleOrWork(queueName string) (produce *Producer, err error) {
	// 初始化实例
	produce = &Producer{
		QueueName:          queueName,
		Exchange:           "",
		Key:                "",
		retryConnSleepTime: 2,
	}

	// 获取连接
	produce.conn, err = pool.ConnectionPool.GetConnection()
	if err != nil {
		return produce, fmt.Errorf("获取connection失败：%s", err.Error())
	}
	return produce, nil
}

// SendMessageSimpleOrWork simple或work模式的消息发送
func (p *Producer) SendMessageSimpleOrWork(message string) (err error) {
	// 检测连接是否正常
	err = p.CheckOrGetNewConnection()
	if err != nil {
		return err
	}

	//每次发布消息均使用新的通道
	p.channel, err = p.conn.Channel()
	if err != nil {
		return fmt.Errorf("获取新的channel失败：%s", err.Error())
	}
	//消息发送结束关闭通道
	defer p.CloseChannel()

	// 队列申明，不存在则会创建队列
	_, err = p.channel.QueueDeclare(
		p.QueueName, //队列，名称
		true,        //是否持久化
		false,       //是否自动删除
		false,       //是否具有排他性
		false,       //是否阻塞处理
		nil,         //额外的属性
	)
	if err != nil {
		return fmt.Errorf("队列QueueDeclare失败：%s", err.Error())
	}

	//调用channel发送消息到队列中
	err = p.channel.Publish(
		"",
		p.QueueName,
		false, //如果为true，根据自身exchange类型和routeKey规则无法找到符合条件的队列会把消息返还给发送者
		false, //如果为true，当exchange发送消息到队列后发现队列上没有消费者，则会把消息返还给发送者
		amqp.Publishing{
			DeliveryMode: amqp.Persistent, //消息持久化
			ContentType:  "text/plain",
			Body:         []byte(message),
			MessageId:    uuid.New().String(),
		},
	)

	if err != nil {
		return fmt.Errorf("消息发送失败：%s", err.Error())
	}
	return nil
}

// ================================= Subscribe 订阅模式 ================================= //

// NewProducerSubscribe Subscribe订阅模式创建RabbitMQ实例
func NewProducerSubscribe(exchangeName string) (produce *Producer, err error) {
	// 初始化实例
	produce = &Producer{
		Exchange:           exchangeName,
		Key:                "",
		retryConnSleepTime: 2,
	}

	// 获取连接
	produce.conn, err = pool.ConnectionPool.GetConnection()
	if err != nil {
		return produce, fmt.Errorf("获取connection失败：%s", err.Error())
	}
	return produce, nil
}

// SendMessageSubscribe 订阅模式的消息发送
func (p *Producer) SendMessageSubscribe(message string) (err error) {
	// 检测连接是否正常
	err = p.CheckOrGetNewConnection()
	if err != nil {
		return err
	}

	//每次发布消息均使用新的通道
	p.channel, err = p.conn.Channel()
	if err != nil {
		return fmt.Errorf("获取新的channel失败：%s", err.Error())
	}
	//消息发送结束关闭通道
	defer p.CloseChannel()

	//尝试创建交换机
	err = p.channel.ExchangeDeclare(p.Exchange, "fanout", true, false, false, false, nil)
	if err != nil {
		return fmt.Errorf("创建交换机失败：%s", err.Error())
	}

	//发送消息
	err = p.channel.Publish(
		p.Exchange,
		"",
		false,
		false,
		amqp.Publishing{
			DeliveryMode: amqp.Persistent, //消息持久化
			ContentType:  "text/plain",
			Body:         []byte(message),
			MessageId:    uuid.New().String(),
		},
	)
	if err != nil {
		return fmt.Errorf("发送消息给交换机失败：%s", err.Error())
	}

	return nil
}

// ================================= Routing 路由模式 ================================= //

// NewProducerRouting Routing路由模式创建RabbitMQ实例
func NewProducerRouting(exchangeName string, routeKey string) (produce *Producer, err error) {
	// 初始化实例
	produce = &Producer{
		Exchange:           exchangeName,
		Key:                routeKey,
		retryConnSleepTime: 2,
	}

	// 获取连接
	produce.conn, err = pool.ConnectionPool.GetConnection()
	if err != nil {
		return produce, fmt.Errorf("获取connection失败：%s", err.Error())
	}
	return produce, nil
}

// SendMessageRouting 路由模式的消息发送
func (p *Producer) SendMessageRouting(message string) (err error) {
	// 检测连接是否正常
	err = p.CheckOrGetNewConnection()
	if err != nil {
		return err
	}

	//每次发布消息均使用新的通道
	p.channel, err = p.conn.Channel()
	if err != nil {
		return fmt.Errorf("获取新的channel失败：%s", err.Error())
	}
	//消息发送结束关闭通道
	defer p.CloseChannel()

	//尝试创建交换机 kind=direct（不能修改）
	err = p.channel.ExchangeDeclare(p.Exchange, "direct", true, false, false, false, nil)
	if err != nil {
		return fmt.Errorf("创建交换机失败：%s", err.Error())
	}

	//发送消息
	err = p.channel.Publish(
		p.Exchange,
		p.Key, // 必须设置路由key，消费者根据此key判断是否能够读取消息
		false,
		false,
		amqp.Publishing{
			DeliveryMode: amqp.Persistent, //消息持久化
			ContentType:  "text/plain",
			Body:         []byte(message),
			MessageId:    uuid.New().String(),
		},
	)
	if err != nil {
		return fmt.Errorf("发送消息给交换机失败：%s", err.Error())
	}

	return nil
}

// ================================= Topic 话题模式 ================================= //

// NewProducerTopic 话题模式创建RabbitMQ实例：一个消息被多个消费者获取，消息的目标queue可用BindingKey以通配符，（#：一个或多个词，*：一个词）的方式指定
func NewProducerTopic(exchangeName string, routeKey string) (produce *Producer, err error) {
	// 初始化实例
	produce = &Producer{
		Exchange:           exchangeName,
		Key:                routeKey,
		retryConnSleepTime: 2,
	}

	// 获取连接
	produce.conn, err = pool.ConnectionPool.GetConnection()
	if err != nil {
		return produce, fmt.Errorf("获取connection失败：%s", err.Error())
	}
	return produce, nil
}

// SendMessageTopic 话题模式的消息发送
func (p *Producer) SendMessageTopic(message string) (err error) {
	// 检测连接是否正常
	err = p.CheckOrGetNewConnection()
	if err != nil {
		return err
	}

	//每次发布消息均使用新的通道
	p.channel, err = p.conn.Channel()
	if err != nil {
		return fmt.Errorf("获取新的channel失败：%s", err.Error())
	}
	//消息发送结束关闭通道
	defer p.CloseChannel()

	//尝试创建交换机 kind=topic（不能修改）
	err = p.channel.ExchangeDeclare(p.Exchange, "topic", false, false, false, false, nil)
	if err != nil {
		return fmt.Errorf("创建交换机失败：%s", err.Error())
	}

	// 设置监听器来处理无法被正确路由的消息：当 mandatory 参数设为 true 时，交换器无法根据自身的类型和路由键找到一个符合条件的队列，那么 RabbitMQ 会调用 Basic.Return 命令将消息返回给生产者，NotifyReturn 功能只对直接交换器（Direct Exchange）和主题交换器（Topic Exchange）有效
	// 需要结合参数 mandatory=true一起使用
	p.notifyReturnListen()

	// 设置监听器来处理已经正确被推送到交换机（Rabbitmq服务器）
	p.notifyPublishListen()

	//发送消息
	err = p.channel.Publish(
		p.Exchange,
		p.Key, // 必须设置路由key，交换机根据key的规则模糊匹配到对应的队列,由队列的监听消费者接收消息消费
		true,  //设置为true时交换机会把错误的消息推送到注册的监听器 p.channel.NotifyReturn(make(chan amqp.Return))
		false,
		amqp.Publishing{
			DeliveryMode: amqp.Persistent, //消息持久化
			ContentType:  "text/plain",
			Body:         []byte(message),
			MessageId:    uuid.New().String(),
		},
	)
	if err != nil {
		return fmt.Errorf("发送消息给交换机失败：%s", err.Error())
	}

	return nil
}

// ================================= 延迟队列 ================================= //

// NewProducerDelay 实例化一个延迟消息生产者，通过ttl+死信交换机实现
func NewProducerDelay(exchangeName string, queueName string) (produce *Producer, err error) {
	// 初始化实例
	produce = &Producer{
		Exchange:           exchangeName,
		QueueName:          queueName,
		retryConnSleepTime: 2,
	}

	// 获取连接
	produce.conn, err = pool.ConnectionPool.GetConnection()
	if err != nil {
		return produce, fmt.Errorf("获取connection失败：%s", err.Error())
	}
	return produce, nil
}

// SendMessageDelay 发送延迟消息
func (p *Producer) SendMessageDelay(message string, ttlSecond int) (err error) {
	// 检测连接是否正常
	err = p.CheckOrGetNewConnection()
	if err != nil {
		return err
	}

	//每次发布消息均使用新的通道
	p.channel, err = p.conn.Channel()
	if err != nil {
		return fmt.Errorf("获取新的channel失败：%s", err.Error())
	}
	defer p.CloseChannel() //消息发送结束关闭通道

	// 指定死信交换机和死信队列名称
	deadExchangeName := p.Exchange + ".dead"
	deadQueueName := p.QueueName + ".dead"

	//【消息消费者消费时只需要监听过期的队列】声明消息过期后进入的交换机、队列，并且将队列和交换机进行绑定
	// ---------------- 死信交换机和队列声明绑定 ----------------
	err = p.channel.ExchangeDeclare(deadExchangeName, "direct", true, false, false, false, nil)
	if err != nil {
		return fmt.Errorf("DeadExchangeDeclare error：%s", err.Error())
	}

	_, err = p.channel.QueueDeclare(deadQueueName, true, false, false, false, nil)
	if err != nil {
		return fmt.Errorf("DeadQueueDeclare error：%s", err.Error())
	}

	err = p.channel.QueueBind(deadQueueName, "", deadExchangeName, false, nil)
	if err != nil {
		return fmt.Errorf("DeadQueueBindExchange error：%s", err.Error())
	}

	// ---------------- 消息实际进入的交换机和队列声明绑定 ----------------
	//【消息生产者只需要把消息放入普通的队列，并且需要声明消息过期后需要把消息放入的交换机名称】声明一个普通的交换机、队列，并且将队列和交换机进行绑定
	err = p.channel.ExchangeDeclare(p.Exchange, "direct", true, false, false, false, nil)
	if err != nil {
		return fmt.Errorf("ExchangeDeclare error：%s", err.Error())
	}

	_, err = p.channel.QueueDeclare(p.QueueName, true, false, false, false, amqp.Table{
		//"x-message-ttl":          10000, //消息过期时间设置，与消息发送时设置的值功能一致，二者同时存在时取其中最小的过期时间
		"x-dead-letter-exchange": deadExchangeName, //死信交换机名称
	})
	if err != nil {
		return fmt.Errorf("QueueDeclare error：%s", err.Error())
	}

	err = p.channel.QueueBind(p.QueueName, "", p.Exchange, false, nil)
	if err != nil {
		return fmt.Errorf("QueueBindExchange error：%s", err.Error())
	}

	// 需要结合参数 mandatory=true一起使用
	p.notifyReturnListen()

	// 设置监听器来处理已经正确被推送到交换机（Rabbitmq服务器）
	p.notifyPublishListen()

	//调用channel发送消息到队列中
	err = p.channel.Publish(
		p.Exchange,
		"",
		true,  //如果为true，根据自身exchange类型和routeKey规则无法找到符合条件的队列会把消息返还给发送者
		false, //如果为true，当exchange发送消息到队列后发现队列上没有消费者，则会把消息返还给发送者
		amqp.Publishing{
			DeliveryMode: amqp.Persistent, //消息持久化
			ContentType:  "text/plain",
			Body:         []byte(message),
			MessageId:    uuid.New().String(),
			Expiration:   strconv.Itoa(ttlSecond * 1000), // 设置TTL（毫秒），ttlSecond=传参数（单位秒）
		},
	)
	if err != nil {
		return fmt.Errorf("消息发送失败：%s", err.Error())
	}
	return nil
}
