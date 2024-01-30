package producer

import (
	"fmt"
	"github.com/google/uuid"
	"github.com/streadway/amqp"
	"rabbitmq/pool"
	"time"
)

// 参考文档：介绍rabbitmq的几种模式 https://www.topgoer.com/%E6%95%B0%E6%8D%AE%E5%BA%93%E6%93%8D%E4%BD%9C/go%E6%93%8D%E4%BD%9CRabbitMQ/RabbitMQ%E4%BB%8B%E7%BB%8D.html

// Producer 生产者
type Producer struct {
	conn               *amqp.Connection
	channel            *amqp.Channel
	retryConnSleepTime int    //断开连接后重连等待时长，默认2秒，发送消息可设置较短的重连间隔
	QueueName          string //队列名称
	Exchange           string //交换机名称
	Key                string //路由名称key
}

// ================================= 生产者通用方法 ================================= //

// CheckOrGetNewConnection 检测或者获取新的连接
func (p *Producer) CheckOrGetNewConnection() (err error) {
	if pool.IsConnHealthy(p.conn) == true {
		return nil
	}

	p.producerSimpleLog(fmt.Sprintf("无效的connection，%d 秒后获取新的连接", p.retryConnSleepTime))

	// 睡眠一下，提高重连的成功率
	time.Sleep(time.Duration(p.retryConnSleepTime) * time.Second)

	p.conn, err = pool.ConnectionPool.GetConnection()
	if err != nil {
		return fmt.Errorf("获取新的connection失败：%s", err.Error())
	}

	return nil
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
			p.producerSimpleLog(fmt.Sprintf("channel关闭失败：%s", err.Error()))
		}
	}
}

func (p *Producer) producerSimpleLog(logContent string) {
	fmt.Printf("【%s】生产者队列：%s \r\n", p.QueueName, logContent)
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
	err = p.channel.ExchangeDeclare(p.Exchange, "topic", true, false, false, false, nil)
	if err != nil {
		return fmt.Errorf("创建交换机失败：%s", err.Error())
	}

	//发送消息
	err = p.channel.Publish(
		p.Exchange,
		p.Key, // 必须设置路由key，交换机根据key的规则模糊匹配到对应的队列,由队列的监听消费者接收消息消费
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
