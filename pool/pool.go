package pool

import (
	"fmt"
	"github.com/streadway/amqp"
	"rabbitmq"
	"sync"
)

// ConnectionPool 管理Rabbitmq的连接池，目的是为了随时能够获取有效的连接，常规操作连接池中只需要存在两个有效连接即可，mq连接可复用
var ConnectionPool = &connectionPool{}

type connectionPool struct {
	mutex       sync.Mutex         //因为使用了数组管理连接池，所以当连接池中的连接无效后，需要替换新的连接必须加锁修改结构体中的属性值connections，防止并发修改同一个数据异常
	connections []*amqp.Connection // 使用数组管理连接池，在获取到连接使用后无需关闭连接，复用连接
	url         string
	size        int //因为mq连接可以复用，因此基本上只会用到第一个有效连接
}

// InitConnectionPool 初始mq化连接池
func InitConnectionPool(url string, size int) error {
	if size > rabbitmq.MaxPoolSize {
		size = rabbitmq.MaxPoolSize
	}

	// 初始化配置信息
	ConnectionPool.url = url
	ConnectionPool.size = size
	ConnectionPool.connections = make([]*amqp.Connection, 0, size)

	for i := 0; i < size; i++ {
		conn, err := newConnectionRabbitmq(url)
		if err != nil {
			return err
		}
		if IsConnHealthy(conn) {
			ConnectionPool.connections = append(ConnectionPool.connections, conn)
			fmt.Printf("Rabbitmq初始化连接池：%s \r\n", conn.LocalAddr())
		}
	}

	fmt.Printf("Rabbitmq连接池初始化: initSize=%d；successInitSize=%d, url=%s \r\n", size, len(ConnectionPool.connections), url)

	return nil
}

// newConnectionRabbitmq 建立一个新的mq连接
func newConnectionRabbitmq(url string) (*amqp.Connection, error) {
	return amqp.Dial(url)
}

// IsConnHealthy 判断连接是否有效
func IsConnHealthy(conn *amqp.Connection) bool {
	return conn != nil && conn.IsClosed() == false
}

// GetConnection 获取Mq连接
func (pool *connectionPool) GetConnection() (*amqp.Connection, error) {
	pool.mutex.Lock()
	defer pool.mutex.Unlock()

	for _, conn := range pool.connections {
		if IsConnHealthy(conn) {
			return conn, nil
		}
	}

	//当连接池为空时，重新初始化连接池
	err := InitConnectionPool(ConnectionPool.url, ConnectionPool.size)
	if err != nil {
		return nil, err
	}

	//返回第一个有效的连接
	if len(ConnectionPool.connections) > 0 {
		conn := ConnectionPool.connections[0]
		if IsConnHealthy(conn) {
			return conn, nil
		}
	}

	return nil, fmt.Errorf("连接池暂无有效的连接：%d", len(ConnectionPool.connections))
}

// CloseConnection 关闭全部连接
func (pool *connectionPool) CloseConnection() {
	for _, conn := range pool.connections {
		if IsConnHealthy(conn) {
			err := conn.Close()
			if err != nil {
				fmt.Printf("connection[%s] close error: %s \r\n", conn.LocalAddr(), err.Error())
			} else {
				fmt.Printf("connection[%s] close success \r\n", conn.LocalAddr())
			}
		}
	}
	fmt.Println("connections all close success")
}
