package redis_nats_proxy

import (
	"context"
	"github.com/nats-io/nats.go"
	"io"
	"net"
	"strconv"
)

// NetConnManager represents an interface for managing network connections.
type NetConnManager interface {
	io.Closer
	// Get returns a connection from the pool or creates a new one.
	Get(addr *net.TCPAddr) (net.Conn, error)
}

// NatsConnProxy represents a type that proxies network connections over NATS.
type NatsConnProxy struct {
	nc       *nats.Conn
	subject  string
	connPool NetConnManager
}

// NewNatsConnProxy creates a new NatsConnProxy with the provided NATS connection, subject, and connection pool.
//
// The NatsConnProxy is responsible for handling read and write requests from NATS messages and forwarding them to the corresponding network connections.
//
// Parameters:
// - nc: The NATS connection to use for communication.
// - subject: The subject to listen for NATS messages on.
// - connPool: The connection pool to use for network connections.
//
// Returns:
// - *NatsConnProxy: The created NatsConnProxy instance.
func NewNatsConnProxy(nc *nats.Conn, subject string, connPool NetConnManager) *NatsConnProxy {
	return &NatsConnProxy{
		nc:       nc,
		subject:  subject,
		connPool: connPool,
	}
}

// NewNatsConnProxyWithDefaultConnManager creates a new NatsConnProxy with the provided NATS connection and subject, using the default connection manager.
// The NatsConnProxy is responsible for handling read and write requests from NATS messages and forwarding them to the corresponding network connections.
func NewNatsConnProxyWithDefaultConnManager(nc *nats.Conn, subject string) *NatsConnProxy {
	return &NatsConnProxy{nc: nc, subject: subject, connPool: NewNetConnPullManager(DefaultDial)}
}

// Start starts the NatsConnProxy instance by subscribing to NATS messages for read and write requests and handling them.
//
// Parameters:
// - ctx: The context.Context used to control the execution of the method.
//
// Returns:
// - error: An error if there was a problem subscribing to the NATS messages, otherwise nil.
func (ncp NatsConnProxy) Start(ctx context.Context) error {
	readSub, err := ncp.nc.Subscribe(ncp.subject+readSuffix, ncp.readHandler)
	if err != nil {
		return err
	}
	writeSub, err := ncp.nc.Subscribe(ncp.subject+writeSuffix, ncp.writeHandler)
	if err != nil {
		return err
	}
	go func() {
		<-ctx.Done()
		_ = readSub.Unsubscribe()
		_ = writeSub.Unsubscribe()
	}()
	return nil
}

// readHandler handles read requests from NATS messages by reading data from the network connection and responding with the read data.
func (ncp NatsConnProxy) readHandler(msg *nats.Msg) {
	network := msg.Header.Get(natsNetworkHeaderKey)
	addr := msg.Header.Get(natsAddrHeaderKey)
	readSize := msg.Header.Get(natsReadSizeHeaderKey)

	tcpAddr, err := net.ResolveTCPAddr(network, addr)
	if err != nil {
		msg.Header.Set(natsErrHeaderKey, err.Error())
		_ = msg.Respond(nil)
		return
	}

	conn, err := ncp.connPool.Get(tcpAddr)
	if err != nil {
		msg.Header.Set(natsErrHeaderKey, err.Error())
		_ = msg.Respond(nil)
		return
	}

	bufSize, err := strconv.Atoi(readSize)
	if err != nil {
		msg.Header.Set(natsErrHeaderKey, err.Error())
		_ = msg.Respond(nil)
		return
	}
	buf := make([]byte, bufSize)
	n, err := conn.Read(buf)
	if err != nil {
		msg.Header.Set(natsErrHeaderKey, err.Error())
		_ = msg.Respond(nil)
		return
	}
	_ = msg.Respond(buf[:n])
}

// zeroLenStr represents a byte array containing the value "0".
var zeroLenStr = []byte("0")

// writeHandler handles write requests from NATS messages by writing data to the network connection and responding with the number of bytes written.
func (ncp NatsConnProxy) writeHandler(msg *nats.Msg) {
	network := msg.Header.Get(natsNetworkHeaderKey)
	addr := msg.Header.Get(natsAddrHeaderKey)

	tcpAddr, err := net.ResolveTCPAddr(network, addr)
	if err != nil {
		msg.Header.Set(natsErrHeaderKey, err.Error())
		_ = msg.Respond(zeroLenStr)
		return
	}

	conn, err := ncp.connPool.Get(tcpAddr)
	if err != nil {
		msg.Header.Set(natsErrHeaderKey, err.Error())
		_ = msg.Respond(zeroLenStr)
		return
	}

	n, err := conn.Write(msg.Data)
	if err != nil {
		msg.Header.Set(natsErrHeaderKey, err.Error())
		_ = msg.Respond(zeroLenStr)
		return
	}
	_ = msg.Respond([]byte(strconv.Itoa(n)))
}
