package net_conn_nats_proxy

import (
	"context"
	"github.com/nats-io/nats.go"
	"net"
	"strconv"
	"time"
)

// NatsConnProxy represents a proxy for NATS connections.
// It is responsible for handling read and write requests from NATS messages and forwarding them to the appropriate network connections.
// NatsConnProxy uses the NetConnManager interface to manage network connections from a pool or create new ones.
// The proxy starts handling requests by calling the Start method, which takes a context.Context as a parameter and returns an error if any occurs.
//
// Example usage:
//
// ncp := NewNatsConnProxy(nc, subject, connPool)
// err := ncp.Start(ctx)
type NatsConnProxy struct {
	nc       *nats.Conn
	subject  string
	connPool NetConnManager

	stopHandler func()
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
	ncp := &NatsConnProxy{nc: nc, subject: subject, connPool: connPool}
	if connPool == nil {
		ncp.connPool = NewNetConnPullManager(DefaultDial)
		ncp.stopHandler = func() { _ = ncp.connPool.Close() }
	}
	return ncp
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
	closeSub, err := ncp.nc.Subscribe(ncp.subject+closeSuffix, ncp.closeHandler)
	if err != nil {
		return err
	}
	go func() {
		<-ctx.Done()
		_ = readSub.Unsubscribe()
		_ = writeSub.Unsubscribe()
		_ = closeSub.Unsubscribe()
		if ncp.stopHandler != nil {
			ncp.stopHandler()
		}
	}()
	return nil
}

// readHandler handles read requests from NATS messages by reading data from the network connection and responding with the read data.
func (ncp NatsConnProxy) readHandler(msg *nats.Msg) {
	network := msg.Header.Get(networkHeaderKey)
	addr := msg.Header.Get(addrHeaderKey)
	readSize := msg.Header.Get(readSizeHeaderKey)
	rdls := msg.Header.Get(readDeadlineHeaderKey)
	uuid := msg.Header.Get(connectionUUIDHeaderKey)

	conn, err := ncp.getNetConn(network, addr, uuid)
	if err != nil {
		msg.Header.Set(errHeaderKey, err.Error())
		_ = msg.Respond(nil)
		return
	}

	bufSize, err := strconv.Atoi(readSize)
	if err != nil {
		msg.Header.Set(errHeaderKey, err.Error())
		_ = msg.Respond(nil)
		return
	}
	buf := make([]byte, bufSize)
	if readDeadline, err := time.Parse(time.RFC3339Nano, rdls); err == nil && !readDeadline.IsZero() {
		_ = conn.SetReadDeadline(readDeadline)
	}
	n, err := conn.Read(buf)
	if err != nil {
		msg.Header.Set(errHeaderKey, err.Error())
		_ = msg.Respond(nil)
		return
	}
	_ = msg.Respond(buf[:n])
}

// zeroLenStr represents a byte array containing the value "0".
var zeroLenStr = []byte("0")

// writeHandler handles write requests from NATS messages by writing data to the network connection and responding with the number of bytes written.
func (ncp NatsConnProxy) writeHandler(msg *nats.Msg) {
	network := msg.Header.Get(networkHeaderKey)
	addr := msg.Header.Get(addrHeaderKey)
	wdls := msg.Header.Get(writeDeadlineHeaderKey)
	uuid := msg.Header.Get(connectionUUIDHeaderKey)

	conn, err := ncp.getNetConn(network, addr, uuid)
	if err != nil {
		msg.Header.Set(errHeaderKey, err.Error())
		_ = msg.Respond(zeroLenStr)
		return
	}

	if writeDeadline, err := time.Parse(time.RFC3339Nano, wdls); err == nil && !writeDeadline.IsZero() {
		_ = conn.SetWriteDeadline(writeDeadline)
	}
	n, err := conn.Write(msg.Data)
	if err != nil {
		msg.Header.Set(errHeaderKey, err.Error())
		_ = msg.Respond(zeroLenStr)
		return
	}
	_ = msg.Respond([]byte(strconv.Itoa(n)))
}

func (ncp NatsConnProxy) closeHandler(msg *nats.Msg) {
	network := msg.Header.Get(networkHeaderKey)
	addr := msg.Header.Get(addrHeaderKey)
	uuid := msg.Header.Get(connectionUUIDHeaderKey)

	conn, err := ncp.getNetConn(network, addr, uuid)
	if err != nil {
		msg.Header.Set(errHeaderKey, err.Error())
		_ = msg.Respond(nil)
		return
	}

	if err = conn.Close(); err != nil {
		msg.Header.Set(errHeaderKey, err.Error())
		_ = msg.Respond(nil)
		return
	}
	_ = msg.Respond(nil)
}

// getNetConn returns a net.Conn by resolving the TCP address and calling the Get method of the connPool with the specified UUID options.
func (ncp NatsConnProxy) getNetConn(network, addr, uuid string) (net.Conn, error) {
	tcpAddr, err := net.ResolveTCPAddr(network, addr)
	if err != nil {
		return nil, err
	}
	return ncp.connPool.Get(tcpAddr, WithUUID(uuid))
}
