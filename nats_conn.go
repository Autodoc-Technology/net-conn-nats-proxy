package redis_nats_proxy

import (
	"fmt"
	"github.com/nats-io/nats.go"
	"net"
	"slices"
	"strconv"
	"time"
)

// _ is a variable of type net.Conn
// It is used to assert that the type NatsNetConn implements the net.Conn interface.
var _ net.Conn = &NatsNetConn{}

// NatsNetConn is a type that wraps a nats.Conn and provides methods for reading, writing, closing,
// and managing deadlines on network connections.
type NatsNetConn struct {
	nc      *nats.Conn
	subject string
	addr    *net.TCPAddr

	readDeadLine  time.Time
	writeDeadLine time.Time
}

// NewNatsNetConn returns a new NatsNetConn instance.
func NewNatsNetConn(nc *nats.Conn, subject string, addr *net.TCPAddr) *NatsNetConn {
	return &NatsNetConn{nc: nc, subject: subject, addr: addr}
}

const (
	natsNetworkHeaderKey  = "network"
	natsAddrHeaderKey     = "addr"
	natsErrHeaderKey      = "err"
	natsReadSizeHeaderKey = "read-size"
)

const readSuffix = ".read"

// Read reads data from the underlying nats.Conn into the provided byte slice.
func (c *NatsNetConn) Read(b []byte) (n int, err error) {
	rd := c.readDeadline().Sub(time.Now())
	newMsg := nats.NewMsg(c.subject + readSuffix)
	newMsg.Header.Set(natsReadSizeHeaderKey, fmt.Sprintf("%d", len(b)))
	newMsg.Header.Set(natsNetworkHeaderKey, c.addr.Network())
	newMsg.Header.Set(natsAddrHeaderKey, c.addr.String())
	msg, err := c.nc.RequestMsg(newMsg, rd)
	if err != nil {
		return 0, fmt.Errorf("nats request: %w", err)
	}
	msgErr := msg.Header.Get(natsErrHeaderKey)
	if msgErr != "" {
		return 0, fmt.Errorf("nats error: %s", msgErr)
	}
	if len(msg.Data) > len(b) {
		return 0, fmt.Errorf("message too long")
	}
	copy(b, msg.Data)
	return len(msg.Data), nil
}

const writeSuffix = ".write"

// Write writes the provided byte slice to the underlying nats.Conn.
func (c *NatsNetConn) Write(b []byte) (n int, err error) {
	rd := c.writeDeadline().Sub(time.Now())
	newMsg := nats.NewMsg(c.subject + writeSuffix)
	newMsg.Header.Set(natsNetworkHeaderKey, c.addr.Network())
	newMsg.Header.Set(natsAddrHeaderKey, c.addr.String())
	newMsg.Data = slices.Clone(b)
	msg, err := c.nc.RequestMsg(newMsg, rd)
	if err != nil {
		return 0, fmt.Errorf("nats request: %w", err)
	}
	msgErr := msg.Header.Get(natsErrHeaderKey)
	if msgErr != "" {
		return 0, fmt.Errorf("nats error: %s", msgErr)
	}
	wl, err := strconv.Atoi(string(msg.Data))
	if err != nil {
		return 0, fmt.Errorf("parse write length: %w", err)
	}
	return wl, nil
}

func (c *NatsNetConn) Close() error {
	return nil
}

func (c *NatsNetConn) LocalAddr() net.Addr {
	return nil
}

func (c *NatsNetConn) RemoteAddr() net.Addr {
	return c.addr
}

func (c *NatsNetConn) SetDeadline(t time.Time) error {
	if err := c.SetReadDeadline(t); err != nil {
		return fmt.Errorf("set read deadline: %w", err)
	}
	if err := c.SetWriteDeadline(t); err != nil {
		return fmt.Errorf("set write deadline: %w", err)
	}
	return nil
}

func (c *NatsNetConn) SetReadDeadline(t time.Time) error {
	c.readDeadLine = t
	return nil
}

func (c *NatsNetConn) writeDeadline() time.Time {
	return c.writeDeadLine
}

func (c *NatsNetConn) SetWriteDeadline(t time.Time) error {
	c.writeDeadLine = t
	return nil
}

func (c *NatsNetConn) readDeadline() time.Time {
	return c.readDeadLine
}
