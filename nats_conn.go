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
	uuid    string

	readDeadLine  time.Time
	writeDeadLine time.Time
}

// NewNatsNetConn returns a new NatsNetConn instance.
func NewNatsNetConn(nc *nats.Conn, subject string, addr *net.TCPAddr) (*NatsNetConn, error) {
	// generate a UUID for the connection to prevent message collisions
	uuid, err := _UUIDFromCryptoRand()
	if err != nil {
		return nil, fmt.Errorf("generate uuid: %w", err)
	}
	return &NatsNetConn{nc: nc, subject: subject, addr: addr, uuid: uuid}, nil
}

const (
	networkHeaderKey        = "network"
	addrHeaderKey           = "addr"
	errHeaderKey            = "err"
	readSizeHeaderKey       = "read-size"
	readDeadlineHeaderKey   = "read-deadline"
	writeDeadlineHeaderKey  = "write-deadline"
	connectionUUIDHeaderKey = "conn-uuid"
)

const readSuffix = ".read"

// Read reads data from the underlying nats.Conn into the provided byte slice.
func (c *NatsNetConn) Read(b []byte) (n int, err error) {
	newMsg := nats.NewMsg(c.subject + readSuffix)
	newMsg.Header.Set(readSizeHeaderKey, fmt.Sprintf("%d", len(b)))
	newMsg.Header.Set(networkHeaderKey, c.addr.Network())
	newMsg.Header.Set(addrHeaderKey, c.addr.String())
	newMsg.Header.Set(readDeadlineHeaderKey, c.readDeadLine.Format(time.RFC3339Nano))
	newMsg.Header.Set(connectionUUIDHeaderKey, c.uuid)

	rd := time.Until(c.readDeadline())
	msg, err := c.nc.RequestMsg(newMsg, rd)
	if err != nil {
		return 0, fmt.Errorf("nats request: %w", err)
	}
	msgErr := msg.Header.Get(errHeaderKey)
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
	newMsg := nats.NewMsg(c.subject + writeSuffix)
	newMsg.Header.Set(networkHeaderKey, c.addr.Network())
	newMsg.Header.Set(addrHeaderKey, c.addr.String())
	newMsg.Header.Set(writeDeadlineHeaderKey, c.writeDeadLine.Format(time.RFC3339Nano))
	newMsg.Header.Set(connectionUUIDHeaderKey, c.uuid)
	newMsg.Data = slices.Clone(b)

	rd := time.Until(c.writeDeadline())
	msg, err := c.nc.RequestMsg(newMsg, rd)
	if err != nil {
		return 0, fmt.Errorf("nats request: %w", err)
	}
	msgErr := msg.Header.Get(errHeaderKey)
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
