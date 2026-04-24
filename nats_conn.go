package net_conn_nats_proxy

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

// rawAddr implements net.Addr using plain network and address strings,
// without requiring prior DNS resolution.
type rawAddr struct {
	network string
	addr    string
}

func (a rawAddr) Network() string { return a.network }
func (a rawAddr) String() string  { return a.addr }

// NatsNetConn is a type that wraps a nats.Conn and provides methods for reading, writing, closing,
// and managing deadlines on network connections.
type NatsNetConn struct {
	nc      *nats.Conn
	subject string
	network string
	addr    string
	uuid    string

	readDeadLine  time.Time
	writeDeadLine time.Time
}

// NewNatsNetConn returns a new NatsNetConn instance from an already-resolved *net.TCPAddr.
// Use NewNatsNetConnAddr when the address may be a cluster-internal hostname that only the
// proxy server can resolve.
func NewNatsNetConn(nc *nats.Conn, subject string, addr *net.TCPAddr) (*NatsNetConn, error) {
	return NewNatsNetConnAddr(nc, subject, addr.Network(), addr.String())
}

// NewNatsNetConnAddr returns a new NatsNetConn instance using raw network and address strings.
// The address is forwarded as-is to the proxy server, which performs DNS resolution there.
// This allows cluster-internal hostnames (e.g. redis-node-1.internal:6379) to be resolved
// on the server side where the cluster DNS is reachable, rather than on the client side.
// Plain IP addresses (e.g. 192.168.1.10:6379) are handled transparently by the server.
func NewNatsNetConnAddr(nc *nats.Conn, subject, network, addr string) (*NatsNetConn, error) {
	uuid, err := _UUIDFromCryptoRand()
	if err != nil {
		return nil, fmt.Errorf("generate uuid: %w", err)
	}
	return &NatsNetConn{nc: nc, subject: subject, network: network, addr: addr, uuid: uuid}, nil
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
	newMsg.Header.Set(networkHeaderKey, c.network)
	newMsg.Header.Set(addrHeaderKey, c.addr)
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
	newMsg.Header.Set(networkHeaderKey, c.network)
	newMsg.Header.Set(addrHeaderKey, c.addr)
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

const closeSuffix = ".close"

func (c *NatsNetConn) Close() error {
	newMsg := nats.NewMsg(c.subject + closeSuffix)
	newMsg.Header.Set(networkHeaderKey, c.network)
	newMsg.Header.Set(addrHeaderKey, c.addr)
	newMsg.Header.Set(connectionUUIDHeaderKey, c.uuid)

	msg, err := c.nc.RequestMsg(newMsg, time.Second)
	if err != nil {
		return fmt.Errorf("nats request: %w", err)
	}
	msgErr := msg.Header.Get(errHeaderKey)
	if msgErr != "" {
		return fmt.Errorf("nats error: %s", msgErr)
	}
	return nil
}

func (c *NatsNetConn) LocalAddr() net.Addr {
	return nil
}

func (c *NatsNetConn) RemoteAddr() net.Addr {
	return rawAddr{network: c.network, addr: c.addr}
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
