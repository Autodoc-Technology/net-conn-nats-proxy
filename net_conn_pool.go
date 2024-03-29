package redis_nats_proxy

import (
	"fmt"
	"net"
	"sync"
)

// DialFn represents a function that dials a network address.
type DialFn func(network, addr string) (net.Conn, error)

// DefaultDial is the default dial function that dials a network address using net.Dial.
var DefaultDial DialFn = net.Dial

// NetConnPullManager represents a pool manager for network connections.
type NetConnPullManager struct {
	mu   sync.Mutex
	pool map[string]connEnvelop
	dial DialFn
}

// NewNetConnPullManager creates a new instance of NetConnPullManager.
// It takes a DialFn function as a parameter, which is a function type for dialing a network connection,
// and returns a pointer to a NetConnPullManager.
// If fn is nil, it uses the DefaultDial function.
func NewNetConnPullManager(fn DialFn) *NetConnPullManager {
	if fn == nil {
		fn = DefaultDial
	}
	return &NetConnPullManager{pool: make(map[string]connEnvelop), dial: fn}
}

// Get retrieves a network connection from the NetConnPullManager pool based on the given address.
// It takes a *net.TCPAddr as the address parameter and returns a net.Conn and an error.
// The function first attempts to find the connection in the pool using the generated key from the address.
// If the connection is found, it is returned along with a nil error.
// If the connection is not found, the function uses the cp.dial function to create a new connection.
// If an error occurs while dialing, an error is returned with a formatted message.
// Otherwise, the newly created connection is added to the pool using the generated key,
// and the connection along with a nil error is returned.
func (cp *NetConnPullManager) Get(addr *net.TCPAddr) (net.Conn, error) {
	cp.mu.Lock()
	defer cp.mu.Unlock()

	key := addr.String()
	cEnv, ok := cp.pool[key]
	if ok {
		return cEnv, nil
	}

	conn, err := cp.dial(addr.Network(), addr.String())
	if err != nil {
		return nil, fmt.Errorf("dial: %w", err)
	}
	cp.pool[key] = connEnvelop{Conn: conn, pm: cp}
	return conn, nil
}

// Close closes all the connections in the NetConnPullManager pool.
// It iterates over each connection in the pool and calls the Close() method on them.
// If an error occurs while closing a connection, it returns an error with a formatted message.
// If all connections are successfully closed, it returns a nil error.
func (cp *NetConnPullManager) Close() error {
	for _, conn := range cp.pool {
		err := conn.Close()
		if err != nil {
			return fmt.Errorf("close connection: %w", err)
		}
	}
	return nil
}

// delete removes a connection from the NetConnPullManager pool based on the given key.
func (cp *NetConnPullManager) delete(key string) {
	cp.mu.Lock()
	defer cp.mu.Unlock()
	delete(cp.pool, key)
}

// connEnvelop represents a connection envelop that wraps a net.Conn instance and a NetConnPullManager pointer.
type connEnvelop struct {
	net.Conn
	pm *NetConnPullManager
}

// Close closes the connection and removes it from the NetConnPullManager pool.
func (ce connEnvelop) Close() error {
	ce.pm.delete(ce.Conn.RemoteAddr().String())
	return ce.Conn.Close()
}
