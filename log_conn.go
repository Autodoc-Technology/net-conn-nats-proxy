package redis_nats_proxy

import (
	"fmt"
	"log/slog"
	"net"
	"slices"
	"time"
)

// `_` is a variable of type `net.Conn` that is assigned the address of a `DebugLogNetConn` object.
// `DebugLogNetConn` is a type that implements the `net.Conn` interface, providing methods for reading, writing, closing, and managing deadlines on network connections.
// The `Read` method reads data from the underlying connection, while logging debug information before and after the read operation.
// The `Write` method writes data to the underlying connection, while logging debug information before and after the write operation.
// The `Close` method closes the connection and logs debug information.
// The `LocalAddr` method returns the local network address of the connection.
// The `RemoteAddr` method returns the remote network address of the connection.
// The `SetDeadline` method sets the deadline for future network operations and logs debug information.
// The `SetReadDeadline` method sets the deadline for future read operations and logs debug information.
// The `SetWriteDeadline` method sets the deadline for future write operations and logs debug information.
var _ net.Conn = &DebugLogNetConn{}

// DebugLogNetConn is a type that wraps a net.Conn and logs debugging information for read, write, and close operations.
// Read reads data from the connection and logs the bytes read and the length of the buffer.
type DebugLogNetConn struct {
	conn net.Conn
}

// NewDebugLogNetConn returns a new DebugLogNetConn instance.
// It wraps the provided net.Conn and adds debug logging to Read,
// Write, Close, LocalAddr, RemoteAddr, SetDeadline,
// SetReadDeadline, and SetWriteDeadline methods.
func NewDebugLogNetConn(conn net.Conn) *DebugLogNetConn {
	return &DebugLogNetConn{conn: conn}
}

// Read reads data from the underlying net.Conn into the provided byte slice.
func (lc *DebugLogNetConn) Read(b []byte) (n int, err error) {
	read, err := lc.conn.Read(b)
	if err != nil {
		return 0, fmt.Errorf("read: %w", err)
	}

	bb := slices.Compact(slices.Clone(b))
	defer slog.Debug("read", "bytes", bb, "len", len(b))
	return read, nil
}

// Write writes the provided byte slice to the underlying net.Conn.
// Before writing, the byte slice is compacted and cloned.
// After writing, debug logging is performed, including the number of bytes written and the length of the original byte slice.
func (lc *DebugLogNetConn) Write(b []byte) (n int, err error) {
	bb := slices.Compact(slices.Clone(b))
	defer slog.Debug("write", "bytes", bb, "len", len(b))
	return lc.conn.Write(b)
}

// Close closes the underlying net.Conn and logs a debug message.
func (lc *DebugLogNetConn) Close() error {
	defer slog.Debug("close connection")
	return lc.conn.Close()
}

// LocalAddr returns the local network address.
func (lc *DebugLogNetConn) LocalAddr() net.Addr {
	return lc.conn.LocalAddr()
}

// RemoteAddr returns the remote network address of the underlying net.Conn connection.
func (lc *DebugLogNetConn) RemoteAddr() net.Addr {
	return lc.conn.RemoteAddr()
}

// SetDeadline sets the deadline for all I/O operations on the underlying net.Conn.
// The deadline is an absolute time after which I/O operations will fail with a timeout error.
// If `t` is in the past, I/O operations will fail immediately with a timeout error.
//
// The logs the deadline information including the deadline time and the time remaining until the deadline.
//
// This method returns an error if the net.Conn implementation returns an error when setting the deadline.
func (lc *DebugLogNetConn) SetDeadline(t time.Time) error {
	defer slog.Debug("deadline", "time", t, "diff", t.Sub(time.Now()))
	return lc.conn.SetDeadline(t)
}

// SetReadDeadline sets the read deadline for the underlying net.Conn.
// After the specified time, if no data is read, the Read operation will return with an error.
// The time difference between the specified time and the current time is logged.
// It returns an error if there was an error while setting the read deadline.
func (lc *DebugLogNetConn) SetReadDeadline(t time.Time) error {
	defer slog.Debug("read deadline", "time", t, "diff", time.Until(t))
	return lc.conn.SetReadDeadline(t)
}

// SetWriteDeadline sets the write deadline for the current DebugLogNetConn.
// The provided time `t` specifies the deadline.
// After the deadline, any write operation will fail with a timeout error.
// The difference between the current time and the deadline time is logged using slog.Debug.
func (lc *DebugLogNetConn) SetWriteDeadline(t time.Time) error {
	defer slog.Debug("write deadline", "time", t, "diff", time.Until(t))
	return lc.conn.SetWriteDeadline(t)
}
