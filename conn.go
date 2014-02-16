package riago

import (
	"errors"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"code.google.com/p/goprotobuf/proto"
)

var (
	ErrInvalidResponseHeader  = errors.New("invalid response header")
	ErrIncompleteResponseBody = errors.New("incomplete response body")
)

// Conn represents an individual connection to a Riak host.
type Conn struct {
	addr         string
	conn         net.Conn
	ok           bool
	padlock      int32
	mutex        sync.Mutex
	dialTimeout  time.Duration
	readTimeout  time.Duration
	writeTimeout time.Duration
}

// Create a new Conn instance for the given address
func NewConn(addr string) *Conn {
	return &Conn{
		addr: addr,
	}
}

// Closes the connection, closing the socket (if open) and marking
// the connection as down.
func (c *Conn) Close() error {
	c.lock()
	defer c.unlock()
	return c.close()
}

// Attempts to recover a downed connection by re-dialing and marking
// the connection as up in the case of success.
func (c *Conn) Recover() error {
	c.lock()
	defer c.unlock()

	return c.dial()
}

// Attempts to connect to the Riak server. Must be called from within a lock.
func (c *Conn) dial() (err error) {
	c._assert_locked(true) // xxx: temp

	if c.dialTimeout > 0 {
		c.conn, err = net.DialTimeout("tcp", c.addr, c.dialTimeout)
	} else {
		c.conn, err = net.Dial("tcp", c.addr)
	}

	if err != nil {
		c.conn = nil
		return
	}

	c.ok = true

	return
}

// Closes the connection, closing the socket (if open) and marking
// the connection as down. Must be called from within a lock.
func (c *Conn) close() (err error) {
	c._assert_locked(true) // xxx: temp

	c.ok = false
	if c.conn != nil {
		err = c.conn.Close()
		c.conn = nil
	}

	return
}

// Encode and write a request to the Riak server. Must be called from
// within a lock.
func (c *Conn) request(code byte, req proto.Message) (err error) {
	c._assert_locked(true) // xxx: temp

	var buf []byte

	if c.conn == nil || !c.ok {
		if err = c.dial(); err != nil {
			return
		}
	}

	if buf, err = encode(code, req); err != nil {
		return
	}

	err = c.write(buf)

	return
}

// Read and decode a response from the Riak server. Must be called from
// within a lock.
func (c *Conn) response(resp proto.Message) (err error) {
	c._assert_locked(true) // xxx: temp

	var buf []byte

	if buf, err = c.read(); err != nil {
		return
	}

	if err = decode(buf, resp); err != nil {
		return
	}

	return
}

// Write a fully encoded buffer to the connection, establishing a deadline if
// a timeout is set.
func (c *Conn) write(buf []byte) (err error) {
	c._assert_locked(true) // xxx: temp

	if c.writeTimeout > 0 {
		c.conn.SetWriteDeadline(time.Now().Add(c.writeTimeout))
	}

	_, err = c.conn.Write(buf)
	return
}

// Read a length-prefixed buffer from the connection, establishing a deadline
// if a timeout is set.
func (c *Conn) read() (buf []byte, err error) {
	c._assert_locked(true) // xxx: temp

	var sizebuf []byte
	var size int
	var n int

	if c.readTimeout > 0 {
		c.conn.SetReadDeadline(time.Now().Add(c.readTimeout))
	}

	sizebuf = make([]byte, 4)
	if n, err = c.conn.Read(sizebuf); err != nil {
		return
	}

	if n != 4 {
		err = ErrInvalidResponseHeader
		return
	}

	size = int(sizebuf[0])<<24 + int(sizebuf[1])<<16 + int(sizebuf[2])<<8 + int(sizebuf[3])
	buf = make([]byte, size)

	if n, err = c.conn.Read(buf); err != nil {
		return
	}

	if n != size {
		err = ErrIncompleteResponseBody
	}

	return
}

// Temporary method to validate nesting of method calls
func (c *Conn) _assert_locked(b bool) {
	if (atomic.LoadInt32(&c.padlock) == 1) != b {
		panic("lock error")
	}
}

// Obtain a big lock on everything scary
func (c *Conn) lock() {
	c.mutex.Lock()
	atomic.StoreInt32(&c.padlock, 1)
}

// Release a big lock on everything scary
func (c *Conn) unlock() {
	c.mutex.Unlock()
	atomic.StoreInt32(&c.padlock, 0)
}
