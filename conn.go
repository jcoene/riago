package riago

import (
	"io"
	"net"
	"sync"
	"time"

	"github.com/golang/protobuf/proto"
)

// Conn represents an individual connection to a Riak host.
type Conn struct {
	addr         string
	conn         *net.TCPConn
	ok           bool
	padlock      int32
	mutex        sync.Mutex
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

// Attempts to ping the remote server. Returns an error if the
// request/response fails.
func (c *Conn) Ping() (err error) {
	c.lock()
	defer c.unlock()

	if err = c.request(MsgRpbPingReq, nil); err != nil {
		return
	}

	err = c.response(nil)

	return
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
	var tcpAddr *net.TCPAddr

	if tcpAddr, err = net.ResolveTCPAddr("tcp", c.addr); err != nil {
		return
	}

	if c.conn, err = net.DialTCP("tcp", nil, tcpAddr); err != nil {
		return
	}

	if err != nil {
		c.conn = nil
		return
	}

	c.conn.SetKeepAlive(true)

	c.ok = true

	return
}

// Closes the connection, closing the socket (if open) and marking
// the connection as down. Must be called from within a lock.
func (c *Conn) close() (err error) {
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
	if c.writeTimeout > 0 {
		c.conn.SetWriteDeadline(time.Now().Add(c.writeTimeout))
	}

	_, err = c.conn.Write(buf)
	return
}

// Read a length-prefixed buffer from the connection, establishing a deadline
// if a timeout is set.
func (c *Conn) read() (buf []byte, err error) {
	var sizebuf []byte
	var size int

	if c.readTimeout > 0 {
		c.conn.SetReadDeadline(time.Now().Add(c.readTimeout))
	}

	sizebuf = make([]byte, 4)
	if _, err = io.ReadFull(c.conn, sizebuf); err != nil {
		return
	}

	size = int(sizebuf[0])<<24 + int(sizebuf[1])<<16 + int(sizebuf[2])<<8 + int(sizebuf[3])
	buf = make([]byte, size)

	if _, err = io.ReadFull(c.conn, buf); err != nil {
		return
	}

	return
}

// Obtain a big lock on everything scary
func (c *Conn) lock() {
	c.mutex.Lock()
}

// Release a big lock on everything scary
func (c *Conn) unlock() {
	c.mutex.Unlock()
}
