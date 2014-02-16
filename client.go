package riago

import (
	"time"

	"code.google.com/p/goprotobuf/proto"
)

// Client represents a Riak client instance.
type Client struct {
	pool         *Pool
	dialTimeout  time.Duration
	readTimeout  time.Duration
	writeTimeout time.Duration
}

// NewClient creates a new Riago client with a given address and pool count.
func NewClient(addr string, count int) (c *Client) {
	return &Client{
		pool: NewPool(addr, count),
	}
}

// SetReadTimeout establishes a timeout deadline for all connection reads.
func (c *Client) SetReadTimeout(dur time.Duration) {
	c.readTimeout = dur
}

// SetWriteTimeout establishes a timeout deadline for all connection write.
func (c *Client) SetWriteTimeout(dur time.Duration) {
	c.writeTimeout = dur
}

// SetDialTimeout establishes a timeout deadline for how long to wait for
// a connection to connect before returning an error.
func (c *Client) SetDialTimeout(dur time.Duration) {
	c.dialTimeout = dur
}

// SetWaitTimeout establishes a timeout deadline for how long to wait for
// a connection to become available from the pool before returning an error.
func (c *Client) SetWaitTimeout(dur time.Duration) {
	c.pool.waitTimeout = dur
}

// Performs a Riak Get request.
func (c *Client) Get(req *RpbGetReq) (resp *RpbGetResp, err error) {
	resp = &RpbGetResp{}
	err = c.do(MsgRpbGetReq, req, resp)
	return
}

// Performs a Riak Put request.
func (c *Client) Put(req *RpbPutReq) (resp *RpbPutResp, err error) {
	resp = &RpbPutResp{}
	err = c.do(MsgRpbPutReq, req, resp)
	return
}

// Perform a Riak Get Bucket request.
func (c *Client) GetBucket(req *RpbGetBucketReq) (resp *RpbGetBucketResp, err error) {
	resp = &RpbGetBucketResp{}
	err = c.do(MsgRpbGetBucketReq, req, resp)
	return
}

// Perform a Riak Set Bucket request.
func (c *Client) SetBucket(req *RpbSetBucketReq) (err error) {
	err = c.do(MsgRpbSetBucketReq, req, nil)
	return
}

func (c *Client) do(code byte, req proto.Message, resp proto.Message) (err error) {
	var conn *Conn

	if conn, err = c.pool.Get(); err != nil {
		return
	}

	conn.lock()
	defer conn.unlock()

	conn.dialTimeout = c.dialTimeout
	conn.readTimeout = c.readTimeout
	conn.writeTimeout = c.writeTimeout

	if err = conn.request(code, req); err != nil {
		conn.close()
		c.pool.Fail(conn)
		return
	}

	if err = conn.response(resp); err != nil {
		conn.close()
		c.pool.Fail(conn)
		return
	}

	c.pool.Put(conn)

	return
}
