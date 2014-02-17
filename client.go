package riago

import (
	"time"

	"code.google.com/p/goprotobuf/proto"
)

// Client represents a Riak client instance.
type Client struct {
	pool          *Pool
	retryAttempts int
	retryDelay    time.Duration
	readTimeout   time.Duration
	writeTimeout  time.Duration
}

// NewClient creates a new Riago client with a given address and pool count.
func NewClient(addr string, count int) (c *Client) {
	return &Client{
		pool:          NewPool(addr, count),
		retryAttempts: 0,
		retryDelay:    500 * time.Millisecond,
	}
}

// SetRetryAttempts sets the number of times an operation will be retried before
// returning an error.
func (c *Client) SetRetryAttempts(n int) {
	c.retryAttempts = n
}

// SetRetryDelay sets the delay between retries.
func (c *Client) SetRetryDelay(dur time.Duration) {
	c.retryDelay = dur
}

// SetReadTimeout establishes a timeout deadline for all connection reads.
func (c *Client) SetReadTimeout(dur time.Duration) {
	c.readTimeout = dur
}

// SetWriteTimeout establishes a timeout deadline for all connection write.
func (c *Client) SetWriteTimeout(dur time.Duration) {
	c.writeTimeout = dur
}

// SetWaitTimeout establishes a timeout deadline for how long to wait for
// a connection to become available from the pool before returning an error.
func (c *Client) SetWaitTimeout(dur time.Duration) {
	c.pool.waitTimeout = dur
}

// Performs a Riak Get request.
func (c *Client) Get(req *RpbGetReq) (resp *RpbGetResp, err error) {
	resp = &RpbGetResp{}
	err = c.retry(func() error {
		return c.do(MsgRpbGetReq, req, resp)
	})
	return
}

// Performs a Riak Put request.
func (c *Client) Put(req *RpbPutReq) (resp *RpbPutResp, err error) {
	resp = &RpbPutResp{}
	err = c.retry(func() error {
		return c.do(MsgRpbPutReq, req, resp)
	})
	return
}

// Performs a Riak Del request.
func (c *Client) Del(req *RpbDelReq) (err error) {
	err = c.retry(func() error {
		return c.do(MsgRpbDelReq, req, nil)
	})
	return
}

// Perform a Riak Get Bucket request.
func (c *Client) GetBucket(req *RpbGetBucketReq) (resp *RpbGetBucketResp, err error) {
	resp = &RpbGetBucketResp{}
	err = c.retry(func() error {
		return c.do(MsgRpbGetBucketReq, req, resp)
	})
	return
}

// Perform a Riak Set Bucket request.
func (c *Client) SetBucket(req *RpbSetBucketReq) (err error) {
	err = c.retry(func() error {
		return c.do(MsgRpbSetBucketReq, req, nil)
	})
	return
}

// Perform a Riak List Buckets request.
func (c *Client) ListBuckets(req *RpbListBucketsReq) (resp *RpbListBucketsResp, err error) {
	resp = &RpbListBucketsResp{}
	err = c.do(MsgRpbListBucketsReq, req, resp)
	return
}

// Performs a single request with a single response
func (c *Client) do(code byte, req proto.Message, resp proto.Message) (err error) {
	err = c.with(func(conn *Conn) (e error) {
		if e = conn.request(code, req); e == nil {
			e = conn.response(resp)
		}

		return
	})

	return
}

// Gets and prepares a connection, yields it to the given function and returns the error.
func (c *Client) with(fn func(*Conn) error) (err error) {
	var conn *Conn

	if conn, err = c.pool.Get(); err != nil {
		return
	}

	conn.lock()

	conn.readTimeout = c.readTimeout
	conn.writeTimeout = c.writeTimeout

	if err = fn(conn); err != nil {
		conn.close()
		conn.unlock()
		c.pool.Fail(conn)
		return
	}

	conn.unlock()
	c.pool.Put(conn)

	return
}

// Retries a function multiple times until it does not return an error.
func (c *Client) retry(fn func() error) (err error) {
	for i := 0; i <= c.retryAttempts; i++ {
		if err = fn(); err == nil {
			return
		}

		if c.retryDelay > 0 {
			<-time.After(c.retryDelay)
		}
	}

	return
}
