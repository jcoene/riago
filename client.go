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
	instrumenter  func(*Profile)
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

// SetInstrumenter establishes an instrument function to be called after each
// operation and given a payload of operation profile data.
func (c *Client) SetInstrumenter(fn func(*Profile)) {
	c.instrumenter = fn
}

// Performs a Riak Get request.
func (c *Client) Get(req *RpbGetReq) (resp *RpbGetResp, err error) {
	prof := NewProfile("get", string(req.GetBucket()))
	defer c.instrument(prof, err)

	resp = &RpbGetResp{}
	err = c.retry(func() error {
		return c.do(MsgRpbGetReq, req, resp, prof)
	}, prof)

	return
}

// Performs a Riak Put request.
func (c *Client) Put(req *RpbPutReq) (resp *RpbPutResp, err error) {
	prof := NewProfile("put", string(req.GetBucket()))
	defer c.instrument(prof, err)

	resp = &RpbPutResp{}
	err = c.retry(func() error {
		return c.do(MsgRpbPutReq, req, resp, prof)
	}, prof)

	return
}

// Performs a Riak Del request.
func (c *Client) Del(req *RpbDelReq) (err error) {
	prof := NewProfile("del", string(req.GetBucket()))
	defer c.instrument(prof, err)

	err = c.retry(func() error {
		return c.do(MsgRpbDelReq, req, nil, prof)
	}, prof)

	return
}

// Perform a Riak Get Bucket request.
func (c *Client) GetBucket(req *RpbGetBucketReq) (resp *RpbGetBucketResp, err error) {
	prof := NewProfile("get_bucket", string(req.GetBucket()))
	defer c.instrument(prof, err)

	resp = &RpbGetBucketResp{}
	err = c.retry(func() error {
		return c.do(MsgRpbGetBucketReq, req, resp, prof)
	}, prof)

	return
}

// Perform a Riak Set Bucket request.
func (c *Client) SetBucket(req *RpbSetBucketReq) (err error) {
	prof := NewProfile("set_bucket", string(req.GetBucket()))
	defer c.instrument(prof, err)

	err = c.retry(func() error {
		return c.do(MsgRpbSetBucketReq, req, nil, prof)
	}, prof)

	return
}

// Perform a Riak List Buckets request.
func (c *Client) ListBuckets(req *RpbListBucketsReq) (resp *RpbListBucketsResp, err error) {
	prof := NewProfile("list_buckets", "")
	defer c.instrument(prof, err)

	resp = &RpbListBucketsResp{}
	err = c.do(MsgRpbListBucketsReq, req, resp, prof)

	return
}

// Perform a Riak Map Reduce request. Returns multiple map-reduce responses.
func (c *Client) MapRed(req *RpbMapRedReq) (resps []*RpbMapRedResp, err error) {
	prof := NewProfile("map_red", "")
	defer c.instrument(prof, err)

	err = c.with(func(conn *Conn) (e error) {
		// Issue the MapRed request once
		t := time.Now()
		if e = conn.request(MsgRpbMapRedReq, req); e != nil {
			return
		}
		prof.Request = time.Now().Sub(t)

		// MapRed may produce multiple responses
		resps = make([]*RpbMapRedResp, 0)
		for {
			// Receive the next response
			resp := &RpbMapRedResp{}
			t = time.Now()
			if e = conn.response(resp); e != nil {
				return
			}
			prof.Response += time.Now().Sub(t)

			// Add the response to the result
			resps = append(resps, resp)

			// Stop receiving responses if the server tells us we're done
			if resp.GetDone() {
				break
			}
		}

		return
	}, prof)

	return
}

// Performs a single request with a single response
func (c *Client) do(code byte, req proto.Message, resp proto.Message, prof *Profile) (err error) {
	err = c.with(func(conn *Conn) (e error) {
		t := time.Now()
		if e = conn.request(code, req); e != nil {
			return
		}
		prof.Request = time.Now().Sub(t)

		t = time.Now()
		if e = conn.response(resp); e != nil {
			return
		}
		prof.Response = time.Now().Sub(t)

		return
	}, prof)

	return
}

// Gets and prepares a connection, yields it to the given function and returns the error.
func (c *Client) with(fn func(*Conn) error, prof *Profile) (err error) {
	var conn *Conn

	t := time.Now()
	if conn, err = c.pool.Get(); err != nil {
		return
	}
	prof.ConnWait = time.Now().Sub(t)

	t = time.Now()
	conn.lock()
	prof.ConnLock = time.Now().Sub(t)

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
func (c *Client) retry(fn func() error, prof *Profile) (err error) {
	for i := 0; i <= c.retryAttempts; i++ {
		if i > 0 {
			prof.Retries += 1
		}

		if err = fn(); err == nil {
			return
		}

		if c.retryDelay > 0 {
			<-time.After(c.retryDelay)
		}
	}

	return
}

// Send a profile to the instrumenter
func (c *Client) instrument(prof *Profile, err error) {
	if c.instrumenter != nil {
		prof.Error = err
		prof.Total = time.Now().Sub(prof.start)
		c.instrumenter(prof)
	}
}
