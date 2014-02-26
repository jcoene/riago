package riago

import (
	"time"
)

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

// Perform a Riak List Buckets request. The protobufs say that it will return
// multiple responses but it in fact does not.
func (c *Client) ListBuckets(req *RpbListBucketsReq) (resp *RpbListBucketsResp, err error) {
	prof := NewProfile("list_buckets", "")
	defer c.instrument(prof, err)

	resp = &RpbListBucketsResp{}
	err = c.do(MsgRpbListBucketsReq, req, resp, prof)

	return
}

// Perform a Riak List Keys request. Returns multiple list keys responses.
func (c *Client) ListKeys(req *RpbListKeysReq) (resps []*RpbListKeysResp, err error) {
	prof := NewProfile("list_keys", string(req.GetBucket()))
	defer c.instrument(prof, err)

	err = c.with(func(conn *Conn) (e error) {
		// Issue the ListKeys request once
		t := time.Now()
		if e = conn.request(MsgRpbListKeysReq, req); e != nil {
			return
		}
		prof.Request = time.Now().Sub(t)

		// MapRed may produce multiple responses
		resps = make([]*RpbListKeysResp, 0)
		for {
			// Receive the next response
			resp := &RpbListKeysResp{}
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
