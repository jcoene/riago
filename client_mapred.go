package riago

import (
	"encoding/json"
	"time"
)

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

// GetManyJson is a convenience method that uses map-reduce with
// Riak built-in erlang functions to get many documents at once.
//
// It takes a bucket and list of keys and returns a slice of strings
// that are expected to be JSON-encoded values. Due to the way Riak
// map-reduce works, you'll need to unmarshal them individually.
//
// Warning: If you specify an empty list of keys, it will do a full
// map-reduce on the bucket (usually not not a good idea).
func (c *Client) GetManyJson(bucket string, keys []string) (results []string, err error) {
	var req *RpbMapRedReq
	var resps []*RpbMapRedResp

	req = &RpbMapRedReq{
		Request:     []byte(genUnionMapRedQuery(bucket, keys)),
		ContentType: []byte("application/json"),
	}

	resps, err = c.MapRed(req)
	if err != nil {
		return
	}

	results = make([]string, 0)
	for _, resp := range resps {
		if resp.Response == nil {
			continue
		}

		var raws []string
		if err = json.Unmarshal(resp.GetResponse(), &raws); err != nil {
			return
		}

		results = append(raws, results...)
	}

	return
}

func genUnionMapRedQuery(bucket string, keys []string) (query string) {
	query = `{"inputs":`

	if len(keys) == 0 {
		query += `"` + bucket + `"`
	} else {
		query += `[`
		for i, key := range keys {
			if i > 0 {
				query += `,`
			}
			query += `["` + bucket + `", "` + key + `"]`
		}
		query += `]`
	}

	query += `,"query": [{"map":{"language":"erlang","module":"riak_kv_mapreduce","function":"map_object_value"}},{"reduce":{"language":"erlang","module":"riak_kv_mapreduce","function":"reduce_set_union"}}]}`

	return
}
