package riago

import (
	"encoding/json"
	"fmt"
	"os"
	"regexp"
	"sort"
	"testing"
	"time"

	"github.com/golang/protobuf/proto"
	"github.com/stretchr/testify/assert"
)

type WithId struct {
	Id int64 `json:"id"`
}

func inst(p *Profile) {
	p.String()
}

func TestClientInstance(t *testing.T) {
	assert := assert.New(t)
	client := NewClient("127.0.0.1:8087", 1)

	dur := 500 * time.Millisecond

	// Can set an instrumenter
	client.SetInstrumenter(inst)
	assert.Equal(inst, client.instrumenter, inst)

	// Can set retry attempts
	client.SetRetryAttempts(3)
	assert.Equal(3, client.retryAttempts)

	// Can set retry delay
	dur = time.Duration(500 * time.Millisecond)
	client.SetRetryDelay(dur)
	assert.Equal(dur, client.retryDelay)

	// Can set read timeout
	client.SetReadTimeout(dur)
	assert.Equal(dur, client.readTimeout)

	// Can set write timeout
	client.SetWriteTimeout(dur)
	assert.Equal(dur, client.writeTimeout)

	// Can set pool wait timeout
	client.SetWaitTimeout(dur)
	assert.Equal(dur, client.pool.waitTimeout)
}

func TestClientErrorHandling(t *testing.T) {
	assert := assert.New(t)
	client := NewClient("127.0.0.1:8087", 1)

	client.SetInstrumenter(inst)
	client.SetReadTimeout(2 * time.Second)
	client.SetWriteTimeout(2 * time.Second)

	// Returns client errors properly
	putReq := &RpbPutReq{}
	_, err := client.Put(putReq)
	assert.Contains(err.Error(), "required field")

	// Returns service errors properly
	getReq := &RpbMapRedReq{
		Request:     []byte("this isn't going to work at all"),
		ContentType: []byte("application/json"),
	}
	_, err = client.MapRed(getReq)
	assert.Contains(err.Error(), "invalid_json")
}

func TestClientServerOperations(t *testing.T) {
	assert := assert.New(t)
	client := NewClient("127.0.0.1:8087", 1)

	// ServerInfo
	resp, err := client.ServerInfo()

	assert.Nil(err)
	assert.Contains(string(resp.GetNode()), "@")
	assert.Contains(string(resp.GetServerVersion()), ".")
}

func TestClientBucketOperations(t *testing.T) {
	assert := assert.New(t)
	client := NewClient("127.0.0.1:8087", 1)

	// SetBucket and GetBucket
	setReq := &RpbSetBucketReq{
		Bucket: []byte("riago_test"),
		Props: &RpbBucketProps{
			NVal:          proto.Uint32(2),
			AllowMult:     proto.Bool(false),
			LastWriteWins: proto.Bool(true),
		},
	}

	err := client.SetBucket(setReq)
	assert.Nil(err)

	getReq := &RpbGetBucketReq{
		Bucket: []byte("riago_test"),
	}

	getResp, err := client.GetBucket(getReq)
	assert.Nil(err)
	assert.Equal(uint32(2), getResp.GetProps().GetNVal())
	assert.Equal(false, getResp.GetProps().GetAllowMult())
	assert.Equal(true, getResp.GetProps().GetLastWriteWins())
}

func TestClientListBucket(t *testing.T) {
	assert := assert.New(t)
	client := NewClient("127.0.0.1:8087", 1)

	putReq := &RpbPutReq{
		Bucket: []byte("riago_test"),
		Key:    []byte("exists"),
		Content: &RpbContent{
			Value:       []byte("{}"),
			ContentType: []byte("application/json"),
		},
	}
	_, err := client.Put(putReq)
	assert.Nil(err)

	setReq := &RpbSetBucketReq{
		Bucket: []byte("riago_test"),
		Props:  &RpbBucketProps{},
	}

	err = client.SetBucket(setReq)
	assert.Nil(err)

	listReq := &RpbListBucketsReq{}
	listResp, err := client.ListBuckets(listReq)
	assert.Nil(err)

	found := false
	for _, b := range listResp.GetBuckets() {
		if string(b) == "riago_test" {
			found = true
			break
		}
	}
	assert.True(found)
}

func TestClient2iOperations(t *testing.T) {
	assert := assert.New(t)
	client := NewClient("127.0.0.1:8087", 1)

	n := 8
	keys := make([]string, n)
	expect := make([]string, 0)

	for i := 0; i < n; i++ {
		k := fmt.Sprintf("client_test_2i_%d", i)
		v := fmt.Sprintf("{\"id\": %d}", i)

		if i%2 == 0 {
			expect = append(expect, fmt.Sprintf("client_test_2i_%d", i))
		}

		indexes := make([]*RpbPair, 1)
		indexes[0] = &RpbPair{
			Key:   []byte("test_id_rem_int"),
			Value: []byte(fmt.Sprintf("%d", i%2)),
		}

		keys[i] = k
		putReq := &RpbPutReq{
			Bucket: []byte("riago_test"),
			Key:    []byte(k),
			Content: &RpbContent{
				Value:       []byte(v),
				ContentType: []byte("application/json"),
				Indexes:     indexes,
			},
		}

		_, err := client.Put(putReq)
		assert.Nil(err)
	}

	qtype := RpbIndexReq_eq
	req := &RpbIndexReq{
		Bucket: []byte("riago_test"),
		Index:  []byte("test_id_rem_int"),
		Qtype:  &qtype,
		Key:    []byte("0"),
	}
	resp, err := client.Index(req)
	assert.Nil(err)

	got := make([]string, len(resp.GetKeys()))
	for i, bs := range resp.GetKeys() {
		got[i] = string(bs)
	}
	sort.Strings(got)

	assert.Equal(expect, got)
	assert.Equal(n/2, len(resp.GetKeys()))
}

func TestClientKeyOperations(t *testing.T) {
	assert := assert.New(t)
	client := NewClient("127.0.0.1:8087", 1)

	// Put and Get
	putValue := "{\"hello\": \"world\"}"
	putContentType := "application/json"

	putReq := &RpbPutReq{
		Bucket: []byte("riago_test"),
		Key:    []byte("client_test_put_get"),
		Content: &RpbContent{
			Value:       []byte(putValue),
			ContentType: []byte(putContentType),
		},
	}

	_, err := client.Put(putReq)
	assert.Nil(err)

	getReq := &RpbGetReq{
		Bucket: []byte("riago_test"),
		Key:    []byte("client_test_put_get"),
	}

	getResp, err := client.Get(getReq)
	assert.Nil(err)

	getValue := string(getResp.GetContent()[0].GetValue())
	getContentType := string(getResp.GetContent()[0].GetContentType())

	assert.Equal(putValue, getValue)
	assert.Equal(putContentType, getContentType)

	// Del
	delReq := &RpbDelReq{
		Bucket: []byte("riago_test"),
		Key:    []byte("client_test_del"),
	}

	err = client.Del(delReq)
	assert.Nil(err)

	// ListKeys

	n := 11
	for i := 0; i < n; i++ {
		putReq := &RpbPutReq{
			Bucket: []byte("riago_test"),
			Key:    []byte(fmt.Sprintf("client_test_list_keys_%d", i)),
			Content: &RpbContent{
				Value:       []byte(putValue),
				ContentType: []byte(putContentType),
			},
		}

		_, err = client.Put(putReq)
		assert.Nil(err)
	}

	listReq := &RpbListKeysReq{
		Bucket: []byte("riago_test"),
	}
	listResps, err := client.ListKeys(listReq)
	assert.Nil(err)

	found := 0
	for _, r := range listResps {
		for _, k := range r.GetKeys() {
			if f, _ := regexp.Match("client_test_list_keys_", k); f {
				found += 1
			}
		}
	}

	assert.Equal(n, found)
}

func TestClientCRDTOperations(t *testing.T) {
	assert := assert.New(t)
	client := NewClient("127.0.0.1:8087", 1)

	// Initial fetch
	startReq := &DtFetchReq{
		Bucket: []byte("riago_test"),
		Key:    []byte("client_test_put_get"),
		Type:   []byte("riago_dt_test"),
	}

	startResp, err := client.DtFetch(startReq)
	assert.Nil(err)

	startType := DtFetchResp_DataType(startResp.GetType())
	startCounterValue := int64(startResp.GetValue().GetCounterValue())

	assert.Equal(DtFetchResp_COUNTER, startType)

	// Increment operation
	var putIncrement int64 = 1

	putReq := &DtUpdateReq{
		Bucket: []byte("riago_test"),
		Key:    []byte("client_test_put_get"),
		Type:   []byte("riago_dt_test"),
		Op: &DtOp{
			CounterOp: &CounterOp{
				Increment: &putIncrement,
			},
		},
	}

	_, err = client.DtUpdate(putReq)
	assert.Nil(err)

	getReq := &DtFetchReq{
		Bucket: []byte("riago_test"),
		Key:    []byte("client_test_put_get"),
		Type:   []byte("riago_dt_test"),
	}

	getResp, err := client.DtFetch(getReq)
	assert.Nil(err)

	getType := DtFetchResp_DataType(getResp.GetType())
	getCounterValue := int64(getResp.GetValue().GetCounterValue())

	assert.Equal(DtFetchResp_COUNTER, getType)
	assert.Equal(startCounterValue+1, getCounterValue)

	// Decrement
	var deIncrement int64 = -1

	deReq := &DtUpdateReq{
		Bucket: []byte("riago_test"),
		Key:    []byte("client_test_put_get"),
		Type:   []byte("riago_dt_test"),
		Op: &DtOp{
			CounterOp: &CounterOp{
				Increment: &deIncrement,
			},
		},
	}

	_, err = client.DtUpdate(deReq)
	assert.Nil(err)
}

func TestClientMapReduce(t *testing.T) {
	assert := assert.New(t)
	client := NewClient("127.0.0.1:8087", 1)

	n := 8
	keys := make([]string, n)

	for i := 0; i < n; i++ {
		k := fmt.Sprintf("client_test_mapred_%d", i)
		v := fmt.Sprintf("{\"id\": %d}", i)

		keys[i] = k
		putReq := &RpbPutReq{
			Bucket: []byte("riago_test"),
			Key:    []byte(k),
			Content: &RpbContent{
				Value:       []byte(v),
				ContentType: []byte("application/json"),
			},
		}

		_, err := client.Put(putReq)
		assert.Nil(err)
	}

	query := genUnionMapRedQuery("riago_test", keys)

	mapRedReq := &RpbMapRedReq{
		Request:     []byte(query),
		ContentType: []byte("application/json"),
	}
	mapRedResps, err := client.MapRed(mapRedReq)
	assert.Nil(err)

	found := 0
	for _, r := range mapRedResps {
		if r.Response == nil {
			continue
		}

		var rvals []string
		err = json.Unmarshal(r.GetResponse(), &rvals)
		assert.Nil(err)
		for _, _ = range rvals {
			found += 1
		}
	}

	assert.Equal(n, found)
}

func TestClientGetManyJson(t *testing.T) {
	assert := assert.New(t)
	client := NewClient("127.0.0.1:8087", 1)

	n := 15
	keys := make([]string, n)
	for i := 0; i < n; i++ {
		k := fmt.Sprintf("client_test_getmanyjson_%d", i)
		v := fmt.Sprintf("{\"id\": %d}", i)

		keys[i] = k
		putReq := &RpbPutReq{
			Bucket: []byte("riago_test"),
			Key:    []byte(k),
			Content: &RpbContent{
				Value:       []byte(v),
				ContentType: []byte("application/json"),
			},
		}

		_, err := client.Put(putReq)
		assert.Nil(err)
	}

	jsons, err := client.GetManyJson("riago_test", keys)
	assert.Nil(err)
	assert.Equal(n, len(jsons))

	for _, j := range jsons {
		tmp := new(WithId)
		err = json.Unmarshal([]byte(j), &tmp)
		assert.Nil(err)
	}
}
