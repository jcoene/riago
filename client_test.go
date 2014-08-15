package riago

import (
	"encoding/json"
	"fmt"
	"os"
	"regexp"
	"sort"
	"testing"
	"time"

	. "github.com/smartystreets/goconvey/convey"
)

type WithId struct {
	Id int64 `json:"id"`
}

func inst(p *Profile) {
	p.String()
}

func TestClientInstance(t *testing.T) {
	Convey("Client instance", t, func() {
		client := NewClient("127.0.0.1:8087", 1)
		dur := 500 * time.Millisecond

		Convey("Can set an instrumenter", func() {
			client.SetInstrumenter(inst)
			So(client.instrumenter, ShouldEqual, inst)
		})

		Convey("Can set retry attempts", func() {
			client.SetRetryAttempts(3)
			So(client.retryAttempts, ShouldEqual, 3)
		})

		Convey("Can set retry delay", func() {
			dur := time.Duration(500 * time.Millisecond)
			client.SetRetryDelay(dur)
			So(client.retryDelay, ShouldEqual, dur)
		})

		Convey("Can set read timeout", func() {
			client.SetReadTimeout(dur)
			So(client.readTimeout, ShouldEqual, dur)
		})

		Convey("Can set write timeout", func() {
			client.SetWriteTimeout(dur)
			So(client.writeTimeout, ShouldEqual, dur)
		})

		Convey("Can set pool wait timeout", func() {
			client.SetWaitTimeout(dur)
			So(client.pool.waitTimeout, ShouldEqual, dur)
		})
	})
}

func TestClientErrorHandling(t *testing.T) {
	Convey("Client Error Handling", t, func() {
		client := NewClient("127.0.0.1:8087", 1)
		client.SetInstrumenter(inst)
		client.SetReadTimeout(2 * time.Second)
		client.SetWriteTimeout(2 * time.Second)

		Convey("Returns client errors properly", func() {
			req := &RpbPutReq{}
			_, err := client.Put(req)
			So(err.Error(), ShouldContainSubstring, "required field")
		})

		Convey("Returns service errors properly", func() {
			req := &RpbMapRedReq{
				Request:     []byte("this isn't going to work at all"),
				ContentType: []byte("application/json"),
			}
			_, err := client.MapRed(req)
			So(err.Error(), ShouldContainSubstring, "invalid_json")
		})
	})
}

func TestClientServerOperations(t *testing.T) {
	Convey("Client Server Operations", t, func() {
		Convey("ServerInfo", func() {
			client := NewClient("127.0.0.1:8087", 1)
			resp, err := client.ServerInfo()
			So(err, ShouldEqual, nil)
			So(string(resp.GetNode()), ShouldContainSubstring, "@")
			So(string(resp.GetServerVersion()), ShouldContainSubstring, ".")
		})
	})
}

func TestClientBucketOperations(t *testing.T) {
	Convey("Client Bucket Operations", t, func() {
		client := NewClient("127.0.0.1:8087", 1)

		Convey("SetBucket and GetBucket", func() {
			two := uint32(2)
			t := true
			f := false
			setReq := &RpbSetBucketReq{
				Bucket: []byte("riago_test"),
				Props: &RpbBucketProps{
					NVal:          &two,
					AllowMult:     &f,
					LastWriteWins: &t,
				},
			}

			setErr := client.SetBucket(setReq)
			So(setErr, ShouldEqual, nil)

			getReq := &RpbGetBucketReq{
				Bucket: []byte("riago_test"),
			}

			getResp, getErr := client.GetBucket(getReq)
			So(getErr, ShouldEqual, nil)
			So(getResp.GetProps().GetNVal(), ShouldEqual, 2)
			So(getResp.GetProps().GetAllowMult(), ShouldEqual, false)
			So(getResp.GetProps().GetLastWriteWins(), ShouldEqual, true)
		})

		Convey("ListBuckets", func() {
			// Skip this test unless we're in CI
			if os.Getenv("CI") == "" {
				t.Logf("Skipping list bucket test outside of CI environment.")
				return
			}

			putReq := &RpbPutReq{
				Bucket: []byte("riago_test"),
				Key:    []byte("exists"),
				Content: &RpbContent{
					Value:       []byte("{}"),
					ContentType: []byte("application/json"),
				},
			}
			_, putErr := client.Put(putReq)
			So(putErr, ShouldEqual, nil)

			setReq := &RpbSetBucketReq{
				Bucket: []byte("riago_test"),
				Props:  &RpbBucketProps{},
			}

			setErr := client.SetBucket(setReq)
			So(setErr, ShouldEqual, nil)

			listReq := &RpbListBucketsReq{}
			listResp, listErr := client.ListBuckets(listReq)
			So(listErr, ShouldEqual, nil)

			found := false
			for _, b := range listResp.GetBuckets() {
				if string(b) == "riago_test" {
					found = true
					break
				}
			}
			So(found, ShouldEqual, true)
		})
	})
}

func TestClient2iOperations(t *testing.T) {
	Convey("Client 2i Operations", t, func() {
		client := NewClient("127.0.0.1:8087", 1)

		Convey("Index", func() {
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
				So(err, ShouldEqual, nil)
			}

			qtype := RpbIndexReq_eq
			req := &RpbIndexReq{
				Bucket: []byte("riago_test"),
				Index:  []byte("test_id_rem_int"),
				Qtype:  &qtype,
				Key:    []byte("0"),
			}
			resp, err := client.Index(req)
			So(err, ShouldEqual, nil)

			got := make([]string, len(resp.GetKeys()))
			for i, bs := range resp.GetKeys() {
				got[i] = string(bs)
			}
			sort.Strings(got)

			So(got, ShouldResemble, expect)
			So(len(resp.GetKeys()), ShouldEqual, n/2)
		})
	})
}

func TestClientKeyOperations(t *testing.T) {
	Convey("Client Key Operations", t, func() {
		client := NewClient("127.0.0.1:8087", 1)

		Convey("Put and Get", func() {
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

			_, putErr := client.Put(putReq)
			So(putErr, ShouldEqual, nil)

			getReq := &RpbGetReq{
				Bucket: []byte("riago_test"),
				Key:    []byte("client_test_put_get"),
			}

			getResp, getErr := client.Get(getReq)
			So(getErr, ShouldEqual, nil)

			getValue := string(getResp.GetContent()[0].GetValue())
			getContentType := string(getResp.GetContent()[0].GetContentType())

			So(getValue, ShouldEqual, putValue)
			So(getContentType, ShouldEqual, putContentType)
		})

		Convey("Del", func() {
			delReq := &RpbDelReq{
				Bucket: []byte("riago_test"),
				Key:    []byte("client_test_del"),
			}

			delErr := client.Del(delReq)
			So(delErr, ShouldEqual, nil)
		})

		Convey("ListKeys", func() {
			putValue := "{\"hello\": \"world\"}"
			putContentType := "application/json"

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

				_, err := client.Put(putReq)
				So(err, ShouldEqual, nil)
			}

			listReq := &RpbListKeysReq{
				Bucket: []byte("riago_test"),
			}
			listResps, listErr := client.ListKeys(listReq)
			So(listErr, ShouldEqual, nil)

			found := 0
			for _, r := range listResps {
				for _, k := range r.GetKeys() {
					if f, _ := regexp.Match("client_test_list_keys_", k); f {
						found += 1
					}
				}
			}

			So(found, ShouldEqual, n)
		})
	})
}

func TestClientCRDTOperations(t *testing.T) {
	Convey("Client CRDT Operations", t, func() {
		client := NewClient("127.0.0.1:8087", 1)

		Convey("Update and Fetch", func() {
			startReq := &DtFetchReq{
				Bucket: []byte("riago_test"),
				Key:    []byte("client_test_put_get"),
				Type:   []byte("riago_dt_test"),
			}

			startResp, startErr := client.DtFetch(startReq)
			So(startErr, ShouldEqual, nil)

			startType := DtFetchResp_DataType(startResp.GetType())
			startCounterValue := int64(startResp.GetValue().GetCounterValue())

			So(startType, ShouldEqual, DtFetchResp_COUNTER)

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

			_, putErr := client.DtUpdate(putReq)
			So(putErr, ShouldEqual, nil)

			getReq := &DtFetchReq{
				Bucket: []byte("riago_test"),
				Key:    []byte("client_test_put_get"),
				Type:   []byte("riago_dt_test"),
			}

			getResp, getErr := client.DtFetch(getReq)
			So(getErr, ShouldEqual, nil)

			getType := DtFetchResp_DataType(getResp.GetType())
			getCounterValue := int64(getResp.GetValue().GetCounterValue())

			So(getType, ShouldEqual, DtFetchResp_COUNTER)
			So(getCounterValue, ShouldEqual, startCounterValue+1)

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

			_, deErr := client.DtUpdate(deReq)
			So(deErr, ShouldEqual, nil)
		})
	})
}

func TestClientMapReduce(t *testing.T) {
	Convey("Client Map Reduce", t, func() {
		client := NewClient("127.0.0.1:8087", 1)

		Convey("MapRed", func() {
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
				So(err, ShouldEqual, nil)
			}

			query := genUnionMapRedQuery("riago_test", keys)

			mapRedReq := &RpbMapRedReq{
				Request:     []byte(query),
				ContentType: []byte("application/json"),
			}
			mapRedResps, mapRedErr := client.MapRed(mapRedReq)
			So(mapRedErr, ShouldEqual, nil)

			found := 0
			for _, r := range mapRedResps {
				if r.Response == nil {
					continue
				}

				var rvals []string
				err := json.Unmarshal(r.GetResponse(), &rvals)
				So(err, ShouldEqual, nil)
				for _, _ = range rvals {
					found += 1
				}
			}

			So(found, ShouldEqual, n)
		})

		Convey("GetManyJson", func() {
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
				So(err, ShouldEqual, nil)
			}

			jsons, err := client.GetManyJson("riago_test", keys)
			So(len(jsons), ShouldEqual, n)
			So(err, ShouldEqual, nil)

			for _, j := range jsons {
				tmp := new(WithId)
				err = json.Unmarshal([]byte(j), &tmp)
				So(err, ShouldEqual, nil)
				So(tmp.Id, ShouldBeGreaterThan, -1)
				So(tmp.Id, ShouldBeLessThan, n+1)
			}
		})
	})
}
