package riago

import (
	"encoding/json"
	"fmt"
	"regexp"
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
		client := NewClient("127.0.0.1:8087", 3)
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

	Convey("Server Operations", t, func() {
		Convey("Can get server version", func() {
			client := NewClient("127.0.0.1:8087", 1)
			resp, err := client.ServerInfo()
			So(err, ShouldEqual, nil)
			So(string(resp.GetNode()), ShouldContainSubstring, "@")
			So(string(resp.GetServerVersion()), ShouldContainSubstring, ".")
		})
	})
}

func TestClientKV(t *testing.T) {
	Convey("Client KV", t, func() {
		client := NewClient("127.0.0.1:8087", 3)
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

		Convey("Can set and get bucket properties", func() {
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

		Convey("Can put and get an object", func() {
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

		Convey("Can delete an object", func() {
			delReq := &RpbDelReq{
				Bucket: []byte("riago_test"),
				Key:    []byte("client_test_del"),
			}

			delErr := client.Del(delReq)
			So(delErr, ShouldEqual, nil)
		})

		Convey("Can list buckets", func() {
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

		Convey("Can list keys", func() {
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

		Convey("Can map reduce", func() {
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
	})
}

func TestGetManyJson(t *testing.T) {
	client := NewClient("127.0.0.1:8087", 3)

	Convey("GetManyJson", t, func() {
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

		Convey("Gets many json documents", func() {
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
