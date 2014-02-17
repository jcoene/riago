package riago

import (
	"testing"
	"time"

	. "github.com/smartystreets/goconvey/convey"
)

func TestClientInstance(t *testing.T) {
	Convey("Client instance", t, func() {
		client := NewClient("127.0.0.1:8087", 3)
		dur := 500 * time.Millisecond

		Convey("Can set retry attempts", func() {
			client.SetRetryAttempts(3)
			So(client.retryAttempts, ShouldEqual, 3)
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

func TestClientKV(t *testing.T) {
	Convey("Client KV", t, func() {
		client := NewClient("127.0.0.1:8087", 3)

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
	})
}
