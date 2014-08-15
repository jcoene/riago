package riago

import (
	"errors"

	"code.google.com/p/goprotobuf/proto"
)

const (
	MsgRpbErrorResp              = 0
	MsgRpbPingReq                = 1
	MsgRpbPingResp               = 2
	MsgRpbGetClientIdReq         = 3
	MsgRpbGetClientIdResp        = 4
	MsgRpbSetClientIdReq         = 5
	MsgRpbSetClientIdResp        = 6
	MsgRpbGetServerInfoReq       = 7
	MsgRpbGetServerInfoResp      = 8
	MsgRpbGetReq                 = 9
	MsgRpbGetResp                = 10
	MsgRpbPutReq                 = 11
	MsgRpbPutResp                = 12
	MsgRpbDelReq                 = 13
	MsgRpbDelResp                = 14
	MsgRpbListBucketsReq         = 15
	MsgRpbListBucketsResp        = 16
	MsgRpbListKeysReq            = 17
	MsgRpbListKeysResp           = 18
	MsgRpbGetBucketReq           = 19
	MsgRpbGetBucketResp          = 20
	MsgRpbSetBucketReq           = 21
	MsgRpbSetBucketResp          = 22
	MsgRpbMapRedReq              = 23
	MsgRpbMapRedResp             = 24
	MsgRpbIndexReq               = 25
	MsgRpbIndexResp              = 26
	MsgRpbSearchQueryReq         = 27
	MsgRbpSearchQueryResp        = 28
	MsgRpbResetBucketReq         = 29
	MsgRpbResetBucketResp        = 30
	MsgRpbCSBucketReq            = 40
	MsgRpbCSBucketResp           = 41
	MsgRpbCounterUpdateReq       = 50
	MsgRpbCounterUpdateResp      = 51
	MsgRpbCounterGetReq          = 52
	MsgRpbCounterGetResp         = 53
	MsgRpbYokozunaIndexGetReq    = 54
	MsgRpbYokozunaIndexGetResp   = 55
	MsgRpbYokozunaIndexPutReq    = 56
	MsgRpbYokozunaIndexDeleteReq = 57
	MsgRpbYokozunaSchemaGetReq   = 58
	MsgRpbYokozunaSchemaGetResp  = 59
	MsgRpbYokozunaSchemaPutReq   = 60
	MsgDtFetchReq                = 80
	MsgDtFetchResp               = 81
	MsgDtUpdateReq               = 82
	MsgDtUpdateResp              = 83
)

var (
	ErrInvalidResponseBody = errors.New("invalid response body")
	ErrInvalidResponseCode = errors.New("invalid response code")
	ErrInvalidRequestCode  = errors.New("invalid request code")
)

// Encodes a request code and proto structure into a message byte buffer
func encode(code uint8, req proto.Message) (buf []byte, err error) {
	var reqbuf []byte
	var size int32

	if req != nil {
		if reqbuf, err = proto.Marshal(req); err != nil {
			return
		}
	}

	size = int32(len(reqbuf) + 1)
	buf = []byte{byte(size >> 24), byte(size >> 16), byte(size >> 8), byte(size), code}
	buf = append(buf, reqbuf...)

	return
}

// Decodes a message byte buffer into a proto response, error code or nil
// Resulting object depends on response type.
func decode(buf []byte, resp proto.Message) (err error) {
	var code uint8
	var respbuf []byte

	if len(buf) < 1 {
		err = ErrInvalidResponseCode
		return
	}

	code = buf[0]

	if len(buf) > 1 {
		respbuf = buf[1:]
	} else {
		respbuf = make([]byte, 0)
	}

	switch code {
	case MsgRpbErrorResp:
		errResp := &RpbErrorResp{}
		if err = proto.Unmarshal(respbuf, errResp); err == nil {
			err = errors.New(string(errResp.Errmsg))
		}

	case MsgRpbPingResp, MsgRpbSetClientIdResp, MsgRpbSetBucketResp, MsgRpbDelResp:
		resp = nil

	default:
		err = proto.Unmarshal(respbuf, resp)
	}

	return
}
