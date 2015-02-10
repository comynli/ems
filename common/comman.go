package common

import (
	"encoding/json"
	"git.apache.org/thrift.git/lib/go/thrift"
	"time"
)

type LogItem struct {
	RequestId string    `json:"request_id"`
	Path      string    `json:"path"`
	Host      string    `json:"host"`
	Seq       int32     `json:"seq"`
	Status    int       `json:"status"`
	RT        int64     `json:"rt"`
	TimeStamp time.Time `json:"timestamp"`
}

type TraceItem struct {
	RequestId string    `json:"request_id"`
	Path      string    `json:"path"`
	Host      string    `json:"host"`
	Seq       int32     `json:"seq"`
	Client    string    `json:"client"`
	Server    string    `json:"server"`
	Status    int       `json:"status"`
	RT        int64     `json:"rt"`
	TimeStamp time.Time `json:"timestamp"`
}

func (ti TraceItem) Encode() ([]byte, error) {
	return json.Marshal(ti)
}

func (li LogItem) Encode() ([]byte, error) {
	return json.Marshal(li)
}

func LogDecode(data []byte) (LogItem, error) {
	li := LogItem{}
	err := json.Unmarshal(data, &li)
	if err == nil {
		if li.TimeStamp.Unix() <= 0 {
			li.TimeStamp = time.Now()
		}
	}
	return li, err
}

func Convert(r RequestHeader, path, host string) TraceItem {
	status := 0
	if !r.Status {
		status = 1
	}
	ti := TraceItem{
		RequestId: r.RequestId,
		Seq:       r.Seq,
		Host:      host,
		Path:      path,
		Client:    r.Client,
		Server:    r.Server,
		Status:    status,
		RT:        r.End - r.Start,
		TimeStamp: time.Unix(r.Start/1000, 0),
	}
	return ti
}

func RpcDecode(data []byte) (RequestHeader, error) {
	ri := RequestHeader{}
	dec := thrift.NewTDeserializer()
	err := dec.Read(&ri, data)
	return ri, err
}
