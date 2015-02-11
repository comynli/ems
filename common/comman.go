package common

import (
	"encoding/json"
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

type RpcItem struct {
	RequestId string `json:"request_id"`
	Seq       int32  `json:"seq"`
	Client    string `json:"client"`
	Server    string `json:"server"`
	Api       string `json:"api"`
	Status    bool   `json:"status"`
	Start     int64  `json:"start"`
	End       int64  `json:"end"`
}

type TraceItem struct {
	RequestId string    `json:"request_id"`
	Path      string    `json:"path"`
	Host      string    `json:"host"`
	Seq       int32     `json:"seq"`
	Client    string    `json:"client"`
	Server    string    `json:"server"`
	Api       string    `json:"api"`
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

func Convert(r RpcItem, path, host string) TraceItem {
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
		Api:       r.Api,
		Status:    status,
		RT:        r.End - r.Start,
		TimeStamp: time.Unix(r.Start/1000, 0),
	}
	return ti
}

func RpcDecode(data []byte) (RpcItem, error) {
	ri := RpcItem{}
	err := json.Unmarshal(data, &ri)
	return ri, err
}
