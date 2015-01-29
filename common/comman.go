package common

import (
	"encoding/json"
	"time"
)

type TraceItem struct {
	RequestId string    `json:"request_id"`
	Path      string    `json:"path"`
	Host      string    `json:"host"`
	Seq       int       `json:"seq"`
	Client    string    `json:"client"`
	Server    string    `json:"server"`
	Start     int64     `json:"start"`
	Status    int       `json:"status"`
	End       int64     `json:"end"`
	RT        int64     `json:"rt"`
	TimeStamp time.Time `json:"timestamp"`
}

func (ti TraceItem) Encode() ([]byte, error) {
	return json.Marshal(ti)
}

func Decode(data []byte) (TraceItem, error) {
	ti := TraceItem{}
	err := json.Unmarshal(data, &ti)
	if err == nil {
		if ti.End != 0 {
			ti.RT = ti.End - ti.Start
		}
		if ti.TimeStamp.Unix() <= 0 {
			ti.TimeStamp = time.Now()
		}
	}
	return ti, err
}
