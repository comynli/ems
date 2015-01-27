package common

import (
	"encoding/json"
)

type TraceItem struct {
	RequestId string `json:"request_id"`
	Path      string `json:"path"`
	Seq       int    `json:"seq"`
	Client    string `json:"client"`
	Server    string `json:"server"`
	Start     int64  `json:"start"`
	Status    int    `json:"status"`
	End       int64  `json:"end"`
	RT        int64  `json:"rt"`
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
	}
	return ti, err
}
