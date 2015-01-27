package store

import (
	"encoding/json"
	"io"
)

type Trace struct {
	Seq    int    `json:"seq"`
	Client string `json:"client"`
	Server string `json:"server"`
	Start  int64  `json:"start"`
	Status int    `json:"status"`
	End    int64  `json:"end"`
	RT     int64  `json:"rt"`
}

type Item struct {
	Path   string           `json:"path"`
	Status int              `json:"status"`
	RT     int64            `json:"rt"`
	Host   string           `json:"host"`
	Trace  map[string]Trace `json:"trace"`
}

type Hit struct {
	Index  string  `json:"_index"`
	Type   string  `json:"_type"`
	ID     string  `json:"_id"`
	Score  float32 `json:"_score"`
	Source Item    `json:"_source"`
}

type Hits struct {
	Total    int     `json:"total"`
	MaxScore float32 `json:"max_score"`
	Hits     []Hit   `json:"hits"`
}

type Shards struct {
	Total      int `json:"total"`
	Successful int `json:"successful"`
	Failed     int `json:"failed"`
}

type Result struct {
	Took     int    `json"took"`
	TimedOut bool   `json:"timed_out"`
	Shards   Shards `json:"_shards"`
	Hits     Hits   `json:"hits"`
}

func Parse(data []byte) (Result, error) {
	result := Result{}
	err := json.Unmarshal(data, &result)
	return result, err
}

func ParseStream(r io.Reader) (Result, error) {
	result := Result{}
	dec := json.NewDecoder(r)
	err := dec.Decode(&result)
	return result, err
}
