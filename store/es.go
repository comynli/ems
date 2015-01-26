package store

import (
	"encoding/json"
	"github.com/lixm/ems/common"
	"io"
)

func queryBuild(ti common.TraceItem) []byte {
	terms := []map[string]map[string]string{}
	term := make(map[string]map[string]string)
	term["term"] = make(map[string]string)
	term["term"]["request_id"] = ti.RequestId
	terms = append(terms, term)
	delete(term["term"], "request_id")
	term["term"]["client"] = ti.Client
	terms = append(terms, term)
	delete(term["term"], "client")
	term["term"]["server"] = ti.Server
	terms = append(terms, term)
	bool := make(map[string][]map[string]map[string]string)
	bool["must"] = terms
	query := make(map[string]map[string][]map[string]map[string]string)
	query["bool"] = bool
	Q := make(map[string]map[string]map[string][]map[string]map[string]string)
	Q["query"] = query
	buf, _ := json.Marshal(Q)
	return buf
}

type Hit struct {
	Index  string           `json:"_index"`
	Type   string           `json:"_type"`
	ID     string           `json:"_id"`
	Score  float32          `json:"_score"`
	Source common.TraceItem `json:"_source"`
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
