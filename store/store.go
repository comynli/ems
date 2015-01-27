package store

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/lixm/ems/common"
	tomb "gopkg.in/tomb.v2"
	"log"
	"net"
	"net/http"
	"net/http/httputil"
	"net/url"
	"strconv"
	"strings"
	"time"
)

const (
	IDLE     = 0
	BUSY     = 1
	CLOSED   = 2
	ERROR    = 3
	BUF_SIZE = 1024
)

type endPoint struct {
	Conn   *httputil.ClientConn
	Url    *url.URL
	RawUrl string
	Status int
}

type StoreServer struct {
	endPoints  []endPoint
	queue      chan common.TraceItem
	sendQueue  chan []byte
	index_name string
	index_type string
	cur        int
	tomb.Tomb
}

func (ss *StoreServer) encode(ti common.TraceItem) ([]byte, error) {

	retry := 0
	var (
		item       Item
		index_name string
	)
	for {
		retry += 1
		if retry > len(ss.endPoints) {
			return nil, errors.New("no more backend to re-try")
		}
		url := fmt.Sprintf("%s/%s-*/%s/_search?q=_id:%s", ss.endPoints[ss.cur].RawUrl, ss.index_name, ss.index_type, ti.RequestId)
		//host := ss.endPoints[ss.cur].Url.Host
		ss.cur += 1
		if ss.cur >= len(ss.endPoints) {
			ss.cur = 0
		}
		req, err := http.NewRequest("GET", url, nil)
		if err != nil {
			return nil, err
		}
		//req.Host = host
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			log.Printf("search %s fail %s", ti.RequestId, err)
			continue
		}

		result, err := ParseStream(resp.Body)
		if err != nil {
			log.Printf("parse search result fail %s", err)
			continue
		}
		if result.Hits.Total == 0 {
			item = Item{}
			index_name = fmt.Sprintf("%s-%s", ss.index_name, time.Now().Format("2006.01.02"))
		} else {
			item = result.Hits.Hits[0].Source
			index_name = result.Hits.Hits[0].Index
		}
		break
	}
	if ti.Host != "" {
		item.Host = ti.Host
		item.Path = ti.Path
		item.RT = ti.RT
		item.Status = ti.Status
		item.Trace = make(map[string]Trace)
	} else {
		key := strings.Join([]string{ti.Client, ti.Server, strconv.Itoa(ti.Seq)}, "#")
		if t, ok := item.Trace[key]; !ok {
			t = Trace{}
			t.Client = ti.Client
			t.End = ti.End
			t.RT = ti.RT
			t.Seq = ti.Seq
			t.Server = ti.Server
			t.Start = ti.Start
			t.Status = ti.Status
			if item.Trace == nil {
				item.Trace = make(map[string]Trace)
			}
			item.Trace[key] = t
		} else {
			if t.End == 0 {
				t.End = ti.End
				t.Status = ti.Status
			}
			if t.Start == 0 {
				t.Start = ti.Start
			}
			t.RT = t.End - t.Start
			item.Trace[key] = t
		}
	}
	meta := make(map[string]map[string]string)
	meta["index"] = make(map[string]string)
	meta["index"]["_index"] = index_name
	meta["index"]["_type"] = ss.index_type
	meta["index"]["_id"] = ti.RequestId
	m, err := json.Marshal(meta)
	if err != nil {
		return nil, err
	}
	d, err := json.Marshal(item)
	if err != nil {
		return nil, err
	}
	return toBytes(m, d), nil
}

func toBytes(meta, ti []byte) []byte {
	data := append(meta, '\n')
	data = append(data, ti...)
	data = append(data, '\n')
	return data
}

func (ss *StoreServer) sendLoop(err chan error) {
	for _, conn := range ss.endPoints {
		switch conn.Status {
		case IDLE:
			conn.Status = BUSY
			go func() {
				ss.send(conn, ss.sendQueue, err)
				conn.Conn.Close()
				conn.Status = CLOSED
			}()
		case CLOSED:
			c, err := net.Dial("tcp", conn.Url.Host)
			if err == nil {
				conn.Conn = httputil.NewClientConn(c, nil)
				conn.Status = IDLE
			} else {
				log.Printf("connect to backend <%s> fail %s", conn.Url.Host, err)
			}
		}
	}
}

func (ss *StoreServer) store() error {
	err := make(chan error)
	ss.sendLoop(err)
	for {
		select {
		case ti := <-ss.queue:
			buf, e := ss.encode(ti)
			if e != nil {
				log.Printf("encode trace item fail %s", e)
				continue
			}
			ss.sendQueue <- buf
		case e := <-err:
			log.Printf("send to backend fail: %s", e)
		case <-ss.Dying():
			return nil
		case <-time.After(time.Duration(3) * time.Second):
			ss.sendLoop(err)
		}
	}
}

func (ss *StoreServer) send(conn endPoint, queue chan []byte, err chan error) {
	for {
		select {
		case data := <-queue:
			req, e := http.NewRequest("POST", "/_bulk", bytes.NewReader(data))
			if e != nil {
				err <- e
				continue
			}
			resp, e := conn.Conn.Do(req)
			if e != nil {
				if e.Error() == httputil.ErrClosed.Error() {
					return
				}
				err <- e
			}
			if resp.StatusCode != 200 {
				buf := []byte{}
				for {
					b := make([]byte, BUF_SIZE)
					length, err := resp.Body.Read(b)
					buf = append(buf, b[0:length]...)
					if length < BUF_SIZE {
						break
					}
					if err != nil {
						break
					}
				}
				log.Printf("store to %s fail %s", conn.Url.Host, buf)
			}
		}

	}
}

func (ss *StoreServer) Stop() error {
	for _, endPoint := range ss.endPoints {
		endPoint.Conn.Close()
	}
	ss.Kill(nil)
	return ss.Wait()
}

func New(urls []string, queue chan common.TraceItem, index_name, index_type string) (*StoreServer, error) {
	endPoints := []endPoint{}
	for _, u := range urls {
		nurl, err := url.Parse(u)
		if err != nil {
			log.Printf("wrong url <%s>: %s", u, err)
			return nil, err
		}
		c, err := net.Dial("tcp", nurl.Host)
		if err != nil {
			log.Printf("connect to %s fail %s", u, err)
			endPoints = append(endPoints, endPoint{Conn: nil, Url: nurl, RawUrl: u, Status: CLOSED})
		} else {
			endPoints = append(endPoints, endPoint{Conn: httputil.NewClientConn(c, nil), Url: nurl, RawUrl: u, Status: IDLE})
		}
	}
	if len(endPoints) <= 0 {
		return nil, errors.New("no more endpoints to try")
	}
	ss := &StoreServer{
		endPoints:  endPoints,
		queue:      queue,
		sendQueue:  make(chan []byte),
		index_name: index_name,
		index_type: index_type,
	}
	ss.Go(ss.store)
	return ss, nil
}
