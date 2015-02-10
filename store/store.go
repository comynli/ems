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
	endPoints    []endPoint
	redisCluster *Cluster
	logQueue     chan common.LogItem
	rpcQueue     chan common.RequestHeader
	sendQueue    chan []byte
	index_name   string
	tomb.Tomb
}

func toBytes(meta, ti []byte) []byte {
	data := append(meta, '\n')
	data = append(data, ti...)
	data = append(data, '\n')
	return data
}

func (ss *StoreServer) meta(index_type string, timestamp time.Time) ([]byte, error) {
	index_name := fmt.Sprintf("%s-%s", ss.index_name, timestamp.Format("2006.01.02"))
	meta := make(map[string]map[string]string)
	meta["index"] = make(map[string]string)
	meta["index"]["_index"] = index_name
	meta["index"]["_type"] = index_type
	return json.Marshal(meta)
}

func (ss *StoreServer) logEncode(li common.LogItem) ([]byte, error) {
	data := []byte{}
	rs, err := ss.redisCluster.HGetAll(li.RequestId)
	if err != nil {
		return nil, err
	}
	for k, r := range rs {
		t := common.Convert(r, li.Host, li.Path)
		buf, err := t.Encode()
		if err != nil {
			log.Printf("%s %s encode fail %s", t.RequestId, k, err)
			continue
		}
		meta, _ := ss.meta("trace", t.TimeStamp)
		data = append(data, toBytes(meta, buf)...)
	}
	meta, _ := ss.meta("log", li.TimeStamp)
	buf, err := li.Encode()
	if err != nil {
		return nil, err
	}
	data = append(data, toBytes(meta, buf)...)
	ss.redisCluster.Del(li.RequestId)
	return data, nil
}

func (ss *StoreServer) rpcEncode(ri common.RequestHeader) {
	key := strings.Join([]string{ri.Client, ri.Server, strconv.Itoa(int(ri.Seq))}, "#")
	t, err := ss.redisCluster.HGet(ri.RequestId, key)
	if err != nil {
		ss.redisCluster.HSet(ri.RequestId, key, ri)
		return
	}
	if t.RequestId == "" {
		ss.redisCluster.HSet(ri.RequestId, key, ri)
		return
	}
	if ri.Start != 0 {
		t.Start = ri.Start
	}
	if ri.End != 0 {
		t.End = ri.End
		t.Status = ri.Status
	}
	ss.redisCluster.HSet(ri.RequestId, key, ri)
	return
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
		case li := <-ss.logQueue:
			buf, e := ss.logEncode(li)
			if e != nil {
				log.Printf("encode trace item fail %s", e)
				continue
			}
			if buf != nil {
				ss.sendQueue <- buf
			}
		case ri := <-ss.rpcQueue:
			ss.rpcEncode(ri)
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
			if resp == nil {
				log.Printf("store to %s fail no responce", conn.Url.Host)
				continue
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
	ss.redisCluster.Close()
	ss.Kill(nil)
	return ss.Wait()
}

func New(urls, redisServers []string, redisPoolSize int, logQueue chan common.LogItem, rpcQueue chan common.RequestHeader, index_name string) (*StoreServer, error) {
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
	c, err := NewCluster(redisServers, redisPoolSize)
	if err != nil {
		return nil, err
	}
	ss := &StoreServer{
		endPoints:    endPoints,
		logQueue:     logQueue,
		rpcQueue:     rpcQueue,
		sendQueue:    make(chan []byte),
		index_name:   index_name,
		redisCluster: c,
	}
	ss.Go(ss.store)
	return ss, nil
}
