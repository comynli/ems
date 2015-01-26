package store

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/lixm/ems/common"
	tomb "gopkg.in/tomb.v2"
	"log"
	"math/rand"
	"net"
	"net/http"
	"net/http/httputil"
	"net/url"
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
	Status int
}

type StoreServer struct {
	endPoints  []endPoint
	queue      chan common.TraceItem
	sendQueue  chan []byte
	index_name string
	index_type string
	tomb.Tomb
}

func (ss *StoreServer) create_meta() ([]byte, error) {
	index_name := fmt.Sprintf("%s-%s", ss.index_name, time.Now().Format("2006.01.02"))
	meta := make(map[string]map[string]string)
	meta["index"] = make(map[string]string)
	meta["index"]["_index"] = index_name
	meta["index"]["_type"] = ss.index_type
	return json.Marshal(meta)
}

func (ss *StoreServer) update_meta(r Result) ([]byte, error) {
	index_name := fmt.Sprintf("%s-%s", ss.index_name, time.Now().Format("2006.01.02"))
	meta := make(map[string]map[string]string)
	meta["update"] = make(map[string]string)
	meta["update"]["_index"] = index_name
	meta["update"]["_type"] = ss.index_type
	meta["update"]["_id"] = r.Hits.Hits[0].ID
	return json.Marshal(meta)
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
			buf, e := ti.Encode()
			if e != nil {
				log.Printf("encode trace item fail %s", e)
			}
			if ti.End != 0 && ti.Start != 0 {
				meta, _ := ss.create_meta()
				ss.sendQueue <- toBytes(meta, buf)
			} else {
				query := queryBuild(ti)
				url := fmt.Sprintf("/%s-*/%s/_search?q=request_id:%s", ss.index_name, ss.index_type, ti.RequestId)
				req, e := http.NewRequest("GET", url, bytes.NewReader(query))
				if e != nil {
					log.Printf("make request fail %s", err)
					continue
				}
				resp, e := ss.endPoints[rand.Intn(len(ss.endPoints))].Conn.Do(req)
				if e != nil {
					meta, _ := ss.create_meta()
					ss.sendQueue <- toBytes(meta, buf)
					log.Printf("%v", e)
					continue
				}
				if resp.StatusCode == 200 {
					result, e := ParseStream(resp.Body)
					if e != nil {
						meta, _ := ss.create_meta()
						ss.sendQueue <- toBytes(meta, buf)
						log.Printf("%v", e)
						continue
					}
					if len(result.Hits.Hits) <= 0 {
						meta, _ := ss.create_meta()
						ss.sendQueue <- toBytes(meta, buf)
						continue
					}
					meta, _ := ss.update_meta(result)
					ss.sendQueue <- toBytes(meta, buf)
				} else {
					meta, _ := ss.create_meta()
					ss.sendQueue <- toBytes(meta, buf)
				}
			}
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
			endPoints = append(endPoints, endPoint{Conn: nil, Url: nurl, Status: CLOSED})
		} else {
			endPoints = append(endPoints, endPoint{Conn: httputil.NewClientConn(c, nil), Url: nurl, Status: IDLE})
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
