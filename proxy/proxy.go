package proxy

import (
	"github.com/lixm/ems/common"
	tomb "gopkg.in/tomb.v2"
	"log"
	"net"
	"time"
)

const (
	IDLE     = 0
	BUSY     = 1
	CLOSED   = 2
	ERROR    = 3
	BUF_SIZE = 1024
)

type ProxyServer struct {
	addr  string
	queue chan common.TraceItem
	tomb.Tomb
}

func (ps *ProxyServer) listen() error {
	addr, err := net.ResolveUDPAddr("udp", ps.addr)
	if err != nil {
		log.Printf("wrong listen address %s", err)
		return err
	}
	conn, err := net.ListenUDP("udp", addr)
	if err != nil {
		log.Printf("lesten fail %s", err)
		return err
	}
	defer conn.Close()

	isClose := false
	for {
		select {
		case <-ps.Dying():
			isClose = true
			return nil
		default:
			buf := []byte{}
			for !isClose {
				b := make([]byte, BUF_SIZE)
				conn.SetReadDeadline(time.Now().Add(time.Duration(3) * time.Second))
				length, err := conn.Read(b)
				buf = append(buf, b[0:length]...)
				if length < BUF_SIZE {
					break
				}
				if err != nil {
					log.Printf("read from %s fail: %s", conn.RemoteAddr().Network(), err)
					break
				}
			}
			if len(buf) > 0 {
				ti, err := common.Decode(buf)
				if err != nil {
					log.Printf("Decode message<%s> fail: %s", string(buf), err)
					continue
				}
				ps.queue <- ti
			}
			conn.WriteTo([]byte("ok"), conn.RemoteAddr())
		}
	}
}

func (ps *ProxyServer) Queue() chan common.TraceItem {
	return ps.queue
}

func (ps *ProxyServer) Stop() error {
	ps.Kill(nil)
	return ps.Wait()
}

func New(addr string) (*ProxyServer, error) {
	queue := make(chan common.TraceItem, 1024)
	ps := &ProxyServer{addr: addr, queue: queue}
	ps.Go(ps.listen)
	return ps, nil
}
