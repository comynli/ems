package frontend

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

type FrontendServer struct {
	addr  string
	queue chan common.TraceItem
	tomb.Tomb
}

func (fs *FrontendServer) listen() error {
	addr, err := net.ResolveUDPAddr("udp", fs.addr)
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
		case <-fs.Dying():
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
				select {
				case fs.queue <- ti:
				default:
					log.Println("overflow queue")
				}

			}
			//conn.WriteTo([]byte("ok"), conn.RemoteAddr())
		}
	}
}

func (fs *FrontendServer) Queue() chan common.TraceItem {
	return fs.queue
}

func (fs *FrontendServer) Stop() error {
	fs.Kill(nil)
	return fs.Wait()
}

func New(addr string, queue_size int64) (*FrontendServer, error) {
	queue := make(chan common.TraceItem, queue_size)
	fs := &FrontendServer{addr: addr, queue: queue}
	fs.Go(fs.listen)
	return fs, nil
}
