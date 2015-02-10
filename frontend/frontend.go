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
	BUF_SIZE = 1500
)

type FrontendServer struct {
	logAddr  string
	rpcAddr  string
	LogQueue chan common.LogItem
	RpcQueue chan common.RequestHeader
	tomb.Tomb
}

func (fs *FrontendServer) listen() error {
	logAddr, err := net.ResolveUDPAddr("udp", fs.logAddr)
	if err != nil {
		log.Printf("wrong listen address %s", err)
		return err
	}
	rpcAddr, err := net.ResolveUDPAddr("udp", fs.rpcAddr)
	if err != nil {
		log.Printf("wrong listen address %s", err)
		return err
	}
	logConn, err := net.ListenUDP("udp", logAddr)
	if err != nil {
		log.Printf("lesten fail %s", err)
		return err
	}
	rpcConn, err := net.ListenUDP("udp", rpcAddr)
	if err != nil {
		log.Printf("lesten fail %s", err)
		return err
	}
	//defer logConn.Close()
	//defer rpcConn.Close()

	logCtl := make(chan int, 1)
	rpcCtl := make(chan int, 1)
	logCtl <- 1
	rpcCtl <- 1

	for {
		select {
		case <-fs.Dying():
			return nil
		case <-logCtl:
			go func(conn *net.UDPConn) {
				defer func() {
					logCtl <- 1
				}()
				if conn == nil {
					return
				}
				buf := []byte{}
				b := make([]byte, BUF_SIZE)
				conn.SetReadDeadline(time.Now().Add(time.Duration(3) * time.Second))
				length, err := conn.Read(b)
				buf = append(buf, b[0:length]...)
				if err != nil {
					log.Panic(err)
					log.Printf("read from %s fail: %s", conn.RemoteAddr().Network(), err)
					return
				}
				if len(buf) > 0 {
					li, err := common.LogDecode(buf)
					if err != nil {
						log.Printf("Decode message<%s> fail: %s", string(buf), err)
						return
					}
					select {
					case fs.LogQueue <- li:
					default:
						log.Println("log overflow queue")
					}
				}
			}(logConn)
		case <-rpcCtl:
			go func(conn *net.UDPConn) {
				defer func() {
					rpcCtl <- 1
				}()
				if conn == nil {
					return
				}
				buf := []byte{}
				b := make([]byte, BUF_SIZE)
				conn.SetReadDeadline(time.Now().Add(time.Duration(3) * time.Second))
				length, err := conn.Read(b)
				buf = append(buf, b[0:length]...)
				if err != nil {
					log.Panic(err)
					log.Printf("read from %s fail: %s", conn.RemoteAddr().Network(), err)
					return
				}
				if len(buf) > 0 {
					ri, err := common.RpcDecode(buf)
					if err != nil {
						log.Printf("Decode message<%s> fail: %s", string(buf), err)
						return
					}
					select {
					case fs.RpcQueue <- ri:
					default:
						log.Println("rpc overflow queue")
					}
				}
			}(rpcConn)
		}
	}
	logConn.Close()
	rpcConn.Close()
	return nil
}

func (fs *FrontendServer) Stop() error {
	fs.Kill(nil)
	return fs.Wait()
}

func New(logAddr, rpcAddr string, queue_size int64) (*FrontendServer, error) {
	logQueue := make(chan common.LogItem, queue_size)
	rpcQueue := make(chan common.RequestHeader, queue_size)
	fs := &FrontendServer{logAddr: logAddr, rpcAddr: rpcAddr, LogQueue: logQueue, RpcQueue: rpcQueue}
	fs.Go(fs.listen)
	return fs, nil
}
