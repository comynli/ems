package main

import (
	"github.com/lixm/ems/config"
	"github.com/lixm/ems/proxy"
	"github.com/lixm/ems/store"
	"log"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"
)

var conf config.Conf

func init() {
	dir, err := filepath.Abs(filepath.Dir(os.Args[0]))
	if err != nil {
		log.Fatalln(err)
	}
	conf, err = config.Load((filepath.Join(dir, "ems.yml")))
	if err != nil {
		log.Fatalln(err)
	}
}

func main() {
	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt, syscall.SIGQUIT, syscall.SIGTERM)

	p, err := proxy.New(conf.Proxy.Listen)
	if err != nil {
		log.Fatalln(err)
	}
	s, err := store.New(conf.Store.Elasticsearchs, p.Queue(), conf.Store.Index, conf.Store.Type)
	if err != nil {
		p.Stop()
		log.Fatal(err)
	}
	sig := <-c
	log.Printf("%s received, exiting", sig.String())
	s.Stop()
	log.Println("store stopped")
	p.Stop()
	log.Println("proxy stopped")
	log.Println("exited")
}
