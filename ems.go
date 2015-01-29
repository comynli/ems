package main

import (
	"github.com/lixm/ems/config"
	"github.com/lixm/ems/frontend"
	"github.com/lixm/ems/store"
	"log"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"
)

var conf config.Conf

func init() {
	log.SetFlags(log.LstdFlags | log.Lshortfile)
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

	f, err := frontend.New(conf.Frontend.Listen, conf.Frontend.QueueSize)
	if err != nil {
		log.Fatalln(err)
	}
	s, err := store.New(conf.Store.Elasticsearchs, conf.Store.Redis.Server, conf.Store.Redis.PoolSize, f.Queue(), conf.Store.Index)
	if err != nil {
		f.Stop()
		log.Fatal(err)
	}
	sig := <-c
	log.Printf("%s received, exiting", sig.String())
	s.Stop()
	log.Println("store stopped")
	f.Stop()
	log.Println("proxy stopped")
	log.Println("exited")
}
