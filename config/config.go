package config

import (
	yaml "gopkg.in/yaml.v2"
	"io/ioutil"
)

type Conf struct {
	Frontend FrontendConfig `frontend`
	Store    StoreConfig    `store`
}

type FrontendConfig struct {
	Listen    string `listen`
	QueueSize int64  `queue_size`
}

type StoreConfig struct {
	Elasticsearchs []string    `elasticsearchs`
	Redis          RedisConfig `redis`
	Index          string      `index`
	Type           string      `type`
}

type RedisConfig struct {
	Server   []string `server`
	PoolSize int      `pool_size`
}

func Load(cfg string) (Conf, error) {
	conf := Conf{}
	c, err := ioutil.ReadFile(cfg)
	if err != nil {
		return conf, err
	}
	err = yaml.Unmarshal(c, &conf)
	return conf, err
}
