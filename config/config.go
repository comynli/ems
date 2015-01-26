package config

import (
	yaml "gopkg.in/yaml.v2"
	"io/ioutil"
)

type Conf struct {
	Proxy ProxyConfig `proxy`
	Store StoreConfig `store`
}

type ProxyConfig struct {
	Listen string `listen`
}

type StoreConfig struct {
	Elasticsearchs []string `elasticsearchs`
	Index          string   `index`
	Type           string   `type`
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