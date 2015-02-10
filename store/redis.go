package store

import (
	"bytes"
	"encoding/gob"
	"github.com/fzzy/radix/extra/pool"
	"github.com/lixm/ems/common"
	"github.com/stathat/consistent"
)

type Cluster struct {
	cons  *consistent.Consistent
	pools map[string]*pool.Pool
}

func NewCluster(servers []string, poolSize int) (*Cluster, error) {
	cons := consistent.New()
	pools := make(map[string]*pool.Pool)
	for _, server := range servers {
		p, err := pool.NewPool("tcp", server, poolSize)
		if err != nil {
			return nil, err
		}
		pools[server] = p
		cons.Add(server)
	}
	return &Cluster{cons: cons, pools: pools}, nil
}

func (c *Cluster) decode(data []byte) (common.RpcItem, error) {
	ri := common.RpcItem{}
	dec := gob.NewDecoder(bytes.NewReader(data))
	err := dec.Decode(&ri)
	return ri, err
}

func (c *Cluster) encode(ri common.RpcItem) ([]byte, error) {
	var buf bytes.Buffer
	enc := gob.NewEncoder(&buf)
	err := enc.Encode(ri)
	return buf.Bytes(), err
}

func (c *Cluster) HSet(key, filed string, val common.RpcItem) error {
	name, err := c.cons.Get(key)
	if err != nil {
		return err
	}
	r, err := c.pools[name].Get()
	if err != nil {
		return err
	}
	defer c.pools[name].Put(r)
	v, err := c.encode(val)
	if err != nil {
		return err
	}
	r.Cmd("HSET", key, filed, v)
	r.Cmd("EXPIRE", key, 60)
	return err
}

func (c *Cluster) HGet(key, filed string) (common.RpcItem, error) {
	name, err := c.cons.Get(key)
	if err != nil {
		return common.RpcItem{}, err
	}
	r, err := c.pools[name].Get()
	if err != nil {
		return common.RpcItem{}, err
	}
	defer c.pools[name].Put(r)
	val := r.Cmd("HGET", key, filed)
	data, err := val.Bytes()
	if err != nil {
		return common.RpcItem{}, err
	}
	return c.decode(data)
}

func (c *Cluster) HGetAll(key string) (map[string]common.RpcItem, error) {
	ret := make(map[string]common.RpcItem)
	name, err := c.cons.Get(key)

	if err != nil {
		return ret, err
	}
	r, err := c.pools[name].Get()

	if err != nil {
		return ret, err
	}

	defer c.pools[name].Put(r)
	val := r.Cmd("HGETALL", key)
	val.Hash()
	var field string
	for idx, ele := range val.Elems {
		buf, err := ele.Bytes()
		if err != nil {
			return ret, err
		}
		if idx%2 == 0 {
			field = string(buf)
		} else {
			ret[field], err = c.decode(buf)
			if err != nil {
				return ret, err
			}
		}
	}
	return ret, err
}

func (c *Cluster) Del(key string) error {
	name, err := c.cons.Get(key)
	if err != nil {
		return err
	}
	r, err := c.pools[name].Get()
	if err != nil {
		return err
	}
	defer c.pools[name].Put(r)
	val := r.Cmd("DEL", key)
	return val.Err
}

func (c *Cluster) Close() {
	for key, p := range c.pools {
		p.Empty()
		c.cons.Remove(key)
	}
}
