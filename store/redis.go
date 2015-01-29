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

func (c *Cluster) decode(data []byte) (common.TraceItem, error) {
	ti := common.TraceItem{}
	dec := gob.NewDecoder(bytes.NewReader(data))
	err := dec.Decode(&ti)
	return ti, err
}

func (c *Cluster) encode(ti common.TraceItem) ([]byte, error) {
	var buf bytes.Buffer
	enc := gob.NewEncoder(&buf)
	err := enc.Encode(ti)
	return buf.Bytes(), err
}

func (c *Cluster) HSet(key, filed string, val common.TraceItem) error {
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

func (c *Cluster) HGet(key, filed string) (common.TraceItem, error) {
	name, err := c.cons.Get(key)
	if err != nil {
		return common.TraceItem{}, err
	}
	r, err := c.pools[name].Get()
	if err != nil {
		return common.TraceItem{}, err
	}
	defer c.pools[name].Put(r)
	val := r.Cmd("HGET", key, filed)
	data, err := val.Bytes()
	if err != nil {
		return common.TraceItem{}, err
	}
	return c.decode(data)
}

func (c *Cluster) HGetAll(key string) (map[string]common.TraceItem, error) {
	ret := make(map[string]common.TraceItem)
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
