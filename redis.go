package redismaint

import (
	"github.com/pkg/errors"
	"github.com/gomodule/redigo/redis"
)

type redisc struct {
	pool *redis.Pool
	conn redis.Conn
}

func (r *redisc) gconn()redis.Conn{
	return r.pool.Get()
}

func dial(url string) (*redisc, error) {
	if url == "" {
		return nil, errors.New("empty string url")
	}
	pool := &redis.Pool{
		Dial: func() (redis.Conn, error) {
			return redis.Dial("tcp", url)
		},
	}
	if pool == nil {
		return nil, errors.New("unable to create redis pool")
	}
	conn := pool.Get()
	if conn == nil {
		return nil, errors.New("unableto get connection from pool")
	}
	defer conn.Close()
	if _, err := conn.Do("PING"); err != nil {
		return nil, err
	}
	return &redisc{pool: pool, conn: conn}, nil
}
