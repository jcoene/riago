package riago

import (
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"
)

var (
	ErrPoolClosing     = errors.New("pool closing")
	ErrPoolWaitTimeout = errors.New("pool wait timed out")
)

// Pool represents a pool of connections to Riak hosts.
type Pool struct {
	count       int
	closing     int32
	conns       chan *Conn
	mutex       sync.Mutex
	waitTimeout time.Duration
}

// Creates a new Pool for a given host and connection count.
// Dials all connections before returning to prevent a stampede.
// Connections that fail to connect will retry in the background.
func NewPool(addr string, count int) (p *Pool) {
	p = &Pool{
		count:       count,
		conns:       make(chan *Conn, count),
		waitTimeout: 5 * time.Second,
	}

	for i := 0; i < count; i++ {
		c := NewConn(addr)

		if err := c.Recover(); err != nil {
			p.Fail(c)
		} else {
			p.Put(c)
		}
	}

	go p.pinger()

	return
}

// Close the connection pool after waiting for connections to
// gracefully return.
func (p *Pool) Close() (err error) {
	atomic.StoreInt32(&p.closing, 1)

	for i := 0; i < p.count; i++ {
		c := <-p.conns
		c.Close()
	}

	return
}

// Get a connection from the pool. Returns an error if the
// operation takes longer than the pool wait timeout duration.
func (p *Pool) Get() (c *Conn, err error) {
	if p.isClosing() {
		err = ErrPoolClosing
		return
	}

	// Optimistically try a non-blocking read to avoid a timer.
	select {
	case c = <-p.conns:
		return
	default:
		break
	}

	// Fall back to waiting on a timer.
	select {
	case c = <-p.conns:
		break
	case <-time.After(p.waitTimeout):
		err = ErrPoolWaitTimeout
		break
	}

	return
}

// Release a connection back to the pool.
func (p *Pool) Put(c *Conn) {
	p.conns <- c
	return
}

// Fail a connection, making it unavailable. Spawns a goroutine
// that attempts to recover the connection. It is not made
// available to the pool while failed.
func (p *Pool) Fail(c *Conn) {
	go func() {
		var err error
		i := 0
		for {
			i++
			if p.isClosing() {
				p.Put(c)
				return
			}

			if err = c.Recover(); err != nil {
				time.Sleep(1 * time.Second)
				continue
			}

			p.Put(c)
			return
		}
	}()
}

func (p *Pool) pinger() {
	t := time.NewTicker(time.Duration(10000.0/p.count) * time.Millisecond)
	for {
		<-t.C

		if p.isClosing() {
			t.Stop()
			return
		}

		if c, err := p.Get(); err == nil {
			fmt.Println("conn got ok")
			if err = c.Ping(); err != nil {
				fmt.Println("conn ping fail", err)
				p.Fail(c)
			} else {
				fmt.Println("conn ping ok")
				p.Put(c)
			}
		} else {
			fmt.Println("conn got err", err)
		}
	}
}
func (p *Pool) isClosing() bool {
	return atomic.LoadInt32(&p.closing) == 1
}
