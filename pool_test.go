package riago

import (
	"math/rand"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestPoolSingleConn(t *testing.T) {
	assert := assert.New(t)

	p := NewPool("127.0.0.1:8087", 1)
	p.waitTimeout = 5 * time.Millisecond

	// Get a single connection
	conn, err := p.Get()
	assert.Nil(err)

	// The second get should fail
	_, err = p.Get()
	assert.Equal(ErrPoolWaitTimeout, err)

	// Put back the first connection
	p.Put(conn)

	// The next get should succeed
	conn, err = p.Get()
	assert.Nil(err)

	// Put back
	p.Put(conn)

	// Close the pool
	err = p.Close()
	assert.Nil(err)
}

func TestPoolManyConnections(t *testing.T) {
	assert := assert.New(t)

	n := 5
	p := NewPool("127.0.0.1:8087", n)
	p.waitTimeout = 5 * time.Millisecond

	var err error
	conns := make([]*Conn, n)

	// Check out n
	for i := 0; i < n; i++ {
		conns[i], err = p.Get()
		assert.Nil(err)
	}

	// The next get should fail
	_, err = p.Get()
	assert.Equal(ErrPoolWaitTimeout, err)

	// Put back the first connection
	p.Put(conns[0])

	// The next get should succeed
	conns[0], err = p.Get()
	assert.Nil(err)

	// Put back all
	for _, conn := range conns {
		p.Put(conn)
	}

	// Close the pool
	err = p.Close()
	assert.Nil(err)
}

func TestPoolManyConnectionsConcurrently(t *testing.T) {
	assert := assert.New(t)

	n := 5
	p := NewPool("127.0.0.1:8087", n)

	doStuff := func(p *Pool) {
		// Check out a conn
		conn, err := p.Get()
		assert.Nil(err)

		// Do some stuff
		time.Sleep(time.Duration(rand.Int31n(100)) * time.Millisecond)

		// Put it back
		p.Put(conn)
	}

	// Do a bunch of stuff
	for i := 0; i < n; i++ {
		go doStuff(p)
	}

	// Let them spawn
	time.Sleep(10 * time.Millisecond)

	err := p.Close()
	assert.Nil(err)
}

func TestPoolManyFailingConnectionsConcurrently(t *testing.T) {
	assert := assert.New(t)

	n := 5
	p := NewPool("127.0.0.1:8087", n)

	doStuffBadly := func(p *Pool) {
		// Check out a conn
		conn, err := p.Get()
		assert.Nil(err)

		// Do some stuff
		time.Sleep(time.Duration(rand.Int31n(100)) * time.Millisecond)

		// Sometimes things fail
		if rand.Intn(10) < 5 {
			p.Fail(conn)
		} else {
			p.Put(conn)
		}
	}

	// Do a bunch of stuff, some of it will fail
	for i := 0; i < n; i++ {
		go doStuffBadly(p)
	}

	// Let them spawn
	time.Sleep(10 * time.Millisecond)

	err := p.Close()
	assert.Nil(err)
}

func TestPoolConnectionClosing(t *testing.T) {
	assert := assert.New(t)

	n := 10
	p := NewPool("127.0.0.1:8087", n)

	// Get a single connection, wait and re-put
	doStuff := func(p *Pool) {
		conn, err := p.Get()
		assert.Nil(err)
		time.Sleep(100 * time.Millisecond)
		p.Put(conn)
	}

	doFailDial := func(p *Pool) {
		// Check out a conn
		conn, err := p.Get()
		assert.Nil(err)

		// Do some stuff
		time.Sleep(time.Duration(rand.Intn(10)) * time.Millisecond)
		conn.addr = "127.0.0.1:999999"
		p.Fail(conn)
	}

	doFailOther := func(p *Pool) {
		conn, err := p.Get()
		assert.Nil(err)
		time.Sleep(time.Duration(rand.Intn(10)) * time.Millisecond)
		p.Fail(conn)
	}

	// Spawn healthy connections
	for i := 0; i < n/3; i++ {
		go doStuff(p)
	}

	// Spawn conns that wont dial
	for i := 0; i < n/3; i++ {
		go doFailDial(p)
	}

	// Spawn conns that fail for another reason
	for i := 0; i < n/3; i++ {
		go doFailOther(p)
	}

	// Wait for things to happen
	time.Sleep(500 * time.Millisecond)

	// In the meantime, close the pool
	err := p.Close()
	assert.Nil(err)

	// Try to get another connection
	_, err = p.Get()
	assert.Equal(ErrPoolClosing, err)
}
