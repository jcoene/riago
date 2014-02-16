package riago

import (
	"math/rand"
	"testing"
	"time"

	. "github.com/smartystreets/goconvey/convey"
)

func TestPool(t *testing.T) {
	Convey("Manages out one connection", t, func() {
		p := NewPool("127.0.0.1:8087", 1)
		p.waitTimeout = 5 * time.Millisecond

		// Get a single connection
		conn, err := p.Get()
		So(err, ShouldEqual, nil)

		// The second get should fail
		_, err = p.Get()
		So(err, ShouldEqual, ErrPoolWaitTimeout)

		// Put back the first connection
		p.Put(conn)

		// The next get should succeed
		conn, err = p.Get()
		So(err, ShouldEqual, nil)

		// Put back
		p.Put(conn)

		// Close the pool
		err = p.Close()
		So(err, ShouldEqual, nil)
	})

	Convey("Manages many connections", t, func() {
		p := NewPool("127.0.0.1:8087", 20)
		p.waitTimeout = 5 * time.Millisecond

		var err error
		conns := make([]*Conn, 20)

		// Check out 20
		for i := 0; i < 20; i++ {
			conns[i], err = p.Get()
			So(err, ShouldEqual, nil)
		}

		// The next get should fail
		_, err = p.Get()
		So(err, ShouldEqual, ErrPoolWaitTimeout)

		// Put back the first connection
		p.Put(conns[0])

		// The next get should succeed
		conns[0], err = p.Get()
		So(err, ShouldEqual, nil)

		// Put back all
		for _, conn := range conns {
			p.Put(conn)
		}

		// Close the pool
		err = p.Close()
		So(err, ShouldEqual, nil)
	})

	Convey("Manages many connections concurrently", t, func() {
		p := NewPool("127.0.0.1:8087", 50)

		doStuff := func(p *Pool) {
			// Check out a conn
			conn, err := p.Get()
			So(err, ShouldEqual, nil)

			// Do some stuff
			time.Sleep(time.Duration(rand.Int31n(100)) * time.Millisecond)

			// Put it back
			p.Put(conn)
		}

		// Do a bunch of stuff
		for i := 0; i < 50; i++ {
			go doStuff(p)
		}

		// Let them spawn
		time.Sleep(10 * time.Millisecond)

		err := p.Close()
		So(err, ShouldEqual, nil)
	})

	Convey("Manages many failing connections concurrently", t, func() {
		p := NewPool("127.0.0.1:8087", 50)

		doStuffBadly := func(p *Pool) {
			// Check out a conn
			conn, err := p.Get()
			So(err, ShouldEqual, nil)

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
		for i := 0; i < 50; i++ {
			go doStuffBadly(p)
		}

		// Let them spawn
		time.Sleep(10 * time.Millisecond)

		err := p.Close()
		So(err, ShouldEqual, nil)
	})

	Convey("Manages closing connections in different states", t, func() {
		p := NewPool("127.0.0.1:8087", 50)

		// Get a single connection, wait and re-put
		doStuff := func(p *Pool) {
			conn, err := p.Get()
			So(err, ShouldEqual, nil)
			time.Sleep(100 * time.Millisecond)
			p.Put(conn)
		}

		doFailDial := func(p *Pool) {
			// Check out a conn
			conn, err := p.Get()
			So(err, ShouldEqual, nil)

			// Do some stuff
			time.Sleep(time.Duration(rand.Int31n(50)) * time.Millisecond)
			conn.addr = "127.0.0.1:999999"
			p.Fail(conn)
		}

		// doFailOther := func(p *Pool) {
		// 	conn, err := p.Get()
		// 	So(err, ShouldEqual, nil)
		// 	time.Sleep(time.Duration(rand.Int31n(50)) * time.Millisecond)
		// 	p.Fail(conn)
		// }

		// Spawn healthy connections
		for i := 0; i < 10; i++ {
			go doStuff(p)
		}

		// Spawn conns that wont dial
		for i := 0; i < 10; i++ {
			go doFailDial(p)
		}

		// Spawn conns that fail for another reason
		// for i := 0; i < 10; i++ {
		// 	go doFailOther(p)
		// }

		// Wait for things to happen
		time.Sleep(500 * time.Millisecond)

		// In the meantime, close the pool
		err := p.Close()
		So(err, ShouldEqual, nil)

		// Try to get another connection
		_, err = p.Get()
		So(err, ShouldEqual, ErrPoolClosing)
	})
}
