package gocql

import (
	"errors"
	"log"
	"sync"
	"time"
)

type controlConnection struct {
	heartbeatInterval time.Duration
	errCh             chan error

	ring *ringDescriber

	session *Session

	// TODO: use less locks, should use atomic.Value but that requires 1.4+
	mu   sync.RWMutex
	conn *Conn

	closeMu   sync.RWMutex
	closed    bool
	quit      chan struct{}
	closeWait sync.WaitGroup
}

func newControlConn(session *Session, heartbeatInterval time.Duration) *controlConnection {
	c := &controlConnection{
		heartbeatInterval: heartbeatInterval,
		session:           session,
		quit:              make(chan struct{}),
		errCh:             make(chan error),
	}

	ringDescriber := &ringDescriber{
		session:    session,
		dcFilter:   session.cfg.Discovery.DcFilter,
		rackFilter: session.cfg.Discovery.RackFilter,
		conn:       c,
	}
	c.ring = ringDescriber
	c.closeWait.Add(2)

	go c.run()
	go c.heartBeat()

	return c
}

func (c *controlConnection) heartBeat() {
	defer c.closeWait.Done()

	for {
		select {
		case <-c.quit:
			return
		case <-time.After(100 * time.Millisecond):
		}

		c.mu.RLock()
		if c.conn == nil {
			c.mu.RUnlock()

			select {
			case <-c.quit:
				return
			case c.errCh <- errors.New("nil conn"):
				continue
			}
		}

		// have a select switch to do polling as well? is polling necersarry?
		// TODO: check if resp is an error frame
		_, err := c.conn.exec(&optionsFrame{}, nil)
		c.mu.RUnlock()
		if err == nil {
			select {
			case <-time.After(c.heartbeatInterval):
				continue
			case <-c.quit:
				return
			}
		}

		select {
		case c.errCh <- err:
		case <-c.quit:
			return
		}
	}
}

func (c *controlConnection) run() {
	defer c.closeWait.Done()

	// run a heartbeat on the control connection, possible move this to run on
	// all connections?
	for {
		select {
		case <-c.quit:
			return
		case err := <-c.errCh:
			log.Println(err)
			c.reconnect()
		}

		// hostDown will be called because this error will be passed back from
		// the connections read loop
		// TODO: ensure that the error is actually fatal, not all errors will close
		// the connection and stop the recv loop.
	}
}

func (c *controlConnection) reconnect() {

	c.closeMu.RLock()
	defer c.closeMu.RUnlock()

	if c.closed {
		return
	}

	c.mu.Lock()

	if c.conn != nil {
		c.conn.Close()
	}

	// TODO: pick another host not a conn
	// pick another host from the available pool
	conn := c.session.pool.Pick(nil)
	if conn == nil {
		c.mu.Unlock()
		return
	}
	addr := conn.addr

	conn, err := Connect(addr, conn.cfg, c)
	if err != nil {
		c.mu.Unlock()
		log.Println(err)
		return
	}

	req := &registerFrame{
		events: []string{"TOPOLOGY_CHANGE", "STATUS_CHANGE", "SCHEMA_CHANGE"},
	}

	resp, err := conn.exec(req, nil)
	if err != nil {
		c.mu.Unlock()
		log.Println(err)
		conn.Close()
		return
	}

	switch f := resp.(type) {
	case *readyFrame:
	default:
		log.Println(f)
		c.mu.Unlock()
		conn.Close()
		return
	}

	c.conn = conn
	c.mu.Unlock()

	// update the ring topology and schema as it might have changed since last registering
	c.ring.refreshHosts()
}

type eventHandler interface {
	handleEvent(framer *framer)
}

func (c *controlConnection) handleEvent(framer *framer) {
	defer framerPool.Put(framer)

	frame, err := framer.parseFrame()
	if err != nil {
		// logger? This is bad, close conn and abandon ship!
		log.Println(err)
		return
	}

	switch f := frame.(type) {
	case *schemaChangeEvent:
		// c.schemaChange(f)
	case *topologyChangeEvent:
		// c.topologyChange(f)
	case *statusChageEvent:
		// c.statusChange(f)
	case error:
		// also very bad!
	default:
		log.Printf("unexpected event response: %v", f)
	}
}

func (c *controlConnection) HandleError(conn *Conn, err error, closed bool) {
	if !closed {
		return
	}

	c.hostDown(conn.addr)
}

func (c *controlConnection) hostAdded(addr string) {
	if c.Closed() {
		return
	}

	// TODO: only add this node
	c.ring.refreshHosts()
}

func (c *controlConnection) hostRemoved(addr string) {
	if c.Closed() {
		return
	}

	// TODO: implement this differently
	c.hostDown(addr)
}

func (c *controlConnection) hostUp(addr string) {
	if c.Closed() {
		return
	}

	c.ring.refreshHosts()
}

func (c *controlConnection) hostDown(addr string) {
	if c.Closed() {
		return
	}

	// TODO: ensure that they both have the same format, ip:port
	c.mu.Lock()
	if c.conn == nil {
		c.mu.Unlock()
		return
	}

	if c.conn.addr != addr {
		c.mu.Unlock()
		c.ring.refreshHosts()
		return
	}

	c.conn.Close()
	c.conn = nil
	c.mu.Unlock()
}

func (c *controlConnection) query(statement string, values ...interface{}) *Iter {
	c.mu.RLock()
	// should we hold the lock whilst executing the query?
	defer c.mu.RUnlock()
	if c.conn == nil {
		// TODO: can we block until the connection becomes available?
		return &Iter{err: ErrNoConnections}
	}

	q := &Query{
		stmt:     statement,
		values:   values,
		cons:     One,
		session:  c.session,
		pageSize: c.session.cfg.PageSize,
		prefetch: 0.25,
		rt:       c.session.cfg.RetryPolicy,
	}

	return c.conn.executeQuery(q)
}

func (c *controlConnection) address() string {
	c.mu.RLock()
	defer c.mu.RUnlock()
	if c.conn == nil {
		return ""
	}

	return c.conn.addr
}

func (c *controlConnection) Closed() bool {
	c.closeMu.RLock()
	closed := c.closed
	c.closeMu.RUnlock()
	return closed
}

func (c *controlConnection) Close() {
	c.closeMu.Lock()
	if c.closed {
		c.closeMu.Unlock()
		return
	}
	close(c.quit)
	c.closeWait.Wait()
	c.closed = true

	c.closeMu.Unlock()

	c.mu.Lock()
	if c.conn != nil {
		c.conn.Close()
		c.conn = nil
	}
	c.mu.Unlock()
}
