package bolt

import (
	"io"
	"time"

	"github.com/axiomzen/golang-neo4j-bolt-driver/log"
	"github.com/axiomzen/interpool"
)

// DriverPool is a driver allowing connection to Neo4j with support for connection pooling
// The driver allows you to open a new connection to Neo4j
//
// DriverPool objects should be THREAD SAFE, so you can use them
// to open connections in multiple threads.  The connection objects
// themselves, and any prepared statements/transactions within ARE NOT
// THREAD SAFE.
type DriverPool interface {
	io.Closer
	// Get gets a connection from the pool
	Get() (Conn, error)
	// Put puts the connection back into the pool
	// for later reuse
	Put(Conn) error
	// Closed lets you know if we are closed or not
	Closed() bool
	// Len gives the size of the pool
	Len() int
	// FreeLen gives the number of free connections
	FreeLen() int
}

// PoolOptions are the options for the connection pool
type PoolOptions struct {
	PoolSize           int
	PoolTimeout        time.Duration
	IdleTimeout        time.Duration
	IdleCheckFrequency time.Duration
	MaxAge             time.Duration
}

type boltConnPool struct {
	pool interpool.Pooler
}

// Close implements io.Closer and will close all the connections in the pool
func (bcp *boltConnPool) Close() error {
	st := bcp.pool.Stats()
	if st.TotalConns != st.FreeConns {

		log.Errorf(
			"connection leaking detected: total_conns=%d free_conns=%d",
			st.TotalConns, st.FreeConns,
		)
	}
	return bcp.pool.Close()
}

// Closed implements DriverPool.Closed
func (bcp *boltConnPool) Closed() bool {
	return bcp.pool.Closed()
}

// Get implements DriverPool.Get
func (bcp *boltConnPool) Get() (Conn, error) {
	c, _, e := bcp.pool.Get()
	if e != nil {
		return nil, e
	}
	ourc := c.(Conn)
	return ourc, nil
	// should already come initialized from the factory
}

// Put implements DriverPool.Put
func (bcp *boltConnPool) Put(c Conn) error {
	if err := c.CheckHealth(); err != nil {
		return bcp.pool.Remove(c, err)
	}
	if err := c.Close(); err != nil {
		// remove if we coudln't cleanup properly
		return bcp.pool.Remove(c, err)
	}
	return bcp.pool.Put(c)
}

// Len implements DriverPool.Len
func (bcp *boltConnPool) Len() int {
	return bcp.pool.Len()
}

// FreeLen implements DriverPool.FreeLen
func (bcp *boltConnPool) FreeLen() int {
	return bcp.pool.FreeLen()
}
