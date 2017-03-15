package bolt

import (
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/axiomzen/golang-neo4j-bolt-driver/log"
	"github.com/axiomzen/interpool"
	ginkgo "github.com/onsi/ginkgo"
	check "gopkg.in/check.v1"
)

var (
	_neo4jConnStr = os.Getenv("NEO4J_BOLT")
)

// Hook up gocheck into the "go test" runner.
func Test(t *testing.T) { check.TestingT(t) }

type PoolTest struct {
	db   Driver
	pool DriverPool
}

const poolSize = 10

var _ = check.Suite(&PoolTest{})

func (t *PoolTest) SetUpTest(c *check.C) {
	dops := DefaultDriverOptions()
	if _neo4jConnStr != "" {
		log.Info("Using NEO4J for tests:", _neo4jConnStr)
	} else if os.Getenv("ENSURE_NEO4J_BOLT") != "" {
		log.Fatal("Must give NEO4J_BOLT environment variable")
	}
	dops.Addr = _neo4jConnStr
	t.db = NewDriverWithOptions(dops)
	ops := DefaultPoolOptions()
	ops.IdleTimeout = time.Second
	ops.PoolSize = poolSize
	t.pool = t.db.NewConnPool(ops)
}

func (t *PoolTest) TearDownTest(c *check.C) {
	_ = t.pool.Close()
}

func (t *PoolTest) TestBoltPool_PoolReusesConnection(c *check.C) {
	for i := 0; i < 100; i++ {
		con, err := t.pool.Get()
		c.Assert(err, check.IsNil)
		_, err = con.ExecNeo("MATCH (n) DETACH DELETE n", nil)
		c.Assert(err, check.IsNil)
		err = t.pool.Put(con)
		c.Assert(err, check.IsNil)
	}

	c.Assert(t.pool.Len(), check.Equals, 1)
	c.Assert(t.pool.FreeLen(), check.Equals, 1)
}

func perform(n int, cbs ...func(int)) {
	var wg sync.WaitGroup
	for _, cb := range cbs {
		for i := 0; i < n; i++ {
			wg.Add(1)
			go func(cb func(int), i int) {
				defer ginkgo.GinkgoRecover()
				defer wg.Done()

				cb(i)
			}(cb, i)
		}
	}
	wg.Wait()
}

func (t *PoolTest) TestBoltPool_PoolMaxSize(c *check.C) {
	N := 1000

	perform(N, func(int) {
		con, err := t.pool.Get()
		c.Assert(err, check.IsNil)
		_, err = con.ExecNeo("MATCH (n) DETACH DELETE n", nil)
		c.Assert(err, check.IsNil)
		err = t.pool.Put(con)
		c.Assert(err, check.IsNil)
	})

	c.Assert(t.pool.Len(), check.Equals, poolSize)
	c.Assert(t.pool.FreeLen(), check.Equals, poolSize)
}

func (t *PoolTest) TestCloseClosesAllConnections(c *check.C) {
	N := poolSize

	perform(N, func(int) {
		con, err := t.pool.Get()
		c.Assert(err, check.IsNil)
		_, err = con.ExecNeo("MATCH (n) DETACH DELETE n", nil)
		c.Assert(err, check.IsNil)
		//err = t.pool.Put(con)
		//c.Assert(err, check.IsNil)
	})

	c.Assert(t.pool.Len(), check.Equals, poolSize)
	c.Assert(t.pool.FreeLen(), check.Equals, 0)

	err := t.pool.Close()
	c.Assert(err, check.IsNil)
	c.Assert(t.pool.Len(), check.Equals, 0)
	c.Assert(t.pool.FreeLen(), check.Equals, 0)

}

// func (t *PoolTest) TestCloseClosesAllConnections(c *check.C) {
// 	ln := t.db.Listen("test_channel")

// 	wait := make(chan struct{}, 2)
// 	go func() {
// 		wait <- struct{}{}
// 		_, _, err := ln.Receive()
// 		c.Assert(err, ErrorMatches, `^(.*use of closed (file or )?network connection|EOF)$`)
// 		wait <- struct{}{}
// 	}()

// 	select {
// 	case <-wait:
// 		// ok
// 	case <-time.After(3 * time.Second):
// 		c.Fatal("timeout")
// 	}

// 	c.Assert(t.db.Close(), check.IsNil)

// 	select {
// 	case <-wait:
// 		// ok
// 	case <-time.After(3 * time.Second):
// 		c.Fatal("timeout")
// 	}

// 	c.Assert(t.pool.Len(), check.Equals, 0)
// 	c.Assert(t.pool.FreeLen(), check.Equals, 0)
// }

func (t *PoolTest) TestBoltPool_ClosedDB(c *check.C) {
	c.Assert(t.pool.Close(), check.IsNil)

	c.Assert(t.pool.Len(), check.Equals, 0)
	c.Assert(t.pool.FreeLen(), check.Equals, 0)

	err := t.pool.Close()
	c.Assert(err, check.Not(check.IsNil))
	c.Assert(err.Error(), check.Equals, interpool.ErrClosed.Error())

	_, err = t.pool.Get()
	// defer func() {
	// 	err := t.pool.Put(con)
	// 	c.Assert(err, check.IsNil)
	// }()
	c.Assert(err, check.Not(check.IsNil))
	c.Assert(err.Error(), check.Equals, interpool.ErrClosed.Error())
}

// func (t *PoolTest) TestClosedListener(c *check.C) {
// 	ln := t.db.Listen("test_channel")

// 	c.Assert(t.pool.Len(), check.Equals, 1)
// 	c.Assert(t.pool.FreeLen(), check.Equals, 0)

// 	c.Assert(ln.Close(), check.IsNil)

// 	c.Assert(t.pool.Len(), check.Equals, 0)
// 	c.Assert(t.pool.FreeLen(), check.Equals, 0)

// 	err := ln.Close()
// 	c.Assert(err, Not(check.IsNil))
// 	c.Assert(err.Error(), check.Equals, "pg: listener is closed")

// 	_, _, err = ln.ReceiveTimeout(time.Second)
// 	c.Assert(err, Not(check.IsNil))
// 	c.Assert(err.Error(), check.Equals, "pg: listener is closed")
// }

// this test is problematic
func (t *PoolTest) TestBoltPool_ClosedTx(c *check.C) {
	con, err := t.pool.Get()

	tx, err := con.Begin()
	c.Assert(err, check.IsNil)

	c.Assert(t.pool.Len(), check.Equals, 1)
	c.Assert(t.pool.FreeLen(), check.Equals, 0)

	c.Assert(tx.Rollback(), check.IsNil)

	// for us rolling back doesn't reclaim it
	err = t.pool.Put(con)
	c.Assert(err, check.IsNil)

	c.Assert(t.pool.Len(), check.Equals, 1)
	c.Assert(t.pool.FreeLen(), check.Equals, 1)

	err = tx.Rollback()
	c.Assert(err, check.Not(check.IsNil))
	//c.Assert(strings.Contains(err.Error()))
	//fmt.Printf("err: %s, got: %s\n", err.Error(), ErrTxClose.Error())
	//c.Assert(err.Error(), check.Equals, ErrTxClose.Error())
	c.Assert(strings.Contains(err.Error(), "Transaction already closed"), check.Equals, true)

	// Neo4j lets you do multiple transactions over the same connection?
	con, err = t.pool.Get()
	c.Assert(err, check.IsNil)
	_, err = con.ExecNeo("MATCH (n) DETACH DELETE n", nil)
	c.Assert(err, check.IsNil)
	// TODO:
	//c.Assert(err.Error(), check.Equals, "pg: transaction has already been committed or rolled back")

	err = t.pool.Put(con)
	c.Assert(err, check.IsNil)
}

func (t *PoolTest) TestBoltPool_ClosedStmt(c *check.C) {
	con, err := t.pool.Get()

	stmt, err := con.PrepareNeo("MATCH (n) DETACH DELETE n")
	c.Assert(err, check.IsNil)

	c.Assert(t.pool.Len(), check.Equals, 1)
	c.Assert(t.pool.FreeLen(), check.Equals, 0)

	c.Assert(stmt.Close(), check.IsNil)
	// for us statement close doesn't reclaim it
	err = t.pool.Put(con)
	c.Assert(err, check.IsNil)

	c.Assert(t.pool.Len(), check.Equals, 1)
	c.Assert(t.pool.FreeLen(), check.Equals, 1)

	err = stmt.Close()
	// this driver doesn't care if you close twice
	//c.Assert(err, check.Not(check.IsNil))
	//c.Assert(err.Error(), check.Equals, "pg: statement is closed")
	c.Assert(err, check.IsNil)

	_, err = stmt.ExecNeo(nil)
	c.Assert(err.Error(), check.Equals, ErrStmtAlreadyClosed.Error())

	//err = t.pool.Put(con)
	//c.Assert(err, check.IsNil)
}
