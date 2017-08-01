package bolt

import (
	"fmt"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/axiomzen/golang-neo4j-bolt-driver/log"
	"github.com/axiomzen/interpool"
	ginkgo "github.com/onsi/ginkgo"
)

var (
	_neo4jConnStr = os.Getenv("NEO4J_BOLT")
)

// Hook up gocheck into the "go test" runner.
//func Test(t *testing.T) { check.TestingT(t) }

type PoolTest struct {
	db   Driver
	pool DriverPool
}

const poolSize = 10

//var _ = check.Suite(&PoolTest{})

func (t *PoolTest) SetUpTest() {
	fmt.Println("SETUP TEST")
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

func (t *PoolTest) TearDownTest() {
	fmt.Println("TearDownTest")
	_ = t.pool.Close()
}

func TestBoltPool_PoolReusesConnection(t *testing.T) {
	p := PoolTest{}
	p.SetUpTest()
	defer p.TearDownTest()

	for i := 0; i < 100; i++ {
		con, err := p.pool.Get()
		if err != nil {
			t.Error(err)
		}
		if err != nil {
			t.Error(err)
		}
		_, err = con.ExecNeo("MATCH (n) DETACH DELETE n", nil)
		if err != nil {
			t.Error(err)
		}
		err = p.pool.Put(con)
		if err != nil {
			t.Error(err)
		}
	}

	if p.pool.Len() != 1 {
		t.Errorf("expected Len to be 1, got %d", p.pool.Len())
	}

	//c.Assert(p.pool.Len(), check.Equals, 1)

	if p.pool.FreeLen() != 1 {
		t.Errorf("expected FreeLen to be 1, got %d", p.pool.FreeLen())
	}
	//c.Assert(p.pool.FreeLen(), check.Equals, 1)
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

func TestBoltPool_PoolMaxSize(t *testing.T) {
	p := PoolTest{}
	p.SetUpTest()
	defer p.TearDownTest()

	N := 1000

	perform(N, func(int) {
		con, err := p.pool.Get()
		if err != nil {
			t.Error(err)
		}
		_, err = con.ExecNeo("MATCH (n) DETACH DELETE n", nil)
		if err != nil {
			t.Error(err)
		}
		err = p.pool.Put(con)
		if err != nil {
			t.Error(err)
		}
	})

	//	c.Assert(p.pool.Len(), check.Equals, poolSize)
	//	c.Assert(p.pool.FreeLen(), check.Equals, poolSize)

	if p.pool.Len() != poolSize {
		t.Errorf("expected Len to be %d, got %d", poolSize, p.pool.Len())
	}

	if p.pool.FreeLen() != poolSize {
		t.Errorf("expected FreeLen to be %d, got %d", poolSize, p.pool.FreeLen())
	}
}

func TestCloseClosesAllConnections(t *testing.T) {
	p := PoolTest{}
	p.SetUpTest()
	defer p.TearDownTest()

	N := poolSize

	perform(N, func(int) {
		con, err := p.pool.Get()
		if err != nil {
			t.Error(err)
		}
		_, err = con.ExecNeo("MATCH (n) DETACH DELETE n", nil)
		if err != nil {
			t.Error(err)
		}
		//err = p.pool.Put(con)
		//c.Assert(err, check.IsNil)
	})

	//c.Assert(p.pool.Len(), check.Equals, poolSize)
	//c.Assert(p.pool.FreeLen(), check.Equals, 0)

	if p.pool.Len() != poolSize {
		t.Errorf("expected Len to be %d, got %d", poolSize, p.pool.Len())
	}

	if p.pool.FreeLen() != 0 {
		t.Errorf("expected FreeLen to be %d, got %d", 0, p.pool.FreeLen())
	}

	err := p.pool.Close()
	//c.Assert(err, check.IsNil)

	if err != nil {
		t.Error(err)
	}
	//c.Assert(p.pool.Len(), check.Equals, 0)
	//c.Assert(p.pool.FreeLen(), check.Equals, 0)

	if p.pool.Len() != 0 {
		t.Errorf("expected Len to be %d, got %d", 0, p.pool.Len())
	}

	if p.pool.FreeLen() != 0 {
		t.Errorf("expected FreeLen to be %d, got %d", 0, p.pool.FreeLen())
	}

}

// func TestCloseClosesAllConnections(t *testing.T) {
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

// 	c.Assert(p.pool.Len(), check.Equals, 0)
// 	c.Assert(p.pool.FreeLen(), check.Equals, 0)
// }

func TestBoltPool_ClosedDB(t *testing.T) {
	p := PoolTest{}
	p.SetUpTest()
	defer p.TearDownTest()

	//c.Assert(p.pool.Close(), check.IsNil)
	if err := p.pool.Close(); err != nil {
		t.Error(err)
	}

	//c.Assert(p.pool.Len(), check.Equals, 0)
	//c.Assert(p.pool.FreeLen(), check.Equals, 0)

	if p.pool.Len() != 0 {
		t.Errorf("expected Len to be %d, got %d", 0, p.pool.Len())
	}

	if p.pool.FreeLen() != 0 {
		t.Errorf("expected FreeLen to be %d, got %d", 0, p.pool.FreeLen())
	}

	err := p.pool.Close()
	//c.Assert(err, check.Not(check.IsNil))
	if err == nil {
		t.Errorf("expected err to not be nil, got %v", err)
	}

	//c.Assert(err.Error(), check.Equals, interpool.ErrClosed.Error())
	if err.Error() != interpool.ErrClosed.Error() {
		t.Errorf("expected err to be %s, got %s", interpool.ErrClosed.Error(), err.Error())
	}

	_, err = p.pool.Get()
	// defer func() {
	// 	err := p.pool.Put(con)
	// 	c.Assert(err, check.IsNil)
	// }()
	//c.Assert(err, check.Not(check.IsNil))
	if err == nil {
		t.Error("expected err to not be nil")
	}
	if err.Error() != interpool.ErrClosed.Error() {
		t.Errorf("expected err to be %s, got %s", interpool.ErrClosed.Error(), err.Error())
	}
	//c.Assert(err.Error(), check.Equals, interpool.ErrClosed.Error())
}

// func TestClosedListener(t *testing.T) {
// 	ln := t.db.Listen("test_channel")

// 	c.Assert(p.pool.Len(), check.Equals, 1)
// 	c.Assert(p.pool.FreeLen(), check.Equals, 0)

// 	c.Assert(ln.Close(), check.IsNil)

// 	c.Assert(p.pool.Len(), check.Equals, 0)
// 	c.Assert(p.pool.FreeLen(), check.Equals, 0)

// 	err := ln.Close()
// 	c.Assert(err, Not(check.IsNil))
// 	c.Assert(err.Error(), check.Equals, "pg: listener is closed")

// 	_, _, err = ln.ReceiveTimeout(time.Second)
// 	c.Assert(err, Not(check.IsNil))
// 	c.Assert(err.Error(), check.Equals, "pg: listener is closed")
// }

// this test is problematic
func TestBoltPool_ClosedTx(t *testing.T) {
	p := PoolTest{}
	p.SetUpTest()
	defer p.TearDownTest()

	con, err := p.pool.Get()

	tx, err := con.Begin()
	//c.Assert(err, check.IsNil)
	if err != nil {
		t.Error(err)
	}

	//c.Assert(p.pool.Len(), check.Equals, 1)
	//c.Assert(p.pool.FreeLen(), check.Equals, 0)

	if p.pool.Len() != 1 {
		t.Errorf("expected Len to be %d, got %d", 1, p.pool.Len())
	}

	if p.pool.FreeLen() != 0 {
		t.Errorf("expected FreeLen to be %d, got %d", 0, p.pool.FreeLen())
	}

	if err := tx.Rollback(); err != nil {
		t.Error(err)
	}
	//c.Assert(tx.Rollback(), check.IsNil)

	// for us rolling back doesn't reclaim it
	//err =
	//c.Assert(err, check.IsNil)

	if err := p.pool.Put(con); err != nil {
		t.Error(err)
	}

	//c.Assert(p.pool.Len(), check.Equals, 1)
	//c.Assert(p.pool.FreeLen(), check.Equals, 1)

	if p.pool.Len() != 1 {
		t.Errorf("expected Len to be %d, got %d", 1, p.pool.Len())
	}

	if p.pool.FreeLen() != 1 {
		t.Errorf("expected FreeLen to be %d, got %d", 1, p.pool.FreeLen())
	}

	err = tx.Rollback()
	if err == nil {
		t.Error("expected err not to be nil")
	}
	//c.Assert(err, check.Not(check.IsNil))
	//c.Assert(strings.Contains(err.Error()))
	//fmt.Printf("err: %s, got: %s\n", err.Error(), ErrTxClose.Error())
	//c.Assert(err.Error(), check.Equals, ErrTxClose.Error())
	//c.Assert(strings.Contains(err.Error(), "Transaction already closed"), check.Equals, true)
	if !strings.Contains(err.Error(), "Transaction already closed") {
		t.Errorf("expeced %s to contain %s", err.Error(), "Transaction already closed")
	}

	// Neo4j lets you do multiple transactions over the same connection?
	con, err = p.pool.Get()
	if err != nil {
		t.Error(err)
	}
	_, err = con.ExecNeo("MATCH (n) DETACH DELETE n", nil)
	if err != nil {
		t.Error(err)
	}
	// TODO:
	//c.Assert(err.Error(), check.Equals, "pg: transaction has already been committed or rolled back")

	err = p.pool.Put(con)
	if err != nil {
		t.Error(err)
	}
}

func TestBoltPool_ClosedStmt(t *testing.T) {
	p := PoolTest{}
	p.SetUpTest()
	defer p.TearDownTest()

	con, err := p.pool.Get()

	stmt, err := con.PrepareNeo("MATCH (n) DETACH DELETE n")
	if err != nil {
		t.Error(err)
	}

	//c.Assert(p.pool.Len(), check.Equals, 1)
	//c.Assert(p.pool.FreeLen(), check.Equals, 0)

	if p.pool.Len() != 1 {
		t.Errorf("expected Len to be %d, got %d", 1, p.pool.Len())
	}

	if p.pool.FreeLen() != 0 {
		t.Errorf("expected FreeLen to be %d, got %d", 0, p.pool.FreeLen())
	}

	if err := stmt.Close(); err != nil {
		t.Error(err)
	}
	// for us statement close doesn't reclaim it
	err = p.pool.Put(con)
	if err != nil {
		t.Error(err)
	}

	//c.Assert(p.pool.Len(), check.Equals, 1)
	//c.Assert(p.pool.FreeLen(), check.Equals, 1)

	if p.pool.Len() != 1 {
		t.Errorf("expected Len to be %d, got %d", 1, p.pool.Len())
	}

	if p.pool.FreeLen() != 1 {
		t.Errorf("expected FreeLen to be %d, got %d", 1, p.pool.FreeLen())
	}

	err = stmt.Close()
	// this driver doesn't care if you close twice
	//c.Assert(err, check.Not(check.IsNil))
	//c.Assert(err.Error(), check.Equals, "pg: statement is closed")
	if err != nil {
		t.Error(err)
	}

	_, err = stmt.ExecNeo(nil)
	if err.Error() != ErrStmtAlreadyClosed.Error() {
		t.Errorf("expected %s to equal %s", err.Error(), ErrStmtAlreadyClosed.Error())
	}
	//c.Assert(err.Error(), check.Equals, ErrStmtAlreadyClosed.Error())

	//err = p.pool.Put(con)
	//	if err != nil {
	//	t.Error(err)
	//}
}
