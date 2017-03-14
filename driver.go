package bolt

import (
	"crypto/tls"
	"database/sql"
	"database/sql/driver"
	"math"
	"time"

	"github.com/axiomzen/interpool"
)

var (
	magicPreamble     = []byte{0x60, 0x60, 0xb0, 0x17}
	supportedVersions = []byte{
		0x00, 0x00, 0x00, 0x01,
		0x00, 0x00, 0x00, 0x00,
		0x00, 0x00, 0x00, 0x00,
		0x00, 0x00, 0x00, 0x00,
	}
	handShake          = append(magicPreamble, supportedVersions...)
	noVersionSupported = []byte{0x00, 0x00, 0x00, 0x00}
	// Version is the current version of this driver
	Version = "1.0"
	// ClientID is the id of this client
	ClientID = "GolangNeo4jBolt/" + Version
)

// Driver is a driver allowing connection to Neo4j
// The driver allows you to open a new connection to Neo4j
//
// Implements sql/driver, but also includes its own more neo-friendly interface.
// Some of the features of this interface implement neo-specific features
// unavailable in the sql/driver compatible interface
//
// Driver objects should be THREAD SAFE, so you can use them
// to open connections in multiple threads.  The connection objects
// themselves, and any prepared statements/transactions within ARE NOT
// THREAD SAFE.
type Driver interface {
	// Open opens a sql.driver compatible connection. Used internally
	// by the go sql interface
	Open(constr string) (driver.Conn, error)
	// OpenNeo opens a Neo-specific connection. This should be used
	// directly when not using the golang sql interface
	OpenNeo() (Conn, error)
	// NewConnPool creates a new connection pool instead
	NewConnPool(ops *PoolOptions) DriverPool
}

// DriverOptions are the options for the driver
type DriverOptions struct {
	// Dial timeout for establishing new connections.
	// Default is 5 seconds.
	DialTimeout time.Duration
	// Timeout for socket reads. If reached, commands will fail
	// with a timeout instead of blocking.
	ReadTimeout time.Duration
	// Timeout for socket writes. If reached, commands will fail
	// with a timeout instead of blocking.
	WriteTimeout time.Duration
	// Addr is the connection string
	Addr string
	// TLSNoverify allows you to skip tls verification
	// replaced by TLSConfig.InsecureSkipVerify
	//TLSNoVerify bool
	// TLSConfig is the tls configuration (nil by default)
	TLSConfig *tls.Config
	// PoolOptions are the options for the connection pool
	//PoolOptions *PoolOptions
	// ChunkSize is im not sure
	ChunkSize uint16
}

// DefaultDriverOptions returns the default options
func DefaultDriverOptions() *DriverOptions {
	return &DriverOptions{
		DialTimeout:  time.Second * 5,
		ReadTimeout:  time.Second * 60,
		WriteTimeout: time.Second * 60,
		//TLSNoVerify:  false,
		TLSConfig: nil,
		ChunkSize: math.MaxUint16,
	}
}

// DefaultPoolOptions returns the default connection pool options
func DefaultPoolOptions() *PoolOptions {
	return &PoolOptions{
		PoolSize:           20,
		PoolTimeout:        time.Second * 5,
		IdleTimeout:        time.Hour,
		IdleCheckFrequency: time.Minute,
		MaxAge:             time.Hour * 24,
	}
}

type boltDriver struct {
	recorder *recorder
	options  *DriverOptions
}

// NewDriver creates a new Driver object with defaults
func NewDriver() Driver {
	return NewDriverWithOptions(DefaultDriverOptions())
}

// NewDriverWithOptions creates a new Driver object with the given options
func NewDriverWithOptions(options *DriverOptions) Driver {
	return &boltDriver{
		options: options,
	}
}

// Open opens a new Bolt connection to the Neo4J database
func (d *boltDriver) Open(constring string) (driver.Conn, error) {
	if d.options == nil {
		d.options = DefaultDriverOptions()
		d.options.Addr = constring
	}
	return newBoltConn(d, false) // Never use pooling when using SQL driver
}

// Open opens a new Bolt connection to the Neo4J database. Implements a Neo-friendly alternative to sql/driver.
func (d *boltDriver) OpenNeo() (Conn, error) {
	return newBoltConn(d, false)
}

func (d *boltDriver) NewConnPool(ops *PoolOptions) DriverPool {
	iops := &interpool.Options{
		ConnFactory: interpool.ConnFactoryFunc(func() (interpool.Conn, error) {
			return newBoltConn(d, true)
			//return createBoltCon(d.options), nil
		}),
		OnCloser: interpool.OnCloserFunc(func(c interpool.Conn) error {
			// this allows you to send a message right before you close the connection
			// close is only called on the remove call, or when closing all connections

			// I don't think its needed in our case
			// check out what this is supposed to do
			// need to cast to our con type
			//oc, ok := c.(*boltConn)
			// const terminateMsg        = 'X'
			//var terminateMessage = []byte{terminateMsg, 0, 0, 0, 4}
			//_, err := cn.NetConn().Write(terminateMessage)
			//return nil
			return nil
		}),
		PoolSize:           ops.PoolSize,
		PoolTimeout:        ops.PoolTimeout,
		IdleTimeout:        ops.IdleTimeout,
		IdleCheckFrequency: ops.IdleCheckFrequency,
		MaxAge:             ops.MaxAge,
	}

	pool := &boltConnPool{
		pool: interpool.NewConnPool(iops),
	}

	return pool
}

func init() {
	sql.Register("neo4j-bolt", &boltDriver{})
}
