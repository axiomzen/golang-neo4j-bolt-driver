package bolt

import (
	"bytes"
	"crypto/tls"
	"crypto/x509"
	"database/sql/driver"
	"io"
	"io/ioutil"
	"net"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/axiomzen/golang-neo4j-bolt-driver/encoding"
	"github.com/axiomzen/golang-neo4j-bolt-driver/errors"
	"github.com/axiomzen/golang-neo4j-bolt-driver/log"
	"github.com/axiomzen/golang-neo4j-bolt-driver/structures/messages"
	"github.com/axiomzen/interpool"
)

// Conn represents a connection to Neo4J
//
// Implements a neo-friendly interface.
// Some of the features of this interface implement neo-specific features
// unavailable in the sql/driver compatible interface
//
// Conn objects, and any prepared statements/transactions within ARE NOT
// THREAD SAFE.  If you want to use multipe go routines with these objects,
// you should use a driver to create a new conn for each routine.
type Conn interface {
	interpool.Conn
	// PrepareNeo prepares a neo4j specific statement
	PrepareNeo(query string) (Stmt, error)
	// PreparePipeline prepares a neo4j specific pipeline statement
	// Useful for running multiple queries at the same time
	PreparePipeline(query ...string) (PipelineStmt, error)
	// QueryNeo queries using the neo4j-specific interface
	QueryNeo(query string, params map[string]interface{}) (Rows, error)
	// QueryNeoAll queries using the neo4j-specific interface and returns all row data and output metadata
	QueryNeoAll(query string, params map[string]interface{}) ([][]interface{}, map[string]interface{}, map[string]interface{}, error)
	// QueryPipeline queries using the neo4j-specific interface
	// pipelining multiple statements
	QueryPipeline(query []string, params ...map[string]interface{}) (PipelineRows, error)
	// ExecNeo executes a query using the neo4j-specific interface
	ExecNeo(query string, params map[string]interface{}) (Result, error)
	// ExecPipeline executes a query using the neo4j-specific interface
	// pipelining multiple statements
	ExecPipeline(query []string, params ...map[string]interface{}) ([]Result, error)
	// Begin starts a new transaction
	Begin() (driver.Tx, error)
	// SetChunkSize is used to set the max chunk size of the
	// bytes to send to Neo4j at once
	SetChunkSize(uint16)
	// SetTimeout sets the read/write timeouts for the
	// connection to Neo4j
	//SetTimeout(time.Duration)
	// setPool will set the driver pool
	//setPool(DriverPool)
}

// ErrClosed is when you attempt to use a closed connection
var ErrClosed = errors.New("connection is closed")

// todo: add fields that are needed via the other one
type boltConn struct {
	// todo: perhaps move this to the driver instead
	options       *DriverOptions
	url           *url.URL
	user          string
	password      string
	conn          net.Conn
	connErr       error
	serverVersion []byte
	closed        bool

	// todo: perhaps move this to the driver instead
	tlsConfig *tls.Config
	pooled    bool

	transaction *boltTx
	statement   *boltStmt

	initedAt time.Time
	usedAt   time.Time
}

// newBoltConn Creates a new bolt connection
func newBoltConn(d *boltDriver, pooled bool) (*boltConn, error) {

	c := &boltConn{
		options:       d.options,
		serverVersion: make([]byte, 4),
		usedAt:        time.Now(),
		pooled:        pooled,
	}

	err := c.initialize(d)
	if err != nil {
		return nil, errors.Wrap(err, "An error occurred initializing connection")
	}

	return c, nil
}

// CheckHealth returns an error if the connection is not healthy
func (c *boltConn) CheckHealth() error {
	if c.closed {
		return ErrClosed
	} else if c.connErr != nil {
		return c.connErr
	}
	// TODO: see if this is needed or if there is an equivalent
	// 	if cn.Rd.Buffered() != 0 {
	// 	b, _ := cn.Rd.Peek(cn.Rd.Buffered())
	// 	err := fmt.Errorf("connection has unread data:\n%s", hex.Dump(b))
	// 	return err
	// }
	return nil
}

// GetInitedAt implements interpool.Conn.GetInitedAt
func (c *boltConn) GetInitedAt() time.Time {
	return c.initedAt
}

// SetInitedAt implements interpool.Conn.SetInitedAt
func (c *boltConn) SetInitedAt(t time.Time) {
	c.initedAt = t
}

// GetUsedAt implements interpool.Conn.GetUsedAt
func (c *boltConn) GetUsedAt() time.Time {
	return c.usedAt
}

// SetUsedAt implements interpool.Conn.SetUsedAt
func (c *boltConn) SetUsedAt(t time.Time) {
	c.usedAt = t
}

// URLParse is the result from parsing the url
type URLParse struct {
	//Scheme      string
	URL         *url.URL
	User        string
	Password    string
	DialTimeout time.Duration
	UseTLS      bool
	CertFile    string
	KeyFile     string
	CaCertFile  string
	TLSNoVerify bool
}

// ParseURL parses a bolt url
func (p *URLParse) ParseURL(addr string) error {
	var err error
	p.URL, err = url.Parse(addr)
	if err != nil {
		return errors.Wrap(err, "An error occurred parsing bolt URL")
	} else if strings.ToLower(p.URL.Scheme) != "bolt" {
		return errors.New("Unsupported connection string scheme: %s. Driver only supports 'bolt' scheme.", p.URL.Scheme)
	}

	if p.URL.User != nil {
		p.User = p.URL.User.Username()
		var isSet bool
		p.Password, isSet = p.URL.User.Password()
		if !isSet {
			return errors.New("Must specify password when passing user")
		}
	}

	timeout := p.URL.Query().Get("timeout")
	if timeout != "" {
		timeoutInt, err := strconv.Atoi(timeout)
		if err != nil {
			return errors.New("Invalid format for timeout: %s.  Must be integer", timeout)
		}

		p.DialTimeout = time.Duration(timeoutInt) * time.Second
	}

	useTLSStr := p.URL.Query().Get("tls")
	p.UseTLS = strings.HasPrefix(strings.ToLower(useTLSStr), "t") || useTLSStr == "1"

	if p.UseTLS {
		p.CertFile = p.URL.Query().Get("tls_cert_file")
		p.KeyFile = p.URL.Query().Get("tls_key_file")
		p.CaCertFile = p.URL.Query().Get("tls_ca_cert_file")
		noVerifyStr := p.URL.Query().Get("tls_no_verify")
		p.TLSNoVerify = strings.HasPrefix(strings.ToLower(noVerifyStr), "t") || noVerifyStr == "1"
	}

	log.Trace("Bolt Host: ", p.URL.Host)
	log.Trace("Timeout: ", p.DialTimeout)
	log.Trace("User: ", p.User)
	log.Trace("Password: ", p.Password)
	log.Trace("TLS: ", p.UseTLS)
	log.Trace("TLS No Verify: ", p.TLSNoVerify)
	log.Trace("Cert File: ", p.CertFile)
	log.Trace("Key File: ", p.KeyFile)
	log.Trace("CA Cert File: ", p.CaCertFile)

	return nil
}

// GetTLSConfig is a helper function to extract the tls config
func (p *URLParse) GetTLSConfig() (*tls.Config, error) {

	config, err := TLSHelper(p.TLSNoVerify, p.CaCertFile, p.CertFile, p.KeyFile)
	if err != nil {
		return nil, errors.Wrap(err, "An error occurred setting up TLS configuration")
	}
	return config, nil
}

func (c *boltConn) createConn() (net.Conn, error) {

	var err error
	var p URLParse
	err = p.ParseURL(c.options.Addr)
	if err != nil {
		return nil, errors.Wrap(err, "An error occurred parsing the conn URL")
	}
	// set some things
	c.url = p.URL
	c.user = p.User
	c.password = p.Password

	var conn net.Conn
	if p.UseTLS {
		// get a tls config from the p
		c.tlsConfig, err = p.GetTLSConfig()
		if err != nil {
			return nil, errors.Wrap(err, "An error occurred setting up TLS configuration")
		}
		conn, err = tls.Dial("tcp", c.url.Host, c.tlsConfig)
		if err != nil {
			return nil, errors.Wrap(err, "An error occurred dialing to neo4j (tls)")
		}
	} else {
		conn, err = net.DialTimeout("tcp", c.url.Host, c.options.DialTimeout)
		if err != nil {
			return nil, errors.Wrap(err, "An error occurred dialing to neo4j (no tls)")
		}
	}

	return conn, nil
}

// TLSHelper creates a tls.Config
func TLSHelper(skipVerify bool, caCertFile, certFile, keyFile string) (*tls.Config, error) {
	config := &tls.Config{
		MinVersion: tls.VersionTLS10,
		MaxVersion: tls.VersionTLS12,
	}

	if caCertFile != "" {
		// Load CA cert - usually for self-signed certificates
		caCert, err := ioutil.ReadFile(caCertFile)
		if err != nil {
			return nil, err
		}
		caCertPool := x509.NewCertPool()
		caCertPool.AppendCertsFromPEM(caCert)

		config.RootCAs = caCertPool
	}

	if certFile != "" {
		if keyFile == "" {
			return nil, errors.New("If you're providing a cert file, you must also provide a key file")
		}

		cert, err := tls.LoadX509KeyPair(certFile, keyFile)
		if err != nil {
			return nil, err
		}

		config.Certificates = []tls.Certificate{cert}
	}

	config.InsecureSkipVerify = skipVerify

	return config, nil
}

func (c *boltConn) handShake() error {

	numWritten, err := c.Write(handShake)
	if numWritten != 20 {
		log.Errorf("Couldn't write expected bytes for magic preamble + supported versions. Written: %d. Expected: 4", numWritten)
		if err != nil {
			err = errors.Wrap(err, "An error occurred writing magic preamble + supported versions")
		}
		return err
	}

	numRead, err := c.Read(c.serverVersion)
	if numRead != 4 {
		log.Errorf("Could not read server version response. Read %d bytes. Expected 4 bytes. Output: %s", numRead, c.serverVersion)
		if err != nil {
			err = errors.Wrap(err, "An error occurred reading server version")
		}
		return err
	} else if bytes.Equal(c.serverVersion, noVersionSupported) {
		return errors.New("Server responded with no supported version")
	}

	return nil
}

func (c *boltConn) initialize(d *boltDriver) error {

	// Handle recorder. If there is no conn string, assume we're playing back a recording.
	// If there is a recorder and a conn string, assume we're recording the connection
	// Else, just create the conn normally
	var err error
	if c.options.Addr == "" && d != nil && d.recorder != nil {
		c.conn = d.recorder
	} else if d != nil && d.recorder != nil {
		d.recorder.Conn, err = c.createConn()
		if err != nil {
			return err
		}
		c.conn = d.recorder
	} else {
		c.conn, err = c.createConn()
		if err != nil {
			return err
		}
	}

	if err := c.handShake(); err != nil {
		if e := c.Close(); e != nil {
			log.Errorf("An error occurred closing connection: %s", e)
		}
		return err
	}

	respInt, err := c.sendInit()
	if err != nil {
		if e := c.Close(); e != nil {
			log.Errorf("An error occurred closing connection: %s", e)
		}
		return err
	}

	switch resp := respInt.(type) {
	case messages.SuccessMessage:
		log.Infof("Successfully initiated Bolt connection: %+v", resp)
		c.initedAt = time.Now()
		return nil
	default:
		log.Errorf("Got an unrecognized message when initializing connection :%+v", resp)
		c.connErr = errors.New("Unrecognized response from the server: %#v", resp)
		c.Close()
		return driver.ErrBadConn
	}
}

// Read reads the data from the underlying connection
func (c *boltConn) Read(b []byte) (n int, err error) {
	if err := c.conn.SetReadDeadline(time.Now().Add(c.options.ReadTimeout)); err != nil {
		c.connErr = errors.Wrap(err, "An error occurred setting read deadline")
		return 0, driver.ErrBadConn
	}

	n, err = c.conn.Read(b)

	if log.GetLevel() >= log.TraceLevel {
		log.Tracef("Read %d bytes from stream:\n\n%s\n", n, sprintByteHex(b))
	}

	if err != nil && err != io.EOF {
		c.connErr = errors.Wrap(err, "An error occurred reading from stream")
		err = driver.ErrBadConn
	}
	return n, err
}

// Write writes the data to the underlying connection
func (c *boltConn) Write(b []byte) (n int, err error) {
	if err := c.conn.SetWriteDeadline(time.Now().Add(c.options.WriteTimeout)); err != nil {
		c.connErr = errors.Wrap(err, "An error occurred setting write deadline")
		return 0, driver.ErrBadConn
	}

	n, err = c.conn.Write(b)

	if log.GetLevel() >= log.TraceLevel {
		log.Tracef("Wrote %d of %d bytes to stream:\n\n%s\n", len(b), n, sprintByteHex(b[:n]))
	}

	if err != nil {
		c.connErr = errors.Wrap(err, "An error occurred writing to stream")
		err = driver.ErrBadConn
	}
	return n, err
}

// Close closes the connection
// Driver may allow for pooling in the future, keeping connections alive
func (c *boltConn) Close() error {

	if c.closed {
		return nil
	}

	if c.transaction != nil {
		if err := c.transaction.Rollback(); err != nil {
			return err
		}
	}

	if c.statement != nil {
		if err := c.statement.Close(); err != nil {
			return err
		}
	}

	if c.transaction != nil {
		if err := c.transaction.Rollback(); err != nil {
			return errors.Wrap(err, "Error rolling back transaction when closing connection")
		}
	}

	if c.pooled {
		return nil
	}

	// if c.poolDriver != nil {
	// 	log.Infof("pool driver != nil")
	// 	// put back
	// 	if err := c.poolDriver.put(c); err != nil {
	// 		log.Errorf("An error occurred reclaiming connection for pool: %s", err)
	// 		c.connErr = errors.Wrap(err, "An error occurred reclaiming the connection")
	// 		return driver.ErrBadConn
	// 	}
	// 	return nil
	// }

	err := c.conn.Close()
	c.closed = true
	if err != nil {
		c.connErr = errors.Wrap(err, "An error occurred closing the connection")
		return driver.ErrBadConn
	}

	return nil
}

func (c *boltConn) ackFailure(failure messages.FailureMessage) error {
	log.Infof("Acknowledging Failure: %#v", failure)

	ack := messages.NewAckFailureMessage()
	err := encoding.NewEncoder(c, c.options.ChunkSize).Encode(ack)
	if err != nil {
		return errors.Wrap(err, "An error occurred encoding ack failure message")
	}

	for {
		respInt, err := encoding.NewDecoder(c).Decode()
		if err != nil {
			return errors.Wrap(err, "An error occurred decoding ack failure message response")
		}

		switch resp := respInt.(type) {
		case messages.IgnoredMessage:
			log.Infof("Got ignored message when acking failure: %#v", resp)
			continue
		case messages.SuccessMessage:
			log.Infof("Got success message when acking failure: %#v", resp)
			return nil
		case messages.FailureMessage:
			log.Errorf("Got failure message when acking failure: %#v", resp)
			return c.reset()
		default:
			log.Errorf("Got unrecognized response from acking failure: %#v", resp)
			c.connErr = errors.New("Got unrecognized response from acking failure: %#v. CLOSING SESSION!", resp)
			c.Close()
			return driver.ErrBadConn
		}
	}
}

func (c *boltConn) reset() error {
	log.Info("Resetting session")

	reset := messages.NewResetMessage()
	err := encoding.NewEncoder(c, c.options.ChunkSize).Encode(reset)
	if err != nil {
		return errors.Wrap(err, "An error occurred encoding reset message")
	}

	for {
		respInt, err := encoding.NewDecoder(c).Decode()
		if err != nil {
			return errors.Wrap(err, "An error occurred decoding reset message response")
		}

		switch resp := respInt.(type) {
		case messages.IgnoredMessage:
			log.Infof("Got ignored message when resetting session: %#v", resp)
			continue
		case messages.SuccessMessage:
			log.Infof("Got success message when resetting session: %#v", resp)
			return nil
		case messages.FailureMessage:
			log.Errorf("Got failure message when resetting session: %#v", resp)
			err = c.Close()
			if err != nil {
				log.Errorf("An error occurred closing the session: %s", err)
			}
			return errors.Wrap(resp, "Error resetting session. CLOSING SESSION!")
		default:
			log.Errorf("Got unrecognized response from resetting session: %#v", resp)
			c.connErr = errors.New("Got unrecognized response from resetting session: %#v. CLOSING SESSION!", resp)
			c.Close()
			return driver.ErrBadConn
		}
	}
}

// Prepare prepares a new statement for a query
func (c *boltConn) Prepare(query string) (driver.Stmt, error) {
	return c.prepare(query)
}

// Prepare prepares a new statement for a query. Implements a Neo-friendly alternative to sql/driver.
func (c *boltConn) PrepareNeo(query string) (Stmt, error) {
	return c.prepare(query)
}

// PreparePipeline prepares a new pipeline statement for a query.
func (c *boltConn) PreparePipeline(queries ...string) (PipelineStmt, error) {
	if c.statement != nil {
		return nil, errors.New("An open statement already exists")
	}
	if c.closed {
		return nil, errors.New("Connection already closed")
	}
	c.statement = newPipelineStmt(queries, c)
	return c.statement, nil
}

func (c *boltConn) prepare(query string) (*boltStmt, error) {
	if c.statement != nil {
		return nil, errors.New("An open statement already exists")
	}
	if c.closed {
		return nil, errors.New("Connection already closed")
	}
	c.statement = newStmt(query, c)
	return c.statement, nil
}

// Begin begins a new transaction with the Neo4J Database
func (c *boltConn) Begin() (driver.Tx, error) {
	if c.transaction != nil {
		return nil, errors.New("An open transaction already exists")
	}
	if c.statement != nil {
		return nil, errors.New("Cannot open a transaction when you already have an open statement")
	}
	if c.closed {
		return nil, errors.New("Connection already closed")
	}

	successInt, pullInt, err := c.sendRunPullAllConsumeSingle("BEGIN", nil)
	if err != nil {
		return nil, errors.Wrap(err, "An error occurred beginning transaction")
	}

	success, ok := successInt.(messages.SuccessMessage)
	if !ok {
		return nil, errors.New("Unrecognized response type beginning transaction: %#v", success)
	}

	log.Infof("Got success message beginning transaction: %#v", success)

	success, ok = pullInt.(messages.SuccessMessage)
	if !ok {
		return nil, errors.New("Unrecognized response type pulling transaction:  %#v", success)
	}

	log.Infof("Got success message pulling transaction: %#v", success)

	return newTx(c), nil
}

// Sets the size of the chunks to write to the stream
func (c *boltConn) SetChunkSize(chunkSize uint16) {
	c.options.ChunkSize = chunkSize
}

// // Sets the timeout for reading and writing to the stream
// func (c *boltConn) SetTimeout(timeout time.Duration) {
// 	c.timeout = timeout
// }

func (c *boltConn) consume() (interface{}, error) {
	log.Info("Consuming response from bolt stream")

	respInt, err := encoding.NewDecoder(c).Decode()
	if err != nil {
		return respInt, err
	}

	if log.GetLevel() >= log.TraceLevel {
		log.Tracef("Consumed Response: %#v", respInt)
	}

	if failure, isFail := respInt.(messages.FailureMessage); isFail {
		log.Errorf("Got failure message: %#v", failure)
		err := c.ackFailure(failure)
		if err != nil {
			return nil, err
		}
		return failure, errors.Wrap(failure, "Neo4J reported a failure for the query")
	}

	return respInt, err
}

func (c *boltConn) consumeAll() ([]interface{}, interface{}, error) {
	log.Info("Consuming all responses until success/failure")

	responses := []interface{}{}
	for {
		respInt, err := c.consume()
		if err != nil {
			return nil, respInt, err
		}

		if success, isSuccess := respInt.(messages.SuccessMessage); isSuccess {
			log.Infof("Got success message: %#v", success)
			return responses, success, nil
		}

		responses = append(responses, respInt)
	}
}

func (c *boltConn) consumeAllMultiple(mult int) ([][]interface{}, []interface{}, error) {
	log.Info("Consuming all responses %d times until success/failure", mult)

	responses := make([][]interface{}, mult)
	successes := make([]interface{}, mult)
	for i := 0; i < mult; i++ {

		resp, success, err := c.consumeAll()
		if err != nil {
			return responses, successes, err
		}

		responses[i] = resp
		successes[i] = success
	}

	return responses, successes, nil
}

func (c *boltConn) sendInit() (interface{}, error) {
	log.Infof("Sending INIT Message. ClientID: %s User: %s Password: %s", ClientID, c.user, c.password)

	initMessage := messages.NewInitMessage(ClientID, c.user, c.password)
	if err := encoding.NewEncoder(c, c.options.ChunkSize).Encode(initMessage); err != nil {
		return nil, errors.Wrap(err, "An error occurred sending init message")
	}

	return c.consume()
}

func (c *boltConn) sendRun(query string, args map[string]interface{}) error {
	log.Infof("Sending RUN message: query %s (args: %#v)", query, args)
	runMessage := messages.NewRunMessage(query, args)
	if err := encoding.NewEncoder(c, c.options.ChunkSize).Encode(runMessage); err != nil {
		return errors.Wrap(err, "An error occurred running query")
	}

	return nil
}

func (c *boltConn) sendRunConsume(query string, args map[string]interface{}) (interface{}, error) {
	if err := c.sendRun(query, args); err != nil {
		return nil, err
	}

	return c.consume()
}

func (c *boltConn) sendPullAll() error {
	log.Infof("Sending PULL_ALL message")

	pullAllMessage := messages.NewPullAllMessage()
	err := encoding.NewEncoder(c, c.options.ChunkSize).Encode(pullAllMessage)
	if err != nil {
		return errors.Wrap(err, "An error occurred encoding pull all query")
	}

	return nil
}

func (c *boltConn) sendPullAllConsume() (interface{}, error) {
	if err := c.sendPullAll(); err != nil {
		return nil, err
	}

	return c.consume()
}

func (c *boltConn) sendRunPullAll(query string, args map[string]interface{}) error {
	err := c.sendRun(query, args)
	if err != nil {
		return err
	}

	return c.sendPullAll()
}

func (c *boltConn) sendRunPullAllConsumeRun(query string, args map[string]interface{}) (interface{}, error) {
	err := c.sendRunPullAll(query, args)
	if err != nil {
		return nil, err
	}

	return c.consume()
}

func (c *boltConn) sendRunPullAllConsumeSingle(query string, args map[string]interface{}) (interface{}, interface{}, error) {
	err := c.sendRunPullAll(query, args)
	if err != nil {
		return nil, nil, err
	}

	runSuccess, err := c.consume()
	if err != nil {
		return runSuccess, nil, err
	}

	pullSuccess, err := c.consume()
	return runSuccess, pullSuccess, err
}

func (c *boltConn) sendRunPullAllConsumeAll(query string, args map[string]interface{}) (interface{}, interface{}, []interface{}, error) {
	err := c.sendRunPullAll(query, args)
	if err != nil {
		return nil, nil, nil, err
	}

	runSuccess, err := c.consume()
	if err != nil {
		return runSuccess, nil, nil, err
	}

	records, pullSuccess, err := c.consumeAll()
	return runSuccess, pullSuccess, records, err
}

func (c *boltConn) sendDiscardAll() error {
	log.Infof("Sending DISCARD_ALL message")

	discardAllMessage := messages.NewDiscardAllMessage()
	err := encoding.NewEncoder(c, c.options.ChunkSize).Encode(discardAllMessage)
	if err != nil {
		return errors.Wrap(err, "An error occurred encoding discard all query")
	}

	return nil
}

func (c *boltConn) sendDiscardAllConsume() (interface{}, error) {
	if err := c.sendDiscardAll(); err != nil {
		return nil, err
	}

	return c.consume()
}

func (c *boltConn) sendRunDiscardAll(query string, args map[string]interface{}) error {
	err := c.sendRun(query, args)
	if err != nil {
		return err
	}

	return c.sendDiscardAll()
}

func (c *boltConn) sendRunDiscardAllConsume(query string, args map[string]interface{}) (interface{}, interface{}, error) {
	runResp, err := c.sendRunConsume(query, args)
	if err != nil {
		return runResp, nil, err
	}

	discardResp, err := c.sendDiscardAllConsume()
	return runResp, discardResp, err
}

func (c *boltConn) Query(query string, args []driver.Value) (driver.Rows, error) {
	params, err := driverArgsToMap(args)
	if err != nil {
		return nil, err
	}
	return c.queryNeo(query, params)
}

func (c *boltConn) QueryNeo(query string, params map[string]interface{}) (Rows, error) {
	return c.queryNeo(query, params)
}

func (c *boltConn) QueryNeoAll(query string, params map[string]interface{}) ([][]interface{}, map[string]interface{}, map[string]interface{}, error) {
	rows, err := c.queryNeo(query, params)
	if err != nil {
		return nil, nil, nil, err
	}
	defer rows.Close()

	data, metadata, err := rows.All()
	return data, rows.metadata, metadata, err
}

func (c *boltConn) queryNeo(query string, params map[string]interface{}) (*boltRows, error) {
	if c.statement != nil {
		return nil, errors.New("An open statement already exists")
	}
	if c.closed {
		return nil, errors.New("Connection already closed")
	}

	c.statement = newStmt(query, c)

	// Pipeline the run + pull all for this
	successResp, err := c.sendRunPullAllConsumeRun(c.statement.query, params)
	if err != nil {
		return nil, err
	}
	success, ok := successResp.(messages.SuccessMessage)
	if !ok {
		return nil, errors.New("Unexpected response querying neo from connection: %#v", successResp)
	}

	c.statement.rows = newQueryRows(c.statement, success.Metadata)
	return c.statement.rows, nil
}

func (c *boltConn) QueryPipeline(queries []string, params ...map[string]interface{}) (PipelineRows, error) {
	if c.statement != nil {
		return nil, errors.New("An open statement already exists")
	}
	if c.closed {
		return nil, errors.New("Connection already closed")
	}

	c.statement = newPipelineStmt(queries, c)
	rows, err := c.statement.QueryPipeline(params...)
	if err != nil {
		return nil, err
	}

	// Since we're not exposing the statement,
	// tell the rows to close it when they are closed
	rows.(*boltRows).closeStatement = true
	return rows, nil
}

// Exec executes a query that returns no rows. See sql/driver.Stmt.
// You must bolt encode a map to pass as []bytes for the driver value
func (c *boltConn) Exec(query string, args []driver.Value) (driver.Result, error) {
	if c.statement != nil {
		return nil, errors.New("An open statement already exists")
	}
	if c.closed {
		return nil, errors.New("Connection already closed")
	}

	stmt := newStmt(query, c)
	defer stmt.Close()

	return stmt.Exec(args)
}

// ExecNeo executes a query that returns no rows. Implements a Neo-friendly alternative to sql/driver.
func (c *boltConn) ExecNeo(query string, params map[string]interface{}) (Result, error) {
	if c.statement != nil {
		return nil, errors.New("An open statement already exists")
	}
	if c.closed {
		return nil, errors.New("Connection already closed")
	}

	stmt := newStmt(query, c)
	defer stmt.Close()

	return stmt.ExecNeo(params)
}

func (c *boltConn) ExecPipeline(queries []string, params ...map[string]interface{}) ([]Result, error) {
	if c.statement != nil {
		return nil, errors.New("An open statement already exists")
	}
	if c.closed {
		return nil, errors.New("Connection already closed")
	}

	stmt := newPipelineStmt(queries, c)
	defer stmt.Close()

	return stmt.ExecPipeline(params...)
}
