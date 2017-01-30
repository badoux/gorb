package gorb

import (
	"database/sql"
	"errors"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/go-gorp/gorp"
)

// Balancer embeds multiple connections to physical db and automatically distributes
// queries with a round-robin scheduling around a master/slave replication.
// Write queries are executed by the Master.
// Read queries(SELECTs) are executed by the slaves.
type Balancer struct {
	*gorp.DbMap   // master
	slaves        []*gorp.DbMap
	count         uint64
	mu            sync.Mutex
	masterCanRead bool
}

// NewBalancer opens a connection to each physical db.
// dataSourceNames must be a semi-comma separated list of DSNs with the first
// one being used as the master and the rest as slaves.
func NewBalancer(driverName string, dialect gorp.Dialect, sources string) (*Balancer, error) {
	conns := strings.Split(sources, ";")
	if len(conns) == 0 {
		return nil, errors.New("empty servers list")

	}
	b := &Balancer{}
	for i, c := range conns {
		s, err := sql.Open(driverName, c)
		if err != nil {
			return nil, err
		}
		mapper := &gorp.DbMap{Db: s, Dialect: dialect}
		if i == 0 { // first is the master
			b.DbMap = mapper
		} else {
			b.slaves = append(b.slaves, mapper)
		}
	}
	if len(b.slaves) == 0 {
		b.slaves = append(b.slaves, b.DbMap)
		b.masterCanRead = true
	}

	return b, nil
}

// Ping verifies if a connection to each physical database is still alive, establishing a connection if necessary.
func (b *Balancer) Ping() error {
	var err error
	for _, db := range b.GetAllDbs() {
		err = db.Db.Ping()
		if err != nil {
			return err
		}
	}
	return nil
}

// SetMaxIdleConns sets the maximum number of connections
// If MaxOpenConns is greater than 0 but less than the new MaxIdleConns then the
// new MaxIdleConns will be reduced to match the MaxOpenConns limit
// If n <= 0, no idle connections are retained.
func (b *Balancer) SetMaxIdleConns(n int) {
	for _, db := range b.GetAllDbs() {
		db.Db.SetMaxIdleConns(n)
	}
}

// SetMaxOpenConns sets the maximum number of open connections
// If MaxIdleConns is greater than 0 and the new MaxOpenConns
// is less than MaxIdleConns, then MaxIdleConns will be reduced to match
// the new MaxOpenConns limit. If n <= 0, then there is no limit on the number
// of open connections. The default is 0 (unlimited).
func (b *Balancer) SetMaxOpenConns(n int) {
	for _, db := range b.GetAllDbs() {
		db.Db.SetMaxOpenConns(n)
	}
}

// SetConnMaxLifetime sets the maximum amount of time a connection may be reused.
// Expired connections may be closed lazily before reuse.
// If d <= 0, connections are reused forever.
func (b *Balancer) SetConnMaxLifetime(d time.Duration) {
	for _, db := range b.GetAllDbs() {
		db.Db.SetConnMaxLifetime(d)
	}
}

// MasterCanRead adds the master physical database to the slaves list if read==true
// so that the master can perform WRITE queries AND READ queries .
func (b *Balancer) MasterCanRead(read bool) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.masterCanRead && read == false {
		b.slaves = b.slaves[:len(b.slaves)-2]
	}
	if !b.masterCanRead && read == true {
		b.slaves = append(b.slaves, b.DbMap)
	}
	b.masterCanRead = read
}

// Master returns the master database
func (b *Balancer) Master() *gorp.DbMap {
	return b.DbMap
}

// Slave returns one of the slaves databases
func (b *Balancer) Slave() *gorp.DbMap {
	b.mu.Lock()
	b.mu.Unlock()
	return b.slaves[b.slave()]
}

// GetAllDbs returns each underlying physical database,
// the first one is the master
func (b *Balancer) GetAllDbs() []*gorp.DbMap {
	dbs := []*gorp.DbMap{}
	dbs = append(dbs, b.DbMap)
	dbs = append(dbs, b.slaves...)
	return dbs
}

// Close closes all physical databases
func (b *Balancer) Close() error {
	var err error
	for _, db := range b.GetAllDbs() {
		err = db.Db.Close()
		if err != nil {
			return err
		}
	}
	return nil
}

func (b *Balancer) slave() int {
	if len(b.slaves) == 1 {
		return 0
	}
	return int((atomic.AddUint64(&b.count, 1) % uint64(len(b.slaves))))
}
