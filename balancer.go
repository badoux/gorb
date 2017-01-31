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
// queries with a round-robin scheduling around a master/replica replication.
// Write queries are executed by the Master.
// Read queries(SELECTs) are executed by the replicas.
type Balancer struct {
	*gorp.DbMap   // master
	replicas      []*gorp.DbMap
	count         uint64
	mu            sync.RWMutex
	masterCanRead bool
}

// NewBalancer opens a connection to each physical db.
// dataSourceNames must be a semi-comma separated list of DSNs with the first
// one being used as the master and the rest as replicas.
func NewBalancer(driverName string, dialect gorp.Dialect, sources string) (*Balancer, error) {
	conns := strings.Split(sources, ";")
	if len(conns) == 0 {
		return nil, errors.New("empty servers list")

	}
	b := &Balancer{}
	for i, c := range conns {
		if len(c) == 0 { // trailing ;
			continue
		}
		s, err := sql.Open(driverName, c)
		if err != nil {
			return nil, err
		}
		mapper := &gorp.DbMap{Db: s, Dialect: dialect}
		if i == 0 { // first is the master
			b.DbMap = mapper
		} else {
			b.replicas = append(b.replicas, mapper)
		}
	}
	if len(b.replicas) == 0 {
		b.replicas = append(b.replicas, b.DbMap)
		b.masterCanRead = true
	}
	return b, nil
}

// MasterCanRead adds the master physical database to the replicas list if read==true
// so that the master can perform WRITE queries AND READ queries .
func (b *Balancer) MasterCanRead(read bool) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if read == true && b.masterCanRead == false {
		b.replicas = append(b.replicas, b.DbMap)
		b.masterCanRead = read
	}
	if read == false && b.masterCanRead == true && len(b.replicas) > 1 {
		replicas := []*gorp.DbMap{}
		for _, db := range b.replicas {
			if db != b.DbMap {
				replicas = append(replicas, db)
			}
		}
		b.replicas = replicas
		b.masterCanRead = read
	}
}

// Ping verifies if a connection to each physical database is still alive, establishing a connection if necessary.
func (b *Balancer) Ping() error {
	var err, innerErr error
	for _, db := range b.GetAllDbs() {
		innerErr = db.Db.Ping()
		if innerErr != nil {
			err = innerErr
		}
	}
	return err
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

// Master returns the master database
func (b *Balancer) Master() *gorp.DbMap {
	return b.DbMap
}

// Replica returns one of the replicas databases
func (b *Balancer) Replica() *gorp.DbMap {
	b.mu.RLock()
	b.mu.RUnlock()
	return b.replicas[b.replica()]
}

// GetAllDbs returns each underlying physical database,
// the first one is the master
func (b *Balancer) GetAllDbs() []*gorp.DbMap {
	dbs := []*gorp.DbMap{}
	dbs = append(dbs, b.DbMap)
	dbs = append(dbs, b.replicas...)
	return dbs
}

// Close closes all physical databases
func (b *Balancer) Close() error {
	var err, innerErr error
	for _, db := range b.GetAllDbs() {
		innerErr = db.Db.Close()
		if innerErr != nil {
			err = innerErr
		}

	}
	return err
}

func (b *Balancer) replica() int {
	if len(b.replicas) == 1 {
		return 0
	}
	return int((atomic.AddUint64(&b.count, 1) % uint64(len(b.replicas))))
}
