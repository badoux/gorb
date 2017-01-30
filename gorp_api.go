package gorb

import (
	"database/sql"
	"fmt"

	"github.com/go-gorp/gorp"
)

// Get https://godoc.org/gopkg.in/gorp.v2#DbMap.Get
func (b *Balancer) Get(i interface{}, keys ...interface{}) (interface{}, error) {
	return b.Slave().Get(i, keys...)
}

// Select https://godoc.org/gopkg.in/gorp.v2#DbMap.Select
func (b *Balancer) Select(i interface{}, query string, args ...interface{}) ([]interface{}, error) {
	return b.Slave().Select(i, query, args...)
}

// SelectFloat https://godoc.org/gopkg.in/gorp.v2#DbMap.SelectFloat
func (b *Balancer) SelectFloat(query string, args ...interface{}) (float64, error) {
	return b.Slave().SelectFloat(query, args...)
}

// SelectInt https://godoc.org/gopkg.in/gorp.v2#DbMap.SelectInt
func (b *Balancer) SelectInt(query string, args ...interface{}) (int64, error) {
	return b.Slave().SelectInt(query, args...)
}

// SelectNullFloat https://godoc.org/gopkg.in/gorp.v2#DbMap.SelectNullFloat
func (b *Balancer) SelectNullFloat(query string, args ...interface{}) (sql.NullFloat64, error) {
	return b.Slave().SelectNullFloat(query, args...)
}

// SelectNullInt https://godoc.org/gopkg.in/gorp.v2#DbMap.SelectNullInt
func (b *Balancer) SelectNullInt(query string, args ...interface{}) (sql.NullInt64, error) {
	return b.Slave().SelectNullInt(query, args...)
}

// SelectNullStr https://godoc.org/gopkg.in/gorp.v2#DbMap.SelectNullStr
func (b *Balancer) SelectNullStr(query string, args ...interface{}) (sql.NullString, error) {
	return b.Slave().SelectNullStr(query, args...)
}

// SelectOne https://godoc.org/gopkg.in/gorp.v2#DbMap.SelectOne
func (b *Balancer) SelectOne(holder interface{}, query string, args ...interface{}) error {
	return b.Slave().SelectOne(holder, query, args...)
}

// SelectStr https://godoc.org/gopkg.in/gorp.v2#DbMap.SelectStr
func (b *Balancer) SelectStr(query string, args ...interface{}) (string, error) {
	return b.Slave().SelectStr(query, args...)
}

// Prepare creates a prepared statement for later queries or executions on each physical database.
// Multiple queries or executions may be run concurrently from the returned statement.
// This is equivalent to running: Prepare() using database/sql
//
// https://godoc.org/gopkg.in/gorp.v2#DbMap.Prepare
func (b *Balancer) Prepare(query string) (Stmt, error) {
	dbs := b.GetAllDbs()
	stmts := make([]*sql.Stmt, len(dbs))
	for i := range stmts {
		s, err := dbs[i].Prepare(query)
		if err != nil {
			return nil, err
		}
		stmts[i] = s
	}
	return &stmt{bl: b, stmts: stmts}, nil
}

// TraceOn https://godoc.org/gopkg.in/gorp.v2#DbMap.TraceOn
func (b *Balancer) TraceOn(prefix string, logger gorp.GorpLogger) {
	for _, s := range b.slaves {
		s.TraceOn(fmt.Sprintf("%s <slave>", prefix), logger)
	}
	b.DbMap.TraceOn(fmt.Sprintf("%s <master>", prefix), logger)
}

// TraceOff https://godoc.org/gopkg.in/gorp.v2#DbMap.TraceOff
func (b *Balancer) TraceOff() {
	for _, db := range b.GetAllDbs() {
		db.TraceOff()
	}
}
