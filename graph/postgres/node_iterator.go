// Copyright 2014 The Cayley Authors. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package postgres

import (
	"database/sql"
	"fmt"
	"github.com/barakmich/glog"
	"github.com/jmoiron/sqlx"
	"strings"
	"code.google.com/p/go-uuid/uuid"
	"github.com/google/cayley/graph"
	"github.com/google/cayley/graph/iterator"
	"github.com/google/cayley/quad"
)

const maxNodeCacheSize = 150

type NodeIterator struct {
	uid  uint64
	tags graph.Tagger
	tx   *sqlx.Tx
	qs   *QuadStore
	size int64
	dir  quad.Direction

	sqlQuery   string
	sqlWhere   string
	cursorName string

	resultCache []NodeValue
	cacheIndex  int
}

func NewNodeIterator(qs *QuadStore) *NodeIterator {
	var m NodeIterator
	iterator.BaseInit(&m.Base)

	m.sqlQuery = "SELECT id FROM nodes"
	m.dir = graph.Any
	m.tx = nil
	m.qs = qs
	m.cursorName = "j" + strings.Replace(uuid.NewRandom().String(), "-", "", -1)
	m.cacheIndex = -1

	r := qs.db.QueryRowx("SELECT COUNT(*) FROM nodes;")
	err := r.Scan(&m.size)
	if err != nil {
		glog.Fatalln(err.Error())
		return nil
	}

	return &m
}

func NewNodeIteratorWhere(qs *QuadStore, dir quad.Direction, where string) *NodeIterator {
	var m NodeIterator
	iterator.BaseInit(&m.Base)

	m.dir = dir
	m.sqlWhere = where
	m.sqlQuery = "SELECT " + dirToSchema(m.dir) + " FROM triples WHERE " + m.sqlWhere
	m.tx = nil
	m.qs = qs
	m.cursorName = "j" + strings.Replace(uuid.NewRandom().String(), "-", "", -1)
	m.cacheIndex = -1

	r := qs.db.QueryRowx("SELECT COUNT(*) FROM nodes WHERE id IN (" + m.sqlQuery + ")")
	err := r.Scan(&m.size)
	if err != nil {
		glog.Fatalln(err.Error())
		return nil
	}

	return &m
}

func (it *NodeIterator) beginTx() error {
	var err error
	it.tx, err = it.qs.db.Beginx()
	if err == nil {
		_, err = it.tx.Exec("DECLARE " + it.cursorName + " CURSOR FOR " + it.sqlQuery + ";")
	}
	return err
}

func (it *NodeIterator) Reset() {
	// just Close, Next() will re-open
	it.Close()
}

func (it *NodeIterator) Close() {
	if it.tx != nil {
		it.tx.Exec("CLOSE " + it.cursorName + ";")
		it.tx.Commit()
		it.tx = nil
	}
	it.cacheIndex = -1
}

func (it *NodeIterator) Clone() graph.Iterator {
	newM := &NodeIterator{}
	iterator.BaseInit(&newM.Base)
	newM.qs = it.qs
	newM.size = it.size
	newM.sqlQuery = it.sqlQuery
	newM.sqlWhere = it.sqlWhere
	newM.dir = it.dir
	newM.tx = nil
	newM.cacheIndex = -1
	newM.resultCache = it.resultCache
	newM.cursorName = "j" + strings.Replace(uuid.NewRandom().String(), "-", "", -1)
	newM.CopyTagsFrom(it)
	return newM
}

func (it *NodeIterator) Next() (graph.Value, bool) {
	graph.NextLogIn(it)
	if it.size == int64(len(it.resultCache)) {
		it.cacheIndex++
		if int64(it.cacheIndex) >= it.size {
			return graph.NextLogOut(it, nil, false)
		}
		return graph.NextLogOut(it, it.resultCache[it.cacheIndex], true)
	}
	var tid int64

	if it.tx == nil {
		if err := it.beginTx(); err != nil {
			glog.Fatalln("error beginning in Next() - ", err.Error())
		}
	}

	r := it.tx.QueryRowx("FETCH NEXT FROM " + it.cursorName + ";")
	if err := r.Scan(&tid); err != nil {
		if err != sql.ErrNoRows {
			glog.Errorln("Error Nexting Iterator: ", err)
		}
		it.Close()
		return graph.NextLogOut(it, nil, false)
	}
	it.Last = NodeValue(tid)
	if it.size <= maxNodeCacheSize {
		it.resultCache = append(it.resultCache, NodeValue(tid))
	}
	return graph.NextLogOut(it, NodeValue(tid), true)
}

func (it *NodeIterator) Check(v graph.Value) bool {
	graph.CheckLogIn(it, v)
	if it.sqlWhere == "" {
		it.Last = v
		return graph.CheckLogOut(it, v, true)
	}
	if it.size > maxNodeCacheSize {
		// un-cachable triple-based query
		var res int
		SQL := "SELECT 1 FROM triples WHERE " + dirToSchema(it.dir) + "=$1::bigint AND " + it.sqlWhere
		row := it.qs.db.QueryRowx(SQL, v)
		err := row.Scan(&res)
		if err != nil {
			panic(err)
		}
		if res == 1 {
			it.Last = v
			return graph.CheckLogOut(it, v, true)
		}

		return graph.CheckLogOut(it, v, false)
	}

	// check the cache
	for _, v2 := range it.resultCache {
		if v2 == v {
			it.Last = v
			return graph.CheckLogOut(it, v, true)
		}
	}

	// if the cache is full, then not found
	if it.size == int64(len(it.resultCache)) {
		return graph.CheckLogOut(it, v, false)
	}

	lastLast := it.Last
	curIndex := it.cacheIndex
	for {
		v2, ok := it.Next()
		if ok && v2 == v {
			it.cacheIndex = curIndex
			it.Last = v
			return graph.CheckLogOut(it, v, true)
		}
		if !ok {
			it.cacheIndex = curIndex
			it.Last = lastLast
			break
		}
	}

	return graph.CheckLogOut(it, v, false)
}

func (it *NodeIterator) Size() (int64, bool) {
	return it.size, true
}

func (it *NodeIterator) Type() graph.Type {
	if it.sqlWhere != "" {
		return postgresNodeType
	}
	return postgresAllType
}

func (it *NodeIterator) Sorted() bool                     { return false }
func (it *NodeIterator) Optimize() (graph.Iterator, bool) { return it, false }

func (it *NodeIterator) DebugString(indent int) string {
	return fmt.Sprintf("%s(%s size:%d %s %s)", strings.Repeat(" ", indent), it.Type(), it.size,
		it.dir, it.sqlWhere)
}

func (it *NodeIterator) GetStats() *graph.IteratorStats {
	size, _ := it.Size()
	return &graph.IteratorStats{
		CheckCost: 0,
		NextCost:  1,
		Size:      size,
	}
}
