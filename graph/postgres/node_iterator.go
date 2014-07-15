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
)

type NodeIterator struct {
	iterator.Base
	tx   *sqlx.Tx
	ts   *TripleStore
	size int64

	sqlQuery   string
	cursorName string
}

func NewNodeIterator(ts *TripleStore) *NodeIterator {
	var m NodeIterator
	iterator.BaseInit(&m.Base)

	m.sqlQuery = "SELECT id FROM nodes"

	var err error
	m.tx, err = ts.db.Beginx()
	if err != nil {
		glog.Fatalln(err.Error())
		return nil
	}
	r := m.tx.QueryRowx("SELECT COUNT(*) FROM nodes;")
	err = r.Scan(&m.size)
	if err != nil {
		glog.Fatalln(err.Error())
		return nil
	}

	m.cursorName = "j" + strings.Replace(uuid.NewRandom().String(), "-", "", -1)
	m.tx.MustExec("DECLARE " + m.cursorName + " CURSOR FOR " + m.sqlQuery + ";")
	m.ts = ts
	return &m
}

func (it *NodeIterator) Reset() {
	var err error
	it.tx.MustExec("CLOSE " + it.cursorName + ";")
	it.tx.Commit()
	it.tx, err = it.ts.db.Beginx()
	if err != nil {
		glog.Fatalln(err.Error())
	}

	it.tx.MustExec("DECLARE " + it.cursorName + " CURSOR FOR " + it.sqlQuery + ";")
}

func (it *NodeIterator) Close() {
	it.tx.MustExec("CLOSE " + it.cursorName + ";")
	it.tx.Commit()
}

func (it *NodeIterator) Clone() graph.Iterator {
	newM := &NodeIterator{}
	iterator.BaseInit(&newM.Base)
	newM.ts = it.ts
	newM.size = it.size
	newM.sqlQuery = it.sqlQuery

	var err error
	newM.cursorName = "j" + strings.Replace(uuid.NewRandom().String(), "-", "", -1)
	newM.tx, err = newM.ts.db.Beginx()
	if err != nil {
		glog.Fatalln(err.Error())
		return nil
	}

	newM.tx.MustExec("DECLARE " + newM.cursorName + " CURSOR FOR " + newM.sqlQuery + ";")
	newM.CopyTagsFrom(it)
	return newM
}

func (it *NodeIterator) Next() (graph.Value, bool) {
	var tid int64
	graph.NextLogIn(it)

	r := it.tx.QueryRowx("FETCH NEXT FROM " + it.cursorName + ";")
	if err := r.Scan(&tid); err != nil {
		if err != sql.ErrNoRows {
			glog.Errorln("Error Nexting Iterator: ", err)
		}
		return graph.NextLogOut(it, nil, false)
	}
	it.Last = NodeValue(tid)
	return graph.NextLogOut(it, NodeValue(tid), true)
}

func (it *NodeIterator) Check(v graph.Value) bool {
	graph.CheckLogIn(it, v)
	it.Last = v
	return graph.CheckLogOut(it, v, true)
}

func (it *NodeIterator) Size() (int64, bool) {
	return it.size, true
}

func (it *NodeIterator) Type() graph.Type {
	return graph.All
}

func (it *NodeIterator) Sorted() bool                     { return false }
func (it *NodeIterator) Optimize() (graph.Iterator, bool) { return it, false }

func (it *NodeIterator) DebugString(indent int) string {
	return fmt.Sprintf("%s(%s size:%d %s %s)", strings.Repeat(" ", indent), it.Type(), it.size)
}

func (it *NodeIterator) GetStats() *graph.IteratorStats {
	size, _ := it.Size()
	return &graph.IteratorStats{
		CheckCost: 0,
		NextCost:  1,
		Size:      size,
	}
}
