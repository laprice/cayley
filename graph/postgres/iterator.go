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
	"strings"

	"code.google.com/p/go-uuid/uuid"
	"github.com/google/cayley/graph"
)

type Iterator struct {
	graph.BaseIterator
	ts         *TripleStore
	dir        string
	val        graph.TSVal
	name       string
	size       int64
	isAll      bool
	collection string

	sqlQuery   string
	cursorName string
}

func NewIterator(ts *TripleStore, collection string, dir string, val graph.TSVal) *Iterator {
	var m Iterator
	graph.BaseIteratorInit(&m.BaseIterator)

	m.val = val
	m.name = ts.GetNameFor(val)
	m.collection = collection
	m.dir = dir

	if collection == "triples" {
		m.sqlQuery = `FROM triples t`
		switch dir {

		case "s":
			m.sqlQuery += ", nodes n WHERE t.subj=n.id AND n.name=$1::text"
		case "p":
			m.sqlQuery += ", nodes n WHERE t.pred=n.id AND n.name=$1::text"
		case "o":
			m.sqlQuery += ", nodes n WHERE t.obj=n.id AND n.name=$1::text"
		case "c":
			m.sqlQuery += " WHERE t.prov=$1::text"
		}
	} else {
		// nodes -- does this ever get called?
		m.sqlQuery = "FROM nodes t WHERE name=$1::text"
	}

	m.cursorName = "j" + strings.Replace(uuid.NewRandom().String(), "-", "", -1)
	r := ts.db.QueryRowx("SELECT COUNT(t.id) "+m.sqlQuery+";", m.name)
	r.Scan(&m.size)

	ts.db.MustExec("DECLARE "+m.cursorName+" CURSOR FOR SELECT t.id "+m.sqlQuery+";", m.name)
	m.ts = ts
	m.isAll = false
	return &m
}

func NewAllIterator(ts *TripleStore, collection string) *Iterator {
	var m Iterator
	graph.BaseIteratorInit(&m.BaseIterator)

	m.collection = collection
	m.sqlQuery = "SELECT id FROM " + collection + ";"

	r := ts.db.QueryRowx("SELECT COUNT(*) FROM " + collection + ";")
	r.Scan(&m.size)

	m.cursorName = "j" + strings.Replace(uuid.NewRandom().String(), "-", "", -1)
	ts.db.MustExec("DECLARE " + m.cursorName + " CURSOR FOR " + m.sqlQuery + ";")
	m.ts = ts
	m.isAll = true
	return &m
}

func (it *Iterator) Reset() {
	it.ts.db.MustExec("CLOSE " + it.cursorName + ";")
	if it.isAll {
		it.ts.db.MustExec("DECLARE " + it.cursorName + " CURSOR FOR " + it.sqlQuery + ";")
	} else {
		it.ts.db.MustExec("DECLARE "+it.cursorName+" CURSOR FOR "+it.sqlQuery+";", it.name)
	}
}

func (it *Iterator) Close() {
	it.ts.db.MustExec("CLOSE " + it.cursorName + ";")
}

func (it *Iterator) Clone() graph.Iterator {
	var newM *Iterator
	if it.isAll {
		newM = NewAllIterator(it.ts, it.collection)
	} else {
		newM = NewIterator(it.ts, it.collection, it.dir, it.val)
	}
	newM.CopyTagsFrom(it)
	return newM
}

func (it *Iterator) Next() (graph.TSVal, bool) {
	var tid int64
	r := it.ts.db.QueryRowx("FETCH NEXT FROM " + it.cursorName + ";")
	if err := r.Scan(&tid); err != nil {
		if err != sql.ErrNoRows {
			glog.Errorln("Error Nexting Iterator: ", err)
		}
		return nil, false
	}

	it.Last = tid
	return tid, true
}

func (it *Iterator) Check(v graph.TSVal) bool {
	if it.isAll {
		return true
	}

	hit := 0
	r := it.ts.db.QueryRowx("SELECT COUNT(t.id) "+it.sqlQuery+" AND t.id=$2;", it.name, v.(int64))
	r.Scan(&hit)
	return hit > 0
}

func (it *Iterator) Size() (int64, bool) {
	return it.size, true
}

func (it *Iterator) Type() string {
	if it.isAll {
		return "all"
	}
	return "postgres"
}
func (it *Iterator) Sorted() bool                     { return false }
func (it *Iterator) Optimize() (graph.Iterator, bool) { return it, false }

func (it *Iterator) DebugString(indent int) string {
	size, _ := it.Size()
	return fmt.Sprintf("%s(%s size:%d %s %s)", strings.Repeat(" ", indent), it.Type(), size, it.val, it.name)
}

func (it *Iterator) GetStats() *graph.IteratorStats {
	size, _ := it.Size()
	return &graph.IteratorStats{
		CheckCost: 10,
		NextCost:  1,
		Size:      size,
	}
}
