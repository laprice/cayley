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

type TripleIterator struct {
	iterator.Base
	tx   *sqlx.Tx
	ts   *TripleStore
	dir  graph.Direction
	val  NodeValue
	size int64

	sqlQuery   string
	sqlWhere   string
	cursorName string
}

func (it *TripleIterator) sqlClause() string {
	if it.sqlWhere != "" {
		return it.sqlWhere
	}

	return fmt.Sprintf("%s=%d", dirToSchema(it.dir), int64(it.val))
}

func NewTripleIterator(ts *TripleStore, dir graph.Direction, val graph.Value) *TripleIterator {
	var m TripleIterator
	iterator.BaseInit(&m.Base)

	m.cursorName = "j" + strings.Replace(uuid.NewRandom().String(), "-", "", -1)
	m.sqlQuery = "SELECT id, subj, pred, obj, prov FROM triples"
	var err error
	m.tx, err = ts.db.Beginx()
	if err != nil {
		glog.Fatalln(err.Error())
		return nil
	}

	m.dir = dir
	if dir != graph.Any {
		var ok bool
		m.val, ok = val.(NodeValue)
		if !ok {
			var v2 int64
			v2, ok = val.(int64)
			m.val = NodeValue(v2)
		}
		where := fmt.Sprintf(" WHERE %s=$1", dirToSchema(dir))
		r := m.tx.QueryRowx("SELECT COUNT(*) FROM triples"+where, m.val)
		err = r.Scan(&m.size)
		if err != nil {
			glog.Fatalln(err.Error())
			return nil
		}
		m.sqlQuery += where
		m.tx.MustExec("DECLARE "+m.cursorName+" CURSOR FOR "+m.sqlQuery+";", m.val)

	} else {
		r := m.tx.QueryRowx("SELECT COUNT(*) FROM triples;")
		err = r.Scan(&m.size)
		if err != nil {
			glog.Fatalln(err.Error())
			return nil
		}
		m.tx.MustExec("DECLARE " + m.cursorName + " CURSOR FOR " + m.sqlQuery + ";")
	}

	m.ts = ts
	return &m
}

func NewTripleIteratorWhere(ts *TripleStore, where string) *TripleIterator {
	var m TripleIterator
	iterator.BaseInit(&m.Base)

	m.cursorName = "j" + strings.Replace(uuid.NewRandom().String(), "-", "", -1)
	m.sqlQuery = "SELECT id, subj, pred, obj, prov FROM triples WHERE "
	var err error
	m.tx, err = ts.db.Beginx()
	if err != nil {
		glog.Fatalln(err.Error())
		return nil
	}

	m.sqlWhere = where
	m.dir = graph.Any
	m.val = NodeValue(0)

	r := m.tx.QueryRowx("SELECT COUNT(*) FROM triples WHERE " + where)
	err = r.Scan(&m.size)
	if err != nil {
		glog.Fatalln("select count failed "+where, err.Error())
		return nil
	}
	m.sqlQuery += where
	m.tx.MustExec("DECLARE " + m.cursorName + " CURSOR FOR " + m.sqlQuery + ";")
	fmt.Println(m.sqlQuery)

	m.ts = ts
	return &m
}

func NewAllIterator(ts *TripleStore) *TripleIterator {
	return NewTripleIterator(ts, graph.Any, nil)
}

func (it *TripleIterator) Reset() {
	var err error
	it.tx.MustExec("CLOSE " + it.cursorName + ";")
	it.tx.Commit()
	it.tx, err = it.ts.db.Beginx()
	if err != nil {
		glog.Fatalln(err.Error())
	}

	if it.dir == graph.Any {
		it.tx.MustExec("DECLARE " + it.cursorName + " CURSOR FOR " + it.sqlQuery + ";")
	} else {
		it.tx.MustExec("DECLARE "+it.cursorName+" CURSOR FOR "+it.sqlQuery+";", it.val)
	}
}

func (it *TripleIterator) Close() {
	if it.tx != nil {
		it.tx.Exec("CLOSE " + it.cursorName + ";")
		it.tx.Commit()
		it.tx = nil
	}
}

func (it *TripleIterator) Clone() graph.Iterator {
	newM := &TripleIterator{}
	iterator.BaseInit(&newM.Base)
	newM.dir = it.dir
	newM.val = it.val
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
	if newM.dir == graph.Any {
		newM.tx.MustExec("DECLARE " + newM.cursorName + " CURSOR FOR " + newM.sqlQuery + ";")
	} else {
		newM.tx.MustExec("DECLARE "+newM.cursorName+" CURSOR FOR "+newM.sqlQuery+";", newM.val)
	}
	newM.CopyTagsFrom(it)
	return newM
}

func (it *TripleIterator) Next() (graph.Value, bool) {
	graph.NextLogIn(it)

	var nullProv sql.NullInt64
	var trv TripleValue
	r := it.tx.QueryRowx("FETCH NEXT FROM " + it.cursorName + ";")
	if err := r.Scan(&trv[0], &trv[1], &trv[2], &trv[3], &nullProv); err != nil {
		if err != sql.ErrNoRows {
			glog.Errorln("Error Nexting Iterator: ", err)
		}
		return graph.NextLogOut(it, nil, false)
	}
	if nullProv.Valid {
		nv, _ := nullProv.Value()
		trv[4] = nv.(int64)
	} else {
		trv[4] = int64(-1)
	}
	it.Last = trv
	return graph.NextLogOut(it, trv, true)
}

func (it *TripleIterator) Check(v graph.Value) bool {
	graph.CheckLogIn(it, v)
	if it.dir == graph.Any {
		it.Last = v
		return graph.CheckLogOut(it, v, true)
	}

	trv := v.(TripleValue)
	hit := 0
	r := it.tx.QueryRowx("SELECT COUNT(*) FROM ("+it.sqlQuery+") x WHERE x.id=$2;", it.val, trv[0])
	err := r.Scan(&hit)
	if err != nil {
		glog.Fatalln(err.Error())
	}
	if hit > 0 {
		it.Last = v
		return graph.CheckLogOut(it, v, true)
	}
	return graph.CheckLogOut(it, v, false)
}

func (it *TripleIterator) Size() (int64, bool) {
	return it.size, true
}

func (it *TripleIterator) Type() graph.Type {
	if it.sqlWhere == "" && it.dir == graph.Any {
		return graph.All
	}
	return postgresType
}
func (it *TripleIterator) Sorted() bool                     { return false }
func (it *TripleIterator) Optimize() (graph.Iterator, bool) { return it, false }

func (it *TripleIterator) DebugString(indent int) string {
	if it.sqlWhere != "" {
		return fmt.Sprintf("%s(%s size:%d WHERE %s)", strings.Repeat(" ", indent), it.Type(), it.size,
			it.sqlWhere)
	}
	if it.dir == graph.Any {
		return fmt.Sprintf("%s(%s size:%d ALL)", strings.Repeat(" ", indent), it.Type(), it.size)
	}
	return fmt.Sprintf("%s(%s size:%d %s=%s)", strings.Repeat(" ", indent), it.Type(), it.size,
		it.dir, it.ts.NameOf(it.val))
}

func (it *TripleIterator) GetStats() *graph.IteratorStats {
	size, _ := it.Size()
	return &graph.IteratorStats{
		CheckCost: 10,
		NextCost:  1,
		Size:      size,
	}
}
