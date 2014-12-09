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
	"github.com/google/cayley/quad"
)

type Iterator struct {
	uid   uint64
	tags  graph.Tagger
	tx    *sqlx.Tx
	qs    *QuadStore
	dir   quad.Direction
	val   NodeValue
	size  int64
	sqlQuery   string
	sqlWhere   string
	cursorName string
}

func (it *Iterator) sqlClause() string {
	if it.sqlWhere != "" {
		return it.sqlWhere
	}

	return fmt.Sprintf("%s=%d", dirToSchema(it.dir), int64(it.val))
}

func NewIterator(qs *QuadStore, dir quad.Direction, val graph.Value) *Iterator {
	var m Iterator
	m.cursorName = "j" + strings.Replace(uuid.NewRandom().String(), "-", "", -1)
	m.sqlQuery = "SELECT id, subj, pred, obj, prov FROM quads"
	var err error
	m.tx = nil
	m.qs = qs

	m.dir = dir
	if dir != quad.Any {
		var ok bool
		m.val, ok = val.(NodeValue)
		if !ok {
			var v2 int64
			v2, ok = val.(int64)
			m.val = NodeValue(v2)
		}
		where := fmt.Sprintf(" WHERE %s=$1", dirToSchema(dir))
		r := qs.db.QueryRowx("SELECT COUNT(*) FROM quads"+where, m.val)
		err = r.Scan(&m.size)
		if err != nil {
			glog.Fatalln(err.Error())
			return nil
		}
		m.sqlQuery += where

	} else {
		r := qs.db.QueryRowx("SELECT COUNT(*) FROM quads;")
		err = r.Scan(&m.size)
		if err != nil {
			glog.Fatalln(err.Error())
			return nil
		}
	}

	return &m
}

func (it *Iterator) beginTx() error {
	var err error
	it.tx, err = it.qs.db.Beginx()
	if err == nil {
		if it.dir == quad.Any {
			_, err = it.tx.Exec("DECLARE " + it.cursorName + " CURSOR FOR " + it.sqlQuery + ";")
		} else {
			_, err = it.tx.Exec("DECLARE "+it.cursorName+" CURSOR FOR "+it.sqlQuery+";", it.val)
		}
	}
	return err
}

func NewIteratorWhere(qs *QuadStore, where string) *Iterator {
	var m Iterator

	m.cursorName = "j" + strings.Replace(uuid.NewRandom().String(), "-", "", -1)
	m.sqlQuery = "SELECT id, subj, pred, obj, prov FROM quads WHERE "
	var err error
	m.tx = nil
	m.qs = qs

	m.sqlWhere = where
	m.dir = quad.Any
	m.val = NodeValue(0)

	r := qs.db.QueryRowx("SELECT COUNT(*) FROM quads WHERE " + where)
	err = r.Scan(&m.size)
	if err != nil {
		glog.Fatalln("select count failed "+where, err.Error())
		return nil
	}
	m.sqlQuery += where

	return &m
}

func NewAllIterator(qs *QuadStore) *Iterator {
	return NewIterator(qs, quad.Any, nil)
}

func (it *Iterator) Reset() {
	// just Close, Next() will re-open
	it.Close()
}

func (it *Iterator) Close() {
	if it.tx != nil {
		it.tx.Exec("CLOSE " + it.cursorName + ";")
		it.tx.Commit()
		it.tx = nil
	}
}

func (it *Iterator) Clone() graph.Iterator {
	newM := &Iterator{}
	newM.dir = it.dir
	newM.val = it.val
	newM.qs = it.qs
	newM.size = it.size
	newM.sqlQuery = it.sqlQuery
	newM.cursorName = "j" + strings.Replace(uuid.NewRandom().String(), "-", "", -1)
	newM.tx = nil
	newM.tags.CopyFrom(it)
	return newM
}

func (it *Iterator) Next() (bool) {
	if it.tx == nil {
		if err := it.beginTx(); err != nil {
			glog.Fatalln("error beginning in Next() - ", err.Error())
		}
	}

	var nullProv sql.NullInt64
	var qv QuadValue
	r := it.tx.QueryRowx("FETCH NEXT FROM " + it.cursorName + ";")
	if err := r.Scan(&qv[0], &qv[1], &qv[2], &qv[3], &nullProv)
 err != nil {
		if err != sql.ErrNoRows {
			glog.Errorln("Error Nexting Iterator: ", err)
		}
		it.Close()
		return false
	}
	if nullProv.Valid {
		nv, _ := nullProv.Value()
		qv[4] = nv.(int64)
	} else {
		qv[4] = int64(-1)
	}
	return true
}

func (it *Iterator) Contains(v graph.Value) bool {
	graph.ContainsLogIn(it, v)
	if it.dir == quad.Any {
		return graph.ContainsLogOut(it, v, true)
	}
	if it.tx == nil {
		if err := it.beginTx(); err != nil {
			glog.Fatalln("error beginning in Contains() - ", err.Error())
		}
	}

	qv := v.(QuadValue)
	hit := 0
	r := it.tx.QueryRowx("SELECT COUNT(*) FROM ("+it.sqlQuery+") x WHERE x.id=$2;", it.val, qv[0])
	err := r.Scan(&hit)
	if err != nil {
		glog.Fatalln(err.Error())
	}
	if hit > 0 {
		return graph.ContainsLogOut(it, v, true)
	}
	return graph.ContainsLogOut(it, v, false)
}

func (it *Iterator) Size() (int64, bool) {
	return it.size, true
}

func (it *Iterator) Type() graph.Type {
	if it.sqlWhere == "" && it.dir == quad.Any {
		return graph.All
	}
	return postgresType
}

func (it *Iterator) Sorted() bool {
	return false
}

func (it *Iterator) Optimize() (graph.Iterator, bool) { 
	return it, false 
}

func (it *Iterator) Result() graph.Value {
	return it.result
}

func (it *Iterator) ResultTree() *graph.ResultTree {
	return graph.NewResultTree(it.Result())
}

func (it *Iterator) NextPath() bool {
	return false
}

func (it *Iterator) SubIterators() []graph.Iterator {
	return nil
}

func (it *Iterator) TagResults(dst map[string]graph.Value) {
	for _, tag := range it.tags.Tags() {
		dst[tag] = it.Result()
	}

	for tag, value := range it.tags.Fixed() {
		dst[tag] = value
	}
}

func (it *Iterator) Tagger() *graph.Tagger {
	return &it.tags
}

func (it *Iterator) UID() uint64 {
	return it.uid
}

func (it *Iterator) Describe() graph.Description {
	size, _ := it.Size()
	return graph.Description{
	        UID:       it.UID(),
	        Name:      it.qs.NameOf(Token(it.checkID)),
		Type:      it.tags.Tags(),
		Size:      size,
		Direction: it.dir,
	}
}

func (it *Iterator) DebugString(indent int) string {
	if it.sqlWhere != "" {
		return fmt.Sprintf("%s(%s size:%d WHERE %s)", strings.Repeat(" ", indent), it.Type(), it.size,
			it.sqlWhere)
	}
	if it.dir == quad.Any {
		return fmt.Sprintf("%s(%s size:%d ALL)", strings.Repeat(" ", indent), it.Type(), it.size)
	}
	return fmt.Sprintf("%s(%s size:%d %s=%s)", strings.Repeat(" ", indent), it.Type(), it.size,
		it.dir, it.qs.NameOf(it.val))
}

func (it *Iterator) Stats() graph.IteratorStats {
	size, _ := it.Size()
	return &graph.IteratorStats{
		ContainsCost: 10,
		NextCost:  1,
		Size:      size,
	}
}
