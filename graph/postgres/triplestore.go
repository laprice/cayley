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
	"github.com/google/cayley/graph"
	"github.com/jmoiron/sqlx"
	_ "github.com/lib/pq"

	"github.com/barakmich/glog"
)

type TripleStore struct {
	db      *sqlx.DB
	idCache *IDLru
}

const pgSchema = `

CREATE TABLE nodes (
	id bigserial primary key,
	name varchar not null
);

CREATE TABLE triples (
	id   bigserial primary key,
	subj bigint references nodes(id),
	obj  bigint references nodes(id),
	pred bigint references nodes(id),
	prov varchar
);

CREATE INDEX node_name_idx ON nodes(name);
CREATE INDEX triple_search_idx ON triples(prov, subj, pred, obj);
CREATE INDEX triple_subj_idx ON triples(subj);
CREATE INDEX triple_pred_idx ON triples(pred);
CREATE INDEX triple_sobj_idx ON triples(obj);
CREATE INDEX triple_prov_idx ON triples(prov);
`

func CreateNewPostgresGraph(addr string, options graph.OptionsDict) bool {
	t := NewTripleStore(addr, options)
	defer t.Close()
	_, err := t.db.Exec(pgSchema)
	if err != nil {
		glog.Fatalln(err)
		return false
	}
	return true
}

// addr = "user=username dbname=dbname"
func NewTripleStore(addr string, options graph.OptionsDict) *TripleStore {
	db, err := sqlx.Connect("postgres", addr+" sslmode=disable")
	if err != nil {
		glog.Fatalln(err.Error())
	}

	return &TripleStore{
		db:      db,
		idCache: NewIDLru(1 << 16),
	}
}

func (t *TripleStore) getOrCreateNode(name string) int64 {
	val, ok := t.idCache.RevGet(name)
	if ok {
		return val
	}

	r := t.db.QueryRowx("SELECT id FROM nodes WHERE name=$1::text;", name)
	err := r.Scan(&val)
	if err != nil { // assume it's ErrNoRows
		r = t.db.QueryRowx("INSERT INTO nodes (name) VALUES ($1) RETURNING id;", name)
		err = r.Scan(&val)
		if err != nil { // concurrent write?
			r = t.db.QueryRowx("SELECT id FROM nodes WHERE name=$1::text;", name)
			r.Scan(&val)
		}
	}

	t.idCache.Put(val, name)
	return val
}

// Add a triple to the store.
func (t *TripleStore) AddTriple(x *graph.Triple) {
	sid := t.getOrCreateNode(x.Sub)
	pid := t.getOrCreateNode(x.Pred)
	oid := t.getOrCreateNode(x.Obj)
	t.db.MustExec(`INSERT INTO triples (subj, pred, obj, prov) VALUES ($1,$2,$3,$4::text);`, sid, pid, oid, x.Provenance)
}

// Add a set of triples to the store, atomically if possible.
func (t *TripleStore) AddTripleSet(xset []*graph.Triple) {
	t.db.MustExec("BEGIN; SET CONSTRAINTS ALL DEFERRED;")
	// TODO: multi-INSERT or COPY FROM
	for _, x := range xset {
		t.AddTriple(x)
	}
	t.db.MustExec("COMMIT;")
}

// Removes a triple matching the given one  from the database,
// if it exists. Does nothing otherwise.
func (t *TripleStore) RemoveTriple(x *graph.Triple) {
	t.db.MustExec(`DELETE FROM triples USING nodes s, nodes p, nodes o
		WHERE prov=$4::text AND s.name=$1::text AND p.name=$2::text AND o.name=$3::text
		AND subj=s.id AND pred=p.id AND obj=o.id;`,
		x.Sub, x.Pred, x.Obj, x.Provenance)
}

// Given an opaque token, returns the triple for that token from the store.
func (t *TripleStore) GetTriple(tid graph.TSVal) (tr *graph.Triple) {
	r := t.db.QueryRowx(`SELECT s.name, p.name, o.name, t.prov 
		FROM triples t, nodes s, nodes p, nodes o
		WHERE t.id=$1 AND t.subj=s.id AND t.pred=p.id AND t.obj=o.id;`, tid.(int64))
	tr = &graph.Triple{}
	r.Scan(&tr.Sub, &tr.Pred, &tr.Obj, &tr.Provenance) // ignore error
	return
}

// Given a direction and a token, creates an iterator of links which have
// that node token in that directional field.
func (ts *TripleStore) GetTripleIterator(dir string, val graph.TSVal) graph.Iterator {
	return NewIterator(ts, "triples", dir, val)
}

// Returns an iterator enumerating all nodes in the graph.
func (ts *TripleStore) GetNodesAllIterator() graph.Iterator {
	return NewAllIterator(ts, "nodes")
}

// Returns an iterator enumerating all links in the graph.
func (ts *TripleStore) GetTriplesAllIterator() graph.Iterator {
	return NewAllIterator(ts, "triples")
}

func (t *TripleStore) MakeFixed() *graph.FixedIterator {
	return graph.NewFixedIteratorWithCompare(func(a, b graph.TSVal) bool {
		return a.(int64) == b.(int64)
	})
}

// Given a node ID, return the opaque token used by the TripleStore
// to represent that id.
func (t *TripleStore) GetIdFor(name string) graph.TSVal {
	res, ok := t.idCache.RevGet(name)
	if ok {
		return res
	}

	r := t.db.QueryRowx("SELECT id FROM nodes WHERE name=$1;", name)
	r.Scan(&res) // ignore error

	t.idCache.Put(res, name)
	return res
}

// Given an opaque token, return the node that it represents.
func (t *TripleStore) GetNameFor(oid graph.TSVal) (res string) {
	val, ok := t.idCache.Get(oid.(int64))
	if ok {
		return val
	}

	r := t.db.QueryRowx("SELECT name FROM nodes WHERE id=$1;", oid.(int64))
	r.Scan(&res) // ignore error

	t.idCache.Put(oid.(int64), res)
	return
}

// Returns the number of triples currently stored.
func (t *TripleStore) Size() (res int64) {
	r := t.db.QueryRowx("SELECT COUNT(*) FROM triples;")
	r.Scan(&res) // ignore error
	return
}

// Close the triple store and clean up. (Flush to disk, cleanly
// sever connections, etc)
func (t *TripleStore) Close() {
	t.db.Close()
}

// Convienence function for speed. Given a triple token and a direction
// return the node token for that direction. Sometimes, a TripleStore
// can do this without going all the way to the backing store, and
// gives the TripleStore the opportunity to make this optimization.
//
// Iterators will call this. At worst, a valid implementation is
// self.GetIdFor(self.GetTriple(triple_id).Get(dir))
func (t *TripleStore) GetTripleDirection(triple_id graph.TSVal, dir string) graph.TSVal {
	// TODO: replace stub implementation
	return t.GetIdFor(t.GetTriple(triple_id).Get(dir))
}

func (ts *TripleStore) OptimizeIterator(it graph.Iterator) (graph.Iterator, bool) {
	switch it.Type() {
	case "linksto":
		return ts.optimizeLinksTo(it.(*graph.LinksToIterator))

	}
	return it, false
}

func (ts *TripleStore) optimizeLinksTo(it *graph.LinksToIterator) (graph.Iterator, bool) {
	l := it.GetSubIterators()
	if l.Len() != 1 {
		return it, false
	}
	primaryIt := l.Front().Value.(graph.Iterator)
	if primaryIt.Type() == "fixed" {
		size, _ := primaryIt.Size()
		if size == 1 {
			val, ok := primaryIt.Next()
			if !ok {
				panic("Sizes lie")
			}
			newIt := ts.GetTripleIterator(it.Direction(), val)
			newIt.CopyTagsFrom(it)
			for _, tag := range primaryIt.Tags() {
				newIt.AddFixedTag(tag, val)
			}
			it.Close()
			return newIt, true
		}
	}
	return it, false
}
