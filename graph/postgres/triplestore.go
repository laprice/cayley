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
	"github.com/google/cayley/graph"
	"github.com/google/cayley/graph/iterator"
	"github.com/google/cayley/quad"
	"github.com/jmoiron/sqlx"
	_ "github.com/lib/pq"

	"github.com/barakmich/glog"
)

type QuadStore struct {
	db      *sqlx.DB
	idCache *IDLru
}

type QuadValue [5]int64
type NodeValue int64

const pgSchema = `

CREATE TABLE nodes (
	id bigserial primary key,
	name varchar not null
);

CREATE TABLE quads (
	id   bigserial primary key,
	subj bigint references nodes(id),
	obj  bigint references nodes(id),
	pred bigint references nodes(id),
	prov bigint null references nodes(id)
);

CREATE INDEX node_name_idx ON nodes(name);
CREATE INDEX quad_search_idx ON quads(subj, obj, pred, prov);
CREATE INDEX quad_subj_idx ON quads(subj);
CREATE INDEX quad_pred_idx ON quads(pred);
CREATE INDEX quad_obj_idx ON quads(obj);
CREATE INDEX quad_prov_idx ON quads(prov);
`

func createNewPostgresGraph(addr string, options graph.Options) error {
	t, err := newQuadStore(addr, options)
	if err == nil {
		defer t.Close()
		_, err = t.(*QuadStore).db.Exec(pgSchema)
	}
	return err
}

// addr = "user=username dbname=dbname"
func newQuadStore(addr string, options graph.Options) (graph.QuadStore, error) {
	db, err := sqlx.Connect("postgres", addr+" sslmode=disable")
	if err != nil {
		return nil, err
	}

	return &QuadStore{
		db:      db,
		idCache: NewIDLru(1 << 16),
	}, nil
}

func (t *QuadStore) getOrCreateNode(name string) int64 {
	val, ok := t.idCache.RevGet(name)
	if ok {
		return val
	}

	r := t.db.QueryRowx("SELECT id FROM nodes WHERE name=$1::text;", name)
	err := r.Scan(&val)
	if err != nil {
		if err != sql.ErrNoRows {
			glog.Fatalln(err.Error())
		} else {
			r = t.db.QueryRowx("INSERT INTO nodes (name) VALUES ($1) RETURNING id;", name)
			err = r.Scan(&val)
			if err != nil {
				glog.Fatalln(err.Error())
			}
		}
	}

	t.idCache.Put(val, name)
	return val
}

// Add a quad to the store.
func (t *QuadStore) AddQuad(x *graph.Quad) {
	sid := t.getOrCreateNode(x.Subject)
	pid := t.getOrCreateNode(x.Predicate)
	oid := t.getOrCreateNode(x.Object)
	if x.Provenance != "" {
		cid := t.getOrCreateNode(x.Provenance)
		t.db.MustExec(`INSERT INTO quads (subj, pred, obj, prov) VALUES ($1,$2,$3,$4);`, sid, pid, oid, cid)
	} else {
		t.db.MustExec(`INSERT INTO quads (subj, pred, obj, prov) VALUES ($1,$2,$3,NULL);`, sid, pid, oid)
	}
}

// Add a set of quads to the store, atomically if possible.
func (t *QuadStore) AddQuadSet(xset []*graph.Quad) {
	t.db.MustExec("BEGIN; SET CONSTRAINTS ALL DEFERRED;")
	// TODO: multi-INSERT or COPY FROM
	for _, x := range xset {
		t.AddQuad(x)
	}
	t.db.MustExec("COMMIT;")
}

// Removes a quad matching the given one  from the database,
// if it exists. Does nothing otherwise.
func (t *QuadStore) RemoveQuad(x *graph.Quad) {
	if x.Provenance != "" {
		t.db.MustExec(`DELETE FROM quads USING nodes s, nodes p, nodes o, nodes c
		WHERE s.name=$1::text AND p.name=$2::text AND o.name=$3::text AND c.name=$4::text
		AND subj=s.id AND obj=o.id AND pred=p.id AND prov=c.id;`,
			x.Subject, x.Predicate, x.Object, x.Provenance)
	} else {
		t.db.MustExec(`DELETE FROM quads USING nodes s, nodes p, nodes o
		WHERE s.name=$1::text AND p.name=$2::text AND o.name=$3::text
		AND subj=s.id AND obj=o.id AND pred=p.id AND prov IS NULL;`,
			x.Subject, x.Predicate, x.Object)
	}
}

// Given an opaque token, returns the quad for that token from the store.
func (t *QuadStore) Quad(tid graph.Value) (tr *graph.Quad) {
	ok := false
	gotAll := true
	tr = &graph.Quad{}
	trv := tid.(QuadValue)

	tr.Subject, ok = t.idCache.Get(trv[1])
	gotAll = gotAll && ok
	tr.Predicate, ok = t.idCache.Get(trv[2])
	gotAll = gotAll && ok
	tr.Object, ok = t.idCache.Get(trv[3])
	gotAll = gotAll && ok
	if trv[4] != -1 {
		tr.Provenance, ok = t.idCache.Get(trv[4])
		gotAll = gotAll && ok
		if !gotAll {
			r := t.db.QueryRowx(`SELECT s.name, p.name, o.name, c.name
				FROM quads t, nodes s, nodes p, nodes o, nodes c
				WHERE t.id=$1 AND t.subj=s.id AND t.pred=p.id AND t.obj=o.id AND t.prov=c.id;`, trv[0])
			err := r.Scan(&tr.Subject, &tr.Predicate, &tr.Object, &tr.Provenance)
			if err != nil {
				glog.Fatalln(err.Error())
			}
			t.idCache.Put(trv[1], tr.Subject)
			t.idCache.Put(trv[2], tr.Predicate)
			t.idCache.Put(trv[3], tr.Object)
			t.idCache.Put(trv[4], tr.Provenance)
		}
	} else {
		tr.Provenance = ""
		if !gotAll {
			r := t.db.QueryRowx(`SELECT s.name, p.name, o.name
				FROM quads t, nodes s, nodes p, nodes o
				WHERE t.id=$1 AND t.subj=s.id AND t.pred=p.id AND t.obj=o.id;`, trv[0])
			err := r.Scan(&tr.Subject, &tr.Predicate, &tr.Object)
			if err != nil {
				glog.Fatalln(err.Error())
			}
			t.idCache.Put(trv[1], tr.Subject)
			t.idCache.Put(trv[2], tr.Predicate)
			t.idCache.Put(trv[3], tr.Object)
		}
	}

	return
}

// Given a direction and a token, creates an iterator of links which have
// that node token in that directional field.
func (ts *QuadStore) QuadIterator(dir graph.Direction, val graph.Value) graph.Iterator {
	it := NewQuadIterator(ts, dir, val)
	if it.size == 0 {
		it.Close()
		return iterator.NewNull()
	}
	return it
}

// Returns an iterator enumerating all nodes in the graph.
func (ts *QuadStore) NodesAllIterator() graph.Iterator {
	return NewNodeIterator(ts)
}

// Returns an iterator enumerating all links in the graph.
func (ts *QuadStore) QuadsAllIterator() graph.Iterator {
	return NewAllIterator(ts)
}

func (t *QuadStore) FixedIterator() graph.FixedIterator {
	return iterator.NewFixedIteratorWithCompare(func(a, b graph.Value) bool {
		switch v := a.(type) {
		case NodeValue:
			if bv, ok := b.(NodeValue); ok {
				return v == bv
			}
			return v == NodeValue(b.(int64))

		case QuadValue:
			w := b.(QuadValue)
			return v[0] == w[0] && v[1] == w[1] && v[2] == w[2] && v[3] == w[3] && v[4] == w[4]
		}
		return false
	})
}

// Given a node ID, return the opaque token used by the QuadStore
// to represent that id.
func (t *QuadStore) ValueOf(name string) graph.Value {
	res, ok := t.idCache.RevGet(name)
	if ok {
		return NodeValue(res)
	}

	r := t.db.QueryRowx("SELECT id FROM nodes WHERE name=$1;", name)
	err := r.Scan(&res)
	if err != nil {
		if err != sql.ErrNoRows {
			glog.Fatalln(err.Error())
		}
	} else {
		t.idCache.Put(res, name)
	}
	return NodeValue(res)
}

// Given an opaque token, return the node that it represents.
func (t *QuadStore) NameOf(oid graph.Value) (res string) {
	var nid int64
	switch v := oid.(type) {
	case int64:
		nid = v
	case NodeValue:
		nid = int64(v)
	}

	val, ok := t.idCache.Get(nid)
	if ok {
		return val
	}

	r := t.db.QueryRowx("SELECT name FROM nodes WHERE id=$1;", nid)
	err := r.Scan(&res)
	if err != nil {
		if err != sql.ErrNoRows {
			glog.Fatalln(err.Error())
		}
	} else {
		t.idCache.Put(nid, res)
	}
	return
}

// Returns the number of quads currently stored.
func (t *QuadStore) Size() (res int64) {
	r := t.db.QueryRowx("SELECT COUNT(*) FROM quads;")
	err := r.Scan(&res)
	if err != nil {
		glog.Fatalln(err.Error())
	}
	return
}

// Close the quad store and clean up. (Flush to disk, cleanly
// sever connections, etc)
func (t *QuadStore) Close() {
	t.db.Close()
}

// Convienence function for speed. Given a quad token and a direction
// return the node token for that direction. Sometimes, a QuadStore
// can do this without going all the way to the backing store, and
// gives the QuadStore the opportunity to make this optimization.
func (t *QuadStore) QuadDirection(quad_id graph.Value, dir graph.Direction) graph.Value {
	qv := quad_id.(QuadValue)
	switch dir {
	case graph.Subject:
		return qv[1]
	case graph.Predicate:
		return qv[2]
	case graph.Object:
		return qv[3]
	case graph.Provenance:
		return qv[4]
	}
	return qv[0]
}

var postgresType graph.Type
var postgresAllType graph.Type
var postgresNodeType graph.Type

func init() {
	postgresType = graph.RegisterIterator("postgres")
	postgresAllType = graph.RegisterIterator("postgres-all-nodes")
	postgresNodeType = graph.RegisterIterator("postgres-nodes")

	graph.RegisterQuadStore("postgres", newQuadStore, createNewPostgresGraph)
}

func dirToSchema(dir graph.Direction) string {
	switch dir {
	case graph.Subject:
		return "subj"
	case graph.Predicate:
		return "pred"
	case graph.Object:
		return "obj"
	case graph.Provenance:
		return "prov"
	}
	return "null"
}
