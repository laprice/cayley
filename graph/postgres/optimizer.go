package postgres

import (
	"github.com/google/cayley/graph"
	"github.com/google/cayley/graph/iterator"
	"strings"

	//"fmt"
)

func wrapIt(newIt, oldIt graph.Iterator) (graph.Iterator, bool) {
	newIt.CopyTagsFrom(oldIt)
	oldIt.Close()
	return newIt, true
}

func (ts *TripleStore) OptimizeIterator(it graph.Iterator) (graph.Iterator, bool) {
	switch it.Type() {
	case graph.And:
		if newIt, ok := ts.optimizeAnd(it.(*iterator.And)); ok {
			return wrapIt(newIt, it)
		}

	case graph.HasA:
		hasa := it.(*iterator.HasA)
		l2 := hasa.SubIterators()

		newSub, changed := ts.OptimizeIterator(l2[0])
		if changed {
			if newSub.Type() == postgresType {
				newNodeIt := NewNodeIteratorWhere(ts, hasa.Direction(), newSub.(*TripleIterator).sqlClause())
				return wrapIt(newNodeIt, hasa)
			}
			newIt := iterator.NewHasA(ts, newSub, hasa.Direction())
			return wrapIt(newIt, hasa)
		}

	case graph.LinksTo:
		linksTo := it.(*iterator.LinksTo)
		l := linksTo.SubIterators()

		if len(l) != 1 {
			panic("invalid linksto iterator")
		}
		sublink := l[0]

		if sublink.Type() == graph.Fixed {
			size, _ := sublink.Size()
			if size == 1 {
				val, ok := sublink.Next()
				if !ok {
					panic("Sizes lie")
				}
				newIt := ts.TripleIterator(linksTo.Direction(), val)
				for _, tag := range sublink.Tags() {
					newIt.AddFixedTag(tag, val)
				}
				return wrapIt(newIt, linksTo)
			}
			// can't help help here
			return it, false
		}

		newIt, ok := ts.OptimizeIterator(sublink)
		if ok {

			pgIter, isPgIter := newIt.(*NodeIterator)
			if isPgIter && pgIter.dir != graph.Any {
				// SELECT * FROM triples WHERE <linksto.direction> IN (SELECT <pgIter.dir> FROM triples WHERE <pgIter.sqlWhere>)
				newWhere := dirToSchema(linksTo.Direction()) + " IN ("
				newWhere += pgIter.sqlQuery + ")"
				return wrapIt(NewTripleIteratorWhere(ts, newWhere), linksTo)
				/*
					} else {
						if newIt.Type() == graph.HasA {
							pgIter, isTrip := newIt.SubIterators()[0].(*TripleIterator)
							if isTrip {
								// SELECT * FROM triples WHERE <linksto.direction> IN (SELECT <hasa.direction> FROM triples WHERE <pgIter.sqlClause>)
								newWhere := dirToSchema(linksTo.Direction()) + " IN (SELECT "
								newWhere += dirToSchema(newIt.(*iterator.HasA).Direction())
								newWhere += " FROM triples WHERE " + pgIter.sqlClause() + ")"
								return wrapIt(NewTripleIteratorWhere(ts, newWhere), linksTo)
							}
						}*/
			}

			newLt := iterator.NewLinksTo(ts, newIt, linksTo.Direction())
			return wrapIt(newLt, linksTo)

		}
	}
	return it, false
}

// If we're ANDing multiple postgres iterators, do it in postgres and use the indexes
func (ts *TripleStore) optimizeAnd(and *iterator.And) (graph.Iterator, bool) {
	allPostgres := true
	types := 0
	nDir := graph.Any

	subQueries := []string{}
	subits := and.SubIterators()
	for _, it := range subits {
		if it.Type() == postgresAllType || it.Type() == graph.All {
			continue
		}
		if it.Type() == postgresType {
			types |= 1
			pit := it.(*TripleIterator)
			subQueries = append(subQueries, pit.sqlClause())
			continue
		}
		if it.Type() == postgresNodeType {

			pit := it.(*NodeIterator)
			if pit.dir == graph.Any {
				panic("invalid postgresNodeType iterator")
			}
			if types != 0 || (types == 2 && nDir != pit.dir) {
				// invalid!
				return and, false
			}
			types |= 2
			nDir = pit.dir
			subQueries = append(subQueries, pit.sqlWhere)
			continue
		}

		allPostgres = false
		break
	}

	if allPostgres {
		if types == 1 {
			return NewTripleIteratorWhere(ts, strings.Join(subQueries, " AND ")), true
		}
		if types == 2 {
			return NewNodeIteratorWhere(ts, nDir, strings.Join(subQueries, " AND ")), true
		}
	}

	newIts := make([]graph.Iterator, len(subits))
	didChange := false
	for i, sub := range subits {
		it, changed := ts.OptimizeIterator(sub)
		if changed {
			didChange = true
			newIts[i] = it
		} else {
			newIts[i] = sub.Clone()
		}
	}
	if didChange {
		for _, sub := range subits {
			sub.Close()
		}
		newIt := iterator.NewAnd()
		allPostgres = true
		for _, newit := range newIts {
			nt := newit.Type()
			if !(nt == postgresType || nt == postgresAllType || nt == postgresNodeType) {
				allPostgres = false
			}
			newIt.AddSubIterator(newit)
		}
		if allPostgres {
			newIt2, _ := ts.optimizeAnd(newIt)
			return newIt2, true
		}
		return newIt, true
	}
	return and, false
}
