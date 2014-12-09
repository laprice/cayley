package postgres

import (
	"github.com/google/cayley/graph"
	"github.com/google/cayley/graph/iterator"
	"strings"
)

func (ts *TripleStore) OptimizeIterator(it graph.Iterator) (graph.Iterator, bool) {
	newIt, didChange := ts.recOptimizeIterator(it)
	if didChange {
		newIt.CopyTagsFrom(it)
		it.Close()
		return newIt, true
	}
	return it, false
}

func (ts *TripleStore) recOptimizeIterator(it graph.Iterator) (graph.Iterator, bool) {
	switch it.Type() {
	case graph.And:

		newIt, ok := ts.optimizeAnd(it.(*iterator.And))
		if ok {
			for ok && newIt.Type() == graph.And {
				newIt, ok = ts.optimizeAnd(newIt.(*iterator.And))
			}
			return newIt, true
		}
		return it, false

	case graph.HasA:
		hasa := it.(*iterator.HasA)
		l2 := hasa.SubIterators()

		newSub, changed := ts.recOptimizeIterator(l2[0])
		if changed {
			l2[0] = newSub
		}
		newSub = l2[0]
		if newSub.Type() == postgresType {
			newNodeIt := NewNodeIteratorWhere(ts, hasa.Direction(), newSub.(*TripleIterator).sqlClause())
			return newNodeIt, true
		}
		if changed {
			newIt := iterator.NewHasA(ts, newSub, hasa.Direction())
			return newIt, true
		}
		return it, false

	case graph.LinksTo:
		linksTo := it.(*iterator.LinksTo)
		l := linksTo.SubIterators()

		if len(l) != 1 {
			panic("invalid linksto iterator")
		}
		sublink := l[0]

		// linksto all nodes? sure...
		if sublink.Type() == postgresAllType {
			linksTo.Close()
			return ts.TriplesAllIterator(), true
		}

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
				return newIt, true
			}
			// can't help help here
			return it, false
		}

		newIt, ok := ts.recOptimizeIterator(sublink)
		if ok {
			pgIter, isPgIter := newIt.(*NodeIterator)
			if isPgIter && pgIter.dir != graph.Any {
				// SELECT * FROM triples WHERE <linksto.direction> IN (SELECT <pgIter.dir> FROM triples WHERE <pgIter.sqlWhere>)
				newWhere := dirToSchema(linksTo.Direction()) + " IN ("
				newWhere += pgIter.sqlQuery + ")"
				return NewTripleIteratorWhere(ts, newWhere), true
			}

			return iterator.NewLinksTo(ts, newIt, linksTo.Direction()), true
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
			if types == 1 || (types == 2 && nDir != pit.dir) {
				//FIXME "cross-direction node join not implemented yet"
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
		it, changed := ts.recOptimizeIterator(sub)
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
		for _, newit := range newIts {
			newIt.AddSubIterator(newit)
		}
		return newIt, true
	}
	return and, false
}
