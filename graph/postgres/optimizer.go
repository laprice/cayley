package postgres

import (
	"github.com/google/cayley/graph"
	"github.com/google/cayley/graph/iterator"
	"strings"
)

func (qs *QuadStore) OptimizeIterator(it graph.Iterator) (graph.Iterator, bool) {
	newIt, didChange := qs.recOptimizeIterator(it)
	if didChange {
		newIt.CopyTagsFrom(it)
		it.Close()
		return newIt, true
	}
	return it, false
}

func (qs *QuadStore) recOptimizeIterator(it graph.Iterator) (graph.Iterator, bool) {
	switch it.Type() {
	case graph.And:

		newIt, ok := qs.optimizeAnd(it.(*iterator.And))
		if ok {
			for ok && newIt.Type() == graph.And {
				newIt, ok = qs.optimizeAnd(newIt.(*iterator.And))
			}
			return newIt, true
		}
		return it, false

	case graph.HasA:
		hasa := it.(*iterator.HasA)
		l2 := hasa.SubIterators()

		newSub, changed := qs.recOptimizeIterator(l2[0])
		if changed {
			l2[0] = newSub
		}
		newSub = l2[0]
		if newSub.Type() == postgresType {
			newNodeIt := NewNodeIteratorWhere(qs, hasa.Direction(), newSub.(*QuadIterator).sqlClause())
			return newNodeIt, true
		}
		if changed {
			newIt := iterator.NewHasA(qs, newSub, hasa.Direction())
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
			return qs.QuadsAllIterator(), true
		}

		if sublink.Type() == graph.Fixed {
			size, _ := sublink.Size()
			if size == 1 {
				val, ok := sublink.Next()
				if !ok {
					panic("Sizes lie")
				}
				newIt := qs.QuadIterator(linksTo.Direction(), val)
				for _, tag := range sublink.Tags() {
					newIt.AddFixedTag(tag, val)
				}
				return newIt, true
			}
			// can't help help here
			return it, false
		}

		newIt, ok := qs.recOptimizeIterator(sublink)
		if ok {
			pgIter, isPgIter := newIt.(*NodeIterator)
			if isPgIter && pgIter.dir != graph.Any {
				// SELECT * FROM triples WHERE <linksto.direction> IN (SELECT <pgIter.dir> FROM triples WHERE <pgIter.sqlWhere>)
				newWhere := dirToSchema(linksTo.Direction()) + " IN ("
				newWhere += pgIter.sqlQuery + ")"
				return NewQuadIteratorWhere(qs, newWhere), true
			}

			return iterator.NewLinksTo(qs, newIt, linksTo.Direction()), true
		}
	}
	return it, false
}

// If we're ANDing multiple postgres iterators, do it in postgres and use the indexes
func (qs *QuadStore) optimizeAnd(and *iterator.And) (graph.Iterator, bool) {
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
			pit := it.(*QuadIterator)
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
			return NewQuadIteratorWhere(qs, strings.Join(subQueries, " AND ")), true
		}
		if types == 2 {
			return NewNodeIteratorWhere(qs, nDir, strings.Join(subQueries, " AND ")), true
		}
	}

	newIts := make([]graph.Iterator, len(subits))
	didChange := false
	for i, sub := range subits {
		it, changed := qs.recOptimizeIterator(sub)
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
