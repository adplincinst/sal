package build

import (
	"bytes"

	rdflibgo "github.com/tggo/goRDFlib"
	"github.com/tggo/goRDFlib/rdfxml"
)

func extractRDFXMLVocabularyTerms(body []byte, base string) (map[string]bool, error) {
	g := rdflibgo.NewGraph(rdflibgo.WithBase(base))
	if err := rdfxml.Parse(g, bytes.NewReader(body), rdfxml.WithBase(base)); err != nil {
		return nil, err
	}

	terms := map[string]bool{}
	g.Namespaces()(func(_ string, ns rdflibgo.URIRef) bool {
		if ns.Value() != "" {
			terms[ns.Value()] = true
		}
		return true
	})
	g.Triples(nil, nil, nil)(func(triple rdflibgo.Triple) bool {
		if subj, ok := triple.Subject.(rdflibgo.URIRef); ok {
			terms[subj.Value()] = true
		}
		terms[triple.Predicate.Value()] = true
		if obj, ok := triple.Object.(rdflibgo.URIRef); ok {
			terms[obj.Value()] = true
		}
		if lit, ok := triple.Object.(rdflibgo.Literal); ok {
			terms[lit.Datatype().Value()] = true
		}
		return true
	})
	return terms, nil
}
