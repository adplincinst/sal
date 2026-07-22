package sparql

import (
	"context"
	"fmt"

	"github.com/apache/iceberg-go/catalog/hadoop"
	"github.com/apache/iceberg-go/table"
)

// ObjectLayoutForTable inspects the triples table schema to select the matching SPARQL object column layout.
func ObjectLayoutForTable(ctx context.Context, warehouse string, namespace string) (ObjectLayout, error) {
	cat, err := hadoop.NewCatalog("local-catalog", warehouse, nil)
	if err != nil {
		return SimpleObjects, fmt.Errorf("failed to create catalog: %w", err)
	}
	tbl, err := cat.LoadTable(ctx, table.Identifier{namespace, "triples"})
	if err != nil {
		return SimpleObjects, fmt.Errorf("load table: %w", err)
	}
	for _, field := range tbl.Schema().Fields() {
		if field.Name == "object_string" {
			return TypedObjects, nil
		}
	}
	return SimpleObjects, nil
}
