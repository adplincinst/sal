package main

import (
	"compress/gzip"
	"context"
	"fmt"
	"log"
	"os"
	"path/filepath"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/iceberg-go"
	"github.com/apache/iceberg-go/catalog"
	"github.com/apache/iceberg-go/catalog/hadoop"
	_ "github.com/apache/iceberg-go/catalog/hadoop"
	"github.com/apache/iceberg-go/table"
	rdf "github.com/tggo/goRDFlib"
	"github.com/tggo/goRDFlib/nq"
)

const batchSize = 10_000

func main() {
	if len(os.Args) < 2 {
		log.Fatal("Usage: loader <path-to-nq-gz-directory>")
	}
	inputDir := os.Args[1]

	ctx := context.Background()

	if err := os.MkdirAll("/tmp/iceberg-warehouse/default", 0755); err != nil {
		log.Fatal("Failed to create warehouse directory:", err)
	}

	cat, err := hadoop.NewCatalog("local-catalog", "/tmp/iceberg-warehouse", nil)
	if err != nil {
		log.Fatal("Failed to create catalog:", err)
	}

	defaultNS := catalog.ToIdentifier("default")
	if err := cat.CreateNamespace(ctx, defaultNS, nil); err != nil &&
		err.Error() != "namespace already exists: default" {
		log.Fatal("Failed to create default namespace:", err)
	}
	log.Println("Namespace ready")

	icebergSchema := iceberg.NewSchemaWithIdentifiers(1, []int{3},
		iceberg.NestedField{ID: 1, Name: "subject", Type: iceberg.PrimitiveTypes.String, Required: true},
		iceberg.NestedField{ID: 2, Name: "predicate", Type: iceberg.PrimitiveTypes.String, Required: true},
		iceberg.NestedField{ID: 3, Name: "object", Type: iceberg.PrimitiveTypes.String, Required: true},
	)

	tableIdent := catalog.ToIdentifier("default", "triples")
	tbl, err := cat.CreateTable(ctx, tableIdent, icebergSchema,
		catalog.WithProperties(map[string]string{"owner": "me"}),
	)
	if err != nil {
		log.Print("Failed to create table:", err)
	}
	log.Println("Table created successfully")

	arrowSchema := arrow.NewSchema(
		[]arrow.Field{
			{Name: "subject", Type: arrow.BinaryTypes.String},
			{Name: "predicate", Type: arrow.BinaryTypes.String},
			{Name: "object", Type: arrow.BinaryTypes.String},
		},
		nil,
	)

	pattern := filepath.Join(inputDir, "*.nq.gz")
	files, err := filepath.Glob(pattern)
	if err != nil {
		log.Fatal("Glob error:", err)
	}
	if len(files) == 0 {
		log.Fatalf("No .nq.gz files found in %s", inputDir)
	}
	log.Printf("Found %d .nq.gz file(s)", len(files))

	for _, fpath := range files {
		log.Printf("Processing: %s", fpath)
		if err := processFile(ctx, fpath, cat, tableIdent, arrowSchema); err != nil {
			log.Fatalf("Error processing %s: %v", fpath, err)
		}
	}

	log.Println("All files loaded successfully.")
	log.Println("Table location:", tbl.Location())
}

// processFile streams a single .nq.gz file into the Iceberg table in batches.
// It reloads the table from the catalog before every flush so that each Append
// sees a fresh snapshot and avoids "branch was created concurrently" conflicts.
func processFile(
	ctx context.Context,
	fpath string,
	cat catalog.Catalog,
	tableIdent table.Identifier,
	arrowSchema *arrow.Schema,
) error {
	f, err := os.Open(fpath)
	if err != nil {
		return fmt.Errorf("open: %w", err)
	}
	defer f.Close()

	gz, err := gzip.NewReader(f)
	if err != nil {
		return fmt.Errorf("gzip: %w", err)
	}
	defer gz.Close()

	pool := memory.NewGoAllocator()

	// tripleC receives parsed triples from the QuadHandler goroutine.
	type triple struct{ s, p, o string }
	tripleC := make(chan triple, batchSize)

	// Parse in background; the QuadHandler feeds tripleC.
	parseErr := make(chan error, 1)
	go func() {
		g := rdf.NewGraph()
		err := nq.Parse(g, gz,
			nq.WithQuadHandler(
				func(s rdf.Subject, p rdf.URIRef, o rdf.Term, _ rdf.Term) {
					tripleC <- triple{
						s: s.String(),
						p: p.String(),
						o: o.String(),
					}
				},
			),
			nq.WithErrorHandler(func(lineNum int, line string, err error) (string, bool) {
				// Skip lines that are too long (or any other parse error)
				log.Printf("  skipping line %d: %v", lineNum, err)
				return "", false // false = don't retry
			}),
		)
		close(tripleC)
		parseErr <- nil
		if err != nil {
			log.Printf("Parse error: %v", err)
		}
	}()

	// Drain tripleC in batches and write each batch to Iceberg.
	buf := make([]triple, 0, batchSize)

	flush := func() error {
		if len(buf) == 0 {
			return nil
		}

		b := array.NewRecordBuilder(pool, arrowSchema)
		defer b.Release()

		subjects := make([]string, len(buf))
		predicates := make([]string, len(buf))
		objects := make([]string, len(buf))
		for i, t := range buf {
			subjects[i] = t.s
			predicates[i] = t.p
			objects[i] = t.o
		}
		b.Field(0).(*array.StringBuilder).AppendValues(subjects, nil)
		b.Field(1).(*array.StringBuilder).AppendValues(predicates, nil)
		b.Field(2).(*array.StringBuilder).AppendValues(objects, nil)

		rec := b.NewRecordBatch()
		defer rec.Release()

		itr, err := array.NewRecordReader(arrowSchema, []arrow.RecordBatch{rec})
		if err != nil {
			return fmt.Errorf("record reader: %w", err)
		}
		defer itr.Release()

		// Reload the table so Append sees the latest committed snapshot.
		tbl, err := cat.LoadTable(ctx, tableIdent)
		if err != nil {
			return fmt.Errorf("reload table: %w", err)
		}

		if _, err = tbl.Append(ctx, itr, iceberg.Properties(nil)); err != nil {
			return fmt.Errorf("iceberg append: %w", err)
		}

		log.Printf("  flushed %d triples", len(buf))
		buf = buf[:0]
		return nil
	}

	for t := range tripleC {
		buf = append(buf, t)
		if len(buf) >= batchSize {
			err := flush()
			if err != nil {
				return err
			}
		}
	}
	// Flush any remaining triples.
	if err := flush(); err != nil {
		return err
	}

	// Check parse error from goroutine.
	if err := <-parseErr; err != nil {
		return fmt.Errorf("parse: %w", err)
	}
	return nil
}
