package main

import (
	"context"
	"log"
	"os"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/iceberg-go"
	"github.com/apache/iceberg-go/catalog"
	"github.com/apache/iceberg-go/catalog/hadoop"
	_ "github.com/apache/iceberg-go/catalog/hadoop"
)

func main() {
	ctx := context.Background()

	err := os.Mkdir("/tmp/iceberg-warehouse/default", 0755)
	if err != nil {
		log.Fatal("Failed to create directory:", err)
	}

	cat, err := hadoop.NewCatalog("local-catalog", "/tmp/iceberg-warehouse", nil)
	if err != nil {
		log.Fatal("Failed to create catalog:", err)
	}

	// Create default namespace first (if it doesn't exist)
	defaultNS := catalog.ToIdentifier("default")
	err = cat.CreateNamespace(ctx, defaultNS, nil)
	if err != nil && err.Error() != "namespace already exists: default" {
		log.Fatal("Failed to create default namespace:", err)
	}
	log.Println("Namespace ready")

	schema := iceberg.NewSchemaWithIdentifiers(1, []int{3},
		iceberg.NestedField{ID: 1, Name: "subject", Type: iceberg.PrimitiveTypes.String, Required: true},
		iceberg.NestedField{ID: 2, Name: "predicate", Type: iceberg.PrimitiveTypes.String, Required: true},
		iceberg.NestedField{ID: 3, Name: "object", Type: iceberg.PrimitiveTypes.String, Required: true},
	)

	// Create a table identifier in the default namespace
	tableIdent := catalog.ToIdentifier("default", "my_table")

	// Create a table with optional properties
	tbl, err := cat.CreateTable(
		ctx,
		tableIdent,
		schema,
		catalog.WithProperties(map[string]string{"owner": "me"}),
	)
	if err != nil {
		log.Fatal("Failed to create table:", err)
	}
	log.Println("Table created successfully")

	pool := memory.NewGoAllocator()

	arrow_schema := arrow.NewSchema(
		[]arrow.Field{
			{Name: "subject", Type: arrow.BinaryTypes.String},
			{Name: "predicate", Type: arrow.BinaryTypes.String},
			{Name: "object", Type: arrow.BinaryTypes.String},
		},
		nil,
	)

	b := array.NewRecordBuilder(pool, arrow_schema)
	defer b.Release()

	b.Field(0).(*array.StringBuilder).AppendValues([]string{"subject1", "subject2", "subject3", "subject4", "subject5", "subject6"}, nil)
	b.Field(1).(*array.StringBuilder).AppendValues([]string{"predicate1", "predicate2", "predicate3", "predicate4", "predicate5", "predicate6"}, nil)
	b.Field(2).(*array.StringBuilder).AppendValues([]string{"object1", "object2", "object3", "object4", "object5", "object6"}, nil)

	rec1 := b.NewRecordBatch()
	defer rec1.Release()

	itr, err := array.NewRecordReader(arrow_schema, []arrow.RecordBatch{rec1})
	if err != nil {
		log.Fatal(err)
	}
	defer itr.Release()

	// Append using a RecordReader (streaming)
	_, err = tbl.Append(ctx, itr, nil)
	if err != nil {
		log.Fatal("Failed to append data:", err)
	}

	log.Println("Successfully wrote data to local Iceberg table!")
	log.Println("Table location:", tbl.Location())
}
