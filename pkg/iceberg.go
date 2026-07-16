package pkg

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"

	"github.com/apache/iceberg-go/catalog"
	"github.com/apache/iceberg-go/catalog/hadoop"
	"github.com/apache/iceberg-go/table"
)

const DefaultSalIcebergTable = "triples"

func SalIcebergCatalog() (catalog.Catalog, error) {
	dataDir, err := SalDataDir()
	if err != nil {
		return nil, err
	}
	cat, err := hadoop.NewCatalog("local-catalog", dataDir, nil)
	return cat, err
}

func GetSalIcebergTable() (*table.Table, error) {
	cat, err := SalIcebergCatalog()
	if err != nil {
		return nil, err
	}
	gitProjectName, err := GitProjectName()
	if err != nil {
		return nil, err
	}
	dataDir, err := SalDataDir()
	if err != nil {
		return nil, err
	}
	if err := os.MkdirAll(dataDir+"/"+gitProjectName, 0755); err != nil {
		slog.Error("Failed to create warehouse directory:", "error", err)
		return nil, err
	}

	ctx := context.Background()
	defaultNS := catalog.ToIdentifier(gitProjectName)
	if err := cat.CreateNamespace(ctx, defaultNS, nil); err != nil &&
		!errors.Is(err, catalog.ErrNamespaceAlreadyExists) {
		slog.Error("Failed to create default namespace:", "error", err)
		return nil, err
	}

	tableIdent := catalog.ToIdentifier(gitProjectName, DefaultSalIcebergTable)
	return cat.LoadTable(ctx, tableIdent)
}

func SetTagOfLatestSnapshot(tbl *table.Table, cat catalog.Catalog) error {
	newSnapshot := tbl.CurrentSnapshot()
	if newSnapshot == nil {
		return fmt.Errorf("failed to get latest snapshot")
	}

	latestGitHash, err := GitCommitHash()
	if err != nil {
		return err
	}

	update := table.NewSetSnapshotRefUpdate(
		latestGitHash,
		newSnapshot.SnapshotID,
		table.TagRef,
		0,
		0,
		0,
	)

	_, _, err = cat.CommitTable(context.Background(), tbl.Identifier(), nil, []table.Update{update})
	return err
}
