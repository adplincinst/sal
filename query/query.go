package query

import (
	"fmt"
	"log/slog"
	"os"
	"os/exec"
	"path"
	"strings"

	"github.com/cgs-earth/sal/pkg"
)

type QueryCmd struct{}

func Run(_ *QueryCmd) error {
	warehouse, err := pkg.SalDataDir()
	if err != nil {
		return err
	}
	entries, err := os.ReadDir(warehouse)
	if err != nil {
		return fmt.Errorf("failed to read SAL data directory: %w", err)
	}
	if len(entries) == 0 {
		return fmt.Errorf("no data has been built yet; run `sal build` to build a data product first")
	}

	namespace, err := pkg.GitProjectName()
	if err != nil {
		return err
	}

	tablePath := joinRemote(warehouse, namespace, "triples")
	escapedTablePath := strings.ReplaceAll(tablePath, "'", "''")

	tmp, err := os.CreateTemp("", "sal-duckdb-*.sql")
	if err != nil {
		return fmt.Errorf("failed to create duckdb init file: %w", err)
	}
	defer func() {
		err = os.Remove(tmp.Name())
		if err != nil {
			slog.Error(err.Error())
		}
	}()

	_, err = fmt.Fprintf(tmp, `
INSTALL iceberg;
LOAD iceberg;

CREATE OR REPLACE VIEW triples AS
SELECT *
FROM iceberg_scan('%s', allow_moved_paths = true);

.mode box

SELECT * FROM triples LIMIT 20;

.print ''
.print 'Connected to Iceberg table as view: triples'
.print 'You can now query it, e.g.:'
.print '  SELECT * FROM triples LIMIT 10;'
.print ''
`, escapedTablePath)
	if err != nil {
		_ = tmp.Close()
		return fmt.Errorf("failed to write duckdb init file: %w", err)
	}

	if err := tmp.Close(); err != nil {
		return fmt.Errorf("failed to close duckdb init file: %w", err)
	}

	duck := exec.Command("duckdb", "-init", tmp.Name())
	duck.Stdin = os.Stdin
	duck.Stdout = os.Stdout
	duck.Stderr = os.Stderr

	if err := duck.Run(); err != nil {
		return fmt.Errorf("failed to open duckdb shell: %w", err)
	}

	return nil
}

func joinRemote(base string, parts ...string) string {
	joined := path.Join(parts...)
	if joined == "." {
		return trimTrailingSlash(base)
	}
	return trimTrailingSlash(base) + "/" + joined
}

func trimTrailingSlash(s string) string {
	for len(s) > 0 && s[len(s)-1] == '/' {
		s = s[:len(s)-1]
	}
	return s
}
