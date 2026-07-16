package clean

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"log/slog"
	"os"
	"path/filepath"
	"strings"

	"github.com/apache/iceberg-go/catalog"
	"github.com/cgs-earth/sal/pkg"
	"oras.land/oras-go/v2/registry/remote"
)

func confirm(prompt string) (bool, error) {

	pkg.Warnf("%s [y/N]", prompt)

	var input string
	if _, err := fmt.Scanln(&input); err != nil {
		// If user just presses enter, Scanln errors → treat as "no"
		return false, nil
	}

	input = strings.ToLower(strings.TrimSpace(input))
	return input == "y" || input == "yes", nil
}

type CleanCmd struct {
	Wipe     bool   `arg:"--wipe,-w" help:"Wipe the entire data product. Useful for debugging and testing purposes"`
	Username string `arg:"--username,env:OCI_USERNAME" help:"Username for the OCI registry"`
	Password string `arg:"--password,env:OCI_PASSWORD" help:"Password for the OCI registry"`
	Artifact string `arg:"positional" help:"Full URL of the OCI artifact to diff against. Example: ghcr.io/my-username/my-repository:latest"`
}

func (cmd *CleanCmd) GetUsername() string {
	return cmd.Username
}

func (cmd *CleanCmd) GetPassword() string {
	return cmd.Password
}

func (cmd *CleanCmd) GetArtifactReference() (pkg.ArtifactReference, error) {
	return pkg.ParseArtifact(cmd.Artifact)
}

func wipe() error {
	ok, err := confirm("This will permanently delete the entire data product. Continue?")
	if err != nil {
		return err
	}

	if !ok {
		slog.Info("Wipe cancelled")
		return nil
	}

	dataProductPath, err := pkg.SalBuiltDataProductPath()
	if err != nil {
		return err
	}

	var totalBytes int64

	err = filepath.WalkDir(dataProductPath, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}

		if d.IsDir() {
			return nil
		}

		info, err := d.Info()
		if err != nil {
			return err
		}

		totalBytes += info.Size()
		return nil
	})

	if errors.Is(err, os.ErrNotExist) {
		slog.Warn("No data product to clean at " + dataProductPath)
		return nil
	}
	if err != nil {
		return err
	}

	if err := os.RemoveAll(dataProductPath); err != nil {
		return err
	}

	msg := fmt.Sprintf("Removed %s of data artifacts from %s", pkg.BytesToHumanReadable(totalBytes), dataProductPath)

	slog.Info(msg)

	return nil
}

func cleanDiff(cmd *CleanCmd) error {
	ctx := context.Background()

	ref, err := cmd.GetArtifactReference()
	if err != nil {
		return err
	}

	repo, err := remote.NewRepository(ref.Repository)
	if err != nil {
		return fmt.Errorf("failed creating OCI registry client: %w", err)
	}

	repo.Client = pkg.NewOciClientWithOptionalAuth(cmd, ref)

	_, manifest, err := pkg.FetchManifest(ctx, repo, ref.Reference)
	if err != nil {
		return err
	}

	remoteSnapshots, err := pkg.GetSnapshotsFromManifest(manifest)
	if err != nil {
		return fmt.Errorf("error getting snapshot data from %s %w", cmd.Artifact, err)
	}

	localSnapshots, err := pkg.GetLocalSalSnapshots()
	// if the error is that the table just doesn't exist yet, that is
	// ok since it will be created upon pull
	if errors.Is(err, catalog.ErrNoSuchTable) {
		slog.Warn("No SAL data product to clean")
		return nil
	} else if err != nil {
		return err
	}

	diff, _ := pkg.SnapshotDiff(localSnapshots, remoteSnapshots)

	if len(diff.SnapshotsInLocalNotRemote) > 0 {
		msg := fmt.Sprintf("Found %d snapshot(s) in local but not remote: %s. Delete them permanently? [y/N]", len(diff.SnapshotsInLocalNotRemote), strings.Join(diff.SnapshotsInLocalNotRemote, ", "))
		ok, err := confirm(msg)
		if err != nil {
			return err
		}

		if !ok {
			slog.Info("Wipe cancelled")
			return nil
		}
	} else {
		slog.Info("Nothing to clean; local table is up to date")
	}

	return nil

}

func (cmd *CleanCmd) Run() error {

	if cmd.Wipe {
		return wipe()
	} else {
		return cleanDiff(cmd)
	}
}
