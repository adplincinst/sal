package main

import (
	"log/slog"
	"os"
	"time"

	"github.com/cgs-earth/sal-cli/build"
	"github.com/cgs-earth/sal-cli/initialization"
	"github.com/cgs-earth/sal-cli/load"

	"github.com/alexflint/go-arg"
	"github.com/lmittmann/tint"
)

type args struct {
	Load  *load.LoadCmd           `arg:"subcommand:load" help:"Load N-Quads gzip files into a local Iceberg triples table."`
	Build *build.BuildCmd         `arg:"subcommand:build" help:"Build a vocabulary."`
	Init  *initialization.InitCmd `arg:"subcommand:init" help:"Initialize a SAL project."`
}

func (args) Description() string {
	return "Validate and process RDF data"
}

func main() {
	slog.SetDefault(slog.New(
		tint.NewHandler(os.Stderr, &tint.Options{
			Level:      slog.LevelDebug,
			TimeFormat: time.Kitchen,
		}),
	))

	var cli args
	arg.MustParse(&cli)
	var err error
	switch {
	case cli.Build != nil:
		// todo change this to an error var
		build.Run(cli.Build, os.Stdout, os.Stderr)
	case cli.Load != nil:
		load.Run(cli.Load)
	case cli.Init != nil:
		err = initialization.Run(cli.Init)
	}
	if err != nil {
		slog.Error(err.Error())
		os.Exit(1)
	}
}
