package main

import (
	"log/slog"
	"os"
	"time"

	"github.com/cgs-earth/sal/build"
	"github.com/cgs-earth/sal/initialization"
	"github.com/cgs-earth/sal/load"

	"github.com/alexflint/go-arg"
	"github.com/lmittmann/tint"
)

// All subcommands that sal supports. These should be in a useful order as
// the order changes how the CLI presents them in the help message.
type args struct {
	Init  *initialization.InitCmd `arg:"subcommand:init" help:"Initialize a SAL project."`
	Load  *load.LoadCmd           `arg:"subcommand:load" help:"Load N-Quads gzip files into a local Iceberg triples table."`
	Build *build.BuildCmd         `arg:"subcommand:build" help:"Build a vocabulary."`
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

	if len(os.Args) == 1 {
		os.Args = append(os.Args, "--help")
	}

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
