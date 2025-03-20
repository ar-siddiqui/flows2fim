package main

import (
	"fmt"
	"log/slog"
	"os"
	"time"

	"flows2fim/cmd/controls"
	"flows2fim/cmd/fim"
	"flows2fim/cmd/validate"
	"flows2fim/internal/config"

	"github.com/lmittmann/tint"
	"github.com/mattn/go-isatty"
)

var usage string = `Usage of flows2fim:
	flows2fim [--version | --help]
	flows2fim COMMAND --help
	flows2fim COMMAND Args

Available Commands:
  - controls: Given a flow file and a rating curves database, create a control table of reach flows and downstream boundary conditions.
  - fim: Given a control table and a fim library folder, create a flood inundation map for the control conditions.
  - validate: Given a fim library folder and a rating curves database, validate there is one to one correspondence between the entries of rating curves table and fim library objects.

Dependencies:
  - GDAL must be installed and available in the PATH.

Env Variables:
  - F2F_LOG_LEVEL: Set the logging level. Options are 'DEBUG', 'INFO', 'WARN', and 'ERROR'. Default is 'INFO'.
  - F2F_NO_COLOR: Set to 'TRUE' to disable colored output. Default is 'FALSE'.

CLI Flag Syntax:
The following forms are permitted:
	-flag
	--flag   // double dashes are also permitted
	-flag=x
	-flag x  // non-boolean flags only
`

var (
	GitTag    = "unknown" // will be injected at build-time
	GitCommit = "unknown" // will be injected at build-time
	BuildDate = "unknown" // will be injected at build-time
)

// run handles the main logic and returns an error if something goes wrong.
func run(args []string) (err error) {

	if len(args) < 2 {
		return fmt.Errorf("missing command. See flows2fim --help for available commands")
	}

	switch args[1] {
	case "-v", "--v", "-version", "--version":
		fmt.Println("Software: flows2fim")
		fmt.Println("Version:", GitTag)
		fmt.Println("Commit:", GitCommit)
		fmt.Println("Build Date:", BuildDate)
	case "-h", "--h", "-help", "--help":
		fmt.Print(usage)
	case "controls":
		err = controls.Run(args[2:])
	case "fim":
		_, err = fim.Run(args[2:])
	case "validate":
		err = validate.Run(args[2:])
	default:
		err = fmt.Errorf("unknown command %s see flows2fim --help for available commands", args[1])
	}

	return err
}

func main() {
	config.LoadConfig()

	w := os.Stderr
	logger := slog.New(tint.NewHandler(w, &tint.Options{
		Level:      config.LogLevel(),
		TimeFormat: time.Kitchen,
		NoColor:    !isatty.IsTerminal(w.Fd()) || config.NoColor(),
	}))
	slog.SetDefault(logger)

	if err := run(os.Args); err != nil {
		fmt.Fprint(os.Stderr, err, "\n")
		os.Exit(1)
	}
	os.Exit(0)
}
