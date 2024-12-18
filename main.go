package main

import (
	"fmt"
	"os"

	"flows2fim/cmd/controls"
	"flows2fim/cmd/fim"
	"flows2fim/cmd/validate"
	"flows2fim/internal/config"
)

var usage string = `Usage of flows2fim:
	flows2fim [--version | --help]
	flows2fim COMMAND --help
	flows2fim COMMAND Args

Commands:
  - controls: Given a flow file and a rating curves database, create a control table of reach flows and downstream boundary conditions.
  - fim: Given a control table and a fim library folder, create a flood inundation map for the control conditions.

Notes:
  - 'fim' command needs access to GDAL programs. They must be installed separately and available in the system's PATH.
`

var (
	GitTag    = "unknown" // will be injected at build-time
	GitCommit = "unknown" // will be injected at build-time
	BuildDate = "unknown" // will be injected at build-time
)

// run handles the main logic and returns an error if something goes wrong.
func run(args []string) (err error) {
	config.LoadConfig()

	if len(args) < 2 {
		fmt.Println("Please provide a command")
		fmt.Print(usage)
		err = fmt.Errorf("missing command")
		return err
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
		fmt.Println("Unknown command:", args[1])
		err = fmt.Errorf("unknown command")
	}

	return err
}

func main() {
	if err := run(os.Args); err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		os.Exit(1)
	}
	os.Exit(0)
}
