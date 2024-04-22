package main

import (
	"fmt"
	"os"
	"time"

	"flows2fim/cmd/branches"
	"flows2fim/cmd/controls"
	"flows2fim/cmd/fim"
	"flows2fim/internal/config"

	log "github.com/sirupsen/logrus"
)

var usage string = `Usage of flows2fim:
	flows2fim COMMAND [Args]
Commands:
 - controls: Given a flow file and a reach database. Create controls table of reach flows and downstream boundary conditions.
 - fim: Given a control table and a fim library folder. Create a flood inundation VRT for the control conditions.
 - branches:
Notes:
 - 'fim' command needs access to 'gdalbuildvrt' program. It must be installed separately and made available in Path.
`

func main() {

	config.LoadConfig()
	log.SetLevel(config.GlobalConfig.LogLevel)

	if len(os.Args) < 2 {
		fmt.Println("Please provide a command")
		fmt.Print(usage)
		os.Exit(1)
	}

	var err error
	startTime := time.Now() // Start timing the execution

	// Run functions should perform all print statements except error

	switch os.Args[1] {
	case "-h", "--h", "-help", "--help":
		fmt.Print(usage)
		os.Exit(0)
	case "controls":
		err = controls.Run(os.Args[2:])
	case "fim":
		_, err = fim.Run(os.Args[2:])
	case "branches":
		err = branches.Run(os.Args[2:])
	default:
		fmt.Println("Unknown command:", os.Args[1])
		os.Exit(1)
	}

	if err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		os.Exit(1)
	}

	elapsed := time.Since(startTime)
	log.Debugf("Program execution compeleted in %v meiliseconds", elapsed.Milliseconds())
}
