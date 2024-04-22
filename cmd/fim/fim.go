package fim

import (
	"encoding/csv"
	"flag"
	"flows2fim/pkg/utils"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
)

var usage string = `Usage of fim:
Given a control table and a fim library folder. Create a flood inundation VRT for the control conditions.
GDAL VSI paths can be used, given GDAL must have access to cloud creds.
Does not support relative cloud paths.
CLI flag syntax. The following forms are permitted:
-flag
--flag   // double dashes are also permitted
-flag=x
-flag x  // non-boolean flags only
Arguments:`

func Run(args []string) (gdalArgs []string, err error) {
	// Create a new flag set
	err = fmt.Errorf("cli arguments error")
	flags := flag.NewFlagSet("fim", flag.ExitOnError)
	flags.Usage = func() {
		fmt.Println(usage)
		flags.PrintDefaults()
	}

	var controlsFile, fimLibDir, outputVRT string
	var relative bool

	// Define flags using flags.StringVar
	flags.StringVar(&fimLibDir, "lib", ".", "Directory containing FIM Library. GDAL VSI paths can be used, given GDAL must have access to cloud creds")
	flags.BoolVar(&relative, "rel", true, "If relative paths should be used in VRT")
	flags.StringVar(&controlsFile, "c", "", "Path to the conrols CSV file")
	flags.StringVar(&outputVRT, "o", "fim.vrt", "Output VRT file path")

	// Parse flags from the arguments
	if err := flags.Parse(args); err != nil {
		return []string{}, fmt.Errorf("error parsing flags: %v", err)
	}

	// Validate required flags
	if controlsFile == "" || fimLibDir == "" || outputVRT == "" {
		fmt.Println(controlsFile, fimLibDir, outputVRT)
		fmt.Println("Missing required flags")
		flags.PrintDefaults()
		return []string{}, fmt.Errorf("missing required flags")
	}

	// Check if gdalbuildvrt is available
	if !utils.CheckGDALAvailable() {
		return []string{}, fmt.Errorf("error: gdalbuildvrt is not available. Please install GDAL and ensure gdalbuildvrt is in your PATH")
	}

	if strings.HasPrefix(fimLibDir, "/vsi") || strings.HasPrefix(outputVRT, "/vsi") {
		relative = false
		// gdalbuildvrt don't support cloud relative paths
		// this does not work gdalbuildvrt /vsis3/fimc-data/fim2d/prototype/2024_03_13/vsi_relative.vrt ./8489318/z0_0/f_1560.tif ./8490370/z0_0/f_130.tif
	}

	var absOutputPath, absFimLibPath, absOutputDir string
	if strings.HasPrefix(outputVRT, "/vsi") {
		absOutputPath = outputVRT
	} else {
		absOutputPath, err = filepath.Abs(outputVRT)
		if err != nil {
			return []string{}, fmt.Errorf("error getting absolute path for output VRT file: %v", err)
		}
	}
	absOutputDir = filepath.Dir(absOutputPath)

	if strings.HasPrefix(fimLibDir, "/vsi") {
		absFimLibPath = fimLibDir
	} else {
		absFimLibPath, err = filepath.Abs(fimLibDir)
		if err != nil {
			return []string{}, fmt.Errorf("error getting absolute path for FIM library directory: %v", err)
		}
	}

	// Processing CSV and creating VRT
	file, err := os.Open(controlsFile)
	if err != nil {
		return []string{}, fmt.Errorf("error opening CSV file: %v", err)
	}
	defer file.Close()

	reader := csv.NewReader(file)
	records, err := reader.ReadAll()
	if err != nil {
		return []string{}, fmt.Errorf("error reading CSV file: %v", err)
	}

	var tifFiles []string
	for _, record := range records[1:] { // Skip header row
		reachID := record[0]
		flow, err := strconv.ParseFloat(record[1], 64)
		if err != nil {
			return []string{}, err
		}
		controlStage, err := strconv.ParseFloat(record[2], 64)
		if err != nil {
			return []string{}, err
		}

		folderName := fmt.Sprintf("z%.1f", controlStage)
		folderName = filepath.Join(absFimLibPath, reachID, folderName)
		folderName = strings.Replace(folderName, ".", "_", -1) // Replace '.' with '_'
		fileName := fmt.Sprintf("f_%d.tif", int(flow))
		filePath := filepath.Join(folderName, fileName)

		if relative {
			filePath, err = filepath.Rel(absOutputDir, filePath)
			if err != nil {
				return []string{}, err
			}
		}

		// join on windows may cause \vsi
		if strings.HasPrefix(filePath, `\vsi`) {
			filePath = strings.ReplaceAll(filePath, `\`, "/")
		}
		tifFiles = append(tifFiles, filePath)

	}

	gdalArgs = append([]string{absOutputPath}, tifFiles...)
	cmd := exec.Command("gdalbuildvrt", gdalArgs...)
	if !strings.HasPrefix(absOutputPath, "/vsi") {
		cmd.Dir = absOutputDir
	}
	// Redirecting the output to the standard output of the Go program
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	if err := cmd.Run(); err != nil {
		return []string{}, fmt.Errorf("error running gdalbuildvrt: %v", err)
	}

	fmt.Printf("VRT created at %s\n", absOutputPath)

	return gdalArgs, nil
}
