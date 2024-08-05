package fim

// to do: add build args for gdal_merge on windows vs gdal_merge.py on linux. Also carefully review all changes"
import (
	"encoding/csv"
	"flag"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
)

var usage string = `Usage of fim:
Given a control table and a fim library folder, create a flood inundation VRT or a merged TIFF for the control conditions.
GDAL VSI paths can be used, given GDAL must have access to cloud creds.
Does not support relative cloud paths.
CLI flag syntax. The following forms are permitted:
-flag
--flag   // double dashes are also permitted
-flag=x
-flag x  // non-boolean flags only
Arguments:`

var gdalCommands = map[string]string{
	"vrt": "gdalbuildvrt",
	"tif": gdalMergeName,
}

// Check GDAL tools available checks if gdalbuildvrt is available in the environment.
func CheckGDALToolAvailable(tool string) bool {
	cmd := exec.Command(tool, "--version")
	if err := cmd.Run(); err != nil {
		return false
	}
	return true
}

func writeFileList(fileList []string) (string, error) {
	tmpfile, err := os.CreateTemp("", "filelist-*.txt")
	if err != nil {
		return "", err
	}
	defer tmpfile.Close()

	for _, file := range fileList {
		if _, err := tmpfile.WriteString(file + "\n"); err != nil {
			return "", err
		}
	}

	return tmpfile.Name(), nil
}

func Run(args []string) (gdalArgs []string, err error) {
	// Create a new flag set
	err = fmt.Errorf("cli arguments error")
	flags := flag.NewFlagSet("fim", flag.ExitOnError)
	flags.Usage = func() {
		fmt.Println(usage)
		flags.PrintDefaults()
	}

	var controlsFile, fimLibDir, outputFile, outputFormat string
	var relative bool

	// Define flags using flags.StringVar
	flags.StringVar(&fimLibDir, "lib", ".", "Directory containing FIM Library. GDAL VSI paths can be used, given GDAL must have access to cloud creds")
	flags.BoolVar(&relative, "rel", true, "If relative paths should be used in VRT")
	flags.StringVar(&controlsFile, "c", "", "Path to the controls CSV file")
	flags.StringVar(&outputFile, "o", "fim.vrt", "Output file path")
	flags.StringVar(&outputFormat, "fmt", "vrt", "Output format: 'vrt' (default) or 'tif'")

	// Parse flags from the arguments
	if err := flags.Parse(args); err != nil {
		return []string{}, fmt.Errorf("error parsing flags: %v", err)
	}

	// Validate required flags
	if controlsFile == "" || fimLibDir == "" || outputFile == "" {
		fmt.Println(controlsFile, fimLibDir, outputFile)
		fmt.Println("Missing required flags")
		flags.PrintDefaults()
		return []string{}, fmt.Errorf("missing required flags")
	}

	// Check if gdalbuildvrt or gdal_merge is available
	if !CheckGDALToolAvailable(gdalCommands[outputFormat]) {
		return []string{}, fmt.Errorf("error: %[1]s is not available. Please install GDAL and ensure %[1]s is in your PATH", gdalCommands[outputFormat])
	}

	if strings.HasPrefix(fimLibDir, "/vsi") || strings.HasPrefix(outputFile, "/vsi") {
		relative = false
		// gdalbuildvrt don't support cloud relative paths
		// this does not work gdalbuildvrt /vsis3/fimc-data/fim2d/prototype/2024_03_13/vsi_relative.vrt ./8489318/z0_0/f_1560.tif ./8490370/z0_0/f_130.tif
	}

	var absOutputPath, absFimLibPath, absOutputDir string
	if strings.HasPrefix(outputFile, "/vsi") {
		absOutputPath = outputFile
	} else {
		absOutputPath, err = filepath.Abs(outputFile)
		if err != nil {
			return []string{}, fmt.Errorf("error getting absolute path for output file: %v", err)
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

	// Processing CSV and creating VRT or TIFF
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

		record[2] = strings.Replace(record[2], ".", "_", -1) // Replace '.' with '_'
		folderName := filepath.Join(absFimLibPath, reachID, fmt.Sprintf("z_%s", record[2]))
		fileName := fmt.Sprintf("f_%s.tif", record[1])
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

	// Write file paths to a temporary file
	tempFileName, err := writeFileList(tifFiles)
	if err != nil {
		return []string{}, fmt.Errorf("error writing file list to temporary file: %v", err)
	}
	defer os.Remove(tempFileName)

	if outputFormat == "vrt" {
		gdalArgs = []string{"-input_file_list", tempFileName, absOutputPath}
	} else if outputFormat == "tif" {
		gdalArgs = []string{"-n", "-9999.0", "-a_nodata", "-9999.0", "-co", "COMPRESS=DEFLATE", "-o", absOutputPath, "--optfile", tempFileName}
	}

	cmd := exec.Command(gdalCommands[outputFormat], gdalArgs...)
	if !strings.HasPrefix(absOutputPath, "/vsi") {
		cmd.Dir = absOutputDir
	}
	// Redirecting the output to the standard output of the Go program
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	if err := cmd.Run(); err != nil {
		return []string{}, fmt.Errorf("error running %s: %v", gdalCommands[outputFormat], err)
	}

	fmt.Printf("FIM created at %s\n", absOutputPath)

	return gdalArgs, nil
}
