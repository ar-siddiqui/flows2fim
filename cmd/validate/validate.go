package validate

import (
	"bufio"
	"bytes"
	"database/sql"
	"encoding/csv"
	"flag"
	"flows2fim/pkg/utils"
	"fmt"
	"log/slog"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync"

	_ "modernc.org/sqlite"
)

var usage string = `Usage of validate:
Given a fim library folder and a rating curves database,
validate there is one to one correspondence between the entries of rating curves table and fim library objects.
GDAL VSI paths can be used, given GDAL must have access to cloud creds.
Intermediate folders for output files are created if they do not exist.

FIM Library Specifications:
- All maps should have same CRS, Resolution, vertical units (if any), and nodata value
- Should have following folder structure:
.
├── 2821866
│   ├── z_nd
│   │   ├── f_10283.tif
│   │   ├── f_104569.tif
│   │   ├── f_11199.tif
│   │   ├── f_112807.tif
│   ├── z_53_5
│       ├── f_102921.tif
│       ├── f_10485.tif
│       ├── f_111159.tif
│       ├── f_11309.tif

Database file must have a table 'rating_curves' and contain following columns
        reach_id INTEGER
        us_flow REAL
        us_depth REAL
        us_wse Real
        ds_depth REAL
        ds_wse REAL
        boundary_condition TEXT CHECK(boundary_condition IN ('nd','kwse'))
        UNIQUE(reach_id, us_flow, ds_wse, boundary_condition)


Arguments:`

// SQL Query Constants
const (
	queryCreateFIMEntTable = `
	CREATE TABLE memdb.fim_entries (
		reach_id INTEGER,
		us_flow INTEGER,
		ds_wse REAL,
		boundary_condition TEXT
	);
	`

	queryMissingFims = `
	SELECT
		rc.reach_id,
		rc.us_flow,
		rc.ds_wse,
		rc.boundary_condition
	FROM
		rating_curves rc
	LEFT JOIN
		memdb.fim_entries f
		ON (rc.reach_id = f.reach_id
			AND rc.us_flow = f.us_flow
			AND (CASE
					WHEN rc.boundary_condition = 'nd' THEN 0
					ELSE rc.ds_wse
				END) = f.ds_wse
			AND rc.boundary_condition = f.boundary_condition)
	WHERE
		f.reach_id IS NULL
	ORDER BY
		rc.reach_id, rc.boundary_condition, rc.ds_wse, rc.us_flow;
	`

	queryMissingRatingCurves = `
	SELECT
		f.reach_id,
		f.us_flow,
		f.ds_wse,
		f.boundary_condition
	FROM
		memdb.fim_entries f
	LEFT JOIN
		rating_curves rc
		ON (f.reach_id = rc.reach_id
			AND f.us_flow = rc.us_flow
			AND f.ds_wse = (CASE
					WHEN f.boundary_condition = 'nd' THEN 0
					ELSE rc.ds_wse
				END)
			AND f.boundary_condition = rc.boundary_condition)
	WHERE
		rc.reach_id IS NULL
	ORDER BY
		f.reach_id, f.boundary_condition, f.ds_wse, f.us_flow;
	`
)

var extIgnore = []string{".aux", ".aux.xml", ".ovr", ".xml", ".tfw"}

// fimRow represents a single record discovered in the FIM library
type fimRow struct {
	reachID           int
	usFlow            int
	dsWse             float64
	boundaryCondition string
}

// dirEntry holds a path + info about whether it's a directory
type dirEntry struct {
	path  string
	isDir bool
}

// readDir is the wrapper that calls either gatherLocalEntries or gatherCloudEntries
// to get all paths (files + dirs).
// If recursive is true, it will recursively list all files and directories.
func readDir(dir string, recursive bool) ([]dirEntry, error) {
	var allEntries []dirEntry
	var err error

	if strings.HasPrefix(dir, "/vsi") {
		allEntries, err = gatherVSIEntries(dir, recursive)
	} else {
		allEntries, err = gatherLocalEntries(dir, recursive)
	}
	if err != nil {
		return nil, fmt.Errorf("error gathering entries from %s: %v", dir, err)
	}

	return allEntries, nil
}

// gatherLocalEntries uses either os.ReadDir (non-recursive) or filepath.WalkDir (recursive)
func gatherLocalEntries(dir string, recursive bool) ([]dirEntry, error) {
	if !recursive {
		// Non-recursive approach: just top-level entries
		entries, err := os.ReadDir(dir)
		if err != nil {
			return nil, err
		}
		var results []dirEntry
		for _, e := range entries {
			results = append(results, dirEntry{
				path:  filepath.Join(dir, e.Name()),
				isDir: e.IsDir(),
			})
		}
		return results, nil
	}

	// Recursive approach with WalkDir
	var results []dirEntry
	err := filepath.WalkDir(dir, func(path string, d os.DirEntry, werr error) error {
		if werr != nil {
			return werr
		}
		results = append(results, dirEntry{path: path, isDir: d.IsDir()})
		return nil
	})
	return results, err
}

// gatherVSIEntries calls gdal_ls (with or without -r) to list entries in a VSI path
func gatherVSIEntries(dir string, recursive bool) ([]dirEntry, error) {
	var args []string
	if recursive {
		args = []string{"-r", dir}
	} else {
		args = []string{dir}
	}

	cmd := exec.Command(gdalLSName, args...)
	out, err := cmd.Output()
	if err != nil {
		return nil, fmt.Errorf("error gathering entries from %s: %v", dir, err)
	}

	scanner := bufio.NewScanner(bytes.NewReader(out))
	var lines []string
	for scanner.Scan() {
		lines = append(lines, scanner.Text())
	}
	if err := scanner.Err(); err != nil {
		slog.Warn("Error reading gdal_ls output", "tool", gdalLSName, "dir", dir, "error", err)
	}

	var results []dirEntry
	for _, line := range lines {
		if line == "" || !strings.HasPrefix(line, "/") { // ignore lines not starting with /
			continue
		}
		isDir := strings.HasSuffix(line, "/")
		results = append(results, dirEntry{path: line, isDir: isDir})
	}
	return results, nil
}

// processLibEntry parse boundary condition folders (z_XXX) and flow tif files (f_*.tif) from dirEntry path.
// It sends the parsed data to fimChan channel
func processLibEntry(e dirEntry, absFimLibDir string, fimChan chan<- fimRow) {
	// Skip directories
	if e.isDir {
		return
	}

	relPath, relErr := filepath.Rel(absFimLibDir, e.path)
	if relErr != nil {
		slog.Error("Relative path resolution failed", "path", e.path, "error", relErr)
		return
	}
	// On windows relPath will have backslashes, convert to forward slashes for /vsi paths
	if strings.HasPrefix(absFimLibDir, "/vsi") {
		relPath = filepath.ToSlash(relPath)
	}

	name := filepath.Base(e.path)
	ext := filepath.Ext(name)
	if utils.SliceContains(extIgnore, ext) {
		return
	} else if !strings.HasSuffix(name, ".tif") {
		slog.Warn("Non-TIFF file found", "path", relPath)
		return
	}

	if !strings.HasPrefix(name, "f_") {
		slog.Warn("Invalid file prefix", "path", relPath)
		return
	}

	usFlowStr := strings.TrimSuffix(strings.TrimPrefix(name, "f_"), ".tif")
	usFlow, convErr := strconv.Atoi(usFlowStr)
	if convErr != nil {
		slog.Error("Invalid flow value", "path", relPath, "value", usFlowStr)
		return
	}

	parts := strings.Split(relPath, string(os.PathSeparator))
	if len(parts) != 3 {
		slog.Error("Invalid path structure", "path", relPath)
		return
	}
	reachIDStr := parts[0]
	dirName := parts[1] // z_XXX
	reachID, err := strconv.Atoi(reachIDStr)
	if err != nil {
		slog.Error("Invalid reach ID", "path", relPath, "value", parts[0])
		return
	}
	if !strings.HasPrefix(dirName, "z_") {
		slog.Error("Invalid boundary condition", "path", relPath)
		return
	}
	dirSuffix := strings.TrimPrefix(dirName, "z_")

	var bc string
	var dsWse float64
	if dirSuffix == "nd" {
		bc = "nd"
		dsWse = 0.0
	} else {
		bc = "kwse"
		dsWseStr := strings.ReplaceAll(dirSuffix, "_", ".")
		dsWseFloat, parseErr := strconv.ParseFloat(dsWseStr, 64)
		if parseErr != nil {
			slog.Error("Invalid downstream WSE", "path", relPath)
			return
		}
		dsWse = dsWseFloat
	}

	fimChan <- fimRow{
		reachID:           reachID,
		usFlow:            usFlow,
		dsWse:             dsWse,
		boundaryCondition: bc,
	}
}

// batchInsertFIMs insert FIM rows in batches
func batchInsertFIMs(db *sql.DB, fimChan <-chan fimRow) error {
	tx, err := db.Begin()
	if err != nil {
		return err
	}
	defer func() {
		if p := recover(); p != nil {
			_ = tx.Rollback()
			panic(p)
		}
	}()

	const batchSize = 1000
	batch := make([]fimRow, 0, batchSize)

	commitBatch := func() error {
		if len(batch) == 0 {
			return nil
		}

		// Can't do prepared statement as the final batch would not be of same size
		// Build single statement for multi-VALUES insert
		// INSERT INTO memdb.fim_entries(reach_id, us_flow, ds_wse, boundary_condition) VALUES (?, ?, ?, ?), (?, ?, ?, ?) ...
		sqlStr := "INSERT INTO memdb.fim_entries(reach_id, us_flow, ds_wse, boundary_condition) VALUES "
		vals := make([]interface{}, 0, len(batch)*4)
		placeholders := make([]string, 0, len(batch))

		for _, r := range batch {
			placeholders = append(placeholders, "(?,?,?,?)")
			vals = append(vals, r.reachID, r.usFlow, r.dsWse, r.boundaryCondition)
		}
		sqlStr += strings.Join(placeholders, ",")

		if _, err := tx.Exec(sqlStr, vals...); err != nil {
			return err
		}
		batch = batch[:0]
		return nil
	}

	for row := range fimChan {
		batch = append(batch, row)
		if len(batch) >= batchSize {
			if err := commitBatch(); err != nil {
				_ = tx.Rollback()
				return err
			}
		}
	}

	// Do final flush when fimChan is closed in main routine
	if err := commitBatch(); err != nil {
		_ = tx.Rollback()
		return err
	}

	return tx.Commit()
}

// writeCSV is a generic function to write results to CSV.
// It is atomic i.e. it either succeeds or no file is created.
// It create intermediate directories if they do not exist.
// It returns number of data rows written in CSV.
func writeCSV(rows *sql.Rows, outFile string, skipEmpty bool) (int, error) {
	// Try reading the first row to check if there's data. We don't want to create empty intermediate directories.
	// An approach that was first adopted and discarded was to write to a temp file in tmp folder and then rename it only if rows exist,
	// but that approach cause inter-device rename error on conatinerized environment with file mounts.

	rowCount := 0
	if !rows.Next() {
		// No row was found if rows.Err() == nil.
		if err := rows.Err(); err != nil {
			return 0, fmt.Errorf("error reading rows: %v", err)
		}
		if skipEmpty {
			return 0, nil
		}
	} else {
		rowCount++
	}

	// Create intermediate directories if they do not exist
	if err := os.MkdirAll(filepath.Dir(outFile), 0755); err != nil {
		return 0, fmt.Errorf("could not create directories for %s: %v", outFile, err)
	}

	// On the same filesystem, os.Rename is atomic so will create a temp file and rename it later.
	tempFile, err := os.CreateTemp(filepath.Dir(outFile), "~f2f_*.tmp")
	if err != nil {
		return 0, fmt.Errorf("error creating temp file: %v", err)
	}
	tempFilePath := tempFile.Name()

	defer func() {
		_ = tempFile.Close()        // Always attempt to close file tempfile even if file is already closed
		_ = os.Remove(tempFilePath) // Always attempt to remove tempfile even if file is already renamed
	}()

	w := csv.NewWriter(tempFile)
	if err := w.Write([]string{"reach_id", "us_flow", "ds_wse", "boundary_condition"}); err != nil {
		return 0, fmt.Errorf("error writing CSV header: %v", err)
	}

	if rowCount != 0 {
		var reachID, usFlow int
		var dsWse float64
		var bc string
		if err := rows.Scan(&reachID, &usFlow, &dsWse, &bc); err != nil {
			return 0, fmt.Errorf("error scanning first row: %v", err)
		}

		if err := w.Write([]string{
			strconv.Itoa(reachID),
			strconv.Itoa(usFlow),
			fmt.Sprintf("%.1f", dsWse),
			bc,
		}); err != nil {
			return 0, fmt.Errorf("error writing first row to CSV: %v", err)
		}

		// Process remaining rows
		for rows.Next() {
			var reachID, usFlow int
			var dsWse float64
			var bc string
			if err := rows.Scan(&reachID, &usFlow, &dsWse, &bc); err != nil {
				return 0, fmt.Errorf("error scanning row: %v", err)
			}
			if err := w.Write([]string{
				strconv.Itoa(reachID),
				strconv.Itoa(usFlow),
				fmt.Sprintf("%.1f", dsWse),
				bc,
			}); err != nil {
				return 0, fmt.Errorf("error writing CSV record: %v", err)
			}
			rowCount++
		}
		if err := rows.Err(); err != nil {
			return 0, fmt.Errorf("error from rows iteration: %v", err)
		}
	}

	w.Flush()
	if err := w.Error(); err != nil {
		return 0, fmt.Errorf("error flushing CSV: %v", err)
	}

	if err := tempFile.Close(); err != nil {
		return 0, fmt.Errorf("error closing temp file: %v", err)
	}

	if err := os.Rename(tempFilePath, outFile); err != nil {
		return 0, fmt.Errorf("error renaming temp file %s to %s: %v", tempFilePath, outFile, err)
	}
	return rowCount, nil
}

func Run(args []string) error {
	flags := flag.NewFlagSet("validate", flag.ExitOnError)
	flags.Usage = func() {
		fmt.Println(usage)
		flags.PrintDefaults()
	}

	var (
		dbPath     string
		fimLibDir  string
		outFims    string
		outRcs     string
		concurrent int
		skipEmpty  bool
	)

	flags.StringVar(&dbPath, "db", "", "Path to the rating curves database file")
	flags.StringVar(&fimLibDir, "lib", "", "Path to the FIM library directory")
	flags.StringVar(&outFims, "o_fims", "missing_fims.csv", "Output CSV for rating curve entries missing corresponding FIM files")
	flags.StringVar(&outRcs, "o_rcs", "missing_rating_curves.csv", "Output CSV for FIM entries missing corresponding rating curve records")
	flags.IntVar(&concurrent, "cc", 25, "Concurrent Count, number of top-level reach directories to process concurrently (default 25)")
	flags.BoolVar(&skipEmpty, "skip_empty", false, "If true, do not create an empty output CSV file")

	if err := flags.Parse(args); err != nil {
		return fmt.Errorf("error parsing flags: %v", err)
	}

	// Validate required flags
	if dbPath == "" || fimLibDir == "" {
		flags.PrintDefaults()
		return fmt.Errorf("missing required flags")
	}

	// Check if gdalbuildvrt or GDAL tool is available
	if strings.HasPrefix(fimLibDir, "/vsi") && !utils.CheckGDALToolAvailable(gdalLSName) {
		return fmt.Errorf(`%[1]s is not available. Please install GDAL and ensure %[1]s is in your PATH. %[1]s is not available in PATH
		by default. Please refer to docs for instructions on how to add it to Path`, gdalLSName)
	}

	// 1) Open the input DB ( we won't modify it).
	if _, err := os.Stat(dbPath); os.IsNotExist(err) {
		return fmt.Errorf("database file does not exist: %s", dbPath)
	}
	db, err := sql.Open("sqlite", dbPath+"?mode=ro")
	if err != nil {
		return fmt.Errorf("error opening DB: %v", err)
	}
	defer db.Close()

	// 2) Attach an in-memory DB for fim_entries
	_, err = db.Exec(`ATTACH ':memory:' AS memdb;`)
	if err != nil {
		return fmt.Errorf("error attaching in-memory db: %v", err)
	}

	// Create table memdb.fim_entries
	// to do: move query to a constant at the top of the file
	_, err = db.Exec(queryCreateFIMEntTable)
	if err != nil {
		return fmt.Errorf("error creating memdb.fim_entries: %v", err)
	}

	var absFimLibDir string
	if strings.HasPrefix(fimLibDir, "/vsi") {
		absFimLibDir = fimLibDir
	} else {
		absFimLibDir, err = filepath.Abs(fimLibDir)
		if err != nil {
			return fmt.Errorf("error getting absolute path for fim library: %v", err)
		}
	}

	// 3) Setup concurrency
	fimChan := make(chan fimRow, 2000) // buffer for discovered rows
	var batchWG sync.WaitGroup

	// Single writer goroutine that batch-inserts rows into memdb.fim_entries
	batchWG.Add(1)
	go func() {
		defer batchWG.Done()
		if err := batchInsertFIMs(db, fimChan); err != nil {
			slog.Error("Could not insert FIM rows into memory db", "error", err)
		}
	}()

	var wg sync.WaitGroup
	sem := make(chan struct{}, concurrent) // limit concurrency to 'cc'
	// sync/semaphore could also have been used here

	// 4) Find top-level directories (reach folders) and process them
	libEntries, err := readDir(absFimLibDir, false)
	if err != nil {
		return fmt.Errorf("error reading fim library directory: %v", err)
	}

	var reachDirs []dirEntry
	for _, de := range libEntries {
		if de.isDir {
			reachDirs = append(reachDirs, de)
		}
	}

	if len(reachDirs) == 0 {
		if strings.HasPrefix(fimLibDir, "/vsi") {
			return fmt.Errorf("no entries found in VSI path. Is it a valid FIM library? Does GDAL have access to cloud credentials?")
		}
		return fmt.Errorf("no entries found in fim library directory. Not a valid fim library")
	}
	slog.Debug("Finished processing reach directories", "reach_dir_count", len(reachDirs))

	var reachDir string
	for _, de := range libEntries {
		if de.isDir {
			wg.Add(1)
			sem <- struct{}{} // Acquire concurrency token
			go func(reachDir string) {
				defer wg.Done()
				defer func() { <-sem }() // Release token
				reachEntries, err := readDir(de.path, true)
				if err != nil {
					slog.Warn("Reach directory read error", "path", de.path, "error", err)
					return
				}
				for _, e := range reachEntries {
					processLibEntry(e, absFimLibDir, fimChan)
				}
			}(reachDir)
		}
	}

	// Wait for all reach processing goroutines to finish
	wg.Wait()
	close(fimChan) // no more FIM rows
	batchWG.Wait() // wait for the DB writer goroutine

	// 5) Query DB for missing data and write to CSV
	tasks := []struct {
		outFile string
		query   string
		label   string
	}{
		{outFims, queryMissingFims, "FIMs"},
		{outRcs, queryMissingRatingCurves, "Rating Curves"},
	}
	for _, task := range tasks {
		rows, err := db.Query(task.query)
		if err != nil {
			return fmt.Errorf("error executing query for %s: %v", task.outFile, err)
		}
		defer rows.Close()

		rowCount, err := writeCSV(rows, task.outFile, skipEmpty)
		if err != nil {
			return fmt.Errorf("error writing %s: %v", task.outFile, err)
		}
		fmt.Printf("Number of missing %s records found: %d\n", task.label, rowCount)
		if rowCount > 0 || !skipEmpty {
			fmt.Printf("Missing %s file created at %s\n", task.label, task.outFile)
		}
	}

	fmt.Println("Validation complete")
	return nil
}
