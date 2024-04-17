package branches

import (
	"database/sql"
	"encoding/json"
	"flag"
	"fmt"
	"math"
	"os"
	"strings"
	"sync"

	_ "modernc.org/sqlite"
)

var usage string = `Usage of branches:
Group a segmented river network into branches, starting from the most downstream segment and traversing upstream.
CLI flag syntax. The following forms are permitted:
-flag
--flag   // double dashes are also permitted
-flag=x
-flag x  // non-boolean flags only
Arguments:`

type reach struct {
	id    int
	outId int
	// streamOrder  int
	length float64
	flow   float64
}

type insertRequest struct {
	id            int
	reaches       string
	controlBranch interface{}
}

var insertRequests = make(chan insertRequest, 100) // Adjust buffer size as necessary

func isSignificantChange(flow1, flow2, threshold float64) bool {
	return abs(flow2-flow1)/flow1 > threshold
}

func abs(x float64) float64 {
	if x < 0 {
		return -x
	}
	return x
}

func reverseSlice(slice []int) {
	for i, j := 0, len(slice)-1; i < j; i, j = i+1, j-1 {
		slice[i], slice[j] = slice[j], slice[i]
	}
}

func fetchUpstreamReaches(db *sql.DB, outReach int) ([]reach, error) {
	rows, err := db.Query("SELECT id, out_id, length, flow FROM reaches WHERE out_id = ?", outReach)
	if err != nil {
		// Check if the error is because of no rows
		if err == sql.ErrNoRows {
			// No rows found, not an error in this context
			return []reach{}, nil
		}
		return nil, err
	}
	defer rows.Close()

	var upstreamReaches []reach
	for rows.Next() {
		var r reach
		if err := rows.Scan(&r.id, &r.outId, &r.length, &r.flow); err != nil {
			return nil, err
		}
		upstreamReaches = append(upstreamReaches, r)
	}

	if err := rows.Err(); err != nil {
		return nil, err
	}

	return upstreamReaches, nil
}

func fetchDownStreamMostReaches(db *sql.DB) ([]reach, error) {
	rows, err := db.Query("SELECT id, length, flow FROM reaches WHERE out_id is null")
	if err != nil {
		// Check if the error is because of no rows
		if err == sql.ErrNoRows {
			// No rows found, not an error in this context
			return []reach{}, nil
		}
		return nil, err
	}
	defer rows.Close()

	var downStreamMostReaches []reach
	for rows.Next() {
		var r reach
		if err := rows.Scan(&r.id, &r.length, &r.flow); err != nil {
			return nil, err
		}
		downStreamMostReaches = append(downStreamMostReaches, r)
	}

	if err := rows.Err(); err != nil {
		return nil, err
	}

	return downStreamMostReaches, nil
}

func processReach(db *sql.DB, current reach, wg *sync.WaitGroup, dbMutex *sync.RWMutex, maxLength, flowDeltaThrsh float64) {
	defer wg.Done()
	grouped := []int{current.id}
	controlBranch := current.outId
	currentLength := current.length
	currentFlow := current.flow

	for {
		dbMutex.RLock()
		upstream, err := fetchUpstreamReaches(db, current.id)
		dbMutex.RUnlock()
		if err != nil {
			fmt.Printf("error fetching upstream reaches for %d: %v\n", current.id, err)
			return
		}

		if len(upstream) != 1 || currentLength+upstream[0].length > maxLength || isSignificantChange(currentFlow, upstream[0].flow, flowDeltaThrsh) {
			reverseSlice(grouped)
			reachesJSON, _ := json.Marshal(grouped)
			reachesStr := string(reachesJSON)
			var controlBranchValue interface{} = controlBranch
			if controlBranch == 0 {
				controlBranchValue = nil
			}

			// Send insert request to the channel instead of executing it
			insertRequests <- insertRequest{
				id:            current.id,
				reaches:       reachesStr,
				controlBranch: controlBranchValue,
			}
			for _, upReach := range upstream {
				wg.Add(1)
				go processReach(db, upReach, wg, dbMutex, maxLength, flowDeltaThrsh)
			}
			break
		} else {
			current = upstream[0]
			grouped = append(grouped, current.id)
			currentLength += current.length
			currentFlow = current.flow
		}
	}
}

func ConnectDB(dbPath string) (*sql.DB, error) {

	// Check if the file exists
	if _, err := os.Stat(dbPath); os.IsNotExist(err) {
		return nil, fmt.Errorf("database file does not exist: %s", dbPath)
	}

	db, err := sql.Open("sqlite", dbPath)
	if err != nil {
		return nil, err
	}
	return db, nil
}

func Run(args []string) error {
	// Create a new flag set
	flags := flag.NewFlagSet("branches", flag.ExitOnError)
	flags.Usage = func() {
		fmt.Println(usage)
		flags.PrintDefaults()
	}

	var dbPath, outputDBPath string
	var maxLength, flowDeltaThrsh float64

	// Define flags
	flags.StringVar(&dbPath, "db", "", "Path to the SQLite database file with reaches table")
	flags.Float64Var(&maxLength, "ml", math.Inf(1), "Maximum length for a branch")
	flags.Float64Var(&flowDeltaThrsh, "df", 0.2, "Delta flow threshold")
	flags.StringVar(&outputDBPath, "o", "", "Path to the output SQLite database file")

	// Parse flags from the arguments
	if err := flags.Parse(args); err != nil {
		return fmt.Errorf("error parsing flags: %v", err)
	}

	// Validate required flags
	if dbPath == "" || outputDBPath == "" {
		flags.PrintDefaults()
		return fmt.Errorf("missing required flags")
	}

	// Database connection
	db, err := ConnectDB(dbPath)
	if err != nil {
		return fmt.Errorf("error connecting to database: %v", err)
	}
	defer db.Close()

	// Database connection for output
	outputDB, err := sql.Open("sqlite", outputDBPath)
	if err != nil {
		return fmt.Errorf("error connecting to output database: %v", err)
	}
	defer outputDB.Close()
	outputDB.SetMaxOpenConns(1) // SQLITE at the database level allow only one write connection at a time.
	// This could be bottleneck

	// Setup the table in the database to store results
	createTableSQL := `DROP TABLE IF EXISTS branches;
	CREATE TABLE branches (
		branch_id INTEGER PRIMARY KEY,
		reaches TEXT,
		control_branch INTEGER
	);`
	if _, err := outputDB.Exec(createTableSQL); err != nil {
		return fmt.Errorf("error creating branches table: %v", err)
	}

	// Fetch the most downstream reaches
	outletReaches, err := fetchDownStreamMostReaches(db)
	if err != nil {
		return fmt.Errorf("error fetching downstream most reaches: %v", err)
	}

	var wgTraverse, wgInsert sync.WaitGroup
	var dbMutex sync.RWMutex

	wgInsert.Add(1)
	go handleBatchInserts(outputDB, 100, &wgInsert, &dbMutex) // Handling batch inserts with a batch size of 100

	//initiate concurrency
	for _, r := range outletReaches {
		wgTraverse.Add(1)
		go processReach(db, r, &wgTraverse, &dbMutex, maxLength, flowDeltaThrsh)
	}

	wgTraverse.Wait()

	close(insertRequests)
	// Closing channel means no further values can be sent
	// but the existing values can be received https://go.dev/play/p/LtYOuLoOoQK
	wgInsert.Wait()

	return nil
}

func handleBatchInserts(outputDB *sql.DB, batchSize int, wg *sync.WaitGroup, dbMutex *sync.RWMutex) {
	defer wg.Done()

	// Batching logic for inserts
	var batch []insertRequest

	for req := range insertRequests { // for range loop knows when channel is closed and no further values can be sent
		batch = append(batch, req)
		if len(batch) >= batchSize {
			dbMutex.Lock() // Lock for writing
			executeBatch(outputDB, batch)
			dbMutex.Unlock() // Unlock after write
			batch = nil      // Reset the batch
		}
	}
	if len(batch) > 0 { // Handle any remaining requests
		dbMutex.Lock() // Lock for writing
		executeBatch(outputDB, batch)
		dbMutex.Unlock() // Unlock after write
	}
}

func executeBatch(outputDB *sql.DB, batch []insertRequest) {
	// Batch execution logic
	tx, err := outputDB.Begin()
	if err != nil {
		fmt.Println("Error starting transaction:", err)
		return
	}

	stmtText := "INSERT INTO branches (branch_id, reaches, control_branch) VALUES "
	valPlaceholder := []string{}
	var params []interface{}

	for _, req := range batch {
		valPlaceholder = append(valPlaceholder, "(?, ?, ?)")
		params = append(params, req.id, req.reaches, req.controlBranch)
	}

	stmtText += strings.Join(valPlaceholder, ", ")

	stmt, err := tx.Prepare(stmtText)
	if err != nil {
		fmt.Println("Error preparing batch insert statement:", err)
		tx.Rollback()
		return
	}

	_, err = stmt.Exec(params...)
	if err != nil {
		fmt.Printf("Error executing batch insert: %v\n", err)
		stmt.Close()
		tx.Rollback()
		return
	}
	stmt.Close()
	tx.Commit()
}
