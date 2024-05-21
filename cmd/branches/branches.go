package branches

import (
	"context"
	"database/sql"
	"encoding/json"
	"flag"
	"flows2fim/pkg/utils"
	"fmt"
	"math"
	"os"
	"os/exec"
	"strings"
	"sync"
	"time"

	"golang.org/x/sync/semaphore"

	log "github.com/sirupsen/logrus"

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
	id          int
	outId       int
	streamOrder int
	length      float64
	flow        float64
}

type branch struct {
	id            int
	reaches       string
	controlNodes  string
	controlByNode interface{}
}

var branchInserts = make(chan branch, 100) // Adjust buffer size as necessary

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
	rows, err := db.Query("SELECT id, out_id, stream_order, length, flow FROM reaches WHERE out_id = ?", outReach)
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
		if err := rows.Scan(&r.id, &r.outId, &r.streamOrder, &r.length, &r.flow); err != nil {
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
	rows, err := db.Query("SELECT id, stream_order, length, flow FROM reaches WHERE out_id is null OR out_id = 0;")
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
		if err := rows.Scan(&r.id, &r.streamOrder, &r.length, &r.flow); err != nil {
			return nil, err
		}
		downStreamMostReaches = append(downStreamMostReaches, r)
	}

	if err := rows.Err(); err != nil {
		return nil, err
	}

	return downStreamMostReaches, nil
}

func processReach(db *sql.DB, current reach, wg *sync.WaitGroup, sem *semaphore.Weighted, dbMutex *sync.RWMutex, maxLength, flowDeltaThrsh float64) {
	sem.Acquire(context.Background(), 1)
	defer sem.Release(1)
	defer wg.Done()

	groupedReaches := []int{current.id}
	controlNodes := []int{}
	controlByNode := current.outId
	currentLength := current.length
	currentFlow := current.flow

	for {
		dbMutex.RLock()
		upstreamReaches, err := fetchUpstreamReaches(db, current.id)
		dbMutex.RUnlock()
		if err != nil {
			log.Debugf("error fetching upstream reaches for %d: %v\n", current.id, err)
			return
		}

		if len(upstreamReaches) == 0 || (len(upstreamReaches) == 1 && (upstreamReaches[0].length > maxLength || isSignificantChange(currentFlow, upstreamReaches[0].flow, flowDeltaThrsh))) {
			controlNodes := append(controlNodes, current.id)
			reverseSlice(controlNodes)
			ctrlNodeJSON, err := json.Marshal(controlNodes)
			if err != nil {
				log.Error("Could not marshalized control nodes for branch", current.id)
			}
			ctrlNodeStr := string(ctrlNodeJSON)

			reverseSlice(groupedReaches)
			reachesJSON, err := json.Marshal(groupedReaches)
			if err != nil {
				log.Error("Could not marshalized control nodes for branch", current.id)
			}
			reachesStr := string(reachesJSON)

			var controlByNodeValue interface{} = controlByNode
			if controlByNode == 0 {
				controlByNodeValue = nil
			}

			// Send insert request to the channel instead of executing it
			branchInserts <- branch{
				id:            current.id,
				reaches:       reachesStr,
				controlNodes:  ctrlNodeStr,
				controlByNode: controlByNodeValue,
			}
			for _, upReach := range upstreamReaches {
				wg.Add(1)
				go processReach(db, upReach, wg, sem, dbMutex, maxLength, flowDeltaThrsh)
			}
			break
		} else if len(upstreamReaches) == 1 {
			current = upstreamReaches[0]
			groupedReaches = append(groupedReaches, current.id)
			currentLength += current.length
			currentFlow = current.flow
		} else {
			countMatchReaches := 0
			matchReach := reach{}

			for _, r := range upstreamReaches {
				if !isSignificantChange(currentFlow, r.flow, flowDeltaThrsh) && r.streamOrder == current.streamOrder {
					countMatchReaches += 1
					matchReach = r
				}
			}

			if countMatchReaches != 1 {
				controlNodes := append(controlNodes, current.id)
				reverseSlice(controlNodes)
				ctrlNodeJSON, err := json.Marshal(controlNodes)
				if err != nil {
					log.Error("Could not marshalized control nodes for branch", current.id)
				}
				ctrlNodeStr := string(ctrlNodeJSON)

				reverseSlice(groupedReaches)
				reachesJSON, _ := json.Marshal(groupedReaches)
				reachesStr := string(reachesJSON)
				var controlByNodeValue interface{} = controlByNode
				if controlByNode == 0 {
					controlByNodeValue = nil
				}

				// Send insert request to the channel instead of executing it
				branchInserts <- branch{
					id:            current.id,
					reaches:       reachesStr,
					controlNodes:  ctrlNodeStr,
					controlByNode: controlByNodeValue,
				}
				for _, upReach := range upstreamReaches {
					wg.Add(1)
					go processReach(db, upReach, wg, sem, dbMutex, maxLength, flowDeltaThrsh)
				}
				break
			} else {
				for _, upReach := range upstreamReaches {
					if upReach == matchReach {
						continue
					}
					wg.Add(1)
					go processReach(db, upReach, wg, sem, dbMutex, maxLength, flowDeltaThrsh)
				}

				controlNodes = append(controlNodes, current.id)

				current = matchReach
				groupedReaches = append(groupedReaches, current.id)
				currentLength += current.length
				currentFlow = current.flow

			}
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

func handleBranchesInsert(db *sql.DB, tableName string, batchSize int, wg *sync.WaitGroup, dbMutex *sync.RWMutex) {
	defer wg.Done()

	// Batching logic for inserts
	var batch []branch

	for req := range branchInserts { // for range loop knows when channel is closed and no further values can be sent
		batch = append(batch, req)
		if len(batch) >= batchSize {
			dbMutex.Lock() // Lock for writing
			executeBranchesBatch(db, tableName, batch)
			dbMutex.Unlock() // Unlock after write
			batch = nil      // Reset the batch
		}
	}
	if len(batch) > 0 { // Handle any remaining requests
		dbMutex.Lock() // Lock for writing
		executeBranchesBatch(db, tableName, batch)
		dbMutex.Unlock() // Unlock after write
	}
}

func executeBranchesBatch(db *sql.DB, tableName string, batch []branch) {
	// Batch execution logic
	tx, err := db.Begin()
	if err != nil {
		fmt.Println("Error starting transaction:", err)
		return
	}

	stmtText := fmt.Sprintf("INSERT INTO %s (branch_id, reaches, control_nodes, control_by_node) VALUES ", tableName)
	valPlaceholder := []string{}
	var params []interface{}

	for _, req := range batch {
		valPlaceholder = append(valPlaceholder, "(?, ?, ?, ?)")
		params = append(params, req.id, req.reaches, req.controlNodes, req.controlByNode)
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
		log.Debugf("Error executing batch insert: %v\n", err)
		stmt.Close()
		tx.Rollback()
		return
	}
	stmt.Close()
	tx.Commit()
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
	var poolSize int64

	// Define flags
	flags.StringVar(&dbPath, "db", "", "Path to the SQLite database file with reaches table")
	flags.Float64Var(&maxLength, "ml", math.Inf(1), "Maximum length for a branch")
	flags.Float64Var(&flowDeltaThrsh, "df", 0.2, "Delta flow threshold")
	flags.StringVar(&outputDBPath, "o", "branches.gpkg", "Path to the output SQLite database file")
	flags.Int64Var(&poolSize, "ps", 1000, "Max number of concurrent workers")

	// Parse flags from the arguments
	if err := flags.Parse(args); err != nil {
		return fmt.Errorf("error parsing flags: %v", err)
	}

	// Validate required flags
	if dbPath == "" {
		flags.PrintDefaults()
		return fmt.Errorf("missing required flags")
	}

	// Check if gdalbuildvrt is available
	if !utils.CheckGDALAvailable() {
		log.Errorf("error: ogr2ogr is not available. Please install GDAL and ensure ogr2ogr is in your PATH")
		return nil
	}

	// Database connection
	db, err := ConnectDB(dbPath)
	if err != nil {
		return fmt.Errorf("error connecting to database: %v", err)
	}
	defer db.Close()

	tempTable := fmt.Sprintf("temp_%d", time.Now().Unix())

	// Create SQL string with table name
	createTableSQL := fmt.Sprintf(`
	DROP TABLE IF EXISTS %[1]s;
	CREATE TABLE %[1]s (
		branch_id INTEGER PRIMARY KEY,
		reaches TEXT,
		control_nodes TEXT,
		control_by_node INTEGER
	);`, tempTable)

	if _, err := db.Exec(createTableSQL); err != nil {
		return fmt.Errorf("error creating temp table: %v", err)
	}

	// Fetch the most downstream reaches
	outletReaches, err := fetchDownStreamMostReaches(db)
	if err != nil {
		return fmt.Errorf("error fetching downstream most reaches: %v", err)
	}

	var wgTraverse, wgInsert sync.WaitGroup
	var dbMutex sync.RWMutex

	wgInsert.Add(1)
	go handleBranchesInsert(db, tempTable, 100, &wgInsert, &dbMutex) // Handling batch inserts with a batch size of 100

	startTime := time.Now() // Start timing the execution
	//initiate concurrency
	fmt.Println("Initiating concurrencty")
	sem := semaphore.NewWeighted(poolSize)
	for _, r := range outletReaches {
		wgTraverse.Add(1)
		go processReach(db, r, &wgTraverse, sem, &dbMutex, maxLength, flowDeltaThrsh)
	}
	fmt.Println("Waiting on traverse")

	wgTraverse.Wait()
	elapsed := time.Since(startTime)
	log.Debugf("Network traversed in %v meiliseconds", elapsed.Milliseconds())

	close(branchInserts)
	// Closing channel means no further values can be sent
	// but the existing values can be received https://go.dev/play/p/LtYOuLoOoQK
	wgInsert.Wait()

	ogrBranchesSQL := fmt.Sprintf(`SELECT t.branch_id, t.control_by_node, t.reaches, t.control_nodes, ST_LineMerge(ST_Union(reaches.geom)) AS geom
	FROM %s AS t
	LEFT JOIN reaches
	ON reaches.id IN (SELECT value FROM json_each(t.reaches))
	GROUP BY t.branch_id`, tempTable)
	ogrArgs := []string{"-f", "GPKG", outputDBPath, dbPath, "-dialect", "sqlite", "-sql", ogrBranchesSQL, "-nln", "branches"}

	cmd := exec.Command("ogr2ogr", ogrArgs...)
	// Redirecting the output to the standard output of the Go program
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	if err := cmd.Run(); err != nil {
		log.Errorf("error running ogr2ogr: %v", err)
	}

	elapsed = time.Since(startTime) - elapsed
	log.Debugf("branches table created in %v meiliseconds", elapsed.Milliseconds())

	// first branch_id is being used as FID, then we are overwriting FID through lco, hence that is getting ommitted
	ogrNodesSQL := fmt.Sprintf(`SELECT r.id, t.branch_id, t.branch_id, ST_StartPoint(r.geom) AS geom
		FROM reaches AS r
		JOIN %s AS t
		ON r.id IN (SELECT value FROM json_each(t.control_nodes))`, tempTable)
	ogrArgs = []string{"-f", "GPKG", outputDBPath, dbPath, "-dialect", "sqlite", "-sql", ogrNodesSQL, "-nln", "nodes", "-update", "-lco", "FID=id"}

	cmd = exec.Command("ogr2ogr", ogrArgs...)

	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	if err := cmd.Run(); err != nil {
		log.Errorf("error running ogr2ogr: %v", err)
	}

	elapsed = time.Since(startTime) - elapsed
	log.Debugf("nodes table created in %v meiliseconds", elapsed.Milliseconds())

	if _, err := db.Exec(fmt.Sprintf("DROP TABLE %s", tempTable)); err != nil {
		return fmt.Errorf("error dropping temp table: %v", err)
	}

	return nil
}
