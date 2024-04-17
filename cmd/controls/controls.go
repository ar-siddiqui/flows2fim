package controls

import (
	"bufio"
	"database/sql"
	"encoding/csv"
	"flag"
	"fmt"
	"os"
	"strconv"
	"strings"

	log "github.com/sirupsen/logrus"

	_ "modernc.org/sqlite"
)

var usage string = `Usage of controls:
Given a flow file and a reach database. Create controls table of reach flows and downstream boundary conditions.
CLI flag syntax. The following forms are permitted:
-flag
--flag   // double dashes are also permitted
-flag=x
-flag x  // non-boolean flags only
Arguments:`

type FlowData struct {
	ReachID int
	Flow    float32
}

type ControlData struct {
	ReachID           int
	ControlReachStage float32
}

type RatingCurveRecord struct {
	ReachID           int
	Flow              float32
	Stage             float32
	ControlReachStage float32
}
type ResultRecord struct {
	ReachID           int
	Flow              float32
	ControlReachStage float32
}

func ReadFlows(filePath string) (map[int]float32, error) {
	file, err := os.Open(filePath)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	flows := make(map[int]float32)
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		line := scanner.Text()
		parts := strings.Split(line, ",")
		if len(parts) != 2 {
			continue // Skip invalid lines
		}
		reachID, err := strconv.Atoi(parts[0])
		if err != nil {
			continue // Skip invalid lines
		}
		flow, err := strconv.ParseFloat(parts[1], 32)
		if err != nil {
			continue // Skip invalid lines
		}
		flows[reachID] = float32(flow)
	}
	return flows, scanner.Err()
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

func FetchUpstreamReaches(db *sql.DB, controlReachID int) ([]int, error) {
	rows, err := db.Query("SELECT reach_id FROM reach_controls WHERE control_reach_id = ?", controlReachID)
	if err != nil {
		// Check if the error is because of no rows
		if err == sql.ErrNoRows {
			// No rows found, not an error in this context
			return []int{}, nil
		}
		return nil, err
	}
	defer rows.Close()

	var upstreamReaches []int
	for rows.Next() {
		var r int
		if err := rows.Scan(&r); err != nil {
			return nil, err
		}
		upstreamReaches = append(upstreamReaches, r)
	}

	if err := rows.Err(); err != nil {
		return nil, err
	}

	return upstreamReaches, nil
}

func FetchNearestFlowStage(db *sql.DB, reachID int, flow, controlStage float32) (RatingCurveRecord, error) {
	row := db.QueryRow("SELECT flow, stage, control_reach_stage FROM rating_curves WHERE reach_id = ? ORDER BY ABS(control_reach_stage - ?), ABS(flow - ? ) LIMIT 1", reachID, controlStage, flow)
	var rc RatingCurveRecord
	if err := row.Scan(&rc.Flow, &rc.Stage, &rc.ControlReachStage); err != nil {
		// Check if the error is because of no rows
		if err == sql.ErrNoRows {
			// No rows found, not an error in this context
			return RatingCurveRecord{}, nil
		}
		return RatingCurveRecord{}, err
	}
	rc.ReachID = reachID
	return rc, nil
}

func TraverseUpstream(db *sql.DB, flows map[int]float32, startReachID int, controlStage float32) (results []ResultRecord, err error) {
	queue := []ControlData{{ReachID: startReachID, ControlReachStage: controlStage}}

	for len(queue) > 0 {
		current := queue[0]
		queue = queue[1:]

		// Get the flow for the current reach from the flows map
		flow, ok := flows[current.ReachID]
		if !ok {
			log.Warnf("Flow not found for reach %d", current.ReachID)
			flow = 0
		}

		rc, err := FetchNearestFlowStage(db, current.ReachID, flow, current.ControlReachStage)
		if err != nil {
			return []ResultRecord{}, fmt.Errorf("error fetching rating curve for reach %d: %v", current.ReachID, err)
		}

		if rc.ReachID == 0 {
			continue
		}
		results = append(results, ResultRecord{rc.ReachID, rc.Flow, rc.ControlReachStage})

		// Fetch upstream reaches
		upstream, err := FetchUpstreamReaches(db, current.ReachID)
		if err != nil {
			return []ResultRecord{}, fmt.Errorf("error fetching upstream reaches for %d: %v", current.ReachID, err)
		}
		// Add upstream reaches to queue
		for _, u := range upstream {
			queue = append(queue, ControlData{ReachID: u, ControlReachStage: rc.Stage})
		}
	}

	return results, nil
}

func WriteCSV(data []ResultRecord, filePath string) error {
	file, err := os.Create(filePath)
	if err != nil {
		return err
	}
	defer file.Close()

	writer := csv.NewWriter(file)
	defer writer.Flush()
	if err := writer.Write([]string{"reach_id", "flow", "control_stage"}); err != nil {
		return err
	}

	for _, d := range data {
		record := []string{strconv.Itoa(d.ReachID), fmt.Sprintf("%.1f", d.Flow), fmt.Sprintf("%.1f", d.ControlReachStage)}
		if err := writer.Write(record); err != nil {
			return err
		}
	}

	return nil
}

func Run(args []string) (err error) {
	// Create a new flag set
	flags := flag.NewFlagSet("controls", flag.ExitOnError)
	flags.Usage = func() {
		fmt.Println(usage)
		flags.PrintDefaults()
	}

	// Define flags

	var (
		dbPath, flowsFilePath, outputFilePath, startReachIDStr, startControlStageStr string
	)
	flags.StringVar(&dbPath, "db", "", "Path to the database file")

	flags.StringVar(&flowsFilePath, "f", "", "Path to the input flows CSV file")

	flags.StringVar(&outputFilePath, "c", "", "Path to the output controls CSV file")

	flags.StringVar(&startReachIDStr, "sid", "", "Starting reach ID")

	flags.StringVar(&startControlStageStr, "scs", "0.0", "Starting control stage")

	// Parse flags from the arguments
	if err = flags.Parse(args); err != nil {
		return fmt.Errorf("error parsing flags: %v", err)
	}

	// Validate required flags
	if dbPath == "" || flowsFilePath == "" || outputFilePath == "" || startReachIDStr == "" {
		fmt.Println("Missing required flags")
		flags.PrintDefaults()
		return fmt.Errorf("missing required flags")
	}

	// Parse numerical values from flags
	startReachID, err := strconv.Atoi(startReachIDStr)
	if err != nil {
		return fmt.Errorf("invalid startReachID: %v", err)
	}
	startControlStage, err := strconv.ParseFloat(startControlStageStr, 32)
	if err != nil {
		return fmt.Errorf("invalid startControlStage: %v", err)
	}

	flows, err := ReadFlows(flowsFilePath)
	if err != nil {
		return fmt.Errorf("error reading flows: %v", err)
	}

	db, err := ConnectDB(dbPath)
	if err != nil {
		return fmt.Errorf("error connecting to database: %v", err)
	}
	defer db.Close()

	results, err := TraverseUpstream(db, flows, startReachID, float32(startControlStage))
	if err != nil {
		return fmt.Errorf("error traversing upstream: %v", err)
	}

	if err := WriteCSV(results, outputFilePath); err != nil {
		return fmt.Errorf("error writing to CSV: %v", err)
	}
	return nil
}
