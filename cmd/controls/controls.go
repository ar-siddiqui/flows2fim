package controls

import (
	"bufio"
	"database/sql"
	"encoding/csv"
	"encoding/json"
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

type ReachData struct {
	ReachID            int
	ControlNodes       []int
	ControlByNodeStage float32
}

type ControlRecord struct {
	NodeID           int
	Flow             float32
	ControlNodeStage float32
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

func FetchUpstreamReaches(db *sql.DB, controlNodeIDs []int, controlData ControlRecord) ([]ReachData, error) {
	// Prepare the query with the appropriate number of placeholders for controlNodeIDs
	query := fmt.Sprintf(`
	SELECT branch_id, control_nodes, r.stage FROM branches b
	LEFT JOIN rating_curves r ON b.control_by_node = r.node_id
	WHERE b.control_by_node IN (%s)
	AND r.control_by_node_stage = ? AND r.flow = ?;
	`, strings.Repeat("?,", len(controlNodeIDs)-1)+"?")

	// Convert controlNodeIDs slice to interface{} slice for variadic function argument
	args := make([]interface{}, len(controlNodeIDs)+2)
	for i, id := range controlNodeIDs {
		args[i] = id
	}
	args[len(args)-2] = controlData.ControlNodeStage
	args[len(args)-1] = controlData.Flow

	rows, err := db.Query(query, args...)
	if err != nil {
		// Check if the error is because of no rows
		if err == sql.ErrNoRows {
			// No rows found, not an error in this context
			return []ReachData{}, nil
		}
		return nil, err
	}
	defer rows.Close()

	var upstreamReaches []ReachData
	for rows.Next() {
		var r ReachData
		var ctrlNodesStr string
		if err := rows.Scan(&r.ReachID, &ctrlNodesStr, &r.ControlByNodeStage); err != nil {
			return nil, err
		}

		// Unmarshal the JSON-like list of integers into a slice of integers
		if err := json.Unmarshal([]byte(ctrlNodesStr), &r.ControlNodes); err != nil {
			return nil, err
		}

		upstreamReaches = append(upstreamReaches, r)
	}

	if err := rows.Err(); err != nil {
		return nil, err
	}

	return upstreamReaches, nil
}

func FetchNearestFlowStage(db *sql.DB, reachID int, flow, controlStage float32) (ControlRecord, error) {
	row := db.QueryRow("SELECT flow, control_by_node_stage FROM rating_curves WHERE node_id = ? ORDER BY ABS(control_by_node_stage - ?), ABS(flow - ? ) LIMIT 1", reachID, controlStage, flow)
	var rc ControlRecord
	if err := row.Scan(&rc.Flow, &rc.ControlNodeStage); err != nil {
		if err == sql.ErrNoRows {
			// No rows found, not an error in this context
			return ControlRecord{}, nil
		}
		return ControlRecord{}, err
	}
	rc.NodeID = reachID
	return rc, nil
}

func TraverseUpstream(db *sql.DB, flows map[int]float32, startReach ReachData) (results []ControlRecord, err error) {
	queue := []ReachData{startReach}

	for len(queue) > 0 {
		current := queue[0]
		queue = queue[1:] // there might be memory leak here because the underlying array is keeping data in memory

		fmt.Println("Current:", current)
		// Get the flow for the current reach from the flows map
		flow, ok := flows[current.ReachID]
		if !ok {
			log.Warnf("Flow not found for reach %d", current.ReachID)
			flow = 0
		}
		fmt.Println(flow)

		rc, err := FetchNearestFlowStage(db, current.ReachID, flow, current.ControlByNodeStage)
		if err != nil {
			return []ControlRecord{}, fmt.Errorf("error fetching rating curve for reach %d: %v", current.ReachID, err)
		}
		fmt.Println("RC", rc)

		if rc.NodeID == 0 {
			fmt.Println("---------------------")
			continue
		}
		results = append(results, rc)

		// Fetch upstream reaches
		upstreams, err := FetchUpstreamReaches(db, current.ControlNodes, rc)
		if err != nil {
			return []ControlRecord{}, fmt.Errorf("error fetching upstream reaches for %d: %v", current.ReachID, err)
		}

		fmt.Println("Upstreams:", upstreams)
		// Add upstream reaches to queue
		queue = append(queue, upstreams...)
		fmt.Println("---------------------")
	}

	return results, nil
}

func WriteCSV(data []ControlRecord, filePath string) error {
	file, err := os.Create(filePath)
	if err != nil {
		return err
	}
	defer file.Close()

	writer := csv.NewWriter(file)
	defer writer.Flush()
	if err := writer.Write([]string{"branch_id", "flow", "control_stage"}); err != nil {
		return err
	}

	for _, d := range data {
		record := []string{strconv.Itoa(d.NodeID), fmt.Sprintf("%.1f", d.Flow), fmt.Sprintf("%.1f", d.ControlNodeStage)}
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

	startReach := ReachData{ReachID: startReachID, ControlByNodeStage: float32(startControlStage)}

	// Get Control nodes on the start reach
	row := db.QueryRow("SELECT control_nodes FROM branches WHERE branch_id = ?", startReachID)
	var ctrlNodesStr string
	if err := row.Scan(&ctrlNodesStr); err != nil {
		// Check if the error is because of no rows
		if err == sql.ErrNoRows {
			// No rows found, not an error in this context
			return fmt.Errorf("no such branch exist: %v", startReachID)
		}
	}

	// Unmarshal the JSON-like list of integers into a slice of integers
	if err := json.Unmarshal([]byte(ctrlNodesStr), &startReach.ControlNodes); err != nil {
		return fmt.Errorf("can not get control nodes: %v", startReachID)
	}

	fmt.Println(startReach)
	results, err := TraverseUpstream(db, flows, startReach)
	if err != nil {
		return fmt.Errorf("error traversing upstream: %v", err)
	}

	if err := WriteCSV(results, outputFilePath); err != nil {
		return fmt.Errorf("error writing to CSV: %v", err)
	}
	return nil
}
