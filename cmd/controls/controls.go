package controls

import (
	"bufio"
	"database/sql"
	"encoding/csv"
	"flag"
	"flows2fim/pkg/utils"
	"fmt"
	"log"
	"math"
	"os"
	"strconv"
	"strings"

	_ "modernc.org/sqlite"
)

var usage string = `Usage of controls:
Given a flow file and a reach database. Create controls table of reach flows and downstream boundary conditions.

Flow file's first coloumn values must be reach ids, and second coloumn must be discharges in cfs. Invalid lines are skipped.

Database file must have a table 'rating_curves' and contain following coloumns
        reach_id INTEGER
        us_flow REAL
        us_depth REAL
        us_wse Real
        ds_depth REAL
        ds_wse REAL
        boundary_condition TEXT CHECK(boundary_condition IN ('nd','kwse'))
        UNIQUE(reach_id, us_flow, ds_wse, boundary_condition)
Database file must have a table 'network' and contain following coloumns
        reach_id INTEGER
        updated_to_id INTEGER

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
	NormalDepth       bool
}

type RatingCurveRecord struct {
	ReachID           int
	Flow              int
	Stage             float32
	ControlReachStage float32
	BoundaryCondition string
}

type ResultRecord struct {
	ReachID              int
	Flow                 int
	ControlReachStageStr string
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
		if len(parts) < 2 {
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

func ReadStartReachesCSV(filePath string) ([]ControlData, error) {
	file, err := os.Open(filePath)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	var startReaches []ControlData
	reader := csv.NewReader(file)
	records, err := reader.ReadAll()
	if err != nil {
		return nil, err
	}

	for _, record := range records {
		if len(record) != 2 {
			continue // Skip invalid lines
		}
		reachID, err := strconv.Atoi(record[0])
		if err != nil {
			continue // Skip invalid lines
		}
		controlStageStr := record[1]
		var controlStage float64
		var nd bool
		if controlStageStr != "nd" {
			controlStage, err = strconv.ParseFloat(controlStageStr, 32)
			if err != nil {
				continue // Skip invalid lines
			}
		} else {
			nd = true
		}
		startReaches = append(startReaches, ControlData{ReachID: reachID, ControlReachStage: float32(controlStage), NormalDepth: nd})
	}

	return startReaches, nil
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
	rows, err := db.Query("SELECT reach_id FROM network WHERE updated_to_id = ?;", controlReachID)
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

func FetchNormalDepthFlowStage(db *sql.DB, reachID int, flow float32) (RatingCurveRecord, error) {
	row := db.QueryRow(`
	SELECT us_flow, us_wse, ds_wse
	FROM rating_curves
	WHERE reach_id = ?
	AND boundary_condition = 'nd'
	ORDER BY ABS(us_flow - ? )
	LIMIT 1;
	`, reachID, flow)
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
	rc.BoundaryCondition = "nd"
	return rc, nil
}

func FetchNearestFlowStage(db *sql.DB, reachID int, flow, controlStage float32) (RatingCurveRecord, error) {
	row := db.QueryRow(`
	SELECT us_flow, us_wse, ds_wse, boundary_condition
	FROM rating_curves
	WHERE reach_id = ?
	ORDER BY ABS(us_flow - ? ), ABS(ds_wse - ?)
	LIMIT 1;
	`, reachID, flow, controlStage)
	var rc RatingCurveRecord
	if err := row.Scan(&rc.Flow, &rc.Stage, &rc.ControlReachStage, &rc.BoundaryCondition); err != nil {
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

func TraverseUpstream(db *sql.DB, flows map[int]float32, startReaches []ControlData) (results []ResultRecord, err error) {
	queue := startReaches

	for len(queue) > 0 {
		current := queue[0]
		queue = queue[1:]

		// Get the flow for the current reach from the flows map
		flow, ok := flows[current.ReachID]
		if !ok {
			log.Printf("Flow not found for reach %d", current.ReachID)
			flow = 0
		}

		var rc RatingCurveRecord
		if current.NormalDepth {
			rc, err = FetchNormalDepthFlowStage(db, current.ReachID, flow)
			if err != nil {
				return []ResultRecord{}, fmt.Errorf("error fetching rating curve for reach %d: %v", current.ReachID, err)
			}
		} else {
			rc, err = FetchNearestFlowStage(db, current.ReachID, flow, current.ControlReachStage)
			if err != nil {
				return []ResultRecord{}, fmt.Errorf("error fetching rating curve for reach %d: %v", current.ReachID, err)
			}
			if math.Abs(float64(rc.ControlReachStage)-float64(current.ControlReachStage)) > 1 && // difference is greater than 1
				rc.ReachID != 0 &&
				!(float64(rc.ControlReachStage) > float64(current.ControlReachStage) && rc.BoundaryCondition == "nd") { // sometimes the difference can be because the d/s stage is lower than `nd` stage for this reach, so ignore that condition
				log.Print(utils.ColorizeWarning(fmt.Sprintf("Warning: Large difference in target vs found Control Reach Stage for reach %v: %.1f vs %.1f, Boundary Condition picked is %s",
					current.ReachID, current.ControlReachStage, rc.ControlReachStage, rc.BoundaryCondition)))
			}
		}
		if math.Abs(float64(flow)-float64(rc.Flow))/float64(flow) > 0.25 && rc.ReachID != 0 {
			log.Print(utils.ColorizeWarning(fmt.Sprintf("Warning: Large difference in target vs found flow for reach %v: %.1f vs %d",
				current.ReachID, flow, rc.Flow)))
		}

		// Fetch upstream reaches
		upstream, err := FetchUpstreamReaches(db, current.ReachID)
		if err != nil {
			return []ResultRecord{}, fmt.Errorf("error fetching upstream reaches for %d: %v", current.ReachID, err)
		}

		if rc.ReachID == 0 { // no rating curve record found, add upstream reaches with NormalDepth condition
			for _, u := range upstream {
				queue = append(queue, ControlData{ReachID: u, ControlReachStage: rc.Stage, NormalDepth: true})
			}
			continue // no result to add
		} else {
			for _, u := range upstream {
				queue = append(queue, ControlData{ReachID: u, ControlReachStage: rc.Stage})
			}
		}

		result := ResultRecord{ReachID: rc.ReachID, Flow: rc.Flow}
		if rc.BoundaryCondition == "nd" {
			result.ControlReachStageStr = "nd"
		} else {
			result.ControlReachStageStr = fmt.Sprintf("%.1f", rc.ControlReachStage)
		}

		results = append(results, result)
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
		record := []string{strconv.Itoa(d.ReachID), fmt.Sprint(d.Flow), d.ControlReachStageStr}
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
		dbPath, flowsFilePath, outputFilePath, startReachesCSV, startReachIDsStr, startControlStagesStr string
	)
	flags.StringVar(&dbPath, "db", "", "Path to the database file")
	flags.StringVar(&flowsFilePath, "f", "", "Path to the input flows CSV file")
	flags.StringVar(&startReachesCSV, "scsv", "", "Path to the CSV file containing starting reach IDs and control stages (Coloumn headers do not matter)")
	flags.StringVar(&startReachIDsStr, "sids", "", "Comma-separated list of starting reach IDs (One of -sids or -scsv is required, if both are provided, -sids and -scs flags are ignored)")
	flags.StringVar(&startControlStagesStr, "scs", "nd", "Comma-separated list of starting control stages (corresponding to the reach IDs)")
	flags.StringVar(&outputFilePath, "o", "", "Path to the output controls CSV file")

	// Parse flags from the arguments
	if err = flags.Parse(args); err != nil {
		return fmt.Errorf("error parsing flags: %v", err)
	}

	// Validate required flags
	// Start reaches flags are validated later
	if dbPath == "" || flowsFilePath == "" || outputFilePath == "" {
		fmt.Println("Missing required flags")
		flags.PrintDefaults()
		return fmt.Errorf("missing required flags")
	}

	var startReaches []ControlData

	if startReachesCSV != "" {
		startReaches, err = ReadStartReachesCSV(startReachesCSV)
		if err != nil {
			return fmt.Errorf("error reading start points CSV: %v", err)
		}
	} else if startReachIDsStr != "" && startControlStagesStr != "" {
		// Parse reach IDs
		startReachIDs := strings.Split(startReachIDsStr, ",")
		startControlStages := strings.Split(startControlStagesStr, ",")

		if len(startReachIDs) != len(startControlStages) {
			if startControlStagesStr == "nd" {
				startControlStages = make([]string, len(startReachIDs))
				for i := range startReachIDs {
					startControlStages[i] = "nd"
				}
			} else {
				return fmt.Errorf("the number of startReachIds must match the number of startControlStages")
			}
		}

		for i, sidStr := range startReachIDs {
			startReachID, err := strconv.Atoi(sidStr)
			if err != nil {
				return fmt.Errorf("invalid startReachID: %v", err)
			}
			controlStageStr := startControlStages[i]
			var controlStage float64
			var nd bool
			if controlStageStr != "nd" {
				controlStage, err = strconv.ParseFloat(controlStageStr, 32)
				if err != nil {
					return fmt.Errorf("invalid startControlStage: %v", err)
				}
			} else {
				nd = true
			}
			startReaches = append(startReaches, ControlData{ReachID: startReachID, ControlReachStage: float32(controlStage), NormalDepth: nd})
		}
	} else {
		return fmt.Errorf("either a CSV file or start reach IDs and control stages must be provided")
	}

	flows, err := ReadFlows(flowsFilePath)
	if err != nil {
		return fmt.Errorf("error reading flows: %v", err)
	}

	db, err := ConnectDB(dbPath)
	if err != nil {
		return fmt.Errorf("error connecting to database: %v", err)
	}

	results, err := TraverseUpstream(db, flows, startReaches)
	if err != nil {
		return fmt.Errorf("error traversing upstream: %v", err)
	}

	if err := WriteCSV(results, outputFilePath); err != nil {
		return fmt.Errorf("error writing to CSV: %v", err)
	}

	fmt.Printf("Controls file created at %s\n", outputFilePath)
	return nil
}
