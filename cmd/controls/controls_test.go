package controls

import (
	"reflect"
	"testing"

	_ "modernc.org/sqlite"
)

func TestReadFlows(t *testing.T) {
	tests := []struct {
		name      string
		filePath  string
		wantFlows map[int]float32
		wantErr   bool
	}{
		{
			name:     "empty flow lines should be skipped",
			filePath: "/app/testdata/unit_tests/flow_files/empty_flow_values_no_header.csv",
			wantFlows: map[int]float32{
				2820118: 29171.14,
				2820116: 35.31,
			},
			wantErr: false,
		},
		{
			name:     "file does not exist",
			filePath: "/app/testdata/non_existent_file.csv",
			wantErr:  true,
		},
		{
			name:      "reach_id and flow coloumn swapped",
			filePath:  "/app/testdata/unit_tests/flow_files/coloumns_swapped.csv",
			wantFlows: map[int]float32{},
			wantErr:   false,
		},
		{
			name:      "empty file",
			filePath:  "/app/testdata/unit_tests/flow_files/empty_file.csv",
			wantFlows: map[int]float32{},
			wantErr:   false,
		},
		// {
		// 	name:     "negative flows should be skipped",
		// 	filePath: "testdata/negative_flows.csv",
		// 	wantFlows: map[int]float32{
		// 		2820002: -100.0,
		// 		2820006: -200.0,
		// 	},
		// 	wantErr: false,
		// },
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			flows, err := ReadFlows(tt.filePath)
			if (err != nil) != tt.wantErr {
				t.Errorf("ReadFlows() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !tt.wantErr && !reflect.DeepEqual(flows, tt.wantFlows) {
				t.Errorf("ReadFlows() = %v, want %v", flows, tt.wantFlows)
			}
		})
	}
}

// func TestRun(t *testing.T) {
// 	tests := []struct {
// 		name    string
// 		args    []string
// 		wantErr bool
// 	}{
// 		{"positive", []string{"-db=/app/testdata/reach_data.db", "-f", "/app/testdata/flows_100yr.csv", "-c", "/app/testdata/outputs/controls.csv", "-sid", "8489318", "-scs", "0.0"}, false},
// 		{"db file does not exist", []string{"-db=/app/testdata/not_exist.db", "-f", "/app/testdata/flows_100yr.csv", "-c", "/app/testdata/outputs/controls.csv", "-sid", "8489318", "-scs", "0.0"}, true},
// 		{"flow file does not exist", []string{"-db=/app/testdata/reach_data.db", "-f", "/app/testdata/flows_not_exist.csv", "-c", "/app/testdata/outputs/controls.csv", "-sid", "8489318", "-scs", "0.0"}, true},
// 	}
// 	for _, tt := range tests {
// 		t.Run(tt.name, func(t *testing.T) {
// 			if err := Run(tt.args); (err != nil) != tt.wantErr {
// 				t.Errorf("Run() error = %v, wantErr %v", err, tt.wantErr)
// 			}
// 		})
// 	}
// }
