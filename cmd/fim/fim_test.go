package fim

// import (
// 	"reflect"
// 	"testing"
// )

// func TestRun(t *testing.T) {
// 	tests := []struct {
// 		name         string
// 		args         []string
// 		wantGdalArgs []string
// 		wantErr      bool
// 	}{
// 		{"controls file not exist", []string{"-c", "/app/testdata/outputs/not_exist.csv", "-lib", "/app/testdata/library", "-o", "output.vrt"}, []string{}, true},
// 		{"output in current dir", []string{"-c", "/app/testdata/outputs/controls.csv", "-lib", "/app/testdata/library", "-o", "output.vrt"}, []string{
// 			"/app/cmd/fim/output.vrt",
// 			"../../testdata/library/8489318/z0_0/f_1560.tif",
// 			"../../testdata/library/8490370/z0_0/f_130.tif",
// 			"../../testdata/library/8490230/z0_0/f_373.tif",
// 			"../../testdata/library/8489322/z0_0/f_100.tif",
// 			"../../testdata/library/8489330/z0_0/f_1023.tif",
// 			"../../testdata/library/8489352/z5_0/f_190.tif",
// 			"../../testdata/library/8489296/z0_0/f_1843.tif",
// 			"../../testdata/library/8489316/z0_0/f_1449.tif",
// 			"../../testdata/library/8489306/z0_0/f_1415.tif",
// 			"../../testdata/library/8490350/z0_0/f_1560.tif",
// 			"../../testdata/library/8490228/z5_6/f_435.tif",
// 			"../../testdata/library/8489308/z6_5/f_456.tif",
// 			"../../testdata/library/8489320/z6_4/f_520.tif",
// 			"../../testdata/library/8490352/z5_0/f_250.tif",
// 		}, false},
// 		{"relative", []string{"-c", "/app/testdata/outputs/controls.csv", "-lib", "/app/testdata/library", "-o", "/app/testdata/outputs/relative.vrt"}, []string{
// 			"/app/testdata/outputs/relative.vrt",
// 			"../library/8489318/z0_0/f_1560.tif",
// 			"../library/8490370/z0_0/f_130.tif",
// 			"../library/8490230/z0_0/f_373.tif",
// 			"../library/8489322/z0_0/f_100.tif",
// 			"../library/8489330/z0_0/f_1023.tif",
// 			"../library/8489352/z5_0/f_190.tif",
// 			"../library/8489296/z0_0/f_1843.tif",
// 			"../library/8489316/z0_0/f_1449.tif",
// 			"../library/8489306/z0_0/f_1415.tif",
// 			"../library/8490350/z0_0/f_1560.tif",
// 			"../library/8490228/z5_6/f_435.tif",
// 			"../library/8489308/z6_5/f_456.tif",
// 			"../library/8489320/z6_4/f_520.tif",
// 			"../library/8490352/z5_0/f_250.tif",
// 		}, false},
// 		{"absolute", []string{"-c", "/app/testdata/outputs/controls.csv", "-lib", "/app/testdata/library", "-o", "/app/testdata/outputs/absolute.vrt", "-rel=False"}, []string{
// 			"/app/testdata/outputs/absolute.vrt",
// 			"/app/testdata/library/8489318/z0_0/f_1560.tif",
// 			"/app/testdata/library/8490370/z0_0/f_130.tif",
// 			"/app/testdata/library/8490230/z0_0/f_373.tif",
// 			"/app/testdata/library/8489322/z0_0/f_100.tif",
// 			"/app/testdata/library/8489330/z0_0/f_1023.tif",
// 			"/app/testdata/library/8489352/z5_0/f_190.tif",
// 			"/app/testdata/library/8489296/z0_0/f_1843.tif",
// 			"/app/testdata/library/8489316/z0_0/f_1449.tif",
// 			"/app/testdata/library/8489306/z0_0/f_1415.tif",
// 			"/app/testdata/library/8490350/z0_0/f_1560.tif",
// 			"/app/testdata/library/8490228/z5_6/f_435.tif",
// 			"/app/testdata/library/8489308/z6_5/f_456.tif",
// 			"/app/testdata/library/8489320/z6_4/f_520.tif",
// 			"/app/testdata/library/8490352/z5_0/f_250.tif",
// 		}, false},
// 		{"disk_vrt_s3_lib", []string{"-c", "/app/testdata/outputs/controls.csv", "-lib", "/vsis3/fimc-data/fim2d/prototype/2024_03_13/", "-o", "/app/testdata/outputs/disk_vrt_s3_lib.vrt"}, []string{
// 			"/app/testdata/outputs/disk_vrt_s3_lib.vrt",
// 			"/vsis3/fimc-data/fim2d/prototype/2024_03_13/8489318/z0_0/f_1560.tif",
// 			"/vsis3/fimc-data/fim2d/prototype/2024_03_13/8490370/z0_0/f_130.tif",
// 			"/vsis3/fimc-data/fim2d/prototype/2024_03_13/8490230/z0_0/f_373.tif",
// 			"/vsis3/fimc-data/fim2d/prototype/2024_03_13/8489322/z0_0/f_100.tif",
// 			"/vsis3/fimc-data/fim2d/prototype/2024_03_13/8489330/z0_0/f_1023.tif",
// 			"/vsis3/fimc-data/fim2d/prototype/2024_03_13/8489352/z5_0/f_190.tif",
// 			"/vsis3/fimc-data/fim2d/prototype/2024_03_13/8489296/z0_0/f_1843.tif",
// 			"/vsis3/fimc-data/fim2d/prototype/2024_03_13/8489316/z0_0/f_1449.tif",
// 			"/vsis3/fimc-data/fim2d/prototype/2024_03_13/8489306/z0_0/f_1415.tif",
// 			"/vsis3/fimc-data/fim2d/prototype/2024_03_13/8490350/z0_0/f_1560.tif",
// 			"/vsis3/fimc-data/fim2d/prototype/2024_03_13/8490228/z5_6/f_435.tif",
// 			"/vsis3/fimc-data/fim2d/prototype/2024_03_13/8489308/z6_5/f_456.tif",
// 			"/vsis3/fimc-data/fim2d/prototype/2024_03_13/8489320/z6_4/f_520.tif",
// 			"/vsis3/fimc-data/fim2d/prototype/2024_03_13/8490352/z5_0/f_250.tif",
// 		}, false},
// 		{"s3_vrt_s3_lib", []string{"-c", "/app/testdata/outputs/controls.csv", "-lib", "/vsis3/fimc-data/fim2d/prototype/2024_03_13/", "-o", "/vsis3/fimc-data/flows2fim/testdata/s3_vrt_s3_lib.vrt"}, []string{
// 			"/vsis3/fimc-data/flows2fim/testdata/s3_vrt_s3_lib.vrt",
// 			"/vsis3/fimc-data/fim2d/prototype/2024_03_13/8489318/z0_0/f_1560.tif",
// 			"/vsis3/fimc-data/fim2d/prototype/2024_03_13/8490370/z0_0/f_130.tif",
// 			"/vsis3/fimc-data/fim2d/prototype/2024_03_13/8490230/z0_0/f_373.tif",
// 			"/vsis3/fimc-data/fim2d/prototype/2024_03_13/8489322/z0_0/f_100.tif",
// 			"/vsis3/fimc-data/fim2d/prototype/2024_03_13/8489330/z0_0/f_1023.tif",
// 			"/vsis3/fimc-data/fim2d/prototype/2024_03_13/8489352/z5_0/f_190.tif",
// 			"/vsis3/fimc-data/fim2d/prototype/2024_03_13/8489296/z0_0/f_1843.tif",
// 			"/vsis3/fimc-data/fim2d/prototype/2024_03_13/8489316/z0_0/f_1449.tif",
// 			"/vsis3/fimc-data/fim2d/prototype/2024_03_13/8489306/z0_0/f_1415.tif",
// 			"/vsis3/fimc-data/fim2d/prototype/2024_03_13/8490350/z0_0/f_1560.tif",
// 			"/vsis3/fimc-data/fim2d/prototype/2024_03_13/8490228/z5_6/f_435.tif",
// 			"/vsis3/fimc-data/fim2d/prototype/2024_03_13/8489308/z6_5/f_456.tif",
// 			"/vsis3/fimc-data/fim2d/prototype/2024_03_13/8489320/z6_4/f_520.tif",
// 			"/vsis3/fimc-data/fim2d/prototype/2024_03_13/8490352/z5_0/f_250.tif",
// 		}, false},
// 	}
// 	for _, tt := range tests {
// 		t.Run(tt.name, func(t *testing.T) {
// 			gotGdalArgs, err := Run(tt.args)
// 			if (err != nil) != tt.wantErr {
// 				t.Errorf("Run() error = %v, wantErr %v", err, tt.wantErr)
// 				return
// 			}
// 			if !reflect.DeepEqual(gotGdalArgs, tt.wantGdalArgs) {
// 				t.Errorf("Run() = %v, want %v", gotGdalArgs, tt.wantGdalArgs)
// 			}
// 		})
// 	}
// }
