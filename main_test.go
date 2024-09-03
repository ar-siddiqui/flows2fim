package main

import (
	"bytes"
	"os"
	"testing"
)

func TestVersionFlag(t *testing.T) {
	tests := []struct {
		name           string
		version        string
		commit         string
		buildDate      string
		expectedOutput string
	}{
		{
			name:           "without ldflags",
			version:        "",
			commit:         "",
			buildDate:      "",
			expectedOutput: "Software: flows2fim\nVersion: unknown\nCommit: unknown\nBuild Date: unknown\n",
		},
		{
			name:           "with ldflags",
			version:        "v1.2.3",
			commit:         "abcdefg",
			buildDate:      "2024-08-30",
			expectedOutput: "Software: flows2fim\nVersion: v1.2.3\nCommit: abcdefg\nBuild Date: 2024-08-30\n",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Backup the original stdout
			oldStdout := os.Stdout
			r, w, _ := os.Pipe()
			os.Stdout = w

			// Set the ldflags variables for this test case
			if tt.version != "" {
				GitTag = tt.version
			}
			if tt.commit != "" {
				GitCommit = tt.commit
			}
			if tt.buildDate != "" {
				BuildDate = tt.buildDate
			}

			run([]string{"cmd", "--version"})

			w.Close()
			var buf bytes.Buffer
			buf.ReadFrom(r)
			output := buf.String()

			// Restore the original stdout
			os.Stdout = oldStdout

			// Check if the output matches the expected output
			if output != tt.expectedOutput {
				t.Errorf("expected '%s', got '%s'", tt.expectedOutput, output)
			}
		})
	}
}
