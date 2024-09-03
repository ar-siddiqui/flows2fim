package utils

import (
	"strings"
	"testing"
)

func TestColorizeWarning(t *testing.T) {
	tests := []struct {
		name string
		text string
		want string
	}{
		{
			name: "basic warning",
			text: "This is a warning",
			want: "\x1b[38;5;214mThis is a warning\x1b[0m",
		},
		{
			name: "empty warning",
			text: "",
			want: "\x1b[38;5;214m\x1b[0m",
		},
		{
			name: "special characters",
			text: "Warning with special characters #!@",
			want: "\x1b[38;5;214mWarning with special characters #!@\x1b[0m",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := ColorizeWarning(tt.text)
			if !strings.Contains(got, tt.want) {
				t.Errorf("ColorizeWarning() = %v, want %v", got, tt.want)
			}
		})
	}
}
