package utils

import (
	"fmt"
	"os/exec"
)

// FormatString capitalizes the first letter of each word in a string
func PrintSeparator() {
	fmt.Println("---")
}

// CheckGDALAvailable checks if gdalbuildvrt is available in the environment.
func CheckGDALAvailable() bool {
	cmd := exec.Command("gdalbuildvrt", "--version")
	if err := cmd.Run(); err != nil {
		return false
	}
	return true
}
