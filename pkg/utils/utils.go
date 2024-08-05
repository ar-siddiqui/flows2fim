package utils

import "fmt"

// FormatString capitalizes the first letter of each word in a string
func PrintSeparator() {
	fmt.Println("---")
}

func colorizeString(xtermNum int, text string) string {
	colored := fmt.Sprintf("\x1b[38;5;%dm%s\x1b[0m", xtermNum, text)
	return colored
}

func ColorizeWarning(text string) string {
	colored := colorizeString(214, text)
	return colored
}
