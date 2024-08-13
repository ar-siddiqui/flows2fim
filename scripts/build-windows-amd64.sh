#!/bin/bash

# The script must be executed from the root of the repository
set -eo pipefail

echo "Building for Windows AMD64..."
CGO_ENABLED=0 GOOS=windows GOARCH=amd64 go build -buildvcs=false -o builds/windows-amd64/flows2fim.exe main.go
echo "Windows build completed."
