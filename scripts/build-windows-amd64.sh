#!/bin/bash

# The script must be executed from the root of the repository
set -eo pipefail

echo "Building for Windows AMD64..."
CGO_ENABLED=0 GOOS=windows GOARCH=amd64 go build -buildvcs=false  \
    -ldflags="-X main.GitTag=$(git describe --tags --always --dirty) -X main.GitCommit=$(git rev-parse --short HEAD) -X main.BuildDate=$(date +%Y-%m-%d)" \
    -o builds/windows-amd64/flows2fim.exe main.go
echo "Windows build completed."