#!/bin/bash

# The script must be executed from the root of the repository
set -eo pipefail

echo "Building for Darwin (MacOS) ARM64..."
CGO_ENABLED=0 GOOS=darwin GOARCH=arm64 go build -buildvcs=false -o builds/darwin-arm64/flows2fim main.go
echo "Mac build completed."
