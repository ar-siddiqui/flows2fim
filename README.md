# Flows2FIM

## Overview
flows2fim is a command line utility program that has following commands.
 - branches: Given a reach network, aggregate reaches based on flows and create branches and nodes layers.
 - controls: Given a flow file and a reach database. Create controls table of reach flows and downstream boundary conditions.
 - fim: Given a control table and a fim library folder. Create a flood inundation VRT for the control conditions.

Dependencies:
 - 'Different commands need access to GDAL and OGR programs. GDAL must be installed separately and made available in Path.

## Purpose of Directories

- `cmd`: Contains individual folders for different commands.

- `pkg`: Houses reusable packages potentially useful in other projects.

- `internal`: For private application code not intended for external use.

- `scripts`: Includes useful scripts for building, testing, and more.

## Getting Started

1. Launch a docker container using `docker compose up` and run following commands inside the container

2. Run `go run main.go branches -db=testdata/reach_network.gpkg`

3. Run `go run main.go controls -db=testdata/reach_data.db -f testdata/flows_100yr.csv -c controls.csv -sid 8489318 -scs 0.0` This will create a controls.csv file

4. Download fim-library from `s3://fimc-data/fim2d/prototype/2024_03_13/` to `testdata/library` folder. Run `go run main.go fim -c controls.csv -lib testdata/library -o output.vrt` This will create a VRT file. VRT can be tested by loading in QGIS.

## Testing

1. Provide access to S3 fimc-data bucket to GDAL.

2. Run `go test ./...` to run automated tests.

## Building
