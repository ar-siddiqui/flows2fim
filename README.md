# `flows2fim`
![alt text](image.png)
## Overview
`flows2fim` is a command line utility program that creates composite FIMs for different flow conditions utilizing input FIM libraries and rating curves database.

It has following commands.
 - `controls`: Given a flow file and a rating curves database, create a control table of reach flows and downstream boundary conditions.
 - `fim`: Given a control table and a fim library folder. Create a flood inundation map for the control conditions.

Dependencies:
 - 'fim' command needs access to 'gdalbuildvrt' and 'gdalwarp' program. GDAL must be installed separately and made available in Path.

## For Users

### Getting Started

1. Download `flows2fim` executables from `s3://fimc-data/flows2fim/release-assets/v0.2.0/`.

1. Install `GDAL` if you don't already have it.
   > GDAL can be installed through a vriaety of ways.
   > - On Windows: The easiest way is through `OSGeo4W` installer https://trac.osgeo.org/osgeo4w/#QuickStartforOSGeo4WUsers
   > - On Ubuntu Linux: Run `apt-get update && apt-get install -y gdal-bin`

1. Make sure `flows2fim` and `GDAL` both are available in your Path.
   > - On Windows: The easiest way is to place the downlowded `flows2fim.exe` file from step 1 in `C:\OSGeo4W\bin` and then use `OSGeo4W Shell` for the next steps
   > - On Linux: Simplest option is to place the downlowded `flows2fim` file in `/bin` folder

1. Get familiar using `flows2fim -h` and `flows2fim COMMAND -h`.

1. Download Baxter sample data from ` s3://fimc-data/flows2fim/sample_data/v0_2_0/Baxter` if you don't have a dataset already.

1. To create control file from 100yr flows file run `flows2fim controls -db "Baxter/ripple.gpkg" -f "Baxter/flows/flows_100yr_cfs.csv" -o "Baxter/controls_100yr.csv" -sids 2821866`

1. To create Depth VRT run `flows2fim fim -lib "Baxter/library" -c "Baxter/controls_100yr.csv" -o "Baxter/fim_100yr.vrt"`

## For Developers

### Getting Started

1. Clone the repository and perform following steps from the root of the repo.

1. Download Baxter testadata from `s3://fimc-data/flows2fim/sample_data/v0_2_0/Baxter` to `testdata/Baxter` folder.

2. Launch a docker container using `docker compose up` and run following commands inside the container

3. Run `go run main.go controls -db "testdata/Baxter/ripple.gpkg" -f "testdata/Baxter/flows/flows_100yr_cfs.csv" -o "testdata/Baxter/controls_100yr.csv" -sids 2821866` This will create a controls.csv file

4. Run `go run main.go fim -lib "testdata/Baxter/library" -c "testdata/Baxter/controls_100yr.csv" -o "testdata/Baxter/fim_100yr.vrt"` This will create a VRT file. VRT can be tested by loading in QGIS.


### Purpose of Directories

- `cmd`: Contains individual folders for different commands.

- `pkg`: Houses reusable packages potentially useful in other projects.

- `internal`: For private application code not intended for external use.

- `scripts`: Includes useful scripts for building, testing, and more.


### Testing

1. Run `go test ./...` to run automated tests.

### Building

Run `./scripts/build-linux-amd64.sh` This will place the executable in `builds/linux-amd64`.