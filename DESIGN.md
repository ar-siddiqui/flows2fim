## Purpose of Directories

- `cmd`: Contains individual folders for different commands.

- `pkg`: Houses reusable packages potentially useful in other projects.

- `internal`: For private application code not intended for external use.

- `scripts`: Includes useful scripts for building, testing, and more.

## Design Decisions

### Fim
1. We decided to go against allowing all GDAL args to be passed to `fim` command. The reasons are
   - we need to provide maintenance for small use cases
   - we are overriding default for some of options e.g. resampling method in our case should be max by default not near. Users may assume if they don't pass -r it will take GDAL default.

### Validate
1. A  `-o_<type>` convention is used to allow for multiple outputs across subcommands.
2. An in-memory SQLite database is attached in read-only mode, ensuring the input database is not altered.
3. Batch inserts through a single goroutine are employed to minimize concurrency issues with SQLite and to reduce the number of round trips. This is necessary since consistency is desired over speed.
4. A single SQL query with rc_exist and fim_exist columns is relied upon, allowing the database to handle processing instead of in-memory go logic.
5. CSV rows are written one by one to avoid substantial memory usage at the end of processing.
6. Specified output files, even if empty, are generated to keep API consistent.
7. Atomic writes with a temporary file are performed for CSVs so no partial CSVs remain if errors occur.
8. Local directories are not processed with gdal_ls, preserving fast local operations and avoiding unnecessary dependencies.
