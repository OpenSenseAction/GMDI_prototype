#!/bin/bash
set -e

# This script loads archive data into the database.
# It can run either:
#   - during PostgreSQL init (mounted in /docker-entrypoint-initdb.d/), OR
#   - as a standalone service after the DB is healthy (set PGHOST to connect remotely).
#
# Environment variables:
#   ARCHIVE_DATA_DIR  Path to the directory with metadata_archive.csv / data_archive.csv
#                     (default: /docker-entrypoint-initdb.d/archive_data)
#   PGHOST            Hostname of the PostgreSQL server (blank = Unix socket, i.e. init-time)
#   PGUSER / POSTGRES_USER    Database user (PGUSER takes precedence)
#   PGDATABASE / POSTGRES_DB  Database name (PGDATABASE takes precedence)
#   PGPASSWORD                Database password (required for remote connections)

echo "Loading archive data into database..."

ARCHIVE_DATA_DIR="${ARCHIVE_DATA_DIR:-/docker-entrypoint-initdb.d/archive_data}"
# User ID to tag all loaded rows with. Defaults to demo_openmrg (OpenMRG dataset).
ARCHIVE_USER_ID="${ARCHIVE_USER_ID:-demo_openmrg}"

# Resolve credentials: prefer psql env vars, fall back to Postgres Docker image vars.
DB_USER="${PGUSER:-${POSTGRES_USER:-myuser}}"
DB_NAME="${PGDATABASE:-${POSTGRES_DB:-mydatabase}}"

# Build the common psql flags.  Add -h only when PGHOST is set (external connection).
PSQL_FLAGS="-v ON_ERROR_STOP=1 --username $DB_USER --dbname $DB_NAME"
if [ -n "${PGHOST:-}" ]; then
    PSQL_FLAGS="$PSQL_FLAGS --host $PGHOST"
fi

# Check if archive data exists
if [ ! -f "$ARCHIVE_DATA_DIR/metadata_archive.csv" ] || [ ! -f "$ARCHIVE_DATA_DIR/data_archive.csv" ]; then
    echo "Warning: Archive data files not found. Skipping archive data load."
    echo "Hint: Run 'python mno_data_source_simulator/generate_archive.py' to generate archive data."
    exit 0
fi

# Load metadata first (required for foreign key references).
# Use a temp table + INSERT ON CONFLICT DO NOTHING so that metadata already
# inserted by the parser (from a real-time upload) doesn't abort this script.
echo "Loading metadata archive..."
psql $PSQL_FLAGS <<-EOSQL
    CREATE TEMP TABLE tmp_cml_metadata (LIKE cml_metadata INCLUDING ALL);
    \COPY tmp_cml_metadata (cml_id, sublink_id, site_0_lon, site_0_lat, site_1_lon, site_1_lat, frequency, length, polarization) FROM '$ARCHIVE_DATA_DIR/metadata_archive.csv' WITH (FORMAT csv, HEADER true);
    UPDATE tmp_cml_metadata SET user_id = '$ARCHIVE_USER_ID';
    INSERT INTO cml_metadata SELECT * FROM tmp_cml_metadata ON CONFLICT DO NOTHING;
    DROP TABLE tmp_cml_metadata;
EOSQL

METADATA_COUNT=$(psql $PSQL_FLAGS -t -c "SELECT COUNT(*) FROM cml_metadata;")
echo "Loaded $METADATA_COUNT metadata records"

# Load time-series data using COPY for maximum speed
echo "Loading time-series archive data (this may take 10-30 seconds)..."
START_TIME=$(date +%s)

psql $PSQL_FLAGS <<-EOSQL
    CREATE TEMP TABLE tmp_cml_data (
        time       TIMESTAMPTZ,
        cml_id     TEXT,
        sublink_id TEXT,
        tsl        REAL,
        rsl        REAL
    );
    \COPY tmp_cml_data (time, cml_id, sublink_id, tsl, rsl) FROM '$ARCHIVE_DATA_DIR/data_archive.csv' WITH (FORMAT csv, HEADER true);
    INSERT INTO cml_data (time, cml_id, sublink_id, tsl, rsl, user_id)
        SELECT time, cml_id, sublink_id, tsl, rsl, '$ARCHIVE_USER_ID' FROM tmp_cml_data;
    DROP TABLE tmp_cml_data;
EOSQL

END_TIME=$(date +%s)
DURATION=$((END_TIME - START_TIME))

DATA_COUNT=$(psql $PSQL_FLAGS -t -c "SELECT COUNT(*) FROM cml_data;")
echo "Loaded $DATA_COUNT data records in $DURATION seconds"

# Build index after bulk load (much faster than maintaining it during COPY)
echo "Building index on cml_data..."
INDEX_START=$(date +%s)
psql $PSQL_FLAGS <<-EOSQL
    CREATE INDEX IF NOT EXISTS idx_cml_data_cml_id ON cml_data (cml_id, time DESC);
EOSQL
INDEX_END=$(date +%s)
echo "Index built in $((INDEX_END - INDEX_START)) seconds"

# Display time range of loaded data
psql $PSQL_FLAGS <<-EOSQL
    SELECT 
        'Archive data time range:' as info,
        MIN(time) as start_time,
        MAX(time) as end_time,
        COUNT(*) as total_rows
    FROM cml_data;
EOSQL

echo "Archive data successfully loaded!"
# Note: cml_stats is populated by the parser's background stats thread on startup.

# Refresh the 1-hour continuous aggregate so that Grafana and the webserver can
# immediately serve pre-aggregated data for large time ranges without scanning
# the full raw cml_data table.
echo "Refreshing 1h continuous aggregate (cml_data_1h)..."
psql $PSQL_FLAGS <<-EOSQL
    CALL refresh_continuous_aggregate('cml_data_1h', NULL, NULL);
EOSQL
echo "Continuous aggregate refresh complete."
