#!/bin/bash
set -e

# This script loads archive data into the database during initialization
# It runs after the schema is created (init.sql) but before the database
# accepts external connections.

echo "Loading archive data into database..."

ARCHIVE_DATA_DIR="/docker-entrypoint-initdb.d/archive_data"

# Check if archive data exists (should be included in the repo)
if [ ! -f "$ARCHIVE_DATA_DIR/metadata_archive.csv.gz" ] || [ ! -f "$ARCHIVE_DATA_DIR/data_archive.csv.gz" ]; then
    echo "Warning: Archive data files not found. Skipping archive data load."
    echo "Hint: Run 'python mno_data_source_simulator/generate_archive.py' to generate archive data."
    exit 0
fi

# Load metadata first (required for foreign key references)
echo "Loading metadata archive..."
psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" <<-EOSQL
    \COPY cml_metadata FROM PROGRAM 'gunzip -c $ARCHIVE_DATA_DIR/metadata_archive.csv.gz' WITH (FORMAT csv, HEADER true);
EOSQL

METADATA_COUNT=$(psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" -t -c "SELECT COUNT(*) FROM cml_metadata;")
echo "Loaded $METADATA_COUNT metadata records"

# Load time-series data using COPY for maximum speed
echo "Loading time-series archive data (this may take 10-30 seconds)..."
START_TIME=$(date +%s)

psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" <<-EOSQL
    \COPY cml_data (time, cml_id, sublink_id, tsl, rsl) FROM PROGRAM 'gunzip -c $ARCHIVE_DATA_DIR/data_archive.csv.gz' WITH (FORMAT csv, HEADER true);
EOSQL

END_TIME=$(date +%s)
DURATION=$((END_TIME - START_TIME))

DATA_COUNT=$(psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" -t -c "SELECT COUNT(*) FROM cml_data;")
echo "Loaded $DATA_COUNT data records in $DURATION seconds"

# Display time range of loaded data
psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" <<-EOSQL
    SELECT 
        'Archive data time range:' as info,
        MIN(time) as start_time,
        MAX(time) as end_time,
        COUNT(*) as total_rows
    FROM cml_data;
EOSQL

# Populate cml_stats for all loaded CMLs
echo "Populating CML statistics..."
psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" <<-EOSQL
    SELECT update_cml_stats(cml_id::text) FROM (SELECT DISTINCT cml_id FROM cml_metadata) t;
EOSQL

STATS_COUNT=$(psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" -t -c "SELECT COUNT(*) FROM cml_stats;")
echo "Generated statistics for $STATS_COUNT CMLs"

echo "Archive data successfully loaded!"
