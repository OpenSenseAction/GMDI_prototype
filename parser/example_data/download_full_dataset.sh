#!/bin/bash
# Download the full 3-month CML dataset (200 MB compressed)

set -e

URL="https://bwsyncandshare.kit.edu/s/jSAFftGXcJjQbSJ/download"
OUTPUT_FILE="openMRG_cmls_20150827_3months.nc"

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
OUTPUT_PATH="$SCRIPT_DIR/$OUTPUT_FILE"

echo "Downloading full 3-month CML dataset..."
echo "URL: $URL"
echo "Output: $OUTPUT_PATH"

if [ -f "$OUTPUT_PATH" ]; then
    echo "File already exists: $OUTPUT_PATH"
    read -p "Overwrite? (y/N): " -n 1 -r
    echo
    if [[ ! $REPLY =~ ^[Yy]$ ]]; then
        echo "Download cancelled."
        exit 0
    fi
fi

curl -L "$URL" -o "$OUTPUT_PATH" --progress-bar

if [ $? -eq 0 ]; then
    echo "Download complete!"
    echo "File size: $(du -h "$OUTPUT_PATH" | cut -f1)"
else
    echo "Download failed!"
    exit 1
fi
