#!/bin/bash
set -e

# Fix ownership of upload directories for each SFTP user
chown -R 1001:1001 /home/demo_openmrg/uploads
chown -R 1002:1002 /home/demo_orange_cameroun/uploads

# Execute the original entrypoint
exec /entrypoint "$@"
