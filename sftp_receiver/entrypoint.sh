#!/bin/bash
set -e

# Fix ownership of uploads directory for SFTP user
chown -R 1001:1001 /home/cml_user/uploads

# Execute the original entrypoint
exec /entrypoint "$@"
