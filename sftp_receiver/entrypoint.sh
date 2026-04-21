#!/bin/bash
set -e

# Fix ownership of upload directories for each SFTP user
chown -R 1001:1001 /home/cml_user/uploads
chown -R 1002:1002 /home/user2/uploads

# Execute the original entrypoint
exec /entrypoint "$@"
