"""Parser service entrypoint shim.

This module delegates to the appropriate entrypoint script based on
the PARSER_ENTRYPOINT environment variable. For backward compatibility,
if not set it defaults to running the sftp_push entrypoint.
"""

from parser.entrypoints.sftp_push import main

if __name__ == "__main__":
    main()
