#!/usr/bin/env python3
"""SFTP Fetcher - Main entry point for Docker container."""

import logging
import sys
import os

# Add parent directory to path so we can import fetchers.shared
sys.path.insert(0, '/app')

from fetchers.sftp_fetcher.fetcher import main

if __name__ == '__main__':
    # Configure logging
    logging.basicConfig(
        level=os.environ.get('LOG_LEVEL', 'INFO'),
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    
    main()
