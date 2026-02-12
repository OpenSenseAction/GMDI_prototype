FROM atmoz/sftp:latest

# Copy our custom entrypoint script
COPY entrypoint.sh /custom-entrypoint.sh

# Make it executable
RUN chmod +x /custom-entrypoint.sh

# Set it as the entrypoint
ENTRYPOINT ["/custom-entrypoint.sh"]