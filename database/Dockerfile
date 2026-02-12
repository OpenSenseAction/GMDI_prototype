FROM timescale/timescaledb:latest-pg13

# Set environment variables for TimescaleDB
ENV POSTGRES_DB=mydatabase
ENV POSTGRES_USER=myuser
ENV POSTGRES_PASSWORD=mypassword

# Copy the initialization script to set up the database schema
# Files are executed in alphabetical order, so we prefix with numbers
COPY init.sql /docker-entrypoint-initdb.d/01-init-schema.sql

EXPOSE 5432