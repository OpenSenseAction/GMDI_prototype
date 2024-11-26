FROM timescale/timescaledb:latest-pg13

# Set environment variables for TimescaleDB
ENV POSTGRES_DB=mydatabase
ENV POSTGRES_USER=myuser
ENV POSTGRES_PASSWORD=mypassword

# Copy the initialization script to set up the database schema
COPY init.sql /docker-entrypoint-initdb.d/

EXPOSE 5432