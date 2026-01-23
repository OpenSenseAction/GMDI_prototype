#!/bin/bash
set -e

echo "========================================="
echo "Running E2E Tests Locally (CI Simulation)"
echo "========================================="
echo ""

# Clean up any existing containers
echo "=== Step 1: Cleanup existing containers ==="
docker compose down -v
echo ""

# Generate SSH keys if they don't exist
echo "=== Step 2: Generate SSH keys ==="
if [ ! -f ssh_keys/id_rsa ]; then
    echo "Generating SSH keys..."
    mkdir -p ssh_keys
    
    # Generate SFTP server host keys
    ssh-keygen -t ed25519 -f ssh_keys/sftp_host_ed25519_key -N "" -C "SFTP host ed25519 key"
    ssh-keygen -t rsa -b 4096 -f ssh_keys/sftp_host_rsa_key -N "" -C "SFTP host RSA key"
    
    # Generate client key for MNO simulator
    ssh-keygen -t rsa -b 4096 -f ssh_keys/id_rsa -N "" -C "MNO client key"
    
    # Create authorized_keys with the client public key
    cp ssh_keys/id_rsa.pub ssh_keys/authorized_keys
    
    # Create known_hosts with server host keys
    echo "sftp_receiver $(cat ssh_keys/sftp_host_ed25519_key.pub)" > ssh_keys/known_hosts
    echo "sftp_receiver $(cat ssh_keys/sftp_host_rsa_key.pub)" >> ssh_keys/known_hosts
    
    # Set correct permissions
    chmod 600 ssh_keys/id_rsa ssh_keys/sftp_host_ed25519_key ssh_keys/sftp_host_rsa_key
    chmod 644 ssh_keys/*.pub ssh_keys/authorized_keys ssh_keys/known_hosts
    
    echo "SSH keys generated"
else
    echo "SSH keys already exist"
fi
ls -la ssh_keys/
echo ""

# Start services
echo "=== Step 3: Start services ==="
docker compose up -d database sftp_receiver parser webserver mno_simulator
echo "Waiting 10 seconds for services to initialize..."
sleep 10
echo ""

# Wait for services to be ready
echo "=== Step 4: Wait for services to be ready ==="

echo "Waiting for database..."
for i in {1..60}; do
    if docker compose exec -T database pg_isready -U myuser >/dev/null 2>&1; then
        break
    fi
    sleep 1
done
echo "✓ Database is ready"

echo "Waiting for webserver..."
for i in {1..30}; do
    if curl -s http://localhost:5000/ >/dev/null 2>&1; then
        break
    fi
    echo -n "."
    sleep 2
done
echo ""
echo "✓ Webserver is ready"

echo "Waiting for SFTP server..."
for i in {1..30}; do
    if nc -z localhost 2222 2>/dev/null; then
        break
    fi
    sleep 1
done
echo "✓ SFTP server is ready"

echo ""
echo "=== Step 5: Check service status ==="
docker compose ps
echo ""

echo "=== Step 6: Wait for MNO simulator first generation cycle (40 seconds) ==="
sleep 40
echo ""

echo "=== Step 7: Check directories ==="
echo "SFTP uploads directory:"
docker compose exec -T sftp_receiver ls -la /home/cml_user/uploads/ || echo "ERROR: Could not list SFTP directory"
echo ""

echo "Parser incoming directory:"
docker compose exec -T parser ls -la /app/data/incoming/ || echo "ERROR: Could not list parser directory"
echo ""

echo "Parser archived directory:"
docker compose exec -T parser ls -la /app/data/archived/ 2>/dev/null || echo "No archived files yet"
echo ""

echo "Parser quarantine directory:"
docker compose exec -T parser ls -la /app/data/quarantine/ 2>/dev/null || echo "No quarantined files yet"
echo ""

echo "=== Step 8: Check database ==="
echo "Checking if data reached the database..."
docker compose exec -T database psql -U myuser -d mydatabase -c "SELECT COUNT(*) as metadata_count FROM cml_metadata;"
docker compose exec -T database psql -U myuser -d mydatabase -c "SELECT COUNT(*) as data_count FROM cml_data;"
echo ""

echo "=== Step 9: Show recent logs ==="
echo "--- Parser logs (last 30 lines) ---"
docker compose logs --tail=30 parser
echo ""

echo "--- MNO Simulator logs (last 30 lines) ---"
docker compose logs --tail=30 mno_simulator
echo ""

echo "--- Database logs (last 20 lines) ---"
docker compose logs --tail=20 database
echo ""

echo "=== Step 10: Run integration tests ==="
docker compose --profile testing run --rm integration_tests

echo ""
echo "=== Test Complete ==="
echo "To view all logs: docker compose logs"
echo "To stop services: docker compose down -v"
