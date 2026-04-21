#!/bin/bash
set -e

# SSH Keys Generation Script for SFTP Server
# This script generates all required SSH keys for the SFTP server and MNO simulator

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

echo "=== Generating SSH Keys for SFTP Server ==="
echo

# Check if keys already exist
if [ -f "id_rsa" ] || [ -f "sftp_host_rsa_key" ]; then
    echo "⚠️  Warning: SSH keys already exist!"
    read -p "Do you want to regenerate them? This will overwrite existing keys. (y/N): " -n 1 -r
    echo
    if [[ ! $REPLY =~ ^[Yy]$ ]]; then
        echo "Aborted. Existing keys preserved."
        exit 0
    fi
    echo "Removing old keys..."
    rm -f id_rsa id_rsa.pub id_rsa_orange_cameroun id_rsa_orange_cameroun.pub sftp_host_*_key* authorized_keys known_hosts
fi

echo "1. Generating client key pair for demo_openmrg (OpenMRG simulator)..."
ssh-keygen -t rsa -b 4096 -f id_rsa -N "" -C "demo_openmrg_client"
echo "✓ Client key pair generated: id_rsa, id_rsa.pub"
echo

echo "1b. Generating client key pair for demo_orange_cameroun (Orange Cameroun simulator)..."
ssh-keygen -t rsa -b 4096 -f id_rsa_orange_cameroun -N "" -C "demo_orange_cameroun_client"
echo "✓ Client key pair generated: id_rsa_orange_cameroun, id_rsa_orange_cameroun.pub"
echo

echo "2. Generating SFTP server host keys..."
# Ed25519 (modern, recommended)
ssh-keygen -t ed25519 -f sftp_host_ed25519_key -N "" -C "sftp_server_host"
echo "✓ Ed25519 host key generated"

# RSA (for compatibility)
ssh-keygen -t rsa -b 4096 -f sftp_host_rsa_key -N "" -C "sftp_server_host"
echo "✓ RSA host key generated"
echo

echo "3. Creating authorized_keys files..."
cat id_rsa.pub > authorized_keys
# Per-user directories for the multi-user SFTP setup
mkdir -p demo_openmrg demo_orange_cameroun
cp id_rsa.pub demo_openmrg/authorized_keys
cp id_rsa_orange_cameroun.pub demo_orange_cameroun/authorized_keys
echo "✓ authorized_keys created"
echo

echo "4. Creating known_hosts file..."
# For Docker internal network (sftp_receiver hostname)
{
    echo -n "sftp_receiver "
    cat sftp_host_ed25519_key.pub
    echo -n "sftp_receiver "
    cat sftp_host_rsa_key.pub
    # Also add localhost entries for testing from host
    echo -n "[localhost]:2222 "
    cat sftp_host_ed25519_key.pub
    echo -n "[localhost]:2222 "
    cat sftp_host_rsa_key.pub
} > known_hosts
echo "✓ known_hosts created"
echo

echo "5. Setting proper file permissions..."
chmod 600 id_rsa id_rsa_orange_cameroun sftp_host_*_key
chmod 644 id_rsa.pub id_rsa_orange_cameroun.pub sftp_host_*_key.pub authorized_keys known_hosts
echo "✓ Permissions set"
echo

echo "=== SSH Keys Generated Successfully ==="
echo
echo "Generated files:"
echo "  - id_rsa                          (demo_openmrg private key)"
echo "  - id_rsa.pub                      (demo_openmrg public key)"
echo "  - id_rsa_orange_cameroun          (demo_orange_cameroun private key)"
echo "  - id_rsa_orange_cameroun.pub      (demo_orange_cameroun public key)"
echo "  - sftp_host_ed25519_key           (SFTP server Ed25519 private key)"
echo "  - sftp_host_ed25519_key.pub       (SFTP server Ed25519 public key)"
echo "  - sftp_host_rsa_key               (SFTP server RSA private key)"
echo "  - sftp_host_rsa_key.pub           (SFTP server RSA public key)"
echo "  - authorized_keys                 (Authorized client keys)"
echo "  - known_hosts                     (Known SFTP server host keys)"
echo
echo "⚠️  SECURITY WARNING:"
echo "   These keys are for DEVELOPMENT/TESTING only!"
echo "   DO NOT use these keys in production."
echo "   DO NOT commit private keys to version control."
echo
echo "Next steps:"
echo "  1. Start the services: docker compose up -d"
echo "  2. Test SFTP (OpenMRG):         sftp -P 2222 -i ssh_keys/id_rsa demo_openmrg@localhost"
echo "  3. Test SFTP (Orange Cameroun): sftp -P 2222 -i ssh_keys/id_rsa_orange_cameroun demo_orange_cameroun@localhost"
echo
