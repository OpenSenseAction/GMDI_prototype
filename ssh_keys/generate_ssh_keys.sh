#!/bin/bash
set -e

# SSH Keys Generation Script for SFTP Server
# This script generates all required SSH keys for the SFTP server and MNO simulator


SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}" )" && pwd)"
cd "$SCRIPT_DIR"

echo "=== Generating SSH Keys for SFTP Server (Multi-tenant) ==="
echo

# List of tenants (add new tenants here)
TENANTS=(demo_openmrg demo_orange_cameroun)

# Remove all old keys if any exist
if ls id_rsa* sftp_host_*_key* 1>/dev/null 2>&1; then
    echo "⚠️  Warning: SSH keys already exist!"
    read -p "Do you want to regenerate them? This will overwrite existing keys. (y/N): " -n 1 -r
    echo
    if [[ ! $REPLY =~ ^[Yy]$ ]]; then
        echo "Aborted. Existing keys preserved."
        exit 0
    fi
    echo "Removing old keys..."
    rm -f id_rsa* sftp_host_*_key* authorized_keys known_hosts
    for t in "${TENANTS[@]}"; do
        rm -f "$t/authorized_keys"
    done
fi

# Generate client key pairs and authorized_keys for each tenant
for TENANT in "${TENANTS[@]}"; do
    KEY_NAME="id_rsa_${TENANT}"
    DIR_NAME="$TENANT"
    mkdir -p "$DIR_NAME"
    if [ ! -f "$KEY_NAME" ]; then
        echo "Generating SSH key for $TENANT ..."
        ssh-keygen -t rsa -b 4096 -f "$KEY_NAME" -N "" -C "${TENANT}_client"
    else
        echo "Key for $TENANT already exists, skipping generation."
    fi
    cp "${KEY_NAME}.pub" "$DIR_NAME/authorized_keys"
    echo "✓ $TENANT: $KEY_NAME, $DIR_NAME/authorized_keys"
done
echo

# For backward compatibility: create id_rsa for demo_openmrg
if [ -f id_rsa_demo_openmrg ]; then
    cp id_rsa_demo_openmrg id_rsa
    cp id_rsa_demo_openmrg.pub id_rsa.pub
fi

# Generate SFTP server host keys
echo "Generating SFTP server host keys..."
ssh-keygen -t ed25519 -f sftp_host_ed25519_key -N "" -C "sftp_server_host"
ssh-keygen -t rsa -b 4096 -f sftp_host_rsa_key -N "" -C "sftp_server_host"
echo "✓ SFTP server host keys generated"
echo

# Create known_hosts file
echo "Creating known_hosts file..."
{
    echo -n "sftp_receiver "
    cat sftp_host_ed25519_key.pub
    echo -n "sftp_receiver "
    cat sftp_host_rsa_key.pub
    echo -n "[localhost]:2222 "
    cat sftp_host_ed25519_key.pub
    echo -n "[localhost]:2222 "
    cat sftp_host_rsa_key.pub
} > known_hosts
echo "✓ known_hosts created"
echo

# Set permissions
chmod 600 id_rsa* sftp_host_*_key
chmod 644 id_rsa*.pub sftp_host_*_key.pub known_hosts
for TENANT in "${TENANTS[@]}"; do
    chmod 644 "$TENANT/authorized_keys"
done
echo "✓ Permissions set"
echo

echo "=== SSH Keys Generated Successfully ==="
echo "Tenants: ${TENANTS[*]}"
echo
echo "Generated files:"
for TENANT in "${TENANTS[@]}"; do
    echo "  - id_rsa_${TENANT} (private key)"
    echo "  - id_rsa_${TENANT}.pub (public key)"
    echo "  - $TENANT/authorized_keys (authorized_keys for $TENANT)"
done
echo "  - sftp_host_ed25519_key / .pub (SFTP server Ed25519 key)"
echo "  - sftp_host_rsa_key / .pub (SFTP server RSA key)"
echo "  - known_hosts (Known SFTP server host keys)"
echo
echo "⚠️  SECURITY WARNING:"
echo "   These keys are for DEVELOPMENT/TESTING only!"
echo "   DO NOT use these keys in production."
echo "   DO NOT commit private keys to version control."
echo
echo "Next steps:"
echo "  1. Start the services: docker compose up -d"
for TENANT in "${TENANTS[@]}"; do
    echo "  2. Test SFTP ($TENANT): sftp -P 2222 -i ssh_keys/id_rsa_${TENANT} $TENANT@localhost"
done
echo
