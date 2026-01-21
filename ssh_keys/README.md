# SFTP Server Setup

This directory contains SSH keys for the SFTP server used to receive CML data from MNO simulators.

## Directory Structure

```
ssh_keys/
├── README.md                  # This file
├── id_rsa                     # Private key for MNO simulator (client)
├── id_rsa.pub                 # Public key for MNO simulator
├── authorized_keys            # Public keys authorized to connect
├── sftp_host_ed25519_key      # SFTP server host key (Ed25519)
├── sftp_host_ed25519_key.pub  # SFTP server public host key
├── sftp_host_rsa_key          # SFTP server host key (RSA)
├── sftp_host_rsa_key.pub      # SFTP server public host key
└── known_hosts                # Known hosts file for MNO simulator
```

## Generating Keys

Run the setup script to generate all required SSH keys:

```bash
./generate_ssh_keys.sh
```

This will create:
1. **Client key pair** (`id_rsa`, `id_rsa.pub`) - Used by MNO simulator to authenticate
2. **Server host keys** - Used by SFTP server to identify itself
3. **authorized_keys** - Contains public keys allowed to connect
4. **known_hosts** - Contains SFTP server's host key for client verification

## Security Notes

⚠️ **Important**: These keys are for **development/testing only**. 

For production:
- Generate new keys with strong passphrases
- Use different keys for each environment
- Never commit private keys to version control
- Consider using secret management tools (Vault, AWS Secrets Manager, etc.)
- Rotate keys regularly

## SFTP Server Configuration

The SFTP server (atmoz/sftp Docker image) is configured with:
- **Username**: `cml_user`
- **UID**: 1001
- **Home directory**: `/home/cml_user`
- **Upload directory**: `/home/cml_user/uploads`
- **Authentication**: SSH key only (no password)

## Testing Connection

Test the SFTP connection manually:

```bash
# From host machine
sftp -P 2222 -i ssh_keys/id_rsa cml_user@localhost

# From within docker network
sftp -i /app/ssh_keys/id_rsa cml_user@sftp_receiver
```

## Troubleshooting

### Permission denied (publickey)
- Check that `authorized_keys` contains the public key
- Verify file permissions (private key should be 600)
- Check SFTP server logs: `docker compose logs sftp_receiver`

### Host key verification failed
- Regenerate `known_hosts`: `ssh-keyscan -p 2222 localhost >> ssh_keys/known_hosts`
- Or: `ssh-keygen -R "[localhost]:2222"`

### Connection refused
- Ensure SFTP server is running: `docker compose ps sftp_receiver`
- Check port 2222 is not in use: `lsof -i :2222`
