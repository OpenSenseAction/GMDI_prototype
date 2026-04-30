# Webserver

Flask application serving the GMDI data portal.

## User Management

Users are stored in `configs/users.json`. Each entry maps a **user ID** (which must match the corresponding PostgreSQL role name) to a display name and a hashed password.

```json
{
    "alice": {
        "display_name": "Alice",
        "password_hash": "<hash>"
    }
}
```

### Generating a password hash

Use werkzeug (already installed in the webserver image) to produce a hash:

```bash
python -c "from werkzeug.security import generate_password_hash; print(generate_password_hash('yourpassword'))"
```

Copy the output into `password_hash`. The hash format is `scrypt:32768:8:1$<salt>$<digest>` — werkzeug selects the algorithm and parameters automatically.

### Adding a user

1. Create the PostgreSQL role in the database (see `database/migrations/` for examples).
2. Add an entry to `configs/users.json` with the generated hash.
3. Restart the webserver container (it reads the file at startup).

> **Important:** the user ID in `users.json` must exactly match the PostgreSQL role name, because the webserver issues `SET LOCAL ROLE <user_id>` to scope every DB query to that tenant.

## Running Tests

```bash
docker compose run --rm --no-deps \
  -e DATABASE_URL=postgresql://x:x@localhost/x \
  -e USERS_CONFIG_PATH=/app/configs/users.json \
  -v "$(pwd)/configs:/app/configs:ro" \
  webserver sh -c "pip install pytest pytest-cov && python -m pytest tests/ -v"
```
