"""
Grafana bootstrap script — run once at stack startup.

Creates the per-tenant Grafana Organisations and pre-creates Grafana users so
each user is automatically placed in their correct org with Viewer role.

ORGS and USERS are generated from users.yml by scripts/generate_config.py.
Do not edit them by hand — run the generator instead.

Org 1 datasource is provisioned from grafana/provisioning/datasources/postgres.yml.
Datasources for additional orgs are created via the Grafana API by this script
(those orgs do not exist when Grafana starts, so provisioning cannot cover them).
"""

import json
import sys
import time
from pathlib import Path

import requests

GRAFANA_URL = "http://grafana:3000"
ADMIN_AUTH = ("admin", "admin")

ORGS = [
    {"id": 1, "name": "demo_openmrg"},
    {"id": 2, "name": "demo_orange_cameroun"},
]

USERS = [
    {"login": "demo_openmrg", "org_id": 1, "role": "Viewer"},
    {"login": "demo_orange_cameroun", "org_id": 2, "role": "Viewer"},
]

ADMIN_ORG = None

# Dashboards imported only into the admin org (they query across all tenants).
ADMIN_DASHBOARD_DIR = Path("/admin_dashboards")
# Read-all DB role behind the admin org's datasource (see database/init.sql).
ADMIN_DB_USER = "webserver_role"
ADMIN_DB_PASSWORD = "webserverpassword"


def wait_for_grafana(timeout=120):
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            r = requests.get(GRAFANA_URL + "/api/health", timeout=5)
            if r.status_code == 200 and r.json().get("database") == "ok":
                print("Grafana is up.")
                return
        except Exception:
            pass
        print("Waiting for Grafana...")
        time.sleep(3)
    print("ERROR: Grafana did not become healthy in time.", file=sys.stderr)
    sys.exit(1)


def get_or_create_org(org_id, org_name):
    """Ensure an org with the given numeric id and name exists."""
    r = requests.get(f"{GRAFANA_URL}/api/orgs/{org_id}", auth=ADMIN_AUTH)
    if r.status_code == 200:
        # A name mismatch means two configs claim one org id (tenant vs admin org).
        if r.json().get("name") != org_name:
            print(
                f"ERROR: org {org_id} is named {r.json().get('name')!r}, expected "
                f"{org_name!r}; refusing to share it.",
                file=sys.stderr,
            )
            sys.exit(1)
        print(f"Org {org_id} ({org_name}) already exists.")
        return
    # Org id=1 always exists; for id>=2 we create by name.
    r = requests.post(
        f"{GRAFANA_URL}/api/orgs",
        json={"name": org_name},
        auth=ADMIN_AUTH,
    )
    if r.status_code != 200:
        print(
            f"ERROR creating org {org_name}: {r.status_code} {r.text}", file=sys.stderr
        )
        sys.exit(1)
    if r.json().get("orgId") != org_id:
        print(
            f"ERROR: org {org_name} got id {r.json().get('orgId')}, expected {org_id}.",
            file=sys.stderr,
        )
        sys.exit(1)
    print(f"Created org {org_name}.")


def rename_default_org():
    """Rename org 1 from 'Main Org.' to 'demo_openmrg'."""
    r = requests.put(
        f"{GRAFANA_URL}/api/orgs/1",
        json={"name": "demo_openmrg"},
        auth=ADMIN_AUTH,
    )
    if r.status_code not in (200,):
        print(f"WARN: could not rename org 1: {r.status_code} {r.text}")
    else:
        print("Renamed org 1 to demo_openmrg.")


def get_or_create_user(login, org_id, role):
    """
    Pre-create the Grafana user (so we can control org membership before the
    user ever logs in via the auth proxy).
    """
    # Look up by login
    r = requests.get(
        f"{GRAFANA_URL}/api/users/lookup?loginOrEmail={login}", auth=ADMIN_AUTH
    )
    if r.status_code == 200:
        user_id = r.json()["id"]
        print(f"User {login} already exists (id={user_id}).")
    elif r.status_code == 404:
        # Create the user — password is irrelevant (auth proxy is used at runtime)
        r2 = requests.post(
            f"{GRAFANA_URL}/api/admin/users",
            json={
                "login": login,
                "name": login,
                "password": "change-me-proxy-only",
                "OrgId": org_id,
            },
            auth=ADMIN_AUTH,
        )
        if r2.status_code not in (200,):
            print(
                f"ERROR creating user {login}: {r2.status_code} {r2.text}",
                file=sys.stderr,
            )
            sys.exit(1)
        user_id = r2.json()["id"]
        print(f"Created user {login} (id={user_id}).")
    else:
        print(
            f"ERROR looking up user {login}: {r.status_code} {r.text}", file=sys.stderr
        )
        sys.exit(1)

    assign_user_to_org(user_id, login, org_id, role)


def assign_user_to_org(user_id, login, org_id, role):
    """Remove user from all other orgs, then add/confirm in target org."""
    # Get current orgs for this user
    r = requests.get(f"{GRAFANA_URL}/api/users/{user_id}/orgs", auth=ADMIN_AUTH)
    current_orgs = {o["orgId"] for o in r.json()} if r.status_code == 200 else set()

    # Ensure user is in the target org with the correct role
    if org_id not in current_orgs:
        r2 = requests.post(
            f"{GRAFANA_URL}/api/orgs/{org_id}/users",
            json={"loginOrEmail": login, "role": role},
            auth=ADMIN_AUTH,
        )
        if r2.status_code not in (200,):
            print(
                f"WARN: could not add {login} to org {org_id}: {r2.status_code} {r2.text}"
            )
    else:
        # Patch role to be sure
        requests.patch(
            f"{GRAFANA_URL}/api/orgs/{org_id}/users/{user_id}",
            json={"role": role},
            auth=ADMIN_AUTH,
        )
        print(f"User {login} already in org {org_id}.")

    # Remove from all orgs that are not the target
    for other_org_id in current_orgs - {org_id}:
        r3 = requests.delete(
            f"{GRAFANA_URL}/api/orgs/{other_org_id}/users/{user_id}",
            auth=ADMIN_AUTH,
        )
        print(f"Removed {login} from org {other_org_id}: {r3.status_code}")

    # Set this org as the user's current/default org
    requests.post(
        f"{GRAFANA_URL}/api/users/{user_id}/using/{org_id}",
        auth=ADMIN_AUTH,
    )
    print(f"Set org {org_id} as default for {login}.")


def trigger_provisioning_reload():
    """Ask Grafana to re-read its provisioning files (datasources + dashboards)."""
    for resource in ("datasources", "dashboards"):
        r = requests.post(
            f"{GRAFANA_URL}/api/admin/provisioning/{resource}/reload",
            auth=ADMIN_AUTH,
        )
        print(f"Reload {resource}: {r.status_code}")


def create_datasource_for_org(org_id, name, uid, user, password):
    """Create a PostgreSQL datasource for the given org via API."""
    r = requests.post(
        f"{GRAFANA_URL}/api/datasources",
        json={
            "name": name,
            "uid": uid,
            "type": "grafana-postgresql-datasource",
            "access": "proxy",
            "url": "database:5432",
            "database": "mydatabase",
            "user": user,
            "secureJsonData": {"password": password},
            "jsonData": {"sslmode": "disable"},
            "isDefault": True,
            "editable": False,
        },
        auth=ADMIN_AUTH,
        headers={"X-Grafana-Org-Id": str(org_id)},
    )
    if r.status_code in (200,):
        print(f"Created datasource '{name}' for org {org_id}.")
    elif r.status_code == 409:
        print(f"Datasource '{name}' already exists in org {org_id}.")
    else:
        print(
            f"WARN: could not create datasource for org {org_id}: {r.status_code} {r.text}"
        )


def copy_dashboards_to_org(target_org_id, source_org_id=1):
    """Copy all dashboards from source_org_id into target_org_id via API."""
    r = requests.get(
        f"{GRAFANA_URL}/api/search?type=dash-db",
        auth=ADMIN_AUTH,
        headers={"X-Grafana-Org-Id": str(source_org_id)},
    )
    if r.status_code != 200:
        print(
            f"WARN: could not list dashboards in org {source_org_id}: {r.status_code}"
        )
        return
    dashboards = r.json()
    print(
        f"Copying {len(dashboards)} dashboards from org {source_org_id} to org {target_org_id}..."
    )
    for db in dashboards:
        uid = db.get("uid")
        if not uid:
            continue
        r2 = requests.get(
            f"{GRAFANA_URL}/api/dashboards/uid/{uid}",
            auth=ADMIN_AUTH,
            headers={"X-Grafana-Org-Id": str(source_org_id)},
        )
        if r2.status_code != 200:
            print(f"WARN: could not fetch dashboard {uid}: {r2.status_code}")
            continue
        dashboard_json = r2.json()["dashboard"]
        dashboard_json.pop("id", None)  # remove source org's internal id
        r3 = requests.post(
            f"{GRAFANA_URL}/api/dashboards/db",
            json={"dashboard": dashboard_json, "overwrite": True, "folderId": 0},
            auth=ADMIN_AUTH,
            headers={"X-Grafana-Org-Id": str(target_org_id)},
        )
        if r3.status_code != 200:
            print(
                f"WARN: could not import dashboard {uid} to org {target_org_id}: {r3.status_code} {r3.text}"
            )
        else:
            print(f"Copied dashboard {uid} to org {target_org_id}.")


def import_admin_dashboards(org_id):
    """Import the cross-tenant dashboards into the admin org only."""
    for path in sorted(ADMIN_DASHBOARD_DIR.glob("*.json")):
        dashboard = json.loads(path.read_text())
        dashboard["id"] = None
        r = requests.post(
            f"{GRAFANA_URL}/api/dashboards/db",
            json={"dashboard": dashboard, "overwrite": True, "folderId": 0},
            auth=ADMIN_AUTH,
            headers={"X-Grafana-Org-Id": str(org_id)},
        )
        if r.status_code != 200:
            print(f"WARN: could not import {path.name}: {r.status_code} {r.text}")
        else:
            print(f"Imported {path.name} into org {org_id}.")


if __name__ == "__main__":
    wait_for_grafana()
    rename_default_org()
    for org in ORGS[1:]:  # org 1 always exists; create 2+
        get_or_create_org(org["id"], org["name"])
    # Create a datasource via API for every org whose id > 1.
    # Org 1 datasource is provisioned from the YAML file (postgres.yml); it
    # cannot be provisioned here because provisioning runs before init_grafana.
    # Additional orgs do not exist until this script creates them, so their
    # datasources must be created via the API after the org exists.
    for org in ORGS[1:]:
        create_datasource_for_org(
            org_id=org["id"],
            name="PostgreSQL",
            uid=f'ds_{org["name"]}',
            user=org["name"],
            password=f'{org["name"]}_password',
        )
    # Reload provisioning then copy dashboards from org 1 into all other orgs
    time.sleep(2)
    trigger_provisioning_reload()
    time.sleep(2)
    for org in ORGS[1:]:
        copy_dashboards_to_org(target_org_id=org["id"], source_org_id=1)
    if ADMIN_ORG:
        get_or_create_org(ADMIN_ORG["id"], ADMIN_ORG["name"])
        create_datasource_for_org(
            org_id=ADMIN_ORG["id"],
            name="PostgreSQL",
            uid="ds_admin",
            user=ADMIN_DB_USER,
            password=ADMIN_DB_PASSWORD,
        )
        import_admin_dashboards(ADMIN_ORG["id"])
    for user in USERS:
        get_or_create_user(user["login"], user["org_id"], user["role"])
    print("Grafana bootstrap complete.")
