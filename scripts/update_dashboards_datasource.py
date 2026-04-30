"""
One-shot script: add a hidden ${datasource} template variable to every provisioned
dashboard and replace the hardcoded "uid": "PostgreSQL" datasource references with
"uid": "${datasource}".

Usage:
    python scripts/update_dashboards_datasource.py
"""

import json
import glob

DATASOURCE_VARIABLE = {
    "current": {},
    "hide": 2,
    "includeAll": False,
    "multi": False,
    "name": "datasource",
    "options": [],
    "query": "grafana-postgresql-datasource",
    "refresh": 1,
    "type": "datasource",
    "label": "Datasource",
}


def replace_pg_uid(obj):
    """Recursively replace uid='PostgreSQL' datasource refs with '${datasource}'."""
    if isinstance(obj, dict):
        if (
            obj.get("type") == "grafana-postgresql-datasource"
            and obj.get("uid") == "PostgreSQL"
        ):
            obj["uid"] = "${datasource}"
        for v in obj.values():
            replace_pg_uid(v)
    elif isinstance(obj, list):
        for item in obj:
            replace_pg_uid(item)


def add_datasource_var(d):
    tmpl = d.setdefault("templating", {})
    var_list = tmpl.setdefault("list", [])
    if any(v.get("name") == "datasource" for v in var_list):
        return  # already present
    var_list.insert(0, DATASOURCE_VARIABLE)


pattern = "grafana/provisioning/dashboards/definitions/*.json"
for path in sorted(glob.glob(pattern)):
    with open(path) as f:
        d = json.load(f)
    replace_pg_uid(d)
    add_datasource_var(d)
    with open(path, "w") as f:
        json.dump(d, f, indent=4)
    print("Updated {}".format(path))

print("Done.")
