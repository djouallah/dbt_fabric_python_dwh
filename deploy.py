import argparse
import json
import re
import subprocess
import time
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor
import yaml

parser = argparse.ArgumentParser()
parser.add_argument("--env", default="prod")
parser.add_argument(
    "--full", action="store_true",
    help="Also deploy the Fabric-native orchestration items: the notebook, variable "
         "library and data pipeline (+ schedule). Default is a MINIMAL deploy of just the "
         "lakehouse, warehouse and semantic model. The real orchestration is GitHub Actions; "
         "the Fabric notebook/pipeline are only a fun demo of in-Fabric scheduling.",
)
args = parser.parse_args()

root       = Path(__file__).parent
all_cfg    = yaml.safe_load((root / "deploy_config.yml").read_text())
if args.env not in all_cfg:
    raise SystemExit(f"No '{args.env}' section in deploy_config.yml. Add it for this branch.")
cfg        = {**all_cfg.get("defaults", {}), **all_cfg[args.env]}
WS_ID      = cfg["ws"]
LH_NAME    = cfg["lakehouse_name"]
WH_NAME    = cfg["warehouse_name"]
dbt        = root / "dbt"

print(f"Deploy scope: {'FULL (lh + wh + semantic model + notebook/VL/pipeline)' if args.full else 'MINIMAL (lh + wh + semantic model)'}")

# Derive item names from fabric_items/ folder names
fabric_items = root / "fabric_items"
def find_item(item_type):
    matches = list(fabric_items.glob(f"*.{item_type}"))
    if len(matches) != 1:
        raise SystemExit(f"Expected exactly one {item_type} in fabric_items/, found {len(matches)}")
    return matches[0].name.removesuffix(f".{item_type}")

NB_NAME  = find_item("Notebook")
PL_NAME  = find_item("DataPipeline")
SM_NAME  = find_item("SemanticModel")
VL_NAME  = find_item("VariableLibrary")

# Resolve workspace name from ID (ws can be renamed; ID is stable)
result = subprocess.run(
    ["fab", "api", "-X", "get", f"workspaces/{WS_ID}"],
    capture_output=True, text=True, check=True, cwd=str(root),
)
ws = json.loads(result.stdout)["text"]["displayName"]
print(f"Resolved workspace: {ws} ({WS_ID})")

LAKEHOUSE = f"{ws}.Workspace/{LH_NAME}.Lakehouse"
WAREHOUSE = f"{ws}.Workspace/{WH_NAME}.Warehouse"
NOTEBOOK  = f"{ws}.Workspace/{NB_NAME}.Notebook"
PIPELINE  = f"{ws}.Workspace/{PL_NAME}.DataPipeline"


def fab(args, cwd=root):
    subprocess.run(["fab"] + args, check=True, cwd=str(cwd))


# Extract source workspace_id and item_id from the bim file OneLake URL (placeholder dev GUIDs)
bim_path = root / "fabric_items" / f"{SM_NAME}.SemanticModel" / "model.bim"
bim_text = bim_path.read_text()
url_match = re.search(r'onelake\.dfs\.fabric\.microsoft\.com/([0-9a-f-]{36})/([0-9a-f-]{36})', bim_text)
if not url_match:
    raise SystemExit("Could not find OneLake URL with workspace/item GUIDs in model.bim")
source_ws_id = url_match.group(1)
source_item_id = url_match.group(2)
print(f"Source workspace ID: {source_ws_id}")
print(f"Source item ID:      {source_item_id}")


def get_item_id(path):
    """Get an item's ID using fab get -q id."""
    r = subprocess.run(["fab", "get", path, "-q", "id"],
                       capture_output=True, text=True, check=True, cwd=str(root))
    return r.stdout.strip()


def fab_deploy(item_types):
    """Write a temporary fab deploy config and run deploy, then clean up."""
    content = (
        'core:\n'
        f'  workspace: "{ws}"\n'
        '  repository_directory: "./fabric_items"\n'
        '  item_types_in_scope:\n'
    )
    for t in item_types:
        content += f'    - {t}\n'
    tmp = root / "_fab_deploy_tmp.yml"
    tmp.write_text(content)
    try:
        fab(["deploy", "--config", tmp.name, "-f"])
    finally:
        tmp.unlink(missing_ok=True)


# 1. Lakehouse: provision if missing, keep if it exists. It holds the raw CSVs + archive
#    log the models read via OPENROWSET.
print("=== 1. Ensure lakehouse ===")
exists_result = subprocess.run(["fab", "exists", LAKEHOUSE],
                               capture_output=True, text=True, cwd=str(root))
if "true" not in exists_result.stdout.lower():
    fab(["create", LAKEHOUSE, "-P", "enableSchemas=true"])
    print("New lakehouse — waiting 60s for provisioning...")
    time.sleep(60)
else:
    print(f"Lakehouse '{LH_NAME}' already exists, skipping create.")
target_lh_id = get_item_id(LAKEHOUSE)
print(f"Lakehouse ID: {target_lh_id}")

# 2. Ensure the Warehouse exists (the only storage item this project provisions).
print("=== 2. Ensure warehouse ===")
wh_exists = subprocess.run(["fab", "exists", WAREHOUSE],
                           capture_output=True, text=True, cwd=str(root))
if "true" not in wh_exists.stdout.lower():
    fab(["create", WAREHOUSE])
    print("New warehouse — waiting 60s for provisioning...")
    time.sleep(60)
else:
    print(f"Warehouse '{WH_NAME}' already exists, reusing.")
target_wh_id = get_item_id(WAREHOUSE)
print(f"Warehouse ID: {target_wh_id}")

# 3. (full only) Deploy notebook + variable library (variables.json rewritten per env,
#    reverted after). These drive the in-Fabric orchestration demo; GitHub Actions is the
#    real orchestrator, so they are skipped in the default minimal deploy.
if args.full:
    print("=== 3. Deploy notebook + variable library ===")
    vl_path = fabric_items / f"{VL_NAME}.VariableLibrary" / "variables.json"
    vl_variables = {
        "$schema": "https://developer.microsoft.com/json-schemas/fabric/item/variableLibrary/definition/variables/1.0.0/schema.json",
        "variables": [
            {"name": "download_limit", "type": "String", "value": cfg["download_limit"]},
            {"name": "process_limit",  "type": "String", "value": cfg["process_limit"]},
            {"name": "lakehouse_name", "type": "String", "value": LH_NAME},
            {"name": "warehouse_name", "type": "String", "value": WH_NAME},
            {"name": "workspace_id",   "type": "String", "value": WS_ID},
        ],
    }
    vl_path.write_text(json.dumps(vl_variables, indent=4))
    try:
        fab_deploy(["Notebook", "VariableLibrary"])
    finally:
        subprocess.run(["git", "checkout", str(vl_path)], cwd=str(root))

    target_nb_id = get_item_id(NOTEBOOK)
    print(f"Target notebook ID:  {target_nb_id}")

    # 4. (full only) Copy dbt files to the EXISTING lakehouse OneLake Files (the notebook
    #    reads the project from Files/dbt). Skip build artifacts (target/, logs/).
    print("=== 4. Copy dbt files to OneLake ===")
    SKIP_DIRS = {"target", "logs"}
    files = [f for f in dbt.rglob("*")
             if f.is_file() and not (set(f.relative_to(dbt).parts) & SKIP_DIRS)]

    dirs = set()
    for f in files:
        p = f.relative_to(root).parent
        while p.parts:
            dirs.add(p.as_posix())
            p = p.parent

    for d in sorted(dirs):
        subprocess.run(["fab", "mkdir", f"{LAKEHOUSE}/Files/{d}"], cwd=str(root))

    def copy_file(f):
        rel = f.relative_to(root)
        fab(["cp", rel.as_posix(), f"{LAKEHOUSE}/Files/{rel.parent.as_posix()}/", "-f"])

    with ThreadPoolExecutor(max_workers=8) as executor:
        # Consume the iterator so a failed `fab cp` re-raises here (lazy map silently swallows).
        list(executor.map(copy_file, files))

# 5. Deploy semantic model (Direct Lake on the WAREHOUSE: repoint the OneLake GUID to the
#    warehouse item id, not the lakehouse). Skips fab_deploy if the post-substitution .bim
#    matches the one cached in OneLake from the previous deploy.
print("=== 5. Deploy semantic model ===")
bim_path.write_text(bim_text.replace(source_ws_id, WS_ID).replace(source_item_id, target_wh_id))
local_bim = bim_path.read_bytes()
remote_bim_path = f"{LAKEHOUSE}/Files/semanticmodel/{SM_NAME}.bim"

cache_path = root / "_remote_bim_cache.bim"
cache_path.unlink(missing_ok=True)
download = subprocess.run(
    ["fab", "cp", remote_bim_path, cache_path.as_posix(), "-f"],
    capture_output=True, text=True, cwd=str(root),
)
unchanged = (
    download.returncode == 0
    and cache_path.exists()
    and cache_path.read_bytes() == local_bim
)
cache_path.unlink(missing_ok=True)

try:
    if unchanged:
        print("SemanticModel definition matches cached deploy, skipping fab_deploy.")
    else:
        for attempt in range(1, 4):
            try:
                fab_deploy(["SemanticModel"])
                break
            except subprocess.CalledProcessError:
                if attempt == 3:
                    raise
                print(f"SemanticModel deploy attempt {attempt} failed (likely mid-refresh); waiting 45s and retrying...")
                time.sleep(45)
        subprocess.run(["fab", "mkdir", f"{LAKEHOUSE}/Files/semanticmodel"], cwd=str(root))
        fab(["cp", str(bim_path), remote_bim_path, "-f"])
finally:
    subprocess.run(["git", "checkout", str(bim_path)], cwd=str(root))

# 6. (full only) Deploy DataPipeline + set notebook reference + schedule.
if args.full:
    print("=== 6. Deploy pipeline ===")
    fab_deploy(["DataPipeline"])

    print("=== 6b. Set notebook on pipeline ===")
    for i in (0, 1):
        fab(["set", PIPELINE, "-q",
             f"definition.parts[0].payload.properties.activities[{i}].typeProperties.notebookId",
             "-i", target_nb_id, "-f"])
        fab(["set", PIPELINE, "-q",
             f"definition.parts[0].payload.properties.activities[{i}].typeProperties.workspaceId",
             "-i", WS_ID, "-f"])

    # Reconcile to EXACTLY ONE schedule on the pipeline (REST API with jobType=Pipeline).
    pl_id = get_item_id(PIPELINE)
    sched_url = f"workspaces/{WS_ID}/items/{pl_id}/jobs/Pipeline/schedules"
    result = subprocess.run(["fab", "api", "-X", "get", sched_url],
                            capture_output=True, text=True, cwd=str(root))
    try:
        body = json.loads(result.stdout)
    except json.JSONDecodeError:
        body = {}
    code = body.get("status_code")
    text = body.get("text") if isinstance(body.get("text"), dict) else {}

    if code != 200:
        print(f"::warning::schedule list returned {code} "
              f"({text.get('errorCode')}: {text.get('message')}). Skipping schedule "
              f"management to avoid duplicates — remove the offending schedule in the "
              f"Fabric portal (Pipeline → Schedule) so the list works again.")
    else:
        schedules = text.get("value") or []
        if not schedules:
            print("No existing schedule, creating one.")
            fab(["job", "run-sch", PIPELINE,
                 "--type", "cron", "--interval", cfg["schedule_interval"],
                 "--start", cfg["schedule_start"], "--end", cfg["schedule_end"], "--enable"])
        else:
            schedules.sort(key=lambda s: (
                (s.get("configuration") or {}).get("type") != "Cron",
                not s.get("enabled"),
                s.get("createdDateTime", ""),
            ))
            keep, extras = schedules[0], schedules[1:]
            if extras:
                print(f"Pipeline has {len(schedules)} schedules; keeping {keep['id']}, "
                      f"removing {len(extras)} extra(s).")
                for s in extras:
                    fab(["job", "run-rm", PIPELINE, "--id", s["id"], "-f"])
            else:
                print(f"Pipeline already has exactly one schedule ({keep['id']}), skipping.")

# 7. Refresh semantic model (OneLake permission propagation lags the deploy).
print("=== 7. Refresh semantic model ===")
SEMANTIC_MODEL = f"{ws}.Workspace/{SM_NAME}.SemanticModel"
sm_id = get_item_id(SEMANTIC_MODEL)
for attempt in range(1, 4):
    try:
        fab(["api", "-A", "powerbi", "-X", "post", f"groups/{WS_ID}/datasets/{sm_id}/refreshes"])
        break
    except subprocess.CalledProcessError:
        if attempt == 3:
            raise
        print(f"Refresh attempt {attempt} failed (likely OneLake security still "
              f"propagating); waiting 60s and retrying...")
        time.sleep(60)


print("=== Deploy complete ===")
