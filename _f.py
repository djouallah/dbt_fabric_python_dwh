import struct, json, urllib.request, subprocess
import pyodbc
WS="d5f25039-7f33-4668-b909-2c397b2626fb"
def tok(r): return subprocess.check_output(f'az account get-access-token --resource {r} --query accessToken -o tsv',text=True,shell=True).strip()
pbi=tok("https://analysis.windows.net/powerbi/api")
req=urllib.request.Request(f"https://api.fabric.microsoft.com/v1/workspaces/{WS}/warehouses",headers={"Authorization":f"Bearer {pbi}"})
server=next(w for w in json.load(urllib.request.urlopen(req,timeout=60))["value"] if w["displayName"]=="aemo")["properties"]["connectionString"]
db=tok("https://database.windows.net").encode("utf-16-le"); ts=struct.pack(f"<I{len(db)}s",len(db),db)
drv=[d for d in pyodbc.drivers() if "ODBC Driver" in d][-1]
cur=pyodbc.connect(f"DRIVER={{{drv}}};SERVER={server};DATABASE=aemo;Encrypt=yes;",attrs_before={1256:ts},timeout=120).cursor()
print("distinct files already ingested per fact table:")
for t in ["landing.fct_scada","landing.fct_price","landing.fct_scada_today","landing.fct_price_today"]:
    f=cur.execute(f"SELECT COUNT(DISTINCT [file]) FROM {t}").fetchone()[0]
    n=cur.execute(f"SELECT COUNT_BIG(*) FROM {t}").fetchone()[0]
    print(f"  {t:26s} files={f:>6}  rows={n:>12,}")
