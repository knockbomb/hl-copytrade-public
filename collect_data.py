import json, os, sys, requests
BASE = "https://api.hyperliquid.xyz"

def _load_env_var(key, default=""):
    val = os.environ.get(key)
    if val:
        return val
    env_path = os.path.expanduser("~/hl_copytrade/.env")
    if os.path.exists(env_path):
        try:
            with open(env_path, "r") as f:
                for line in f:
                    line = line.strip()
                    if line.startswith(key + "="):
                        return line[len(key)+1:].strip().strip('"').strip("'")
        except Exception:
            pass
    return default

LEADER = _load_env_var("HL_LEADER_ADDR") or sys.exit("未配置HL_LEADER_ADDR")
USER = _load_env_var("HL_USER_MAIN_ADDR") or sys.exit("未配置HL_USER_MAIN_ADDR")
def post(t, user):
    r = requests.post(f"{BASE}/info", json={"type": t, "user": user}, timeout=15)
    r.raise_for_status()
    return r.json()
print("="*80+"\n1. LEADER POSITIONS\n"+"="*80)
d=post("clearinghouseState",LEADER); print(json.dumps(d,indent=2))
print("="*80+"\n2. LEADER ORDERS\n"+"="*80)
d=post("openOrders",LEADER); print(json.dumps(d,indent=2))
print("="*80+"\n3. USER POSITIONS\n"+"="*80)
d=post("clearinghouseState",USER); print(json.dumps(d,indent=2))
print("="*80+"\n4. USER ORDERS\n"+"="*80)
d=post("openOrders",USER); print(json.dumps(d,indent=2))
print("Done.")
