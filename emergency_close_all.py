#!/usr/bin/env python3
"""
Hyperliquid 跟单机器人 - 紧急一键平仓脚本
独立于主脚本运行，直接通过API一键撤所有单+平所有仓。
作为终极备用方案，当主脚本无法运行时使用。

用法:
  python3 emergency_close_all.py           # dry-run模式，只查看不操作
  python3 emergency_close_all.py --live    # live模式，实际撤单+平仓
"""

import json
import os
import sys
import time

# Windows GBK兼容
if sys.platform == "win32":
    os.environ.setdefault("PYTHONIOENCODING", "utf-8")
    try:
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")
        sys.stderr.reconfigure(encoding="utf-8", errors="replace")
    except Exception:
        pass

import requests

# ============================================================
# 配置
# ============================================================

def _load_env_var(key, default=""):
    """从环境变量或.env文件读取配置"""
    val = os.environ.get(key)
    if val:
        return val
    if sys.platform == "win32":
        env_path = "C:/hl_copytrade/.env"
    else:
        env_path = os.path.expanduser("~/hl_copytrade/.env")
    if os.path.exists(env_path):
        try:
            with open(env_path, "r", encoding="utf-8") as f:
                for line in f:
                    line = line.strip()
                    if line.startswith(key + "="):
                        return line[len(key)+1:].strip().strip('"').strip("'")
        except Exception:
            pass
    return default

LEADER_ADDR = _load_env_var("HL_LEADER_ADDR") or (_ := None, RuntimeError("未配置HL_LEADER_ADDR"))
USER_MAIN_ADDR = _load_env_var("HL_USER_MAIN_ADDR") or (_ := None, RuntimeError("未配置HL_USER_MAIN_ADDR"))
USER_API_ADDR = _load_env_var("HL_USER_API_ADDR") or (_ := None, RuntimeError("未配置HL_USER_API_ADDR"))
BASE_URL = "https://api.hyperliquid.xyz"

# 私钥加载（与主脚本一致）
def _load_private_key() -> str:
    env_pk = os.environ.get("HL_API_PK")
    if env_pk and env_pk.startswith("0x") and len(env_pk) == 66:
        return env_pk
    if sys.platform == "win32":
        env_path = "C:/hl_copytrade/.env"
    else:
        env_path = os.path.expanduser("~/hl_copytrade/.env")
    if os.path.exists(env_path):
        try:
            with open(env_path, "r", encoding="utf-8") as f:
                for line in f:
                    line = line.strip()
                    if line.startswith("HL_API_PK="):
                        pk = line[len("HL_API_PK="):].strip().strip('"').strip("'")
                        if pk and pk.startswith("0x") and len(pk) == 66:
                            return pk
        except Exception:
            pass
    raise RuntimeError("未找到API私钥：请设置环境变量HL_API_PK或配置.env文件")

USER_API_PK = _load_private_key()


# ============================================================
# API 工具
# ============================================================
def hl_post(payload: dict, timeout: int = 15) -> dict:
    resp = requests.post(f"{BASE_URL}/info", json=payload, timeout=timeout)
    resp.raise_for_status()
    return resp.json()


def get_positions(address: str) -> dict:
    data = hl_post({"type": "clearinghouseState", "user": address})
    result = {}
    for ap in data.get("assetPositions", []):
        pos = ap.get("position", {})
        coin = pos.get("coin", "")
        if coin:
            result[coin] = pos
    return result


def get_open_orders(address: str) -> list:
    data = hl_post({"type": "openOrders", "user": address})
    return data if isinstance(data, list) else []


def create_exchange():
    from hyperliquid.exchange import Exchange
    from eth_account import Account
    wallet = Account.from_key(USER_API_PK)
    exchange = Exchange(wallet=wallet, base_url=BASE_URL, account_address=USER_MAIN_ADDR)
    return exchange


# ============================================================
# 紧急操作
# ============================================================
def emergency_close_all(live_mode: bool = False):
    """一键撤所有单 + 平所有仓"""
    print("=" * 60)
    print(f"  Hyperliquid 紧急一键平仓")
    print(f"  模式: {'LIVE - 实际操作!' if live_mode else 'DRY-RUN - 仅查看'}")
    print(f"  用户地址: {USER_MAIN_ADDR}")
    print("=" * 60)

    # 1. 查看当前状态
    print("\n[1/4] 查询当前持仓...")
    try:
        positions = get_positions(USER_MAIN_ADDR)
    except Exception as e:
        print(f"  获取持仓失败: {e}")
        positions = {}

    active_positions = {c: p for c, p in positions.items() if float(p.get("szi", 0)) != 0}
    if active_positions:
        print(f"  当前持仓: {len(active_positions)} 个")
        for coin, pos in sorted(active_positions.items()):
            szi = float(pos.get("szi", 0))
            direction = "做多" if szi > 0 else "做空"
            print(f"    {coin}: {direction} sz={szi} val=${float(pos.get('positionValue', 0)):.2f}")
    else:
        print("  无持仓")

    # 2. 查看挂单
    print("\n[2/4] 查询当前挂单...")
    try:
        orders = get_open_orders(USER_MAIN_ADDR)
    except Exception as e:
        print(f"  获取挂单失败: {e}")
        orders = []

    if orders:
        print(f"  当前挂单: {len(orders)} 个")
        for o in orders:
            coin = o.get("coin", "?")
            side = "买" if o.get("side") == "B" else "卖"
            reduce_only = "(reduce)" if o.get("reduceOnly") else ""
            print(f"    {coin} {side} sz={o.get('sz', 0)} px={o.get('limitPx', 0)} oid={o.get('oid')} {reduce_only}")
    else:
        print("  无挂单")

    if not active_positions and not orders:
        print("\n无需操作，账户已清空。")
        return

    if not live_mode:
        print("\n[DRY-RUN] 以上为当前状态，加 --live 参数执行实际操作")
        return

    # 3. 确认
    print("\n" + "!" * 60)
    print("  WARNING: 即将执行以下操作:")
    print(f"    - 撤销所有 {len(orders)} 个挂单")
    print(f"    - 市价平掉所有 {len(active_positions)} 个仓位")
    print("!" * 60)

    confirm = input("\n输入 YES 确认执行: ")
    if confirm != "YES":
        print("已取消")
        return

    # 4. 执行
    exchange = create_exchange()

    # 撤单
    print("\n[3/4] 撤销所有挂单...")
    cancelled = 0
    for o in orders:
        coin = o.get("coin", "")
        oid = o.get("oid", 0)
        if not oid:
            continue
        try:
            result = exchange.cancel(coin, oid)
            if isinstance(result, dict) and result.get("status") == "ok":
                print(f"  ✓ 撤单成功 {coin} oid={oid}")
                cancelled += 1
            else:
                print(f"  ✗ 撤单失败 {coin} oid={oid}: {result}")
        except Exception as e:
            print(f"  ✗ 撤单异常 {coin} oid={oid}: {e}")
        time.sleep(0.3)
    print(f"  共撤销 {cancelled}/{len(orders)} 个挂单")

    # 平仓
    print("\n[4/4] 市价平掉所有仓位...")
    closed = 0
    for coin, pos in sorted(active_positions.items()):
        try:
            result = exchange.market_close(coin)
            if isinstance(result, dict) and result.get("status") == "ok":
                print(f"  ✓ 平仓成功 {coin}")
                closed += 1
            else:
                print(f"  ✗ 平仓失败 {coin}: {result}")
        except Exception as e:
            print(f"  ✗ 平仓异常 {coin}: {e}")
        time.sleep(0.5)
    print(f"  共平仓 {closed}/{len(active_positions)} 个")

    print("\n" + "=" * 60)
    print("  紧急操作完成!")
    print("=" * 60)


# ============================================================
# 入口
# ============================================================
if __name__ == "__main__":
    live = "--live" in sys.argv
    emergency_close_all(live_mode=live)
