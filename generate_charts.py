#!/usr/bin/env python3
"""
HL跟单效果追踪系统 - 可视化仪表盘 P0
======================================
重构内容:
1. 归一化净值曲线 (起始=1.0, 可横向对比)
2. 时间范围切换 (前端JS过滤)
3. 全账户回撤曲线 (4条线)
4. 风险指标矩阵 (4账户×6指标)
5. 核心指标速览 (KPI Matrix)
6. 事件标注 (chartjs-plugin-annotation)
7. 持仓信息面板
8. 交易统计卡
9. 数据预计算 (Python端计算, JSON嵌入HTML)
"""

import sqlite3
import time
import json
import math
import os
import sys
import traceback
from datetime import datetime, timedelta
from pathlib import Path

from paths import BASE_DIR, DATA_DIR
from shared_config import get_accounts, get_instances
import urllib.request

HL_API = "https://api.hyperliquid.xyz/info"


def hl_api_call(payload):
    """调用 Hyperliquid API"""
    try:
        req = urllib.request.Request(
            HL_API,
            data=json.dumps(payload).encode(),
            headers={"Content-Type": "application/json"}
        )
        with urllib.request.urlopen(req, timeout=10) as resp:
            return json.loads(resp.read().decode())
    except Exception as e:
        print(f"API call failed: {e}")
        return None


def get_ledger_entries(address, start_time_ms=None):
    """从HL ledger API获取真实资金进出记录"""
    result = hl_api_call({"type": "userNonFundingLedgerUpdates", "user": address})
    if not result:
        return []

    entries = result if isinstance(result, list) else []
    
    deposit_types = {"deposit", "deposit_internal"}
    withdraw_types = {"withdraw", "withdraw_internal"}

    flows = []
    for entry in entries:
        delta = entry.get("delta", {})
        entry_type = delta.get("type", "")
        ts_ms = int(entry.get("time", 0))

        if start_time_ms and ts_ms < start_time_ms:
            continue

        amount = 0
        flow_type = None

        if entry_type in deposit_types:
            amount = float(delta.get("usdc", delta.get("amount", 0)))
            if amount > 0:
                flow_type = "deposit"
        elif entry_type in withdraw_types:
            amount = float(delta.get("usdc", delta.get("amount", 0)))
            if amount > 0:
                flow_type = "withdraw"
        elif entry_type == "send":
            dest = delta.get("destination", "")
            amount = float(delta.get("usdc", delta.get("amount", 0)))
            if amount > 0:
                if dest.lower() == address.lower():
                    flow_type = "deposit"
                else:
                    flow_type = "withdraw"
        else:
            continue

        if flow_type and amount > 0:
            flows.append({
                "ts": ts_ms // 1000,
                "type": flow_type,
                "signed_amount": amount if flow_type == "deposit" else -amount,
            })

    return flows


def load_all_fund_flows():
    """为所有账户加载资金进出记录"""
    accounts = get_accounts()
    all_flows = {}
    for inst_id in list(get_instances()):
        acct = accounts.get(inst_id, {})
        leader_addr = acct.get("leader", "")
        follower_addr = acct.get("follower", "")
        if leader_addr:
            flows = get_ledger_entries(leader_addr)
            all_flows[f"{inst_id}_leader"] = flows
        if follower_addr:
            flows = get_ledger_entries(follower_addr)
            all_flows[f"{inst_id}_follower"] = flows
    return all_flows

DB_PATH = str(DATA_DIR / 'net_value_history.db')
CHART_DIR = str(DATA_DIR / 'charts')
KEY_EVENTS_PATH = str(DATA_DIR / 'key_events.json')
POSITION_STATE_PATH = str(DATA_DIR / 'position_state.json')

# 采样间隔(小时)及年化因子
SAMPLE_INTERVAL_HOURS = 1.45
ANNUALIZE_FACTOR = math.sqrt(365 * 24 / SAMPLE_INTERVAL_HOURS)

# 4个账户标识
ACCOUNT_KEYS = ['lf_leader', 'lf_follower', 'hf_leader', 'hf_follower']
ACCOUNT_LABELS = {
    'lf_leader': '低频主账户',
    'lf_follower': '低频跟单',
    'hf_leader': '高频主账户',
    'hf_follower': '高频跟单',
}
ACCOUNT_COLORS = {
    'lf_leader': '#e74c3c',
    'lf_follower': '#e67e22',
    'hf_leader': '#3498db',
    'hf_follower': '#2ecc71',
}


# ============================================================
# 数据加载
# ============================================================

def load_all_snapshots():
    """加载全部快照数据(不限时间)"""
    db = sqlite3.connect(DB_PATH)
    c = db.cursor()
    c.execute('''
        SELECT ts, instance_id, role, account_value, total_equity,
               position_count, total_notional, copy_ratio
        FROM net_value_snapshots
        ORDER BY ts
    ''')
    rows = c.fetchall()
    db.close()
    return rows


def load_key_events():
    """加载事件列表"""
    try:
        with open(KEY_EVENTS_PATH, 'r') as f:
            return json.load(f)
    except Exception:
        return []


def load_position_state():
    """加载当前持仓"""
    try:
        with open(POSITION_STATE_PATH, 'r') as f:
            return json.load(f)
    except Exception:
        return {}


def _fetch_positions_for_addr(addr):
    """拉取单个地址在所有 DEX (默认加密 + xyz 代币化股票/商品) 上的持仓。"""
    role_positions = {}
    # 默认 DEX="" 为加密永续；"xyz" 为代币化股票/商品（AAPL/GOLD/SILVER/BRENTOIL等）
    for dex in ("", "xyz"):
        payload = {"type": "clearinghouseState", "user": addr}
        if dex:
            payload["dex"] = dex
        r = hl_api_call(payload)
        if not r:
            continue
        for ap in (r.get("assetPositions") or []):
            p = ap.get("position", {})
            try:
                sz = float(p.get("szi", 0) or 0)
            except (TypeError, ValueError):
                sz = 0
            if sz == 0:
                continue
            coin = p.get("coin")
            try:
                entry = float(p.get("entryPx", 0) or 0)
            except (TypeError, ValueError):
                entry = 0.0
            try:
                upnl = float(p.get("unrealizedPnl", 0) or 0)
            except (TypeError, ValueError):
                upnl = 0.0
            role_positions[coin] = {
                "size": sz,
                "entry_px": entry,
                "side": "long" if sz > 0 else "short",
                "unrealized_pnl": upnl,
                "dex": dex or "perp",
            }
    return role_positions


def fetch_live_positions():
    """实时从HL API拉取4个地址的当前持仓（含加密与xyz两个DEX）。全部失败时回退本地文件。"""
    accounts = get_accounts()
    live = {}
    ok = False
    for inst_id, info in accounts.items():
        live[inst_id] = {}
        for role in ("leader", "follower"):
            addr = info.get(role)
            if not addr:
                continue
            role_positions = _fetch_positions_for_addr(addr)
            for _coin, _info in role_positions.items():
                _info["role"] = role
            if role_positions:
                ok = True
            live[inst_id][role] = role_positions
    if ok:
        return live
    try:
        with open(POSITION_STATE_PATH, "r") as f:
            legacy = json.load(f)
        wrapped = {}
        for inst_id, coins in legacy.items():
            wrapped[inst_id] = {"leader": coins}
        return wrapped
    except Exception:
        return {}


def load_trade_stats():
    """从trades表读取交易统计"""
    try:
        db = sqlite3.connect(DB_PATH)
        c = db.cursor()
        c.execute("SELECT COUNT(*) FROM trades WHERE status='closed'")
        closed_count = c.fetchone()[0]
        c.execute("SELECT COUNT(*) FROM trades")
        total_count = c.fetchone()[0]
        
        stats = {
            'total_trades': total_count,
            'closed_trades': closed_count,
            'win_rate': None,
            'avg_pnl_ratio': None,
            'avg_hold_time_hours': None,
        }
        
        if closed_count > 0:
            c.execute("""
                SELECT 
                    AVG(CASE WHEN pnl > 0 THEN 1.0 ELSE 0.0 END) * 100 as win_rate,
                    AVG(pnl_pct) as avg_pnl_pct,
                    AVG((close_ts - open_ts) / 3600.0) as avg_hold_hours
                FROM trades 
                WHERE status='closed'
            """)
            row = c.fetchone()
            if row:
                stats['win_rate'] = row[0]
                stats['avg_pnl_ratio'] = row[1]
                stats['avg_hold_time_hours'] = row[2]
        
        db.close()
        return stats
    except Exception:
        return {
            'total_trades': 0,
            'closed_trades': 0,
            'win_rate': None,
            'avg_pnl_ratio': None,
            'avg_hold_time_hours': None,
        }


# ============================================================
# 数据预计算
# ============================================================

def build_series(rows):
    """将原始数据按账户分组为时间序列"""
    series = {}
    for key in ACCOUNT_KEYS:
        instance_id, role = key.split('_', 1)
        series[key] = {
            'ts': [],        # unix seconds
            'equity': [],    # total_equity
            'av': [],        # account_value
            'pos_count': [],
            'notional': [],
            'copy_ratio': [],
        }
        for row in rows:
            ts, iid, r, av, te, pc, tn, cr = row
            if iid == instance_id and r == role:
                s = series[key]
                s['ts'].append(ts)
                s['equity'].append(te if te else 0)
                s['av'].append(av if av else 0)
                s['pos_count'].append(pc if pc else 0)
                s['notional'].append(tn if tn else 0)
                s['copy_ratio'].append(cr if cr else 0)
    return series


def compute_normalized(series, all_flows=None):
    """归一化净值: TWR-based, 消除资金进出影响"""
    result = {}
    for key in ACCOUNT_KEYS:
        s = series[key]
        eq = s['equity']
        ts_list = s['ts']
        if not eq or eq[0] == 0:
            result[key] = []
            continue
        
        flows = (all_flows or {}).get(key, [])
        cumulative = 1.0
        points = [{'x': ts_list[0] * 1000, 'y': 1.0}]
        
        for i in range(1, len(eq)):
            ts_prev = ts_list[i-1]
            ts_curr = ts_list[i]
            eq_prev = eq[i-1]
            eq_curr = eq[i]
            
            # 计算区间内净资金流
            net_flow = sum(f["signed_amount"] for f in flows if ts_prev < f["ts"] <= ts_curr) if flows else 0
            
            if eq_prev > 0:
                sub_return = (eq_curr - eq_prev - net_flow) / eq_prev
            else:
                sub_return = 0.0
            
            cumulative *= (1 + sub_return)
            points.append({'x': ts_curr * 1000, 'y': cumulative})
        
        result[key] = points
    return result


def compute_drawdowns(series):
    """回撤序列: (current - peak) / peak * 100"""
    result = {}
    for key in ACCOUNT_KEYS:
        eq = series[key]['equity']
        ts = series[key]['ts']
        if not eq:
            result[key] = []
            continue
        dds = []
        peak = eq[0]
        for i, v in enumerate(eq):
            if v > peak:
                peak = v
            dd = (v - peak) / peak * 100 if peak > 0 else 0
            dds.append({'x': ts[i] * 1000, 'y': dd})
        result[key] = dds
    return result


def compute_daily_returns(series):
    """日收益率序列 (相邻采样点收益率)"""
    result = {}
    for key in ACCOUNT_KEYS:
        eq = series[key]['equity']
        ts = series[key]['ts']
        rets = []
        for i in range(1, len(eq)):
            if eq[i - 1] > 0:
                rets.append((eq[i] - eq[i - 1]) / eq[i - 1])
        result[key] = rets
    return result


def compute_risk_metrics(series, daily_returns, all_flows=None):
    """为每个账户计算6个风险指标"""
    metrics = {}
    for key in ACCOUNT_KEYS:
        eq = series[key]['equity']
        ts_list = series[key]['ts']
        rets = daily_returns[key]
        
        # 累计收益率 (使用 TWR 扣除资金进出影响)
        if eq and len(eq) >= 2 and eq[0] > 0:
            flows = (all_flows or {}).get(key, [])
            if flows:
                # TWR: 计算考虑资金进出的收益率
                cumulative_factor = 1.0
                for i in range(1, len(eq)):
                    ts_prev = ts_list[i-1]
                    ts_curr = ts_list[i]
                    eq_prev = eq[i-1]
                    eq_curr = eq[i]
                    # 计算两个快照之间的净资金流
                    net_flow = sum(f["signed_amount"] for f in flows if ts_prev < f["ts"] <= ts_curr)
                    # 子期间收益率 = (期末 - 期初 - 净流入) / 期初
                    if eq_prev > 0:
                        sub_return = (eq_curr - eq_prev - net_flow) / eq_prev
                    else:
                        sub_return = 0.0
                    cumulative_factor *= (1 + sub_return)
                cum_return = (cumulative_factor - 1) * 100
            else:
                # 无资金进出记录，使用简单收益率
                cum_return = (eq[-1] - eq[0]) / eq[0] * 100
        else:
            cum_return = 0
        
        # 最大回撤
        peak = eq[0] if eq else 0
        max_dd = 0
        for v in eq:
            if v > peak:
                peak = v
            dd = (v - peak) / peak * 100 if peak > 0 else 0
            if dd < max_dd:
                max_dd = dd
        
        # 当前回撤
        if eq:
            current_dd = (eq[-1] - max(eq)) / max(eq) * 100 if max(eq) > 0 else 0
        else:
            current_dd = 0
        
        # 年化波动率
        if len(rets) >= 2:
            mean_r = sum(rets) / len(rets)
            var_r = sum((r - mean_r) ** 2 for r in rets) / len(rets)
            ann_vol = math.sqrt(var_r) * ANNUALIZE_FACTOR * 100  # percent
        else:
            ann_vol = 0
        
        # 年化收益率
        if len(ts_list) >= 2:
            actual_days = (ts_list[-1] - ts_list[0]) / 86400
        else:
            actual_days = 1
        if actual_days > 0 and cum_return > -100:
            ann_ret = ((1 + cum_return / 100) ** (365 / actual_days) - 1) * 100
        else:
            ann_ret = 0
        
        # 夏普比率 (Rf=0)
        sharpe = ann_ret / ann_vol if ann_vol > 0 else 0
        
        # 索提诺比率
        neg_rets = [r for r in rets if r < 0]
        if len(neg_rets) >= 2:
            mean_neg = sum(neg_rets) / len(neg_rets)
            var_neg = sum((r - mean_neg) ** 2 for r in neg_rets) / len(neg_rets)
            down_vol = math.sqrt(var_neg) * ANNUALIZE_FACTOR * 100
        else:
            down_vol = 0
        sortino = ann_ret / down_vol if down_vol > 0 else 0
        
        # 卡玛比率
        calmar = ann_ret / abs(max_dd) if max_dd != 0 else 0
        
        metrics[key] = {
            'cum_return': round(cum_return, 2),
            'max_dd': round(max_dd, 2),
            'current_dd': round(current_dd, 2),
            'ann_vol': round(ann_vol, 2),
            'sharpe': round(sharpe, 2),
            'sortino': round(sortino, 2),
            'calmar': round(calmar, 2),
            'ann_ret': round(ann_ret, 2),
        }
    
    return metrics



def compute_account_overview(series, all_flows, max_days=0):
    """Compute account overview data for each account."""
    # 按周期过滤数据
    now_ts = int(time.time())
    cutoff = now_ts - max_days * 86400 if max_days > 0 else 0

    overview = {}
    for key in ACCOUNT_KEYS:
        eq = series[key]['equity']
        ts_list = series[key]['ts']
        cr_list = series[key]['copy_ratio']
        if not eq or len(eq) < 2:
            overview[key] = None
            continue

        # 过滤 series：只保留 >= cutoff 的数据点
        if cutoff > 0:
            filtered_indices = [i for i, t in enumerate(ts_list) if t >= cutoff]
            if len(filtered_indices) < 2:
                overview[key] = None
                continue
            eq = [eq[i] for i in filtered_indices]
            ts_list = [ts_list[i] for i in filtered_indices]
            cr_list = [cr_list[i] for i in filtered_indices]

        start_equity = eq[0]
        end_equity = eq[-1]

        # TWR-based return
        flows = (all_flows or {}).get(key, [])
        # 过滤资金进出：只保留 >= cutoff 的记录
        if cutoff > 0:
            flows = [f for f in flows if f['ts'] >= cutoff]
        # 只保留快照区间内的 flows（第一个快照之前的已包含在 start_equity 中）
        flows_in_range = [f for f in flows if ts_list[0] < f["ts"] <= ts_list[-1]]
        if flows_in_range:
            cumulative_factor = 1.0
            for i in range(1, len(eq)):
                ts_prev = ts_list[i-1]
                ts_curr = ts_list[i]
                eq_prev = eq[i-1]
                eq_curr = eq[i]
                net_flow = sum(f["signed_amount"] for f in flows_in_range if ts_prev < f["ts"] <= ts_curr)
                if eq_prev > 0:
                    sub_return = (eq_curr - eq_prev - net_flow) / eq_prev
                else:
                    sub_return = 0.0
                cumulative_factor *= (1 + sub_return)
            return_pct = (cumulative_factor - 1) * 100
        else:
            return_pct = (end_equity - start_equity) / start_equity * 100 if start_equity > 0 else 0

        # PnL = 期末净值 - 期初净值 - 资金进出 (纯交易盈亏)
        net_flow = sum(f["signed_amount"] for f in flows_in_range)
        pnl = end_equity - start_equity - net_flow

        peak = eq[0]
        max_dd = 0
        for v in eq:
            if v > peak:
                peak = v
            dd = (v - peak) / peak * 100 if peak > 0 else 0
            if dd < max_dd:
                max_dd = dd

        valid_cr = [c for c in cr_list if c and c > 0]
        avg_copy_ratio = sum(valid_cr) / len(valid_cr) if valid_cr else None

        overview[key] = {
            'start_equity': round(start_equity, 2),
            'end_equity': round(end_equity, 2),
            'return_pct': round(return_pct, 2),
            'pnl': round(pnl, 2),
            'net_flow': round(net_flow, 2),
            'max_dd': round(max_dd, 2),
            'avg_copy_ratio': round(avg_copy_ratio, 4) if avg_copy_ratio else None,
        }

    deviation = {}
    for freq in ['lf', 'hf']:
        leader_key = freq + '_leader'
        follower_key = freq + '_follower'
        l_data = overview.get(leader_key)
        f_data = overview.get(follower_key)
        if l_data and f_data:
            dev = f_data['return_pct'] - l_data['return_pct']
            deviation[freq] = round(dev, 2)
        else:
            deviation[freq] = None

    return {
        'accounts': overview,
        'deviation': deviation,
    }


def prepare_events_for_js(events):
    """准备事件数据供前端使用"""
    result = []
    for e in events:
        ev = {
            'ts': e.get('ts', 0) * 1000,
            'type': e.get('type', 'unknown'),
            'instance_id': e.get('instance_id', ''),
            'coin': e.get('coin', ''),
            'side': e.get('side', ''),
            'message': e.get('message', ''),
        }
        result.append(ev)
    # 按时间排序
    result.sort(key=lambda x: x['ts'])
    return result


def prepare_positions(pos_state):
    """准备持仓数据。pos_state: {inst_id: {role: {coin: {...}}}}"""
    positions = []
    role_label = {"leader": "Leader", "follower": "用户"}
    for inst_id in list(get_instances()):
        if inst_id not in pos_state:
            continue
        roles_data = pos_state[inst_id]
        # 检测是否已是 role->coins 结构
        if roles_data and all(k in ("leader", "follower") for k in roles_data.keys()):
            iter_roles = list(roles_data.items())
        else:
            iter_roles = [("leader", roles_data)]
        for role, coins in iter_roles:
            if not isinstance(coins, dict):
                continue
            for coin, info in coins.items():
                size = info.get('size', 0)
                entry_px = info.get('entry_px', 0)
                side = info.get('side', 'unknown')
                notional = abs(size * entry_px) if entry_px else 0
                positions.append({
                    'account': get_instances().get(inst_id, {}).get('display_short', inst_id),
                    'instance_id': inst_id,
                    'role': role,
                    'role_label': role_label.get(role, role),
                    'coin': coin,
                    'side': side,
                    'size': size,
                    'entry_px': entry_px,
                    'notional': round(notional, 2),
                    'unrealized_pnl': round(info.get('unrealized_pnl', 0) or 0, 2),
                })
    return positions


# ============================================================
# HTML模板生成
# ============================================================

def generate_html(precomputed):
    """生成完整的HTML仪表盘"""
    
    data_json = json.dumps(precomputed, ensure_ascii=False, default=str)
    
    html = f'''<!DOCTYPE html>
<html lang="zh-CN">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>HL跟单系统仪表盘</title>
<script src="https://cdn.jsdelivr.net/npm/chart.js@4.4.7/dist/chart.umd.min.js"></script>
<script src="https://cdn.jsdelivr.net/npm/chartjs-adapter-date-fns@3.0.0/dist/chartjs-adapter-date-fns.bundle.min.js"></script>
<script src="https://cdn.jsdelivr.net/npm/chartjs-plugin-annotation@3.1.0/dist/chartjs-plugin-annotation.min.js"></script>
<script src="https://cdn.jsdelivr.net/npm/chartjs-plugin-zoom@2.2.0/dist/chartjs-plugin-zoom.min.js"></script>
<style>
* {{ margin: 0; padding: 0; box-sizing: border-box; }}
body {{
    font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, 'PingFang SC', 'Microsoft YaHei', sans-serif;
    background: #0f1419;
    color: #e1e8ed;
    padding: 16px;
    line-height: 1.5;
}}
.dashboard {{
    max-width: 1400px;
    margin: 0 auto;
}}
.header {{
    display: flex;
    justify-content: space-between;
    align-items: center;
    margin-bottom: 20px;
    flex-wrap: wrap;
    gap: 12px;
}}
.header h1 {{
    font-size: 22px;
    color: #fff;
    font-weight: 600;
}}
.header .meta {{
    font-size: 13px;
    color: #8899a6;
}}
.time-switcher {{
    display: flex;
    gap: 6px;
    background: #1c2732;
    padding: 6px 10px;
    border-radius: 10px;
    border: 1px solid #2f3b47;
    width: fit-content;
}}
.time-btn {{
    padding: 8px 18px;
    border: none;
    border-radius: 8px;
    background: transparent;
    color: #8899a6;
    font-size: 14px;
    font-weight: 600;
    cursor: pointer;
    transition: all 0.2s;
}}
.time-btn:hover {{
    color: #fff;
    background: #253341;
}}
.time-btn.active {{
    background: #1d9bf0;
    color: #fff;
    font-weight: 700;
    box-shadow: 0 2px 8px rgba(29,155,240,0.3);
}}
/* KPI Cards */
.kpi-grid {{
    display: grid;
    grid-template-columns: repeat(auto-fit, minmax(200px, 1fr));
    gap: 12px;
    margin-bottom: 20px;
}}
.kpi-card {{
    background: #1c2732;
    border-radius: 10px;
    padding: 16px 20px;
    border: 1px solid #2f3b47;
}}
.kpi-card .label {{
    font-size: 12px;
    color: #8899a6;
    text-transform: uppercase;
    letter-spacing: 0.5px;
    margin-bottom: 6px;
}}
.kpi-card .value {{
    font-size: 28px;
    font-weight: 700;
    color: #fff;
}}
.kpi-card .sub {{
    font-size: 12px;
    color: #8899a6;
    margin-top: 4px;
}}
.kpi-card .up {{ color: #00d37e; }}
.kpi-card .down {{ color: #ff4946; }}
.up {{ color: #00d37e; }}
.down {{ color: #ff4946; }}

/* Account Overview */
.account-overview {{
    display: grid;
    grid-template-columns: 1fr 1fr;
    gap: 16px;
    margin-bottom: 20px;
}}
.freq-group {{
    background: #1c2732;
    border-radius: 10px;
    padding: 20px;
    border: 1px solid #2f3b47;
}}
.freq-group-title {{
    font-size: 14px;
    font-weight: 600;
    color: #8899a6;
    text-transform: uppercase;
    letter-spacing: 1px;
    margin-bottom: 14px;
    display: flex;
    align-items: center;
    gap: 8px;
}}
.freq-badge {{
    display: inline-block;
    padding: 2px 8px;
    border-radius: 4px;
    font-size: 11px;
    font-weight: 700;
}}
.freq-badge.lf {{ background: #e74c3c33; color: #e74c3c; }}
.freq-badge.hf {{ background: #3498db33; color: #3498db; }}
.account-card {{
    background: #161d25;
    border-radius: 8px;
    padding: 18px 20px;
    margin-bottom: 10px;
    border: 1px solid #2f3b4722;
}}
.account-card:last-child {{ margin-bottom: 0; }}
.account-card-header {{
    display: flex;
    justify-content: space-between;
    align-items: center;
    margin-bottom: 10px;
}}
.account-name {{
    font-size: 16px;
    font-weight: 600;
    display: flex;
    align-items: center;
    gap: 6px;
}}
.account-name .dot {{
    width: 8px;
    height: 8px;
    border-radius: 50%;
    display: inline-block;
}}
.account-return {{
    font-size: 28px;
    font-weight: 700;
    color: #e1e8ed;
}}
.equity-bar-container {{ margin: 8px 0; }}
.equity-bar-label {{
    display: flex;
    justify-content: space-between;
    font-size: 13px;
    color: #8899a6;
    margin-bottom: 4px;
}}
.equity-bar {{
    height: 6px;
    background: #2f3b47;
    border-radius: 3px;
    overflow: hidden;
    position: relative;
}}
.equity-bar-fill {{
    height: 100%;
    border-radius: 3px;
    transition: width 0.5s ease;
}}
.account-metrics {{
    display: grid;
    grid-template-columns: 1fr 1fr 1fr 1fr;
    gap: 8px;
    margin-top: 10px;
}}
.account-metric {{ text-align: center; }}
.account-metric .metric-label {{
    font-size: 13px;
    color: #8899a6;
    text-transform: uppercase;
    letter-spacing: 0.3px;
}}
.account-metric .metric-value {{
    font-size: 17px;
    font-weight: 600;
    margin-top: 2px;
    color: #e1e8ed;
}}
.deviation-banner {{
    margin-top: 10px;
    padding: 8px 12px;
    border-radius: 6px;
    font-size: 14px;
    font-weight: 600;
    text-align: center;
    display: flex;
    align-items: center;
    justify-content: center;
    gap: 6px;
}}
.deviation-banner.ok {{
    background: #00d37e18;
    color: #00d37e;
    border: 1px solid #00d37e33;
}}
.deviation-banner.warn {{
    background: #ffaa2c18;
    color: #ffaa2c;
    border: 1px solid #ffaa2c33;
}}
.deviation-banner.bad {{
    background: #ff494618;
    color: #ff4946;
    border: 1px solid #ff494633;
}}
@media (max-width: 768px) {{
    .account-overview {{ grid-template-columns: 1fr; }}
    .account-metrics {{ grid-template-columns: 1fr 1fr; }}
}}
/* Charts */
.chart-section {{
    background: #1c2732;
    border-radius: 10px;
    padding: 20px;
    margin-bottom: 16px;
    border: 1px solid #2f3b47;
}}
.chart-section h2 {{
    font-size: 15px;
    font-weight: 600;
    color: #fff;
    margin-bottom: 14px;
    padding-bottom: 8px;
    border-bottom: 1px solid #2f3b47;
}}
.chart-container {{
    position: relative;
    height: 320px;
}}
.chart-container.tall {{
    height: 380px;
}}
/* Tables */
.data-table {{
    width: 100%;
    border-collapse: collapse;
    font-size: 13px;
}}
.data-table th {{
    background: #253341;
    color: #8899a6;
    font-weight: 600;
    padding: 10px 12px;
    text-align: left;
    font-size: 12px;
    text-transform: uppercase;
    letter-spacing: 0.3px;
}}
.data-table td {{
    padding: 10px 12px;
    border-bottom: 1px solid #2f3b47;
    color: #e1e8ed;
}}
.data-table tr:hover td {{
    background: #253341;
}}
.data-table .positive {{ color: #00d37e; }}
.data-table .negative {{ color: #ff4946; }}
/* Position panel */
.pos-grid {{
    display: grid;
    grid-template-columns: 1fr 1fr;
    gap: 16px;
}}
@media (max-width: 768px) {{
    .pos-grid {{ grid-template-columns: 1fr; }}
}}
.pos-account {{
    background: #253341;
    border-radius: 8px;
    padding: 14px;
}}
.pos-account h3 {{
    font-size: 14px;
    margin-bottom: 10px;
    color: #1d9bf0;
}}
.pos-item {{
    display: flex;
    justify-content: space-between;
    align-items: center;
    padding: 6px 0;
    border-bottom: 1px solid #2f3b47;
    font-size: 13px;
}}
.pos-item:last-child {{ border-bottom: none; }}
.pos-side-long {{ color: #00d37e; font-weight: 600; }}
.pos-side-short {{ color: #ff4946; font-weight: 600; }}
/* Trade stats */
.trade-stats-grid {{
    display: grid;
    grid-template-columns: repeat(auto-fit, minmax(150px, 1fr));
    gap: 12px;
}}
.trade-stat-item {{
    background: #253341;
    border-radius: 8px;
    padding: 16px;
    text-align: center;
}}
.trade-stat-item .stat-label {{
    font-size: 12px;
    color: #8899a6;
    margin-bottom: 6px;
}}
.trade-stat-item .stat-value {{
    font-size: 22px;
    font-weight: 700;
    color: #fff;
}}
.trade-stat-item .stat-value.na {{
    font-size: 14px;
    color: #8899a6;
}}
/* Risk matrix */
.risk-matrix th:first-child {{
    position: sticky;
    left: 0;
    z-index: 1;
    background: #253341;
}}
.risk-matrix td:first-child {{
    position: sticky;
    left: 0;
    z-index: 1;
    background: #1c2732;
    font-weight: 600;
}}
.risk-matrix tr:hover td:first-child {{
    background: #253341;
}}
.dot {{
    display: inline-block;
    width: 10px;
    height: 10px;
    border-radius: 50%;
    margin-right: 6px;
    vertical-align: middle;
}}
.footer {{
    text-align: center;
    padding: 20px;
    color: #8899a6;
    font-size: 12px;
}}
</style>
</head>
<body>
<div class="dashboard">
    <!-- Header -->
    <div class="header">
        <div>
            <h1>HL跟单系统仪表盘</h1>
            <div class="meta" id="update-time">生成时间: {datetime.now().strftime("%Y-%m-%d %H:%M")}</div>
        </div>
        <div class="time-switcher">
            <button class="time-btn" data-days="7" onclick="setTimeRange(7)">7天</button>
            <button class="time-btn active" data-days="30" onclick="setTimeRange(30)">30天</button>
            <button class="time-btn" data-days="90" onclick="setTimeRange(90)">90天</button>
            <button class="time-btn" data-days="180" onclick="setTimeRange(180)">180天</button>
            <button class="time-btn" data-days="0" onclick="setTimeRange(0)">全部</button>
        </div>
    </div>

    <!-- KPI Cards -->
    <div class="kpi-grid" id="kpi-grid">
        <div class="kpi-card">
            <div class="label">低频跟单 累计收益</div>
            <div class="value" id="kpi-lf-return">--</div>
            <div class="sub" id="kpi-lf-return-sub">主账户: --</div>
        </div>
        <div class="kpi-card">
            <div class="label">高频跟单 累计收益</div>
            <div class="value" id="kpi-hf-return">--</div>
            <div class="sub" id="kpi-hf-return-sub">主账户: --</div>
        </div>
        <div class="kpi-card">
            <div class="label">最大回撤</div>
            <div class="value" id="kpi-max-dd">--</div>
            <div class="sub" id="kpi-max-dd-sub">当前: --</div>
        </div>
        <div class="kpi-card">
            <div class="label">最佳夏普比率</div>
            <div class="value" id="kpi-sharpe">--</div>
            <div class="sub" id="kpi-sharpe-sub">--</div>
        </div>
        <div class="kpi-card">
            <div class="label">胜率 (已平仓)</div>
            <div class="value" id="kpi-winrate">--</div>
            <div class="sub" id="kpi-winrate-sub">--</div>
        </div>
    </div>


    <!-- 账户概览 -->
    <div style="margin-bottom: 20px;">
        <div style="padding: 0 4px; margin-bottom: 12px;">
            <h2 style="font-size: 15px; font-weight: 600; color: #e1e8ed; margin: 0;">📋 账户概览</h2>
        </div>
        <div class="account-overview" id="account-overview">
            <div class="freq-group" id="overview-lf">
                <div class="freq-group-title"><span class="freq-badge lf">低频</span> 低频跟单策略</div>
                <div class="account-card" id="card-lf-leader">
                    <div class="account-card-header">
                        <span class="account-name"><span class="dot" style="background:#e74c3c"></span>Leader (主账户)</span>
                        <span class="account-return" id="ret-lf-leader">--</span>
                    </div>
                    <div class="equity-bar-container">
                        <div class="equity-bar-label"><span id="start-lf-leader">起始: --</span><span id="end-lf-leader">结束: --</span></div>
                        <div class="equity-bar"><div class="equity-bar-fill" id="bar-lf-leader" style="width:0%"></div></div>
                    </div>
                    <div class="account-metrics">
                        <div class="account-metric"><div class="metric-label">盈亏</div><div class="metric-value" id="pnl-lf-leader">--</div></div>
                        <div class="account-metric"><div class="metric-label">资金进出</div><div class="metric-value" id="flow-lf-leader">--</div></div>
                        <div class="account-metric"><div class="metric-label">最大回撤</div><div class="metric-value" id="dd-lf-leader">--</div></div>
                        <div class="account-metric"><div class="metric-label">跟单比例</div><div class="metric-value na" id="cr-lf-leader">N/A</div></div>
                    </div>
                </div>
                <div class="account-card" id="card-lf-follower">
                    <div class="account-card-header">
                        <span class="account-name"><span class="dot" style="background:#e67e22"></span>用户 (跟单)</span>
                        <span class="account-return" id="ret-lf-follower">--</span>
                    </div>
                    <div class="equity-bar-container">
                        <div class="equity-bar-label"><span id="start-lf-follower">起始: --</span><span id="end-lf-follower">结束: --</span></div>
                        <div class="equity-bar"><div class="equity-bar-fill" id="bar-lf-follower" style="width:0%"></div></div>
                    </div>
                    <div class="account-metrics">
                        <div class="account-metric"><div class="metric-label">盈亏</div><div class="metric-value" id="pnl-lf-follower">--</div></div>
                        <div class="account-metric"><div class="metric-label">资金进出</div><div class="metric-value" id="flow-lf-follower">--</div></div>
                        <div class="account-metric"><div class="metric-label">最大回撤</div><div class="metric-value" id="dd-lf-follower">--</div></div>
                        <div class="account-metric"><div class="metric-label">跟单比例</div><div class="metric-value" id="cr-lf-follower">--</div></div>
                    </div>
                </div>
                <div class="deviation-banner" id="dev-lf"><span>--</span></div>
            </div>
            <div class="freq-group" id="overview-hf">
                <div class="freq-group-title"><span class="freq-badge hf">高频</span> 高频跟单策略</div>
                <div class="account-card" id="card-hf-leader">
                    <div class="account-card-header">
                        <span class="account-name"><span class="dot" style="background:#3498db"></span>Leader (主账户)</span>
                        <span class="account-return" id="ret-hf-leader">--</span>
                    </div>
                    <div class="equity-bar-container">
                        <div class="equity-bar-label"><span id="start-hf-leader">起始: --</span><span id="end-hf-leader">结束: --</span></div>
                        <div class="equity-bar"><div class="equity-bar-fill" id="bar-hf-leader" style="width:0%"></div></div>
                    </div>
                    <div class="account-metrics">
                        <div class="account-metric"><div class="metric-label">盈亏</div><div class="metric-value" id="pnl-hf-leader">--</div></div>
                        <div class="account-metric"><div class="metric-label">资金进出</div><div class="metric-value" id="flow-hf-leader">--</div></div>
                        <div class="account-metric"><div class="metric-label">最大回撤</div><div class="metric-value" id="dd-hf-leader">--</div></div>
                        <div class="account-metric"><div class="metric-label">跟单比例</div><div class="metric-value na" id="cr-hf-leader">N/A</div></div>
                    </div>
                </div>
                <div class="account-card" id="card-hf-follower">
                    <div class="account-card-header">
                        <span class="account-name"><span class="dot" style="background:#2ecc71"></span>用户 (跟单)</span>
                        <span class="account-return" id="ret-hf-follower">--</span>
                    </div>
                    <div class="equity-bar-container">
                        <div class="equity-bar-label"><span id="start-hf-follower">起始: --</span><span id="end-hf-follower">结束: --</span></div>
                        <div class="equity-bar"><div class="equity-bar-fill" id="bar-hf-follower" style="width:0%"></div></div>
                    </div>
                    <div class="account-metrics">
                        <div class="account-metric"><div class="metric-label">盈亏</div><div class="metric-value" id="pnl-hf-follower">--</div></div>
                        <div class="account-metric"><div class="metric-label">资金进出</div><div class="metric-value" id="flow-hf-follower">--</div></div>
                        <div class="account-metric"><div class="metric-label">最大回撤</div><div class="metric-value" id="dd-hf-follower">--</div></div>
                        <div class="account-metric"><div class="metric-label">跟单比例</div><div class="metric-value" id="cr-hf-follower">--</div></div>
                    </div>
                </div>
                <div class="deviation-banner" id="dev-hf"><span>--</span></div>
            </div>
        </div>
    </div>

    <!-- 归一化净值曲线 -->
    <div class="chart-section">
        <h2>归一化净值曲线 (起始=1.0)</h2>
        <div class="chart-container tall">
            <canvas id="chart-nv"></canvas>
        </div>
    </div>

    <!-- 回撤曲线 -->
    <div class="chart-section">
        <h2>全账户回撤曲线</h2>
        <div class="chart-container">
            <canvas id="chart-dd"></canvas>
        </div>
    </div>

    <!-- 风险指标矩阵 -->
    <div class="chart-section">
        <h2>风险指标矩阵</h2>
        <div style="overflow-x:auto;">
            <table class="data-table risk-matrix" id="risk-matrix">
                <thead>
                    <tr>
                        <th>账户</th>
                        <th>累计收益%</th>
                        <th>年化波动率%</th>
                        <th>夏普比率</th>
                        <th>索提诺比率</th>
                        <th>卡玛比率</th>
                        <th>最大回撤%</th>
                        <th>当前回撤%</th>
                    </tr>
                </thead>
                <tbody id="risk-matrix-body"></tbody>
            </table>
        </div>
    </div>

    <!-- 持仓信息面板 -->
    <div class="chart-section">
        <h2>当前持仓</h2>
        <div class="pos-grid" id="pos-panel"></div>
    </div>

    <!-- 交易统计 -->
    <div class="chart-section">
        <h2>交易统计</h2>
        <div class="trade-stats-grid" id="trade-stats"></div>
    </div>

    <div class="footer">
        HL跟单效果追踪系统 P0 Dashboard | 数据自动更新
    </div>
</div>

<script>
// ====== 预计算数据 ======
const DATA = {data_json};

// ====== 全局变量 ======
let currentDays = 30;
let chartNV = null;
let chartDD = null;

// ====== 工具函数 ======
function filterByDays(seriesArray, days) {{
    if (days === 0 || !seriesArray || seriesArray.length === 0) return seriesArray;
    const now = Date.now();
    const cutoff = now - days * 86400 * 1000;
    return seriesArray.filter(p => p.x >= cutoff);
}}

function formatPct(val) {{
    if (val === null || val === undefined) return 'N/A';
    return (val >= 0 ? '+' : '') + val.toFixed(2) + '%';
}}

function pctClass(val) {{
    if (val >= 0) return 'up';
    return 'down';
}}

// ====== 时间范围切换 ======
function setTimeRange(days) {{
    currentDays = days;
    document.querySelectorAll('.time-btn').forEach(btn => {{
        btn.classList.toggle('active', parseInt(btn.dataset.days) === days);
    }});
    renderAll();
}}

// ====== 渲染KPI卡片 ======
function renderKPI() {{
    const m = DATA.risk_metrics;
    
    // 低频累计收益
    const lfRet = m.lf_follower.cum_return;
    const lfLeaderRet = m.lf_leader.cum_return;
    const el1 = document.getElementById('kpi-lf-return');
    el1.textContent = formatPct(lfRet);
    el1.className = 'value ' + pctClass(lfRet);
    document.getElementById('kpi-lf-return-sub').textContent = '主账户: ' + formatPct(lfLeaderRet);
    
    // 高频累计收益
    const hfRet = m.hf_follower.cum_return;
    const hfLeaderRet = m.hf_leader.cum_return;
    const el2 = document.getElementById('kpi-hf-return');
    el2.textContent = formatPct(hfRet);
    el2.className = 'value ' + pctClass(hfRet);
    document.getElementById('kpi-hf-return-sub').textContent = '主账户: ' + formatPct(hfLeaderRet);
    
    // 最大回撤 (取4个账户中最差的)
    let worstDD = 0;
    let worstKey = '';
    for (const key of DATA.account_keys) {{
        if (m[key].max_dd < worstDD) {{
            worstDD = m[key].max_dd;
            worstKey = key;
        }}
    }}
    const el3 = document.getElementById('kpi-max-dd');
    el3.textContent = formatPct(worstDD);
    el3.className = 'value down';
    // 当前回撤
    const avgCurrentDD = Object.values(m).reduce((s, v) => s + v.current_dd, 0) / 4;
    document.getElementById('kpi-max-dd-sub').textContent = '平均当前: ' + formatPct(avgCurrentDD);
    
    // 最佳夏普
    let bestSharpe = -Infinity;
    let bestSharpeKey = '';
    for (const key of DATA.account_keys) {{
        if (m[key].sharpe > bestSharpe) {{
            bestSharpe = m[key].sharpe;
            bestSharpeKey = key;
        }}
    }}
    const el4 = document.getElementById('kpi-sharpe');
    el4.textContent = bestSharpe.toFixed(2);
    el4.className = 'value up';
    document.getElementById('kpi-sharpe-sub').textContent = DATA.account_labels[bestSharpeKey];
    
    // 胜率
    const ts = DATA.trade_stats;
    const el5 = document.getElementById('kpi-winrate');
    if (ts.closed_trades > 0 && ts.win_rate !== null) {{
        el5.textContent = ts.win_rate.toFixed(1) + '%';
        el5.className = 'value ' + (ts.win_rate >= 50 ? 'up' : 'down');
        document.getElementById('kpi-winrate-sub').textContent = ts.closed_trades + '笔已平仓';
    }} else {{
        el5.textContent = 'N/A';
        el5.className = 'value na';
        document.getElementById('kpi-winrate-sub').textContent = '数据积累中';
    }}
}}

// ====== 渲染归一化净值曲线 ======
function renderNVChart() {{
    const ctx = document.getElementById('chart-nv').getContext('2d');
    
    const datasets = DATA.account_keys.map(key => {{
        const filtered = filterByDays(DATA.normalized[key], currentDays);
        return {{
            label: DATA.account_labels[key],
            data: filtered,
            borderColor: DATA.account_colors[key],
            backgroundColor: DATA.account_colors[key] + '15',
            borderWidth: 2,
            pointRadius: 0,
            pointHoverRadius: 4,
            fill: false,
            tension: 0.1,
        }};
    }});
    
    // 事件标注
    const annotations = {{}};
    const events = DATA.events;
    const maxAnnotations = 20;
    let evCount = 0;
    for (let i = 0; i < events.length && evCount < maxAnnotations; i++) {{
        const ev = events[i];
        // 检查事件时间是否在过滤范围内
        if (currentDays > 0) {{
            const cutoff = Date.now() - currentDays * 86400 * 1000;
            if (ev.ts < cutoff) continue;
        }}
        
        let borderColor, backgroundColor, symbol;
        if (ev.type === 'position_open') {{
            borderColor = '#00d37e';
            backgroundColor = '#00d37e80';
            symbol = 'triangle';
        }} else if (ev.type === 'large_fund_flow') {{
            borderColor = '#1d9bf0';
            backgroundColor = '#1d9bf080';
            symbol = 'circle';
        }} else {{
            borderColor = '#8899a6';
            backgroundColor = '#8899a680';
            symbol = 'rectRot';
        }}
        
        annotations['ev' + i] = {{
            type: 'point',
            xValue: ev.ts,
            yValue: 1.0,
            backgroundColor: backgroundColor,
            borderColor: borderColor,
            borderWidth: 1,
            radius: 5,
            pointStyle: symbol,
        }};
        evCount++;
    }}
    
    if (chartNV) chartNV.destroy();
    
    chartNV = new Chart(ctx, {{
        type: 'line',
        data: {{ datasets }},
        options: {{
            responsive: true,
            maintainAspectRatio: false,
            interaction: {{
                mode: 'index',
                intersect: false,
            }},
            plugins: {{
                legend: {{
                    labels: {{ color: '#e1e8ed', usePointStyle: true, padding: 16 }}
                }},
                tooltip: {{
                    callbacks: {{
                        label: function(ctx) {{
                            return ctx.dataset.label + ': ' + ctx.parsed.y.toFixed(4);
                        }}
                    }}
                }},
                annotation: {{
                    annotations: annotations
                }},
                zoom: {{
                    zoom: {{
                        wheel: {{ enabled: false }},
                        pinch: {{ enabled: false }},
                        mode: 'x',
                    }},
                    pan: {{
                        enabled: false,
                        mode: 'x',
                    }}
                }}
            }},
            scales: {{
                x: {{
                    type: 'time',
                    time: {{
                        unit: 'day',
                        tooltipFormat: 'yyyy-MM-dd HH:mm',
                        displayFormats: {{ day: 'MM-dd', hour: 'MM-dd HH:mm' }}
                    }},
                    ticks: {{ color: '#8899a6' }},
                    grid: {{ color: '#2f3b4730' }},
                }},
                y: {{
                    ticks: {{
                        color: '#8899a6',
                        callback: v => v.toFixed(3)
                    }},
                    grid: {{ color: '#2f3b4750' }},
                    title: {{
                        display: true,
                        text: '归一化净值',
                        color: '#8899a6'
                    }}
                }}
            }}
        }}
    }});
}}

// ====== 渲染回撤曲线 ======
function renderDDChart() {{
    const ctx = document.getElementById('chart-dd').getContext('2d');
    
    const datasets = DATA.account_keys.map(key => {{
        const filtered = filterByDays(DATA.drawdowns[key], currentDays);
        return {{
            label: DATA.account_labels[key],
            data: filtered,
            borderColor: DATA.account_colors[key],
            borderWidth: 2,
            pointRadius: 0,
            pointHoverRadius: 4,
            fill: false,
            tension: 0.1,
        }};
    }});
    
    if (chartDD) chartDD.destroy();
    
    chartDD = new Chart(ctx, {{
        type: 'line',
        data: {{ datasets }},
        options: {{
            responsive: true,
            maintainAspectRatio: false,
            interaction: {{
                mode: 'index',
                intersect: false,
            }},
            plugins: {{
                legend: {{
                    labels: {{ color: '#e1e8ed', usePointStyle: true, padding: 16 }}
                }},
                tooltip: {{
                    callbacks: {{
                        label: function(ctx) {{
                            return ctx.dataset.label + ': ' + ctx.parsed.y.toFixed(2) + '%';
                        }}
                    }}
                }},
                zoom: {{
                    zoom: {{ wheel: {{ enabled: false }}, pinch: {{ enabled: false }}, mode: 'x' }},
                    pan: {{ enabled: false, mode: 'x' }}
                }}
            }},
            scales: {{
                x: {{
                    type: 'time',
                    time: {{
                        unit: 'day',
                        tooltipFormat: 'yyyy-MM-dd HH:mm',
                        displayFormats: {{ day: 'MM-dd', hour: 'MM-dd HH:mm' }}
                    }},
                    ticks: {{ color: '#8899a6' }},
                    grid: {{ color: '#2f3b4730' }},
                }},
                y: {{
                    ticks: {{
                        color: '#8899a6',
                        callback: v => v.toFixed(1) + '%'
                    }},
                    grid: {{ color: '#2f3b4750' }},
                    title: {{
                        display: true,
                        text: '回撤 %',
                        color: '#8899a6'
                    }}
                }}
            }}
        }}
    }});
}}

// ====== 渲染风险指标矩阵 ======
function renderRiskMatrix() {{
    const tbody = document.getElementById('risk-matrix-body');
    const m = DATA.risk_metrics;
    
    let html = '';
    for (const key of DATA.account_keys) {{
        const r = m[key];
        const color = DATA.account_colors[key];
        html += '<tr>';
        html += '<td><span class="dot" style="background:' + color + '"></span>' + DATA.account_labels[key] + '</td>';
        html += '<td class="' + (r.cum_return >= 0 ? 'positive' : 'negative') + '">' + formatPct(r.cum_return) + '</td>';
        html += '<td>' + r.ann_vol.toFixed(2) + '</td>';
        html += '<td class="' + (r.sharpe >= 0 ? 'positive' : 'negative') + '">' + r.sharpe.toFixed(2) + '</td>';
        html += '<td class="' + (r.sortino >= 0 ? 'positive' : 'negative') + '">' + r.sortino.toFixed(2) + '</td>';
        html += '<td class="' + (r.calmar >= 0 ? 'positive' : 'negative') + '">' + r.calmar.toFixed(2) + '</td>';
        html += '<td class="negative">' + formatPct(r.max_dd) + '</td>';
        html += '<td class="negative">' + formatPct(r.current_dd) + '</td>';
        html += '</tr>';
    }}
    tbody.innerHTML = html;
}}

// ====== 渲染持仓面板 ======
function renderPositions() {{
    const panel = document.getElementById('pos-panel');
    const positions = DATA.positions;
    
    if (!positions || positions.length === 0) {{
        panel.innerHTML = '<div style="color:#8899a6;padding:20px;text-align:center;">暂无持仓数据</div>';
        return;
    }}
    
    // 按 instance -> role 两级分组
    const grouped = {{}};
    for (const p of positions) {{
        if (!grouped[p.instance_id]) grouped[p.instance_id] = {{}};
        const rk = p.role || 'leader';
        if (!grouped[p.instance_id][rk]) grouped[p.instance_id][rk] = [];
        grouped[p.instance_id][rk].push(p);
    }}
    
    let html = '';
    for (const instId of ['lf', 'hf']) {{
        const inst = grouped[instId] || {{}};
        const leaderItems = inst.leader || [];
        const followerItems = inst.follower || [];
        const total = leaderItems.length + followerItems.length;
        const label = instId === 'lf' ? '低频账户 (LF)' : '高频账户 (HF)';
        html += '<div class="pos-account">';
        html += '<h3>' + label + ' (' + total + '个持仓)</h3>';
        for (const [rk, rl] of [['leader','Leader'],['follower','用户']]) {{
            const items = inst[rk] || [];
            html += '<div style="font-size:12px;color:#8899a6;margin:6px 0 2px;">' + (rk==='leader'?'Leader':'用户') + '</div>';
            if (items.length === 0) {{
                html += '<div style="color:#657786;font-size:12px;margin-bottom:4px;">无持仓</div>';
            }} else {{
                for (const p of items) {{
                    const sideClass = p.side === 'long' ? 'pos-side-long' : 'pos-side-short';
                    const sideText = p.side === 'long' ? '多 LONG' : '空 SHORT';
                    const upnl = p.unrealized_pnl || 0;
                    const upnlCls = upnl >= 0 ? 'up' : 'down';
                    const upnlStr = (upnl>=0?'+':'') + '$' + upnl.toLocaleString('en-US',{{minimumFractionDigits:2,maximumFractionDigits:2}});
                    html += '<div class="pos-item">';
                    html += '<span><strong>' + p.coin + '</strong> <span class="' + sideClass + '">' + sideText + '</span></span>';
                    html += '<span>' + Math.abs(p.size).toLocaleString('en-US',{{maximumFractionDigits:4}}) + ' @ $' + p.entry_px.toFixed(4) + '</span>';
                    html += '<span class="' + upnlCls + '" style="font-weight:600;">' + upnlStr + '</span>';
                    html += '</div>';
                }}
            }}
        }}
        html += '</div>';
    }}
    panel.innerHTML = html;
}}

// ====== 渲染交易统计 ======
function renderTradeStats() {{
    const container = document.getElementById('trade-stats');
    const ts = DATA.trade_stats;
    
    const stats = [
        {{ label: '总交易数', value: ts.total_trades > 0 ? ts.total_trades : null }},
        {{ label: '已平仓', value: ts.closed_trades > 0 ? ts.closed_trades : null }},
        {{ label: '胜率', value: ts.win_rate !== null ? ts.win_rate.toFixed(1) + '%' : null }},
        {{ label: '平均盈亏%', value: ts.avg_pnl_ratio !== null ? ts.avg_pnl_ratio.toFixed(2) + '%' : null }},
        {{ label: '平均持仓时间', value: ts.avg_hold_time_hours !== null ? ts.avg_hold_time_hours.toFixed(1) + 'h' : null }},
    ];
    
    let html = '';
    for (const s of stats) {{
        html += '<div class="trade-stat-item">';
        html += '<div class="stat-label">' + s.label + '</div>';
        if (s.value !== null) {{
            html += '<div class="stat-value">' + s.value + '</div>';
        }} else {{
            html += '<div class="stat-value na">数据积累中</div>';
        }}
        html += '</div>';
    }}
    container.innerHTML = html;
}}

// ====== 主渲染函数 ======

// ====== 渲染账户概览 ======
function renderAccountOverview() {{
    const ov = DATA.account_overview_by_period[String(currentDays)] || DATA.account_overview;
    if (!ov || !ov.accounts) return;
    const accounts = ov.accounts;
    const deviation = ov.deviation || {{}};

    for (const key of DATA.account_keys) {{
        const d = accounts[key];
        if (!d) continue;
        const parts = key.split('_');
        const freq = parts[0];
        const domKey = key.replace('_', '-');

        // Return
        const retEl = document.getElementById('ret-' + domKey);
        if (retEl) {{
            retEl.textContent = formatPct(d.return_pct);
            retEl.className = 'account-return ' + pctClass(d.return_pct);
        }}

        // Start/End equity
        const startEl = document.getElementById('start-' + domKey);
        if (startEl) startEl.textContent = '起始: $' + d.start_equity.toLocaleString('en-US', {{minimumFractionDigits:2, maximumFractionDigits:2}});
        const endEl = document.getElementById('end-' + domKey);
        if (endEl) endEl.textContent = '结束: $' + d.end_equity.toLocaleString('en-US', {{minimumFractionDigits:2, maximumFractionDigits:2}});

        // Equity bar
        const barEl = document.getElementById('bar-' + domKey);
        if (barEl) {{
            let pct;
            if (d.start_equity > 0) {{
                const ratio = d.end_equity / d.start_equity;
                if (ratio >= 1) {{
                    pct = Math.min(100, 50 + Math.min((ratio - 1) * 30, 50));
                }} else {{
                    pct = Math.max(5, ratio * 100);
                }}
            }} else {{
                pct = 0;
            }}
            barEl.style.width = pct.toFixed(1) + '%';
            barEl.style.background = d.return_pct >= 0 ? '#00d37e' : '#ff4946';
        }}

        // PnL
        const pnlEl = document.getElementById('pnl-' + domKey);
        if (pnlEl) {{
            const sign = d.pnl >= 0 ? '+' : '';
            pnlEl.textContent = sign + '$' + d.pnl.toLocaleString('en-US', {{minimumFractionDigits:2, maximumFractionDigits:2}});
            pnlEl.className = 'metric-value ' + pctClass(d.pnl);
        }}

        // Net flow (资金进出)
        const flowEl = document.getElementById('flow-' + domKey);
        if (flowEl) {{
            const nf = d.net_flow;
            if (nf === undefined || nf === null) {{
                flowEl.textContent = '--';
                flowEl.className = 'metric-value';
            }} else if (nf > 0) {{
                flowEl.textContent = '+$' + nf.toLocaleString('en-US', {{minimumFractionDigits:2, maximumFractionDigits:2}});
                flowEl.className = 'metric-value up';
            }} else if (nf < 0) {{
                flowEl.textContent = '-$' + Math.abs(nf).toLocaleString('en-US', {{minimumFractionDigits:2, maximumFractionDigits:2}});
                flowEl.className = 'metric-value down';
            }} else {{
                flowEl.textContent = '$0.00';
                flowEl.className = 'metric-value';
                flowEl.style.color = '#8899a6';
            }}
        }}

        // Max drawdown
        const ddEl = document.getElementById('dd-' + domKey);
        if (ddEl) {{
            ddEl.textContent = formatPct(d.max_dd);
            ddEl.className = 'metric-value down';
        }}

        // Copy ratio
        const crEl = document.getElementById('cr-' + domKey);
        if (crEl) {{
            if (d.avg_copy_ratio !== null && d.avg_copy_ratio !== undefined) {{
                crEl.textContent = (d.avg_copy_ratio * 100).toFixed(2) + '%';
                crEl.className = 'metric-value';
                crEl.style.color = '#e1e8ed';
            }} else {{
                crEl.textContent = 'N/A';
                crEl.className = 'metric-value na';
            }}
        }}
    }}

    // Deviation banners
    for (const freq of ['lf', 'hf']) {{
        const devEl = document.getElementById('dev-' + freq);
        if (!devEl) continue;
        const dev = deviation[freq];
        if (dev === null || dev === undefined) {{
            devEl.innerHTML = '<span class="na">偏差: 数据不足</span>';
            devEl.className = 'deviation-banner';
            continue;
        }}
        const absDev = Math.abs(dev);
        let cls, icon;
        if (absDev <= 5) {{
            cls = 'ok';
            icon = '✅';
        }} else if (absDev <= 15) {{
            cls = 'warn';
            icon = '⚠️';
        }} else {{
            cls = 'bad';
            icon = '❌';
        }}
        const sign = dev >= 0 ? '+' : '';
        devEl.innerHTML = icon + ' 跟单偏差: ' + sign + dev.toFixed(2) + '%';
        devEl.className = 'deviation-banner ' + cls;
    }}
}}

function renderAll() {{
    try {{ renderAccountOverview(); }} catch(e) {{ console.error('Account overview error:', e); }}
    try {{ renderKPI(); }} catch(e) {{ console.error('KPI error:', e); }}
    try {{ renderNVChart(); }} catch(e) {{ console.error('NV chart error:', e); }}
    try {{ renderDDChart(); }} catch(e) {{ console.error('DD chart error:', e); }}
    try {{ renderRiskMatrix(); }} catch(e) {{ console.error('Risk matrix error:', e); }}
    try {{ renderPositions(); }} catch(e) {{ console.error('Positions error:', e); }}
    try {{ renderTradeStats(); }} catch(e) {{ console.error('Trade stats error:', e); }}
}}

// ====== 初始化 ======
document.addEventListener('DOMContentLoaded', function() {{
    renderAll();
}});
</script>
</body>
</html>'''
    
    return html


# ============================================================
# 主函数
# ============================================================

def generate_chart_html():
    """主入口: 加载数据 -> 预计算 -> 生成HTML"""
    
    # 1. 加载数据
    print("Loading data...")
    rows = load_all_snapshots()
    if not rows:
        return "<html><body><h1>No data</h1><p>暂无数据</p></body></html>"
    
    events = load_key_events()
    print("Fetching live positions from HL API...")
    pos_state = fetch_live_positions()
    trade_stats = load_trade_stats()
    
    _pos_count = sum(len(coins) for roles in pos_state.values() for coins in roles.values() if isinstance(coins, dict))
    print(f"  Snapshots: {len(rows)} rows")
    print(f"  Events: {len(events)}")
    print(f"  Positions (live): {_pos_count} items")
    print(f"  Trades: {trade_stats['total_trades']} total, {trade_stats['closed_trades']} closed")
    
    # 2. 构建序列
    print("Computing series...")
    series = build_series(rows)
    
    # 3. 预计算所有指标
    print("Pre-computing metrics...")
    # 加载资金进出数据用于 TWR 计算（提前到归一化之前）
    print("Loading fund flows for TWR...")
    all_flows = load_all_fund_flows()
    
    normalized = compute_normalized(series, all_flows)
    drawdowns = compute_drawdowns(series)
    daily_returns = compute_daily_returns(series)
    for k, v in all_flows.items():
        print(f"  {k}: {len(v)} flows")
    
    risk_metrics = compute_risk_metrics(series, daily_returns, all_flows)
    # 为5个周期分别计算账户概览
    periods = [7, 30, 90, 180, 0]
    account_overview_by_period = {}
    for days in periods:
        account_overview_by_period[str(days)] = compute_account_overview(series, all_flows, max_days=days)
    # 兼容：account_overview 取全部周期的数据
    account_overview = account_overview_by_period['0']
    events_js = prepare_events_for_js(events)
    positions = prepare_positions(pos_state)
    
    # 4. 组装预计算数据
    precomputed = {
        'account_keys': ACCOUNT_KEYS,
        'account_labels': ACCOUNT_LABELS,
        'account_colors': ACCOUNT_COLORS,
        'normalized': normalized,
        'drawdowns': drawdowns,
        'risk_metrics': risk_metrics,
        'account_overview': account_overview,
        'account_overview_by_period': account_overview_by_period,
        'events': events_js,
        'positions': positions,
        'trade_stats': trade_stats,
    }
    
    # 5. 生成HTML
    print("Generating HTML...")
    html = generate_html(precomputed)
    return html


if __name__ == '__main__':
    Path(CHART_DIR).mkdir(parents=True, exist_ok=True)
    
    chart_html = generate_chart_html()
    date_str = datetime.now().strftime("%Y%m%d")
    output_path = os.path.join(CHART_DIR, f"charts_{date_str}.html")
    
    # Atomic write
    tmp_path = output_path + '.tmp'
    with open(tmp_path, 'w', encoding='utf-8') as f:
        f.write(chart_html)
    os.replace(tmp_path, output_path)
    
    # Update latest symlink
    latest = os.path.join(CHART_DIR, "charts_latest.html")
    if os.path.islink(latest) or os.path.exists(latest):
        os.remove(latest)
    os.symlink(os.path.basename(output_path), latest)
    
    file_size = os.path.getsize(output_path)
    print(f"Charts generated: {output_path}")
    print(f"File size: {file_size / 1024:.1f} KB")
    print(f"Latest: {latest}")
