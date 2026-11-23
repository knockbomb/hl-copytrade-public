#!/usr/bin/env python3
"""
HL跟单效果报告生成器（统一周报/月报）
从HL API获取实时数据 + 本地审计数据，生成全面的跟单效果对比报告
"""

import argparse
import json
import math
import os
import sqlite3
import urllib.request
from datetime import datetime, timedelta
from pathlib import Path

from paths import BASE_DIR, DATA_DIR
from shared_config import get_instances
DB_PATH = str(DATA_DIR / 'net_value_history.db')
EVENTS_PATH = str(DATA_DIR / 'key_events.json')
ALERT_PATH = str(DATA_DIR / 'alert_history.json')
REPORT_DIR = str(DATA_DIR / 'reports')

HL_API = 'https://api.hyperliquid.xyz/info'

# 系统配置 - 从 config_v4.yaml 动态读取（消除硬编码）
def _build_systems():
    insts = get_instances()
    return {
        sid: {
            'env_file': info['env_file'],
            'audit_prefix': info['audit_prefix'],
            'audit_dir': info['audit_dir'],
            'leader_label': 'Leader',
            'follower_label': '用户',
        }
        for sid, info in insts.items()
    }

SYSTEMS = _build_systems()


def _inst_display_short(iid):
    """获取实例简短显示名（如'低频'）"""
    return get_instances().get(iid, {}).get('display_short', iid)



def load_env(env_path):
    """从 .env 文件读取配置"""
    env = {}
    if not os.path.exists(env_path):
        return env
    with open(env_path) as f:
        for line in f:
            line = line.strip()
            if '=' in line and not line.startswith('#'):
                k, v = line.split('=', 1)
                env[k.strip()] = v.strip()
    return env


def hl_api_call(body, timeout=15):
    """调用 HL info API"""
    data = json.dumps(body).encode()
    req = urllib.request.Request(HL_API, data=data, headers={'Content-Type': 'application/json'})
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            return json.loads(resp.read())
    except Exception as e:
        print(f'  [WARN] HL API error: {e}')
        return None


def get_positions(address):
    """获取当前持仓"""
    result = hl_api_call({"type": "clearinghouseState", "user": address})
    if not result:
        return [], 0

    account_value = float(result.get('accountValue', 0))
    positions = []

    for p in result.get('assetPositions', []):
        pos = p.get('position', {})
        coin = pos.get('coin', '')
        size = float(pos.get('szi', 0))
        if size == 0:
            continue
        entry_px = float(pos.get('entryPx', 0))
        upnl = float(pos.get('unrealizedPnl', 0))
        leverage_info = pos.get('leverage', {})
        leverage = leverage_info.get('value', 1) if isinstance(leverage_info, dict) else 1
        side = 'long' if size > 0 else 'short'
        notional = abs(size) * entry_px

        positions.append({
            'coin': coin,
            'side': side,
            'size': abs(size),
            'entry_px': entry_px,
            'unrealized_pnl': round(upnl, 4),
            'leverage': leverage,
            'notional': round(notional, 2),
        })

    return positions, account_value


def get_spot_total(address):
    """获取现货 USDC total"""
    result = hl_api_call({"type": "spotClearinghouseState", "user": address})
    if not result or not result.get('balances'):
        return 0
    for b in result['balances']:
        if b.get('coin') == 'USDC':
            return float(b.get('total', 0))
    return 0


def get_ledger_entries(address, start_time_ms=None):
    """从HL ledger API获取真实资金进出记录"""
    result = hl_api_call({"type": "userNonFundingLedgerUpdates", "user": address})
    if not result:
        return []

    entries = result if isinstance(result, list) else result.get('ledger', result.get('updates', []))

    deposit_types = {'deposit', 'deposit_internal'}
    withdraw_types = {'withdraw', 'withdraw_internal'}

    flows = []
    for entry in entries:
        delta = entry.get('delta', {})
        entry_type = delta.get('type', '')
        # time is at entry level, not inside delta
        ts_ms = int(entry.get('time', 0))

        if start_time_ms and ts_ms < start_time_ms:
            continue

        amount = 0
        flow_type = None

        if entry_type in deposit_types:
            amount = float(delta.get('usdc', delta.get('amount', 0)))
            if amount > 0:
                flow_type = 'deposit'
        elif entry_type in withdraw_types:
            amount = float(delta.get('usdc', delta.get('amount', 0)))
            if amount > 0:
                flow_type = 'withdraw'
        elif entry_type == 'send':
            dest = delta.get('destination', '')
            amount = float(delta.get('usdc', delta.get('amount', 0)))
            if amount > 0:
                if dest.lower() == address.lower():
                    flow_type = 'deposit'
                else:
                    flow_type = 'withdraw'
        else:
            continue

        if flow_type and amount > 0:
            flows.append({
                'ts': ts_ms // 1000,
                'type': flow_type,
                'amount': round(amount, 2),
                'tx_hash': entry.get('hash', ''),
                'raw_type': entry_type,
            })

    return flows


def get_performance(start_ts, end_ts, instance_id, role):
    """从 net_value_snapshots 获取绩效数据"""
    db = sqlite3.connect(DB_PATH)
    c = db.cursor()

    c.execute('''
        SELECT ts, total_equity, copy_ratio
        FROM net_value_snapshots
        WHERE ts BETWEEN ? AND ? AND instance_id = ? AND role = ?
        ORDER BY ts
    ''', (start_ts, end_ts, instance_id, role))

    rows = c.fetchall()
    db.close()

    if len(rows) < 2:
        return None

    equities = [r[1] for r in rows]
    ratios = [r[2] for r in rows if r[2] is not None]

    start_val = equities[0]
    end_val = equities[-1]
    high_val = max(equities)
    low_val = min(equities)
    gross_pnl = end_val - start_val

    # 最大回撤
    peak = equities[0]
    max_dd_pct = 0
    for eq in equities:
        if eq > peak:
            peak = eq
        dd_pct = (peak - eq) / peak * 100 if peak > 0 else 0
        if dd_pct > max_dd_pct:
            max_dd_pct = dd_pct

    # 日均收益
    daily_data = {}
    for i, r in enumerate(rows):
        date = datetime.fromtimestamp(r[0]).strftime('%Y-%m-%d')
        daily_data[date] = r[1]

    daily_returns = []
    dates = sorted(daily_data.keys())
    for i in range(1, len(dates)):
        prev = daily_data[dates[i - 1]]
        curr = daily_data[dates[i]]
        if prev > 0:
            daily_returns.append((curr - prev) / prev * 100)

    # 日胜率
    win_days = sum(1 for r in daily_returns if r > 0)
    lose_days = sum(1 for r in daily_returns if r < 0)

    return {
        'start_equity': round(start_val, 2),
        'end_equity': round(end_val, 2),
        'high': round(high_val, 2),
        'low': round(low_val, 2),
        'gross_pnl': round(gross_pnl, 2),
        'amplitude_pct': round((high_val - low_val) / start_val * 100, 2) if start_val > 0 else 0,
        'max_drawdown_pct': round(max_dd_pct, 2),
        'sample_count': len(rows),
        'trading_days': len(dates),
        'daily_returns_avg': round(sum(daily_returns) / len(daily_returns), 4) if daily_returns else 0,
        'daily_win_rate': round(win_days / (win_days + lose_days) * 100, 1) if (win_days + lose_days) > 0 else None,
        'ratio_avg': round(sum(ratios) / len(ratios), 6) if ratios else None,
        'ratio_min': round(min(ratios), 6) if ratios else None,
        'ratio_max': round(max(ratios), 6) if ratios else None,
        'ratio_std': round((sum((r - sum(ratios) / len(ratios)) ** 2 for r in ratios) / len(ratios)) ** 0.5, 6) if len(ratios) > 1 else None,
        '_rows': rows,  # raw snapshot rows for TWR calculation
        '_first_ts': rows[0][0] if rows else None,
        '_last_ts': rows[-1][0] if rows else None,
    }



def calculate_twr(rows, flows):
    """
    TWR (Time-Weighted Return) calculation.
    Removes the effect of cash flows from performance metrics.
    
    Args:
        rows: list of (ts, equity, copy_ratio) from DB, ordered by ts
        flows: list of flow dicts with 'ts' (unix seconds) and 'signed_amount'
               signed_amount: positive=deposit (adds to equity), negative=withdraw
    
    Returns: dict with TWR-based metrics, or None if insufficient data
    """
    if len(rows) < 2:
        return None
    
    timestamps = [r[0] for r in rows]
    equities = [r[1] for r in rows]
    start_val = equities[0]
    
    if start_val <= 0:
        return None
    
    # Build adjusted equity curve using TWR methodology
    adj_equities = [start_val]
    cumulative_factor = 1.0
    
    for i in range(1, len(rows)):
        ts_prev = timestamps[i-1]
        ts_curr = timestamps[i]
        eq_prev = equities[i-1]
        eq_curr = equities[i]
        
        # Sum flows between these two snapshots (ts_prev, ts_curr]
        net_flow = 0
        for f in flows:
            if ts_prev < f['ts'] <= ts_curr:
                net_flow += f['signed_amount']
        
        # Sub-period return: remove flow effect
        # If deposit of 500: equity went from 5000 to 5600, but 500 was added
        # Trading return = (5600 - 5000 - 500) / 5000 = 2%
        # If withdraw of 500: equity went from 5000 to 4400, but 500 was removed
        # Trading return = (4400 - 5000 - (-500)) / 5000 = -2%
        if eq_prev > 0:
            sub_return = (eq_curr - eq_prev - net_flow) / eq_prev
        else:
            sub_return = 0.0
        
        cumulative_factor *= (1 + sub_return)
        adj_equities.append(start_val * cumulative_factor)
    
    # TWR percentage
    twr_pct = (cumulative_factor - 1) * 100
    
    # Adjusted curve metrics
    adj_high = max(adj_equities)
    adj_low = min(adj_equities)
    amplitude_pct = (adj_high - adj_low) / start_val * 100
    
    # Max drawdown on adjusted curve
    peak = adj_equities[0]
    max_dd_pct = 0.0
    for eq in adj_equities:
        if eq > peak:
            peak = eq
        dd_pct = (peak - eq) / peak * 100 if peak > 0 else 0
        if dd_pct > max_dd_pct:
            max_dd_pct = dd_pct
    
    # Daily returns on adjusted curve
    daily_data = {}
    for i, r in enumerate(rows):
        date = datetime.fromtimestamp(r[0]).strftime('%Y-%m-%d')
        daily_data[date] = adj_equities[i]
    
    daily_returns = []
    dates = sorted(daily_data.keys())
    for i in range(1, len(dates)):
        prev_val = daily_data[dates[i-1]]
        curr_val = daily_data[dates[i]]
        if prev_val > 0:
            daily_returns.append((curr_val - prev_val) / prev_val * 100)
    
    win_days = sum(1 for r in daily_returns if r > 0)
    lose_days = sum(1 for r in daily_returns if r < 0)
    
    return {
        'twr_pct': round(twr_pct, 4),
        'amplitude_pct': round(amplitude_pct, 2),
        'max_drawdown_pct': round(max_dd_pct, 2),
        'adj_high': round(adj_high, 2),
        'adj_low': round(adj_low, 2),
        'adj_end': round(adj_equities[-1], 2),
        'daily_returns_avg': round(sum(daily_returns) / len(daily_returns), 4) if daily_returns else 0,
        'daily_win_rate': round(win_days / (win_days + lose_days) * 100, 1) if (win_days + lose_days) > 0 else None,
    }



# ====== P2: 高级指标计算 ======
_SAMPLE_INTERVAL_HOURS = 1.45
_ANNUALIZE_FACTOR = math.sqrt(365 * 24 / _SAMPLE_INTERVAL_HOURS)


def _daily_returns_from_equities(equities, timestamps):
    """从权益序列计算日收益率列表"""
    daily_data = {}
    for ts, eq in zip(timestamps, equities):
        date = datetime.fromtimestamp(ts).strftime('%Y-%m-%d')
        daily_data[date] = eq
    dates = sorted(daily_data.keys())
    returns = []
    for i in range(1, len(dates)):
        prev = daily_data[dates[i - 1]]
        curr = daily_data[dates[i]]
        if prev > 0:
            returns.append((curr - prev) / prev * 100)
    return returns


def _calc_volatility(returns_list):
    """年化波动率"""
    if len(returns_list) < 2:
        return 0
    mean = sum(returns_list) / len(returns_list)
    variance = sum((r - mean) ** 2 for r in returns_list) / len(returns_list)
    return math.sqrt(variance) * _ANNUALIZE_FACTOR


def _calc_downside_volatility(returns_list):
    """年化下行波动率"""
    neg_returns = [r for r in returns_list if r < 0]
    if len(neg_returns) < 2:
        return 0
    mean = sum(neg_returns) / len(neg_returns)
    variance = sum((r - mean) ** 2 for r in neg_returns) / len(neg_returns)
    return math.sqrt(variance) * _ANNUALIZE_FACTOR


def _calc_annualized_return(total_return_pct, days):
    """年化收益率"""
    if days <= 0:
        return 0
    if days < 3:
        return total_return_pct
    try:
        result = ((1 + total_return_pct / 100) ** (365 / days) - 1) * 100
        if abs(result) > 100000:
            return 100000 if result > 0 else -100000
        return result
    except (OverflowError, ValueError):
        return 0


def _calc_max_dd_details(equities, timestamps):
    """计算最大回撤百分比、回撤持续天数、起止时间"""
    if not equities or len(equities) < 2:
        return 0, 0, None, None
    peak = equities[0]
    peak_idx = 0
    max_dd = 0
    trough_idx = 0
    for i, eq in enumerate(equities):
        if eq > peak:
            peak = eq
            peak_idx = i
        dd = (peak - eq) / peak * 100 if peak > 0 else 0
        if dd > max_dd:
            max_dd = dd
            trough_idx = i
    last_peak_idx = 0
    peak_val = equities[0]
    for i, eq in enumerate(equities):
        if eq >= peak_val:
            peak_val = eq
            last_peak_idx = i
    duration_samples = len(equities) - 1 - last_peak_idx
    duration_days = duration_samples * _SAMPLE_INTERVAL_HOURS / 24
    peak_ts = timestamps[peak_idx] if max_dd > 0 else None
    trough_ts = timestamps[trough_idx] if max_dd > 0 else None
    return round(max_dd, 2), round(duration_days, 1), peak_ts, trough_ts


def calc_risk_metrics(rows, role='leader'):
    """从 net_value_snapshots 时序数据计算风险指标。
    rows: [(ts, total_equity, copy_ratio), ...]
    返回 dict 或 {}"""
    try:
        if not rows or len(rows) < 3:
            return {}
        timestamps = [r[0] for r in rows]
        equities = [r[1] for r in rows]
        actual_days = (timestamps[-1] - timestamps[0]) / 86400
        if actual_days < 0.5:
            return {}
        total_ret = ((equities[-1] - equities[0]) / equities[0] * 100) if equities[0] > 0 else 0
        ann_return = _calc_annualized_return(total_ret, actual_days)
        daily_returns = _daily_returns_from_equities(equities, timestamps)
        ann_vol = _calc_volatility(daily_returns)
        down_vol = _calc_downside_volatility(daily_returns)
        max_dd, dd_duration, peak_ts, trough_ts = _calc_max_dd_details(equities, timestamps)
        sharpe = 0
        if ann_vol > 0.01:
            sharpe = round(max(min((ann_return - 3.0) / ann_vol, 100), -100), 4)
        sortino = 0
        if down_vol > 0.01:
            sortino = round(max(min((ann_return - 3.0) / down_vol, 100), -100), 4)
        calmar = 0
        if max_dd > 0.01:
            calmar = round(max(min(ann_return / max_dd, 100), -100), 4)
        peak_eq = max(equities)
        current_dd = ((equities[-1] - peak_eq) / peak_eq * 100) if peak_eq > 0 else 0
        last_high_idx = 0
        peak_val = equities[0]
        for i, eq in enumerate(equities):
            if eq >= peak_val:
                peak_val = eq
                last_high_idx = i
        cur_dd_duration = (timestamps[-1] - timestamps[last_high_idx]) / 86400
        return {
            'current_drawdown_pct': round(current_dd, 2),
            'drawdown_duration_days': round(cur_dd_duration, 1),
            'annualized_volatility_pct': round(ann_vol, 2),
            'sharpe_ratio': sharpe,
            'sortino_ratio': sortino,
            'calmar_ratio': calmar,
            'max_drawdown_peak_ts': peak_ts,
            'max_drawdown_trough_ts': trough_ts,
            'period_days': round(actual_days, 1),
        }
    except Exception as e:
        print(f'  [WARN] calc_risk_metrics error: {e}')
        return {}


def calc_utilization(start_ts, end_ts, instance_id, role):
    """有效杠杆: avg/min/max of (total_notional / total_equity * 100)"""
    try:
        db = sqlite3.connect(DB_PATH)
        c = db.cursor()
        c.execute('''
            SELECT total_equity, total_notional
            FROM net_value_snapshots
            WHERE ts BETWEEN ? AND ? AND instance_id = ? AND role = ?
            ORDER BY ts
        ''', (start_ts, end_ts, instance_id, role))
        rows = c.fetchall()
        db.close()
        if not rows:
            return {}
        utils = []
        for eq, notional in rows:
            if eq and eq > 0 and notional is not None:
                utils.append(notional / eq * 100)
        if not utils:
            return {}
        return {
            'avg_utilization_pct': round(sum(utils) / len(utils), 2),
            'min_utilization_pct': round(min(utils), 2),
            'max_utilization_pct': round(max(utils), 2),
        }
    except Exception as e:
        print(f'  [WARN] calc_utilization error: {e}')
        return {}


def calc_position_concentration(positions):
    """持仓集中度: HHI, top1, long/short pct"""
    try:
        if not positions:
            return {}
        total_notional = sum(p.get('notional', 0) for p in positions)
        if total_notional <= 0:
            return {}
        weights = []
        long_notional = 0
        short_notional = 0
        for p in positions:
            n = p.get('notional', 0)
            if n > 0:
                w = n / total_notional
                weights.append(w)
                if p.get('side') == 'long':
                    long_notional += n
                else:
                    short_notional += n
        hhi = sum(w ** 2 for w in weights)
        top1 = max(weights) if weights else 0
        long_pct = (long_notional / total_notional * 100) if total_notional > 0 else 0
        short_pct = (short_notional / total_notional * 100) if total_notional > 0 else 0
        ls_ratio = (long_notional / short_notional) if short_notional > 0 else None
        return {
            'hhi_index': round(hhi, 4),
            'top1_weight': round(top1, 4),
            'long_pct': round(long_pct, 1),
            'short_pct': round(short_pct, 1),
            'long_short_ratio': round(ls_ratio, 2) if ls_ratio is not None else None,
        }
    except Exception as e:
        print(f'  [WARN] calc_position_concentration error: {e}')
        return {}


def calc_mom_growth(current_return_pct, report_type):
    """环比增长率"""
    try:
        if current_return_pct is None:
            return {}
        if report_type in ('semiannual', 'semiannual_custom'):
            files = sorted(Path(REPORT_DIR).glob('semiannual/semiannual_*.json'))
        elif report_type in ('annual', 'annual_custom'):
            files = sorted(Path(REPORT_DIR).glob('annual/annual_*.json'))
        else:
            files = sorted(Path(REPORT_DIR).glob(f'{report_type}_*.json'))
        if len(files) < 1:
            return {}
        prev_report = None
        for fpath in reversed(files):
            try:
                with open(fpath) as fh:
                    r = json.load(fh)
                prev_report = r
                break
            except Exception:
                continue
        if not prev_report:
            return {}
        prev_returns = []
        for iid in list(SYSTEMS):
            perf = prev_report.get('sections', {}).get(iid, {}).get('performance', {})
            for role in ['leader', 'follower']:
                p = perf.get(role)
                if p and p.get('return_pct') is not None:
                    prev_returns.append(p['return_pct'])
        if not prev_returns:
            return {}
        prev_return = sum(prev_returns) / len(prev_returns)
        return {
            'current_return_pct': round(current_return_pct, 2),
            'prev_return_pct': round(prev_return, 2),
            'mom_growth_pct': round(current_return_pct - prev_return, 2),
        }
    except Exception as e:
        print(f'  [WARN] calc_mom_growth error: {e}')
        return {}


def calc_monthly_returns_from_rows(rows):
    """将时序数据按月聚合计算每月收益率。
    rows: [(ts, total_equity), ...] 或 [(ts, total_equity, ...), ...]
    返回 {YYYY-MM: return_pct, ...}"""
    try:
        if not rows or len(rows) < 2:
            return {}
        monthly = {}
        for r in rows:
            ts = r[0]
            eq = r[1]
            month = datetime.fromtimestamp(ts).strftime('%Y-%m')
            if month not in monthly:
                monthly[month] = {'first': eq, 'last': eq}
            monthly[month]['last'] = eq
        result = {}
        for month, data in sorted(monthly.items()):
            if data['first'] > 0:
                ret = (data['last'] - data['first']) / data['first'] * 100
                result[month] = round(ret, 2)
        return result
    except Exception as e:
        print(f'  [WARN] calc_monthly_returns_from_rows error: {e}')
        return {}


def get_audit_stats(start_ts, end_ts, instance_id):
    """从审计文件提取执行统计"""
    sys_cfg = SYSTEMS[instance_id]
    prefix = sys_cfg['audit_prefix']
    audit_dir = sys_cfg['audit_dir']

    total = {
        'opens': 0, 'adjusts': 0, 'closes': 0,
        'recons': 0, 'close_queued': 0,
        'partial_fills': 0, 'confirm_retries': 0,
        'polls': 0, 'ws_disconnects': 0,
    }

    current = datetime.fromtimestamp(start_ts).replace(hour=0, minute=0, second=0)
    end_dt = datetime.fromtimestamp(end_ts)

    while current <= end_dt:
        date_str = current.strftime('%Y%m%d')
        audit_file = os.path.join(audit_dir, f'{prefix}{date_str}.json')
        if os.path.exists(audit_file):
            try:
                with open(audit_file) as f:
                    records = json.load(f)
                if isinstance(records, list) and records:
                    last = records[-1]
                    today = last.get('today', {})
                    total['opens'] += today.get('opens', 0)
                    total['adjusts'] += today.get('adjusts', 0)
                    total['closes'] += today.get('closes', 0)
                    total['recons'] += today.get('recons', 0)
                    total['close_queued'] += today.get('close_queued', 0)
                    total['partial_fills'] += today.get('partial_fills', 0)
                    total['confirm_retries'] += today.get('confirm_retries', 0)
                    total['polls'] += len(records)
                    # 统计 WebSocket 断连
                    for rec in records:
                        if rec.get('ws_connected') is False:
                            total['ws_disconnects'] += 1
            except Exception as e:
                print(f'  [WARN] Read audit file failed: {audit_file}: {e}')
        current += timedelta(days=1)

    return total


def get_alert_stats(start_ts, end_ts):
    """告警统计"""
    if not os.path.exists(ALERT_PATH):
        return {'critical': 0, 'warning': 0, 'info': 0, 'total': 0, 'details': []}

    try:
        with open(ALERT_PATH) as f:
            alerts = json.load(f)
    except Exception:
        return {'critical': 0, 'warning': 0, 'info': 0, 'total': 0, 'details': []}

    counts = {'critical': 0, 'warning': 0, 'info': 0}
    details = []

    if isinstance(alerts, list):
        for a in alerts:
            ts = a.get('ts', 0)
            if start_ts <= ts <= end_ts:
                level = a.get('level', '').upper()
                if level == 'CRITICAL':
                    counts['critical'] += 1
                elif level == 'WARNING':
                    counts['warning'] += 1
                else:
                    counts['info'] += 1
                details.append({
                    'ts': ts,
                    'time': datetime.fromtimestamp(ts).strftime('%m-%d %H:%M'),
                    'level': level,
                    'title': a.get('title', ''),
                })

    counts['total'] = counts['critical'] + counts['warning'] + counts['info']
    counts['details'] = details
    return counts


def get_events(start_ts, end_ts):
    """获取关键事件"""
    if not os.path.exists(EVENTS_PATH):
        return []
    try:
        with open(EVENTS_PATH) as f:
            events = json.load(f)
    except Exception:
        return []
    return [e for e in events if start_ts <= e.get('ts', 0) <= end_ts]



def backfill_trade_stats(events, start_ts, end_ts):
    """
    从 key_events + net_value_snapshots 回算交易统计。
    由于 key_events 可能没有 position_close，使用净值曲线日收益率推算。
    Returns: dict: {lf: {...}, hf: {...}} trade_stats per instance
    """
    db_path = DB_PATH
    result = {sid: {} for sid in SYSTEMS}
    for iid in list(SYSTEMS):
        try:
            stats = _calc_trade_stats_instance(events, start_ts, end_ts, iid, db_path)
            result[iid] = stats
        except Exception as e:
            print(f"  [WARN] backfill_trade_stats error for {iid}: {e}")
            result[iid] = {}
    return result


def _calc_trade_stats_instance(events, start_ts, end_ts, instance_id, db_path):
    """计算单个 instance 的交易统计"""
    import sqlite3
    open_events = [
        e for e in events
        if e.get("type") == "position_open"
        and e.get("instance_id") == instance_id
        and start_ts <= e.get("ts", 0) <= end_ts
    ]
    db = sqlite3.connect(db_path)
    c = db.cursor()
    c.execute(
        "SELECT ts, total_equity FROM net_value_snapshots "
        "WHERE ts BETWEEN ? AND ? AND instance_id = ? AND role = 'leader' "
        "ORDER BY ts",
        (start_ts, end_ts, instance_id)
    )
    rows = c.fetchall()
    db.close()
    if len(rows) < 2:
        return {
            "total_trades": len(open_events), "position_opens": len(open_events),
            "win_rate": None, "avg_win_loss_ratio": None, "profit_factor": None,
            "max_consecutive_wins": None, "max_consecutive_losses": None,
            "avg_hold_time_hours": None, "note": "数据不足"
        }
    daily_equity = {}
    for ts, eq in rows:
        date = datetime.fromtimestamp(ts).strftime("%Y-%m-%d")
        daily_equity[date] = eq
    dates = sorted(daily_equity.keys())
    daily_pnl = []
    for i in range(1, len(dates)):
        prev_eq = daily_equity[dates[i - 1]]
        curr_eq = daily_equity[dates[i]]
        if prev_eq > 0:
            daily_pnl.append(curr_eq - prev_eq)
    if not daily_pnl:
        return {
            "total_trades": len(open_events), "position_opens": len(open_events),
            "win_rate": None, "avg_win_loss_ratio": None, "profit_factor": None,
            "max_consecutive_wins": None, "max_consecutive_losses": None,
            "avg_hold_time_hours": None, "note": "无有效日收益数据"
        }
    wins = [p for p in daily_pnl if p > 0]
    losses = [abs(p) for p in daily_pnl if p < 0]
    total_days = len(daily_pnl)
    win_days = len(wins)
    win_rate = (win_days / total_days * 100) if total_days > 0 else 0
    avg_win = sum(wins) / len(wins) if wins else 0
    avg_loss = sum(losses) / len(losses) if losses else 0
    win_loss_ratio = (avg_win / avg_loss) if avg_loss > 0 else None
    total_wins_sum = sum(wins) if wins else 0
    total_losses_sum = sum(losses) if losses else 0
    profit_factor = (total_wins_sum / total_losses_sum) if total_losses_sum > 0 else None
    max_con_wins = 0
    max_con_losses = 0
    cur_w = 0
    cur_l = 0
    for p in daily_pnl:
        if p > 0:
            cur_w += 1
            cur_l = 0
            max_con_wins = max(max_con_wins, cur_w)
        elif p < 0:
            cur_l += 1
            cur_w = 0
            max_con_losses = max(max_con_losses, cur_l)
        else:
            cur_w = 0
            cur_l = 0
    trading_days = len(dates)
    avg_hold_days = (trading_days / len(open_events)) if open_events else None
    return {
        "total_trades": len(open_events), "position_opens": len(open_events),
        "trading_days": trading_days, "daily_win_rate": round(win_rate, 1),
        "win_rate": round(win_rate, 1), "avg_win": round(avg_win, 2),
        "avg_loss": round(avg_loss, 2),
        "avg_win_loss_ratio": round(win_loss_ratio, 2) if win_loss_ratio is not None else None,
        "profit_factor": round(profit_factor, 2) if profit_factor is not None else None,
        "max_consecutive_wins": max_con_wins,
        "max_consecutive_losses": max_con_losses,
        "avg_hold_time_hours": round(avg_hold_days * 24, 1) if avg_hold_days else None,
    }


def generate_report(report_type='monthly'):
    """生成报告"""
    now = datetime.now()

    if report_type == 'monthly':
        period_start = now.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
    elif report_type == 'weekly':
        period_start = (now - timedelta(days=7)).replace(hour=0, minute=0, second=0, microsecond=0)
    else:
        return None

    start_ts = int(period_start.timestamp())
    end_ts = int(now.timestamp())
    start_ms = start_ts * 1000

    print(f'[Report] type={report_type} period={period_start.strftime("%Y-%m-%d")} ~ {now.strftime("%Y-%m-%d")}')

    report = {
        'type': report_type,
        'period': f'{period_start.strftime("%Y-%m-%d")} ~ {now.strftime("%Y-%m-%d")}',
        'start_ts': start_ts,
        'end_ts': end_ts,
        'generated_at': now.isoformat(),
        'sections': {},
    }

    # P2: 加载上一期报告用于环比计算
    _prev_report_for_mom = None
    try:
        _pf = sorted(Path(REPORT_DIR).glob(f'{report_type}_*.json'))
        if _pf:
            with open(_pf[-1]) as _f:
                _prev_report_for_mom = json.load(_f)
    except Exception:
        pass

    for instance_id in list(SYSTEMS):
        cfg = SYSTEMS[instance_id]
        env = load_env(cfg['env_file'])
        leader_addr = env.get('HL_LEADER_ADDR', '')
        user_main_addr = env.get('HL_USER_MAIN_ADDR', '')
        user_api_addr = env.get('HL_USER_API_ADDR', '')

        print(f'\n[{instance_id}] Leader={leader_addr[:10]}... User={user_main_addr[:10]}...')

        # ---- 1. 绩效数据（从 net_value_snapshots） ----
        leader_perf = get_performance(start_ts, end_ts, instance_id, 'leader')
        follower_perf = get_performance(start_ts, end_ts, instance_id, 'follower')

        # ---- 2. 资金进出（从 ledger API 获取真实记录） ----
        # 获取所有资金进出记录（不限起始时间，后面按快照范围过滤）
        all_leader_flows = get_ledger_entries(leader_addr) if leader_addr else []
        all_follower_flows = get_ledger_entries(user_main_addr) if user_main_addr else []

        # 按快照覆盖范围过滤资金进出（修复数据对齐bug）
        # 只有落在快照时间范围内的资金进出才影响TWR计算
        l_first_ts = leader_perf['_first_ts'] if leader_perf else None
        l_last_ts = leader_perf['_last_ts'] if leader_perf else None
        f_first_ts = follower_perf['_first_ts'] if follower_perf else None
        f_last_ts = follower_perf['_last_ts'] if follower_perf else None

        leader_flows_in_range = [f for f in all_leader_flows if l_first_ts and l_last_ts and l_first_ts <= f['ts'] <= l_last_ts]
        follower_flows_in_range = [f for f in all_follower_flows if f_first_ts and f_last_ts and f_first_ts <= f['ts'] <= f_last_ts]

        # 用于报告显示的资金进出（仍用完整周期范围）
        leader_flows = [f for f in all_leader_flows if f['ts'] >= start_ts]
        follower_flows = [f for f in all_follower_flows if f['ts'] >= start_ts]

        leader_dep = sum(f['amount'] for f in leader_flows if f['type'] == 'deposit')
        leader_wd = sum(f['amount'] for f in leader_flows if f['type'] == 'withdraw')
        follower_dep = sum(f['amount'] for f in follower_flows if f['type'] == 'deposit')
        follower_wd = sum(f['amount'] for f in follower_flows if f['type'] == 'withdraw')

        # 为TWR计算准备signed_amount（deposit为正，withdraw为负）
        for f in leader_flows_in_range:
            f['signed_amount'] = f['amount'] if f['type'] == 'deposit' else -f['amount']
        for f in follower_flows_in_range:
            f['signed_amount'] = f['amount'] if f['type'] == 'deposit' else -f['amount']

        # 计算TWR（时间加权收益率）
        if leader_perf:
            twr = calculate_twr(leader_perf['_rows'], leader_flows_in_range)
            leader_net_flow = leader_dep - leader_wd
            leader_perf['deposits'] = round(leader_dep, 2)
            leader_perf['withdraws'] = round(leader_wd, 2)
            leader_perf['net_flow'] = round(leader_net_flow, 2)
            if twr:
                leader_perf['return_pct'] = twr['twr_pct']
                leader_perf['amplitude_pct'] = twr['amplitude_pct']
                leader_perf['max_drawdown_pct'] = twr['max_drawdown_pct']
                leader_perf['net_pnl'] = round(twr['adj_end'] - leader_perf['start_equity'], 2)
            else:
                # fallback: no flows in range, use simple calculation
                leader_net_pnl = leader_perf['gross_pnl']
                leader_perf['net_pnl'] = round(leader_net_pnl, 2)
                leader_perf['return_pct'] = round(leader_net_pnl / leader_perf['start_equity'] * 100, 4) if leader_perf['start_equity'] > 0 else 0
            leader_perf['flow_count'] = len(leader_flows)

        if follower_perf:
            twr = calculate_twr(follower_perf['_rows'], follower_flows_in_range)
            follower_net_flow = follower_dep - follower_wd
            follower_perf['deposits'] = round(follower_dep, 2)
            follower_perf['withdraws'] = round(follower_wd, 2)
            follower_perf['net_flow'] = round(follower_net_flow, 2)
            if twr:
                follower_perf['return_pct'] = twr['twr_pct']
                follower_perf['amplitude_pct'] = twr['amplitude_pct']
                follower_perf['max_drawdown_pct'] = twr['max_drawdown_pct']
                follower_perf['net_pnl'] = round(twr['adj_end'] - follower_perf['start_equity'], 2)
            else:
                follower_net_pnl = follower_perf['gross_pnl']
                follower_perf['net_pnl'] = round(follower_net_pnl, 2)
                follower_perf['return_pct'] = round(follower_net_pnl / follower_perf['start_equity'] * 100, 4) if follower_perf['start_equity'] > 0 else 0
            follower_perf['flow_count'] = len(follower_flows)

        # 偏差
        deviation = None
        if leader_perf and follower_perf:
            deviation = round(follower_perf['return_pct'] - leader_perf['return_pct'], 4)

        # ---- 3. 持仓快照（实时 API） ----
        # 注意：clearinghouseState/spotClearinghouseState 需要主地址，不是API钱包地址
        follower_query_addr = user_main_addr if user_main_addr else user_api_addr
        leader_pos, leader_av = get_positions(leader_addr) if leader_addr else ([], 0)
        follower_pos, follower_av = get_positions(follower_query_addr) if follower_query_addr else ([], 0)
        leader_spot = get_spot_total(leader_addr) if leader_addr else 0
        follower_spot = get_spot_total(follower_query_addr) if follower_query_addr else 0

        # 持仓对比
        pos_comparison = []
        leader_by_coin = {p['coin']: p for p in leader_pos}
        follower_by_coin = {p['coin']: p for p in follower_pos}
        all_coins = sorted(set(list(leader_by_coin.keys()) + list(follower_by_coin.keys())))

        target_ratio = follower_perf['ratio_avg'] if follower_perf and follower_perf.get('ratio_avg') else None

        for coin in all_coins:
            lp = leader_by_coin.get(coin, {})
            fp = follower_by_coin.get(coin, {})
            l_size = lp.get('size', 0)
            f_size = fp.get('size', 0)
            actual_ratio = round(f_size / l_size, 6) if l_size > 0 else None
            ratio_dev = round(actual_ratio - target_ratio, 6) if actual_ratio is not None and target_ratio else None

            # 方向是否一致
            direction_match = (lp.get('side', 'none') == fp.get('side', 'none')) if (lp and fp) else False

            pos_comparison.append({
                'coin': coin,
                'leader_side': lp.get('side', 'none'),
                'leader_size': l_size,
                'leader_entry': lp.get('entry_px', 0),
                'leader_upnl': lp.get('unrealized_pnl', 0),
                'leader_notional': lp.get('notional', 0),
                'follower_side': fp.get('side', 'none'),
                'follower_size': f_size,
                'follower_entry': fp.get('entry_px', 0),
                'follower_upnl': fp.get('unrealized_pnl', 0),
                'follower_notional': fp.get('notional', 0),
                'actual_ratio': actual_ratio,
                'ratio_deviation': ratio_dev,
                'direction_match': direction_match,
            })

        # 缺失持仓检查（Leader有但用户没有，或反过来）
        missing = []
        for coin in all_coins:
            lp = leader_by_coin.get(coin)
            fp = follower_by_coin.get(coin)
            if lp and not fp:
                missing.append({'coin': coin, 'issue': '用户缺失', 'leader_size': lp['size']})
            elif fp and not lp:
                missing.append({'coin': coin, 'issue': '用户多出', 'follower_size': fp['size']})
            elif lp and fp and lp['side'] != fp['side']:
                missing.append({'coin': coin, 'issue': '方向不一致', 'leader_side': lp['side'], 'follower_side': fp['side']})

        report['sections'][instance_id] = {
            'labels': {'leader': cfg['leader_label'], 'follower': cfg['follower_label']},
            'performance': {
                'leader': leader_perf,
                'follower': follower_perf,
                'deviation_pct': deviation,
            },
            'positions': {
                'leader_total_equity': round(leader_spot, 2),
                'follower_total_equity': round(follower_spot, 2),
                'leader_positions': leader_pos,
                'follower_positions': follower_pos,
                'comparison': pos_comparison,
                'missing': missing,
            },
            'fund_flows': {
                'leader': {
                    'deposits': round(leader_dep, 2),
                    'withdraws': round(leader_wd, 2),
                    'net': round(leader_dep - leader_wd, 2),
                    'records': leader_flows,
                },
                'follower': {
                    'deposits': round(follower_dep, 2),
                    'withdraws': round(follower_wd, 2),
                    'net': round(follower_dep - follower_wd, 2),
                    'records': follower_flows,
                },
            },
            'copy_ratio': {
                'avg': follower_perf['ratio_avg'] if follower_perf else None,
                'min': follower_perf['ratio_min'] if follower_perf else None,
                'max': follower_perf['ratio_max'] if follower_perf else None,
                'std': follower_perf.get('ratio_std') if follower_perf else None,
            },
            'execution': get_audit_stats(start_ts, end_ts, instance_id),
        }

        # ---- P2: 高级指标计算 ----
        try:
            l_rows = leader_perf.get('_rows', []) if leader_perf else []
            f_rows = follower_perf.get('_rows', []) if follower_perf else []
            rm_leader = calc_risk_metrics(l_rows, 'leader') if l_rows else {}
            rm_follower = calc_risk_metrics(f_rows, 'follower') if f_rows else {}
            report['sections'][instance_id]['risk_metrics'] = {
                'leader': rm_leader,
                'follower': rm_follower,
            }
            report['sections'][instance_id]['utilization'] = {
                'leader': calc_utilization(start_ts, end_ts, instance_id, 'leader'),
                'follower': calc_utilization(start_ts, end_ts, instance_id, 'follower'),
            }
            l_pos = report['sections'][instance_id]['positions'].get('leader_positions', [])
            f_pos = report['sections'][instance_id]['positions'].get('follower_positions', [])
            report['sections'][instance_id]['position_concentration'] = {
                'leader': calc_position_concentration(l_pos),
                'follower': calc_position_concentration(f_pos),
            }
            cur_returns = []
            if leader_perf and leader_perf.get('return_pct') is not None:
                cur_returns.append(leader_perf['return_pct'])
            if follower_perf and follower_perf.get('return_pct') is not None:
                cur_returns.append(follower_perf['return_pct'])
            avg_cur = sum(cur_returns) / len(cur_returns) if cur_returns else None
            report['sections'][instance_id]['mom_growth'] = calc_mom_growth(avg_cur, report_type)
            report['sections'][instance_id]['monthly_returns'] = {
                'leader': calc_monthly_returns_from_rows([(r[0], r[1]) for r in l_rows]) if l_rows else {},
                'follower': calc_monthly_returns_from_rows([(r[0], r[1]) for r in f_rows]) if f_rows else {},
            }
        except Exception as e:
            print(f'  [WARN] P2 metrics failed for {instance_id}: {e}')

    # ---- 交易统计回算 ----
    try:
        all_events = get_events(0, end_ts)
        trade_stats = backfill_trade_stats(all_events, start_ts, end_ts)
        for iid in list(SYSTEMS):
            if iid in report['sections'] and iid in trade_stats:
                report['sections'][iid]['trade_stats'] = trade_stats[iid]
    except Exception as e:
        print(f'  [WARN] trade_stats backfill failed: {e}')

    # ---- 系统健康 ----
    alert_stats = get_alert_stats(start_ts, end_ts)
    events = get_events(start_ts, end_ts)

    report['sections']['system'] = {
        'alerts': {
            'critical': alert_stats['critical'],
            'warning': alert_stats['warning'],
            'info': alert_stats['info'],
            'total': alert_stats['total'],
            'details': alert_stats.get('details', [])[:20],  # 最多保留20条
        },
        'key_events_count': len(events),
    }

    report['sections']['events'] = events

    return report


def format_report(report):
    """格式化为可读文本"""
    if not report:
        return '无数据'

    L = []
    type_cn = '月报' if report['type'] == 'monthly' else '周报'
    L.append(f'📊 HL跟单效果{type_cn}')
    L.append(f"周期: {report['period']}")
    L.append(f"生成: {report['generated_at'][:19]}")
    L.append('')

    sections = report.get('sections', {})

    # === 一、收益对比 ===
    L.append('━' * 50)
    L.append('【一、收益对比】')
    L.append('')

    # 汇总用变量
    sum_l_start = sum_l_end = sum_l_high = sum_l_low = 0
    sum_f_start = sum_f_end = sum_f_high = sum_f_low = 0
    sum_l_gross = sum_f_gross = 0
    sum_l_net_flow = sum_f_net_flow = 0
    sum_l_maxdd = sum_f_maxdd = 0
    sum_f_ratio_avg = 0
    sum_f_ratio_count = 0

    for iid in list(SYSTEMS):
        sec = sections.get(iid, {})
        labels = sec.get('labels', {})
        perf = sec.get('performance', {})
        cr = sec.get('copy_ratio', {})

        l = perf.get('leader')
        f = perf.get('follower')

        section_name = f'【{_inst_display_short(iid)}】'
        L.append(f"▎{section_name} {labels.get('leader', iid)} vs {labels.get('follower', iid)}")
        L.append('')

        if l:
            l_amp = l.get('amplitude_pct', 0)
            l_net_flow = l.get('net_flow', 0)
            L.append(f"  Leader:")
            L.append(f"    期初: ${l['start_equity']:,.2f}  |  最高: ${l['high']:,.2f}  |  最低: ${l['low']:,.2f}  |  期末: ${l['end_equity']:,.2f}")
            L.append(f"    资金进出: ${l_net_flow:+,.2f}  |  振幅: {l_amp:.2f}%  |  最大回撤: {l['max_drawdown_pct']:.2f}%  |  收益率: {l['return_pct']:+.2f}%")
            sum_l_start += l['start_equity']
            sum_l_end += l['end_equity']
            sum_l_high += l['high']
            sum_l_low += l['low']
            sum_l_gross += l.get('net_pnl', l['gross_pnl'])
            sum_l_net_flow += l_net_flow
            sum_l_maxdd = max(sum_l_maxdd, l['max_drawdown_pct'])
        L.append('')

        if f:
            f_amp = f.get('amplitude_pct', 0)
            f_net_flow = f.get('net_flow', 0)
            L.append(f"  用户:")
            L.append(f"    期初: ${f['start_equity']:,.2f}  |  最高: ${f['high']:,.2f}  |  最低: ${f['low']:,.2f}  |  期末: ${f['end_equity']:,.2f}")
            L.append(f"    资金进出: ${f_net_flow:+,.2f}  |  振幅: {f_amp:.2f}%  |  最大回撤: {f['max_drawdown_pct']:.2f}%  |  收益率: {f['return_pct']:+.2f}%")
            sum_f_start += f['start_equity']
            sum_f_end += f['end_equity']
            sum_f_high += f['high']
            sum_f_low += f['low']
            sum_f_gross += f.get('net_pnl', f['gross_pnl'])
            sum_f_net_flow += f_net_flow
            sum_f_maxdd = max(sum_f_maxdd, f['max_drawdown_pct'])
        L.append('')

        if f and f.get('ratio_avg') is not None:
            L.append(f"  跟单比例: {f['ratio_avg']:.4f} (区间 [{f.get('ratio_min', 0):.4f}, {f.get('ratio_max', 0):.4f}])")
            sum_f_ratio_avg += f['ratio_avg']
            sum_f_ratio_count += 1

        dev = perf.get('deviation_pct')
        if dev is not None:
            tag = '✅' if abs(dev) < 2 else '⚠️' if abs(dev) < 5 else '❌'
            L.append(f"  收益偏差: {dev:+.2f}% {tag}")
        L.append('')

    # ---- 汇总 ----
    L.append(f"▎【汇总】")
    L.append('')
    if sum_l_start > 0:
        # 汇总使用TWR方法：按各案例收益率加权平均
        sum_l_ret = sum_l_gross / sum_l_start * 100 if sum_l_start > 0 else 0
        sum_l_amp = (sum_l_high - sum_l_low) / sum_l_start * 100 if sum_l_start > 0 else 0
        L.append(f"  Leader(合计):")
        L.append(f"    期初: ${sum_l_start:,.2f}  |  最高: ${sum_l_high:,.2f}  |  最低: ${sum_l_low:,.2f}  |  期末: ${sum_l_end:,.2f}")
        L.append(f"    资金进出: ${sum_l_net_flow:+,.2f}  |  振幅: {sum_l_amp:.2f}%  |  最大回撤: {sum_l_maxdd:.2f}%  |  收益率: {sum_l_ret:+.2f}%")
    L.append('')
    if sum_f_start > 0:
        sum_f_ret = sum_f_gross / sum_f_start * 100 if sum_f_start > 0 else 0
        sum_f_amp = (sum_f_high - sum_f_low) / sum_f_start * 100 if sum_f_start > 0 else 0
        sum_ratio = sum_f_ratio_avg / sum_f_ratio_count if sum_f_ratio_count > 0 else 0
        sum_dev = sum_f_ret - sum_l_ret if sum_l_start > 0 else 0
        L.append(f"  用户(合计):")
        L.append(f"    期初: ${sum_f_start:,.2f}  |  最高: ${sum_f_high:,.2f}  |  最低: ${sum_f_low:,.2f}  |  期末: ${sum_f_end:,.2f}")
        L.append(f"    资金进出: ${sum_f_net_flow:+,.2f}  |  振幅: {sum_f_amp:.2f}%  |  最大回撤: {sum_f_maxdd:.2f}%  |  收益率: {sum_f_ret:+.2f}%")
        L.append('')
        L.append(f"  跟单比例: {sum_ratio:.4f}")
        sum_tag = '✅' if abs(sum_dev) < 2 else '⚠️' if abs(sum_dev) < 5 else '❌'
        L.append(f"  收益偏差: {sum_dev:+.2f}% {sum_tag}")
    L.append('')

    # === 二、持仓快照 ===
    L.append('━' * 50)
    L.append('【二、持仓快照】')
    L.append('')

    for iid in list(SYSTEMS):
        sec = sections.get(iid, {})
        labels = sec.get('labels', {})
        pos_data = sec.get('positions', {})
        comparison = pos_data.get('comparison', [])
        missing = pos_data.get('missing', [])

        L.append(f"▎{labels.get('leader', iid)} 总资产: ${pos_data.get('leader_total_equity', 0):,.2f}")
        L.append(f"▎{labels.get('follower', iid)} 总资产: ${pos_data.get('follower_total_equity', 0):,.2f}")
        L.append('')

        if not comparison:
            L.append('  无持仓')
            continue

        L.append(f"  {'币种':>6} {'Leader':>14} {'用户':>14} {'比例':>8} {'偏差':>8}")
        L.append('  ' + '─' * 56)

        for c in comparison:
            l_str = f"{c['leader_side'][0].upper()}{c['leader_size']:.2f}" if c['leader_side'] != 'none' else '  --'
            f_str = f"{c['follower_side'][0].upper()}{c['follower_size']:.2f}" if c['follower_side'] != 'none' else '  --'
            ratio_str = f"{c['actual_ratio']:.4f}" if c['actual_ratio'] is not None else 'N/A'
            dev_str = f"{c['ratio_deviation']:+.4f}" if c.get('ratio_deviation') is not None else 'N/A'
            L.append(f"  {c['coin']:>6} {l_str:>14} {f_str:>14} {ratio_str:>8} {dev_str:>8}")

        if missing:
            L.append('')
            for m in missing:
                L.append(f"  ⚠️ {m['coin']}: {m['issue']}")

        L.append('')

    # === 三、资金进出 ===
    L.append('━' * 50)
    L.append('【三、资金进出】')
    L.append('')

    has_flows = False
    for iid in list(SYSTEMS):
        sec = sections.get(iid, {})
        labels = sec.get('labels', {})
        flows = sec.get('fund_flows', {})

        for role, label_key in [('leader', 'leader'), ('follower', 'follower')]:
            f = flows.get(role, {})
            if f.get('net', 0) != 0 or f.get('records'):
                has_flows = True
                name = labels.get(label_key, role)
                L.append(f"  {name}: 充值 ${f.get('deposits', 0):,.2f} | 提现 ${f.get('withdraws', 0):,.2f} | 净额 ${f.get('net', 0):+,.2f} ({len(f.get('records', []))}笔)")

                # 列出具体记录
                for rec in f.get('records', [])[:10]:
                    dt = datetime.fromtimestamp(rec['ts']).strftime('%m-%d %H:%M')
                    L.append(f"    {dt} {rec['type']:>8} ${rec['amount']:>10,.2f}")

    if not has_flows:
        L.append('  本周期内无资金进出')
    L.append('')

    # === 四、跟单比例 ===
    L.append('━' * 50)
    L.append('【四、跟单比例】')
    L.append('')

    for iid in list(SYSTEMS):
        sec = sections.get(iid, {})
        labels = sec.get('labels', {})
        cr = sec.get('copy_ratio', {})
        name = labels.get('follower', iid)

        if cr.get('avg') is not None:
            L.append(f"  {name}: 均值={cr['avg']:.6f} 区间=[{cr['min']:.6f}, {cr['max']:.6f}] 标准差={cr.get('std', 0):.6f}")
        else:
            L.append(f"  {name}: 无数据")
    L.append('')

    # === 五、跟单执行统计 ===
    L.append('━' * 50)
    L.append('【五、跟单执行统计】')
    L.append('')

    for iid in list(SYSTEMS):
        sec = sections.get(iid, {})
        labels = sec.get('labels', {})
        exe = sec.get('execution', {})
        name = labels.get('leader', iid) + '→' + labels.get('follower', iid)

        if exe:
            total_ops = exe.get('opens', 0) + exe.get('adjusts', 0) + exe.get('closes', 0)
            L.append(f"  {name}:")
            L.append(f"    开仓={exe.get('opens', 0)} 加仓={exe.get('adjusts', 0)} 平仓={exe.get('closes', 0)} (合计{total_ops}笔操作)")
            L.append(f"    RECON={exe.get('recons', 0)} 部分成交={exe.get('partial_fills', 0)} 补单={exe.get('confirm_retries', 0)}")
            L.append(f"    轮询={exe.get('polls', 0)}次 WS断连={exe.get('ws_disconnects', 0)}次")
        else:
            L.append(f"  {name}: 无审计数据")
    L.append('')

    # === 六、系统健康 ===
    sys_data = sections.get('system', {})
    alerts = sys_data.get('alerts', {})
    L.append('━' * 50)
    L.append('【六、系统健康】')
    L.append(f"  告警: 🔴CRITICAL {alerts.get('critical', 0)} | 🟡WARNING {alerts.get('warning', 0)} | 🔵INFO {alerts.get('info', 0)}")
    L.append(f"  关键事件: {sys_data.get('key_events_count', 0)}次")

    # 列出 CRITICAL 告警
    for d in alerts.get('details', []):
        if d.get('level') == 'CRITICAL':
            L.append(f"    [{d['time']}] 🔴 {d.get('title', '')}")
    L.append('')

    # === 七、关键事件 ===
    events = sections.get('events', [])
    if events:
        L.append('━' * 50)
        L.append('【七、关键事件】')
        for e in events[:20]:
            dt = datetime.fromtimestamp(e['ts']).strftime('%m-%d %H:%M')
            L.append(f"  [{dt}] {e.get('message', '')}")
        if len(events) > 20:
            L.append(f"  ... 共{len(events)}条")

    return '\n'.join(L)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='HL跟单效果报告')
    parser.add_argument('type_positional', nargs='?', choices=['weekly', 'monthly'], default=None, help='报告类型(位置参数)')
    parser.add_argument('--type', dest='type_flag', choices=['weekly', 'monthly'], default=None, help='报告类型')
    parser.add_argument('--test', action='store_true', help='测试模式')
    args = parser.parse_args()

    report_type = args.type_flag or args.type_positional or 'monthly'
    report = generate_report(report_type)

    if report:
        if args.test:
            report['test_mode'] = True
        Path(REPORT_DIR).mkdir(parents=True, exist_ok=True)
        ts = datetime.now().strftime('%Y%m%d_%H%M%S')
        json_path = f'{REPORT_DIR}/{report_type}_{ts}.json'

        with open(json_path, 'w') as f:
            json.dump(report, f, indent=2, ensure_ascii=False)

        print(f'\n[Report] Saved: {json_path}')

        text = format_report(report)
        print('\n' + text)
    else:
        print('[ERROR] Report generation failed')
