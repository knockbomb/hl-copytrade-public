#!/usr/bin/env python3
"""
HL跟单效果追踪系统 - 关键事件记录
记录：Leader开仓/平仓、大额资金进出、跟单比例异常等

P1修复记录：
  #7 - 事件保留量从100改为500，避免月度事件被截断
  #8 - query_positions添加重试机制（指数退避，参考net_value_tracker.py）
  #9 - 事件去重使用复合键(ts+type+amount+instance_id+role)，修复同一秒多笔交易漏记
"""

import sqlite3
import json
import requests
import time
from datetime import datetime, timedelta
from pathlib import Path

from paths import BASE_DIR, DATA_DIR
DB_PATH = str(DATA_DIR / 'net_value_history.db')
EVENTS_PATH = str(DATA_DIR / 'key_events.json')

# 【P1修复#7】事件保留量从100提升到500，一个月可能300+事件
MAX_EVENTS = 500

# P1-1: 从统一配置读取账户地址（单一数据源: .orchestrator .env）
from shared_config import get_accounts
from heartbeat_writer import write_heartbeat
ACCOUNTS = get_accounts()

def load_events():
    if Path(EVENTS_PATH).exists():
        with open(EVENTS_PATH, 'r') as f:
            return json.load(f)
    return []

def save_events(events):
    with open(EVENTS_PATH, 'w') as f:
        json.dump(events, f, indent=2)

def query_positions(address, retries=3):
    """查询当前持仓
    【P1修复#8】添加重试机制（指数退避），网络异常时不会直接崩溃
    """
    url = 'https://api.hyperliquid.xyz/info'
    payload = {'type': 'clearinghouseState', 'user': address}
    
    for attempt in range(retries):
        try:
            resp = requests.post(url, json=payload, timeout=10)
            resp.raise_for_status()
            data = resp.json()
            
            positions = {}
            for p in data.get('assetPositions', []):
                pos = p.get('position', {})
                coin = pos.get('coin')
                size = float(pos.get('szi', 0))
                if size != 0:
                    positions[coin] = {
                        'size': size,
                        'entry_px': float(pos.get('entryPx', 0)),
                        'side': 'long' if size > 0 else 'short'
                    }
            return positions
        except Exception as e:
            if attempt < retries - 1:
                wait = 2 ** attempt  # 指数退避：1s, 2s, 4s...
                print(f"query_positions retry {attempt+1}/{retries} for {address[:10]}...: {e}, waiting {wait}s")
                time.sleep(wait)
                continue
            raise e

# 【2026-07-29】以下 detect_position_changes() 函数已禁用调用（用户要求），仅保留定义以备将来需要
def detect_position_changes():
    """检测持仓变化（开仓/平仓）- 已禁用，不再调用"""
    events = load_events()
    last_state_file = DATA_DIR / 'position_state.json'
    
    # 加载上次持仓
    last_positions = {}
    if last_state_file.exists():
        with open(last_state_file, 'r') as f:
            last_positions = json.load(f)
    
    current_positions = {}
    
    for instance_id, accounts in ACCOUNTS.items():
        leader_addr = accounts['leader']
        positions = query_positions(leader_addr)
        current_positions[instance_id] = positions
        
        last = last_positions.get(instance_id, {})
        
        # 检测新开仓
        for coin, pos in positions.items():
            if coin not in last:
                events.append({
                    'ts': int(datetime.now().timestamp()),
                    'type': 'position_open',
                    'instance_id': instance_id,
                    'coin': coin,
                    'side': pos['side'],
                    'size': pos['size'],
                    'entry_px': pos['entry_px'],
                    'message': f"Leader{instance_id}开仓{coin} {pos['side']} {abs(pos['size'])}张 @ ${pos['entry_px']:.2f}"
                })
        
        # 检测平仓
        for coin, pos in last.items():
            if coin not in positions:
                events.append({
                    'ts': int(datetime.now().timestamp()),
                    'type': 'position_close',
                    'instance_id': instance_id,
                    'coin': coin,
                    'side': pos['side'],
                    'size': pos['size'],
                    'message': f"Leader{instance_id}平仓{coin} {pos['side']}"
                })
        
        # P2修复#18: 检测加仓/减仓 - 阈值从10%降至5%，并添加绝对值阈值($100)
        for coin in set(positions.keys()) & set(last.keys()):
            curr_size = positions[coin]['size']
            last_size = last[coin]['size']
            
            # 相对变化>5% 或 绝对变化>$100 才记录
            size_change_pct = abs(abs(curr_size) - abs(last_size)) / abs(last_size) if last_size != 0 else 1
            size_change_usd = abs(abs(curr_size) - abs(last_size)) * positions[coin]['entry_px']
            
            if abs(curr_size) > abs(last_size) and (size_change_pct > 0.05 or size_change_usd > 100):  # 加仓>5%或>$100
                events.append({
                    'ts': int(datetime.now().timestamp()),
                    'type': 'position_add',
                    'instance_id': instance_id,
                    'coin': coin,
                    'side': positions[coin]['side'],
                    'size_before': last_size,
                    'size_after': curr_size,
                    'message': f"Leader{instance_id}加仓{coin} {positions[coin]['side']} {abs(last_size):.2f}→{abs(curr_size):.2f} ({size_change_pct*100:.1f}%)"
                })
            elif abs(curr_size) < abs(last_size) and (size_change_pct > 0.05 or size_change_usd > 100):  # 减仓>5%或>$100
                events.append({
                    'ts': int(datetime.now().timestamp()),
                    'type': 'position_reduce',
                    'instance_id': instance_id,
                    'coin': coin,
                    'side': positions[coin]['side'],
                    'size_before': last_size,
                    'size_after': curr_size,
                    'message': f"Leader{instance_id}减仓{coin} {positions[coin]['side']} {abs(last_size):.2f}→{abs(curr_size):.2f} ({size_change_pct*100:.1f}%)"
                })
    
    # 保存当前持仓
    with open(last_state_file, 'w') as f:
        json.dump(current_positions, f)
    
    # 【P1修复#7】保留最近500条事件（原100条不够一个月）
    events = events[-MAX_EVENTS:]
    save_events(events)
    
    return events[-5:]  # 返回最近5条

# P2修复#19: 大额资金动态阈值 - 根据账户净值调整
def get_dynamic_fund_threshold():
    """根据账户规模动态调整大额资金阈值
    阈值 = max($100, 账户净值的1%)
    多账户时取最大净值
    """
    max_equity = 0
    for instance_id, accounts in ACCOUNTS.items():
        for role, address in accounts.items():
            try:
                url = 'https://api.hyperliquid.xyz/info'
                payload = {'type': 'spotClearinghouseState', 'user': address}
                resp = requests.post(url, json=payload, timeout=10)
                resp.raise_for_status()
                data = resp.json()
                if data.get('balances'):
                    for b in data['balances']:
                        if b.get('coin') == 'USDC':
                            equity = float(b.get('total', 0))
                            max_equity = max(max_equity, equity)
                            break
            except Exception as e:
                print(f"  Warning: Failed to query equity for {address[:10]}...: {e}")
    
    if max_equity <= 0:
        return 100  # 默认$100
    
    threshold = max(100, max_equity * 0.01)
    print(f"  动态大额阈值: ${threshold:.0f} (基于最大净值${max_equity:.0f}的1%)")
    return threshold

def detect_large_fund_flows(threshold=None):
    """检测大额资金进出
    P2修复#19: threshold默认None，运行时动态计算
    """
    # P2修复#19: 动态阈值
    if threshold is None:
        threshold = get_dynamic_fund_threshold()
    
    db = sqlite3.connect(DB_PATH)
    c = db.cursor()
    
    c.execute('''
        SELECT ts, instance_id, role, flow_type, amount
        FROM fund_flows
        WHERE amount > ?
        ORDER BY ts DESC
        LIMIT 10
    ''', (threshold,))
    
    flows = c.fetchall()
    db.close()
    
    events = load_events()
    
    for f in flows:
        ts, instance_id, role, flow_type, amount = f
        
        # 【P1修复#9】使用复合键去重（ts+type+amount+instance_id+role）
        # 原逻辑仅用ts+type去重，同一秒多笔交易会漏记
        event_key = (ts, 'large_fund_flow', amount, instance_id, role)
        if any(
            (e['ts'], e['type'], e.get('amount'), e.get('instance_id'), e.get('role')) == event_key
            for e in events
        ):
            continue
        
        events.append({
            'ts': ts,
            'type': 'large_fund_flow',
            'instance_id': instance_id,
            'role': role,
            'flow_type': flow_type,
            'amount': amount,
            'message': f"{instance_id} {role} 大额{flow_type} ${amount:.2f}"
        })
    
    # 【P1修复#7】保留最近500条事件
    events = events[-MAX_EVENTS:]
    save_events(events)


if __name__ == '__main__':
    _ec = 0
    try:
        # 【2026-07-29】持仓变化检测已禁用（用户要求），不再将Leader持仓变化记录到关键事件中
        # 保留 detect_position_changes() 函数定义以备将来需要，但不再调用
        # print("Detecting position changes...")
        # recent_events = detect_position_changes()

        print("Detecting large fund flows...")
        detect_large_fund_flows()

        # 持仓变化检测已禁用，不再输出持仓相关事件
        # print(f"\nRecent events ({len(recent_events)}):")
        # for e in recent_events:
        #     dt = datetime.fromtimestamp(e['ts']).strftime('%Y-%m-%d %H:%M')
        #     print(f"  [{dt}] {e['message']}")
    except Exception as _e:
        _ec = 1
        print(f"执行异常: {_e}")
    finally:
        try:
            write_heartbeat("key_events_logger", exit_code=_ec)
        except Exception:
            pass
    sys.exit(_ec)
