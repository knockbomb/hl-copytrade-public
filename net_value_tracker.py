#!/usr/bin/env python3
"""
HL跟单效果追踪系统 - 数据采集脚本 v3
修复：
  1. 智能触发采样：启动时检查/tmp/trigger_resample.flag，15分钟内则执行triggered采样
  2. ledger API时间戳：startTime从秒级改为毫秒级
  3. query_hl_api/query_ledger添加重试机制（3次重试+指数退避）
  4. get_copy_ratio优先从.orchestrator子目录读取最新审计文件
  5. 触发后使用at命令调度10分钟后的加密采样
"""

import sqlite3
import requests
import time
import json
import os
import subprocess
from datetime import datetime
from pathlib import Path

# 配置
from paths import BASE_DIR, DATA_DIR
DB_PATH = str(DATA_DIR / 'net_value_history.db')
STATE_PATH = str(DATA_DIR / 'tracker_state.json')
# BASE_DIR already imported from paths
TRIGGER_FLAG = '/tmp/trigger_resample.flag'

# P1-1: 从统一配置读取账户地址（单一数据源: .orchestrator .env）
from shared_config import get_accounts
ACCOUNTS = get_accounts()

# 智能触发阈值
TRIGGER_THRESHOLD_VALUE = 0.05
TRIGGER_THRESHOLD_RATIO = 0.20

# 重试配置
MAX_RETRIES = 3
RETRY_BASE_DELAY = 2  # 秒

def load_state():
    if os.path.exists(STATE_PATH):
        with open(STATE_PATH, 'r') as f:
            return json.load(f)
    return {}

def save_state(state):
    os.makedirs(os.path.dirname(STATE_PATH), exist_ok=True)
    with open(STATE_PATH, 'w') as f:
        json.dump(state, f, indent=2)

# ====== 修复1: 添加重试机制 ======
def query_hl_api(endpoint_type, address):
    """查询HL API，带3次重试+指数退避"""
    url = 'https://api.hyperliquid.xyz/info'
    payload = {'type': endpoint_type, 'user': address}
    
    for attempt in range(MAX_RETRIES):
        try:
            resp = requests.post(url, json=payload, timeout=10)
            resp.raise_for_status()
            return resp.json()
        except Exception as e:
            delay = RETRY_BASE_DELAY * (2 ** attempt)
            if attempt < MAX_RETRIES - 1:
                print(f'  Retry {attempt+1}/{MAX_RETRIES} for {endpoint_type}: {e}, waiting {delay}s...')
                time.sleep(delay)
            else:
                print(f'  Failed after {MAX_RETRIES} retries for {endpoint_type}: {e}')
                raise

# ====== 修复2: ledger API时间戳改为毫秒级 ======
def query_ledger(address, start_time=None):
    """查询ledger记录，检测资金进出
    【修复】使用userNonFundingLedgerUpdates替代已废弃的userLedger
    【修复】startTime使用毫秒级时间戳
    返回格式：[{time, hash, delta: {type, usdc/amount, ...}}, ...]
    """
    url = 'https://api.hyperliquid.xyz/info'
    payload = {
        'type': 'userNonFundingLedgerUpdates',
        'user': address
    }
    if start_time:
        # 【修复】API需要毫秒级时间戳，原代码传的是秒级
        payload['startTime'] = int(start_time * 1000)
    
    # 【修复】添加重试机制
    for attempt in range(MAX_RETRIES):
        try:
            resp = requests.post(url, json=payload, timeout=10)
            resp.raise_for_status()
            return resp.json()
        except Exception as e:
            delay = RETRY_BASE_DELAY * (2 ** attempt)
            if attempt < MAX_RETRIES - 1:
                print(f'  Retry {attempt+1}/{MAX_RETRIES} for ledger: {e}, waiting {delay}s...')
                time.sleep(delay)
            else:
                print(f'  Failed after {MAX_RETRIES} retries for ledger: {e}')
                return None

def query_account_info(address):
    """查询账户信息"""
    # 查询合约账户
    clearinghouse = query_hl_api('clearinghouseState', address)
    
    # 查询现货账户
    spot = query_hl_api('spotClearinghouseState', address)
    
    # Account Value从marginSummary获取
    account_value = float(clearinghouse.get('marginSummary', {}).get('accountValue', 0))
    
    # Total Equity = 现货USDC total
    total_equity = 0
    if spot.get('balances'):
        for b in spot['balances']:
            if b.get('coin') == 'USDC':
                total_equity = float(b.get('total', 0))
                break
    
    # 持仓数量和名义价值
    positions = clearinghouse.get('assetPositions', [])
    position_count = len([p for p in positions if float(p.get('position', {}).get('szi', 0)) != 0])
    total_notional = sum(abs(float(p.get('position', {}).get('szi', 0)) * float(p.get('position', {}).get('entryPx', 0))) 
                         for p in positions)
    
    return {
        'account_value': account_value,
        'total_equity': total_equity,
        'position_count': position_count,
        'total_notional': total_notional
    }

# ====== 修复5+P2#11: get_copy_ratio优先从Orchestrator实时状态文件读取 ======
def get_copy_ratio(instance_id):
    """从Orchestrator实时状态文件读取跟单比例
    优先级：1. .orchestrator/{instance}/hl_copytrade_v3_{instance}_state.json (实时)
           2. .orchestrator/{instance}/审计文件 (近实时，备选)
           3. 主目录审计文件 (旧路径，兼容)
    """
    instance_name = 'lowfreq' if instance_id == 'lf' else 'highfreq'
    
    # 优先级1：从Orchestrator实时状态文件读取（最新、最准确）
    try:
        state_file = Path(BASE_DIR) / '.orchestrator' / instance_name / f'hl_copytrade_v3_{instance_name}_state.json'
        if state_file.exists():
            with open(state_file, 'r') as f:
                state = json.load(f)
            # 状态文件不含copy_ratio时，尝试从user_positions和leader_positions推算
            leader_positions = state.get('leader_positions', {})
            user_positions = state.get('user_positions', {})
            if leader_positions and user_positions:
                # 从任意币种的持仓比例推算copy_ratio
                for coin in leader_positions:
                    if coin in user_positions:
                        leader_val = abs(float(leader_positions[coin].get('positionValue', 0)))
                        user_val = abs(float(user_positions[coin].get('positionValue', 0)))
                        if leader_val > 0:
                            ratio = user_val / leader_val
                            print(f'  Copy ratio from state.json (实时): {ratio:.6f}')
                            return ratio
    except Exception as e:
        print(f'  Warning: Failed to read copy ratio from state.json: {e}')
    
    # 优先级2：从Orchestrator子目录的审计文件读取（备选）
    try:
        audit_files = []
        
        orch_dir = Path(BASE_DIR) / '.orchestrator' / instance_name
        if orch_dir.exists():
            orch_files = sorted(orch_dir.glob(f'hl_copytrade_v3_{instance_name}_audit_*.json'))
            audit_files.extend(orch_files)
        
        # 优先级3：兼容主目录（旧路径）
        if instance_id == 'lf':
            old_files = sorted(Path(BASE_DIR).glob('hl_copytrade_v3_audit_*.json'))
        elif instance_id == 'hf':
            old_files = sorted(Path(BASE_DIR).glob('hl_copytrade_v3_highfreq_audit_*.json'))
        else:
            old_files = []
        audit_files.extend(old_files)
        
        if not audit_files:
            print(f'  Warning: No audit files found for {instance_id}')
            return None
        
        # 取最新的审计文件
        latest_file = sorted(audit_files, key=lambda p: p.stat().st_mtime)[-1]
        
        with open(latest_file, 'r') as f:
            audit_list = json.load(f)
            if isinstance(audit_list, list) and len(audit_list) > 0:
                latest = audit_list[-1]
                ratio = float(latest.get('copy_ratio', 0))
                print(f'  Copy ratio from audit file (备选) {latest_file.name}: {ratio:.6f}')
                return ratio
    except Exception as e:
        print(f'  Warning: Failed to read copy ratio from audit: {e}')
    
    return None

def detect_fund_flow_with_ledger(db, ts, instance_id, role, address, current_equity, last_equity, last_ts):
    """使用ledger API检测资金进出"""
    if last_equity is None or last_equity == 0:
        return None
    
    change = current_equity - last_equity
    change_pct = abs(change) / last_equity
    
    # 变化超过1%才检查
    if change_pct <= 0.01:
        return None
    
    # 查询ledger记录
    ledger = query_ledger(address, last_ts)
    
    if not ledger or 'ledger' not in ledger:
        # [FIX 2026-08-01] ledger查询失败时不推断，避免把交易盈亏误判为资金进出
        return None
    
    # 【修复】userNonFundingLedgerUpdates直接返回列表，不再包裹在ledger字段中
    if isinstance(ledger, list):
        ledger_entries = ledger
    else:
        ledger_entries = ledger.get('ledger', [])
    
    # 筛选资金进出类型的记录
    # userNonFundingLedgerUpdates的delta type包含: deposit, withdraw, send, internalTransfer等
    deposit_types = ['deposit', 'deposit_internal']
    withdraw_types = ['withdraw', 'withdraw_internal']
    send_in_types = ['send']  # send需要判断方向
    
    total_deposits = 0
    total_withdraws = 0
    tx_hash = None
    
    for entry in ledger_entries:
        entry_type = entry.get('delta', {}).get('type', '')
        
        # 对于send类型，需要判断方向（用户是source还是destination）
        if entry_type in send_in_types:
            dest = entry.get('delta', {}).get('destination', '')
            if dest.lower() == address.lower():
                # 用户是接收方 = 充值
                amount = float(entry.get('delta', {}).get('amount', 0))
                total_deposits += amount
                tx_hash = entry.get('hash')
                continue
            else:
                # 用户是发送方 = 提现
                amount = float(entry.get('delta', {}).get('amount', 0))
                total_withdraws += amount
                tx_hash = entry.get('hash')
                continue
        
        # 对于deposit/withdraw类型
        if entry_type in deposit_types:
            amount = float(entry.get('delta', {}).get('usdc', entry.get('delta', {}).get('amount', 0)))
            if amount > 0:
                total_deposits += amount
                tx_hash = entry.get('hash')
        elif entry_type in withdraw_types:
            amount = float(entry.get('delta', {}).get('usdc', entry.get('delta', {}).get('amount', 0)))
            if amount > 0:
                total_withdraws += amount
                tx_hash = entry.get('hash')
        else:
            # 其他类型（如liquidation等）忽略
            continue
    
    # 如果有ledger记录，使用ledger数据
    if total_deposits > 0 or total_withdraws > 0:
        net_flow = total_deposits - total_withdraws
        
        if abs(net_flow) > 1:  # 超过$1
            flow_type = 'deposit' if net_flow > 0 else 'withdraw'
            
            c = db.cursor()
            c.execute('''
                INSERT OR IGNORE INTO fund_flows 
                (ts, instance_id, role, flow_type, amount, balance_before, balance_after, tx_hash)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            ''', (ts, instance_id, role, flow_type, abs(net_flow), last_equity, current_equity, tx_hash))
            db.commit()
            
            return flow_type
    
    return None

def should_trigger_sample(current_data, last_state, instance_id, role):
    """判断是否需要智能触发采样"""
    if not last_state or instance_id not in last_state or role not in last_state[instance_id]:
        return False
    
    last = last_state[instance_id][role]
    
    if last['total_equity'] > 0:
        value_change = abs(current_data['total_equity'] - last['total_equity']) / last['total_equity']
        if value_change > TRIGGER_THRESHOLD_VALUE:
            return True
    
    if role == 'follower' and last.get('copy_ratio') and current_data.get('copy_ratio'):
        if last['copy_ratio'] > 0:
            ratio_change = abs(current_data['copy_ratio'] - last['copy_ratio']) / last['copy_ratio']
            if ratio_change > TRIGGER_THRESHOLD_RATIO:
                return True
    
    return False

# ====== 修复1: 检查触发标记文件 ======
def check_trigger_flag():
    """检查/tmp/trigger_resample.flag标记文件
    如果存在且创建时间在15分钟内，返回True并删除标记文件
    """
    if not os.path.exists(TRIGGER_FLAG):
        return False
    
    try:
        with open(TRIGGER_FLAG, 'r') as f:
            flag_time = int(f.read().strip())
        
        now = int(time.time())
        age_seconds = now - flag_time
        
        if age_seconds < 900:  # 15分钟内
            print(f'  Trigger flag found, age={age_seconds}s (< 900s), executing triggered sample')
            # 删除标记文件，避免重复触发
            os.remove(TRIGGER_FLAG)
            return True
        else:
            print(f'  Trigger flag found but expired, age={age_seconds}s (>= 900s), ignoring')
            os.remove(TRIGGER_FLAG)
            return False
    except Exception as e:
        print(f'  Warning: Error reading trigger flag: {e}')
        try:
            os.remove(TRIGGER_FLAG)
        except:
            pass
        return False

# ====== 修复1: 调度延迟采样 ======
def schedule_triggered_sample():
    """调度10分钟后的triggered采样
    优先使用at命令，不可用则用后台sleep方式
    """
    script_path = str(BASE_DIR / 'net_value_tracker.py')
    python_path = '/usr/bin/python3'
    
    # 方式1: 使用at命令（如果可用）
    try:
        result = subprocess.run(['which', 'at'], capture_output=True, text=True, timeout=5)
        if result.returncode == 0:
            cmd = f'{python_path} {script_path} triggered'
            subprocess.run(
                ['at', 'now + 10 minutes'],
                input=cmd + '\n',
                text=True,
                capture_output=True,
                timeout=10
            )
            print('  Scheduled triggered sample via at command (now + 10 min)')
            return
    except Exception as e:
        print(f'  at command not available: {e}')
    
    # 方式2: 后台sleep方式
    try:
        subprocess.Popen(
            ['nohup', 'bash', '-c', f'sleep 600 && {python_path} {script_path} triggered'],
            stdout=open('/dev/null', 'w'),
            stderr=open('/dev/null', 'w'),
            preexec_fn=os.setpgrp
        )
        print('  Scheduled triggered sample via background sleep (600s)')
    except Exception as e:
        print(f'  Warning: Failed to schedule triggered sample: {e}')

def collect_data(sample_type='scheduled'):
    """采集数据"""
    ts = int(time.time())
    dt = datetime.fromtimestamp(ts)
    
    print(f'\n[{dt.strftime("%Y-%m-%d %H:%M:%S")}] Collecting data (type={sample_type})')
    
    last_state = load_state()
    current_state = {}
    
    db = sqlite3.connect(DB_PATH)
    c = db.cursor()
    
    triggered = False
    
    for instance_id, accounts in ACCOUNTS.items():
        current_state[instance_id] = {}
        
        for role, address in accounts.items():
            try:
                info = query_account_info(address)
                
                copy_ratio = None
                if role == 'follower':
                    copy_ratio = get_copy_ratio(instance_id)
                
                # 记录快照
                c.execute('''
                    INSERT OR REPLACE INTO net_value_snapshots
                    (ts, instance_id, role, account_value, total_equity, position_count, total_notional, copy_ratio, sample_type)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                ''', (ts, instance_id, role, info['account_value'], info['total_equity'], 
                      info['position_count'], info['total_notional'], copy_ratio, sample_type))
                
                # 检测资金进出（使用ledger确认）
                last_equity = None
                last_ts = None
                if instance_id in last_state and role in last_state[instance_id]:
                    last_equity = last_state[instance_id][role]['total_equity']
                    last_ts = last_state[instance_id][role].get('ts')
                
                flow = detect_fund_flow_with_ledger(db, ts, instance_id, role, address, 
                                                      info['total_equity'], last_equity, last_ts)
                if flow:
                    print(f'  Fund flow detected: {instance_id}/{role} {flow} ${abs(info["total_equity"] - (last_equity or 0)):.2f}')
                
                current_data = {**info, 'copy_ratio': copy_ratio}
                if should_trigger_sample(current_data, last_state, instance_id, role):
                    triggered = True
                    print(f'  Trigger: {instance_id}/{role}')
                
                current_state[instance_id][role] = {
                    'total_equity': info['total_equity'],
                    'copy_ratio': copy_ratio,
                    'ts': ts
                }
                
                ratio_str = f', ratio={copy_ratio:.4f}' if copy_ratio else ''
                print(f'  {instance_id}/{role}: AV=${info["account_value"]:.2f}, TE=${info["total_equity"]:.2f}, '
                      f'positions={info["position_count"]}{ratio_str}')
                
            except Exception as e:
                print(f'  Error {instance_id}/{role}: {e}')
    
    db.commit()
    db.close()
    
    save_state(current_state)
    
    print(f'[{dt.strftime("%Y-%m-%d %H:%M:%S")}] Collection completed')
    
    return triggered

if __name__ == '__main__':
    import sys
    
    sample_type = 'scheduled'
    if len(sys.argv) > 1:
        sample_type = sys.argv[1]
    
    # ====== 修复1: 启动时检查触发标记文件 ======
    if sample_type == 'scheduled' and check_trigger_flag():
        sample_type = 'triggered'
        print('Switched to triggered sampling due to flag file')
    
    triggered = collect_data(sample_type)
    
    if triggered and sample_type == 'scheduled':
        print('\nAbnormal volatility detected, scheduling resample in 10 minutes')
        # 创建标记文件，下次执行时检查
        with open(TRIGGER_FLAG, 'w') as f:
            f.write(str(int(time.time())))
        # 立即调度10分钟后的triggered采样
        schedule_triggered_sample()
