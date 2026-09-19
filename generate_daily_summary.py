#!/usr/bin/env python3
"""
HL跟单效果追踪系统 - 日终汇总生成脚本
每天0点执行，生成前一天的汇总数据
"""

import sqlite3
import json
from datetime import datetime, timedelta
from shared_config import get_instances
from pathlib import Path

from paths import DATA_DIR
DB_PATH = str(DATA_DIR / 'net_value_history.db')

def generate_daily_summary():
    """生成前一天的日终汇总"""
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    
    # 获取昨天的日期范围（UTC+8）
    yesterday = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')
    today = datetime.now().strftime('%Y-%m-%d')
    
    # 转换为时间戳（假设是UTC时间）
    yesterday_start = int(datetime.strptime(yesterday, '%Y-%m-%d').timestamp())
    today_start = int(datetime.strptime(today, '%Y-%m-%d').timestamp())
    
    print(f'Generating daily summary for {yesterday}')
    
    # 查询所有实例和角色
    instances = list(get_instances())
    roles = ['leader', 'follower']
    
    for instance_id in instances:
        for role in roles:
            # 查询昨天的快照数据
            cursor.execute("""
                SELECT 
                    total_equity,
                    copy_ratio,
                    ts
                FROM net_value_snapshots
                WHERE instance_id = ? AND role = ?
                AND ts >= ? AND ts < ?
                ORDER BY ts
            """, (instance_id, role, yesterday_start, today_start))
            
            rows = cursor.fetchall()
            
            if not rows:
                print(f'  {instance_id}/{role}: No data for {yesterday}')
                continue
            
            # 计算统计数据
            total_equities = [r[0] for r in rows]
            copy_ratios = [r[1] for r in rows if r[1] is not None]
            
            start_value = total_equities[0]
            end_value = total_equities[-1]
            high_value = max(total_equities)
            low_value = min(total_equities)
            snapshot_count = len(rows)
            
            # 查询资金进出
            cursor.execute("""
                SELECT 
                    SUM(CASE WHEN flow_type IN ('deposit', 'transfer_in') THEN amount ELSE -amount END),
                    COUNT(*)
                FROM fund_flows
                WHERE instance_id = ? AND role = ?
                AND ts >= ? AND ts < ?
            """, (instance_id, role, yesterday_start, today_start))
            
            flow_row = cursor.fetchone()
            net_flow = flow_row[0] if flow_row and flow_row[0] else 0
            flow_count = flow_row[1] if flow_row and flow_row[1] else 0
            
            # 计算收益
            gross_pnl = end_value - start_value
            net_pnl = gross_pnl - net_flow
            return_pct = (net_pnl / start_value * 100) if start_value > 0 else 0
            
            # 计算跟单比例统计（仅follower）
            ratio_avg = None
            ratio_min = None
            ratio_max = None
            ratio_volatility = 0
            
            if copy_ratios:
                ratio_avg = sum(copy_ratios) / len(copy_ratios)
                ratio_min = min(copy_ratios)
                ratio_max = max(copy_ratios)
                if ratio_avg > 0:
                    ratio_volatility = ((ratio_max - ratio_min) / ratio_avg * 100)
            
            # 计算触发采样次数
            cursor.execute("""
                SELECT COUNT(*)
                FROM net_value_snapshots
                WHERE instance_id = ? AND role = ?
                AND ts >= ? AND ts < ?
                AND sample_type = 'triggered'
            """, (instance_id, role, yesterday_start, today_start))
            
            triggered_count = cursor.fetchone()[0]
            
            # 插入或替换汇总记录
            cursor.execute("""
                INSERT OR REPLACE INTO daily_summary
                (date, instance_id, role, start_value, end_value, high_value, low_value,
                 net_flow, flow_count, gross_pnl, net_pnl, return_pct,
                 ratio_avg, ratio_min, ratio_max, ratio_volatility, 
                 snapshot_count, triggered_count)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                yesterday, instance_id, role,
                start_value, end_value, high_value, low_value,
                net_flow, flow_count, gross_pnl, net_pnl, return_pct,
                ratio_avg, ratio_min, ratio_max, ratio_volatility,
                snapshot_count, triggered_count
            ))
            
            print(f'  {instance_id}/{role}: start=${start_value:.2f}, end=${end_value:.2f}, '
                  f'return={return_pct:.2f}%, samples={snapshot_count}')
    
    conn.commit()
    conn.close()
    print(f'Daily summary for {yesterday} completed')

if __name__ == '__main__':
    generate_daily_summary()
