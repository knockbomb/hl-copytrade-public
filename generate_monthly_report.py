#!/usr/bin/env python3
"""
HL跟单效果追踪系统 - 月报生成脚本
生成月度详细分析报告
"""

import sqlite3
import json
from datetime import datetime, timedelta
from shared_config import get_instances
from pathlib import Path

from paths import BASE_DIR, DATA_DIR
DB_PATH = str(DATA_DIR / 'net_value_history.db')
EVENTS_PATH = str(DATA_DIR / 'key_events.json')
REPORT_DIR = str(DATA_DIR / 'reports')

def get_period_data(start_ts, end_ts):
    db = sqlite3.connect(DB_PATH)
    c = db.cursor()
    
    c.execute('''
        SELECT ts, instance_id, role, account_value, total_equity, copy_ratio, sample_type
        FROM net_value_snapshots
        WHERE ts BETWEEN ? AND ?
        ORDER BY ts
    ''', (start_ts, end_ts))
    
    snapshots = c.fetchall()
    
    c.execute('''
        SELECT ts, instance_id, role, flow_type, amount
        FROM fund_flows
        WHERE ts BETWEEN ? AND ?
    ''', (start_ts, end_ts))
    
    flows = c.fetchall()
    
    db.close()
    
    return snapshots, flows

def get_key_events(start_ts, end_ts):
    if not Path(EVENTS_PATH).exists():
        return []
    
    with open(EVENTS_PATH, 'r') as f:
        events = json.load(f)
    
    return [e for e in events if start_ts <= e['ts'] <= end_ts]

# ====== P2修复#12: 获取上期数据用于趋势对比 ======
def get_previous_period_return(current_start_ts, current_end_ts, instance_id, role):
    """计算上期收益率，用于环比对比
    上期 = 与本期等长的前一个时间段
    """
    period_length = current_end_ts - current_start_ts
    prev_start_ts = current_start_ts - period_length
    prev_end_ts = current_start_ts
    
    db = sqlite3.connect(DB_PATH)
    c = db.cursor()
    c.execute(
        "SELECT ts, total_equity FROM net_value_snapshots "
        "WHERE ts BETWEEN ? AND ? AND instance_id = ? AND role = ? ORDER BY ts",
        (prev_start_ts, prev_end_ts, instance_id, role)
    )
    snapshots = c.fetchall()
    db.close()
    
    if len(snapshots) < 2:
        return None
    
    start_value = snapshots[0][1]
    end_value = snapshots[-1][1]
    
    if start_value <= 0:
        return None
    
    return_pct = (end_value - start_value) / start_value * 100
    return return_pct

# P2修复#20: 最小数据点检查阈值
MIN_DATA_POINTS = 10

def calculate_monthly_stats(snapshots, flows, instance_id, role):
    """计算月度统计"""
    data = [s for s in snapshots if s[1] == instance_id and s[2] == role]
    
    if not data:
        return None
    
    # P2修复#20: 数据完整性检查 - 数据点太少时报告无意义
    if len(data) < MIN_DATA_POINTS:
        print(f"Warning: Only {len(data)} data points for {instance_id}/{role}, report may not be meaningful (min={MIN_DATA_POINTS})")
    
    # 按日期分组
    daily_data = {}
    for d in data:
        date = datetime.fromtimestamp(d[0]).strftime('%Y-%m-%d')
        if date not in daily_data:
            daily_data[date] = []
        daily_data[date].append(d[4])  # total_equity
    
    # 计算每日收益
    daily_returns = []
    dates = sorted(daily_data.keys())
    for i in range(1, len(dates)):
        prev_value = daily_data[dates[i-1]][-1]
        curr_value = daily_data[dates[i]][-1]
        if prev_value > 0:
            ret = (curr_value - prev_value) / prev_value * 100
            daily_returns.append(ret)
    
    # 统计数据
    start_value = data[0][4]
    end_value = data[-1][4]
    high_value = max(d[4] for d in data)
    low_value = min(d[4] for d in data)
    
    # 资金进出
    role_flows = [f for f in flows if f[1] == instance_id and f[2] == role]
    deposits = sum(f[4] for f in role_flows if f[3] in ['deposit', 'transfer_in'])
    withdraws = sum(f[4] for f in role_flows if f[3] in ['withdraw', 'transfer_out'])
    net_flow = deposits - withdraws
    
    # 收益
    gross_pnl = end_value - start_value
    net_pnl = gross_pnl - net_flow
    return_pct = (net_pnl / start_value * 100) if start_value > 0 else 0
    
    # 跟单比例
    ratios = [d[5] for d in data if d[5] is not None]
    ratio_avg = sum(ratios) / len(ratios) if ratios else None
    ratio_min = min(ratios) if ratios else None
    ratio_max = max(ratios) if ratios else None
    
    # 波动率
    volatility = 0
    if daily_returns:
        avg_return = sum(daily_returns) / len(daily_returns)
        variance = sum((r - avg_return) ** 2 for r in daily_returns) / len(daily_returns)
        volatility = variance ** 0.5 * (252 ** 0.5)  # P2修复#17: 年化波动率
    
    # 最佳/最差日
    best_day = None
    worst_day = None
    if daily_returns and dates:
        best_idx = daily_returns.index(max(daily_returns))
        worst_idx = daily_returns.index(min(daily_returns))
        best_day = {'date': dates[best_idx + 1], 'return': max(daily_returns)}
        worst_day = {'date': dates[worst_idx + 1], 'return': min(daily_returns)}
    
    return {
        'start_value': start_value,
        'end_value': end_value,
        'high_value': high_value,
        'low_value': low_value,
        'deposits': deposits,
        'withdraws': withdraws,
        'net_flow': net_flow,
        'gross_pnl': gross_pnl,
        'net_pnl': net_pnl,
        'return_pct': return_pct,
        'ratio_avg': ratio_avg,
        'ratio_min': ratio_min,
        'ratio_max': ratio_max,
        'volatility': volatility,
        'best_day': best_day,
        'worst_day': worst_day,
        'sample_count': len(data),
        'flow_count': len(role_flows),
        'trading_days': len(dates)
    }

def generate_monthly_report():
    now = datetime.now()
    # P2修复#13: 使用本月1日作为月报起始，而非滚动30天
    month_start = now.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
    
    start_ts = int(month_start.timestamp())
    end_ts = int(now.timestamp())
    
    snapshots, flows = get_period_data(start_ts, end_ts)
    events = get_key_events(start_ts, end_ts)
    
    if not snapshots:
        return None
    
    report = {
        'type': 'monthly',
        'period': f'{month_start.strftime("%Y-%m-%d")} ~ {now.strftime("%Y-%m-%d")}',
        'generated_at': now.isoformat(),
        'instances': {},
        'key_events': events,
        'summary': {}
    }
    
    for instance_id in list(get_instances()):
        report['instances'][instance_id] = {
            'leader': calculate_monthly_stats(snapshots, flows, instance_id, 'leader'),
            'follower': calculate_monthly_stats(snapshots, flows, instance_id, 'follower')
        }
        
        leader = report['instances'][instance_id]['leader']
        follower = report['instances'][instance_id]['follower']
        
        if leader and follower:
            deviation = follower['return_pct'] - leader['return_pct']
            report['instances'][instance_id]['deviation'] = deviation
            
            # P2修复#12: 添加上期收益率对比
            prev_leader = get_previous_period_return(start_ts, end_ts, instance_id, 'leader')
            prev_follower = get_previous_period_return(start_ts, end_ts, instance_id, 'follower')
            report['instances'][instance_id]['prev_leader_return'] = prev_leader
            report['instances'][instance_id]['prev_follower_return'] = prev_follower
            if prev_follower is not None and follower['return_pct'] is not None:
                report['instances'][instance_id]['follower_qoq_change'] = follower['return_pct'] - prev_follower
            
            # 评估跟单效果
            if abs(deviation) < 2:
                evaluation = '优秀（偏差<2%）'
            elif abs(deviation) < 5:
                evaluation = '良好（偏差<5%）'
            elif abs(deviation) < 10:
                evaluation = '一般（偏差<10%）'
            else:
                evaluation = '需改进（偏差>10%）'
            
            report['instances'][instance_id]['evaluation'] = evaluation
    
    # 总体总结
    total_leader_pnl = sum(
        report['instances'][i]['leader']['net_pnl'] 
        for i in list(get_instances()) 
        if report['instances'][i]['leader']
    )
    total_follower_pnl = sum(
        report['instances'][i]['follower']['net_pnl'] 
        for i in list(get_instances()) 
        if report['instances'][i]['follower']
    )
    
    report['summary'] = {
        'total_leader_pnl': total_leader_pnl,
        'total_follower_pnl': total_follower_pnl,
        'total_events': len(events)
    }
    
    return report

def format_monthly_report(report):
    if not report:
        return "无数据"
    
    lines = []
    lines.append("=" * 70)
    lines.append("📊 HL跟单效果月报")
    lines.append("=" * 70)
    lines.append(f"周期: {report['period']}")
    lines.append(f"生成时间: {report['generated_at'][:19]}")
    lines.append("")
    
    # 总体概览
    lines.append("【总体概览】")
    lines.append(f"  Leader总净收益: ${report['summary']['total_leader_pnl']:.2f}")
    lines.append(f"  用户总净收益: ${report['summary']['total_follower_pnl']:.2f}")
    lines.append(f"  关键事件数: {report['summary']['total_events']}")
    lines.append("")
    
    for instance_id, data in report['instances'].items():
        instance_name = get_instances().get(instance_id, {}).get('display_short', instance_id)
        lines.append("=" * 70)
        lines.append(f"【{instance_name}账户详细分析】")
        lines.append("")
        
        if data['leader']:
            l = data['leader']
            lines.append("▎Leader（Leader）绩效")
            lines.append(f"  起始净值: ${l['start_value']:.2f}")
            lines.append(f"  结束净值: ${l['end_value']:.2f}")
            lines.append(f"  最高/最低: ${l['high_value']:.2f} / ${l['low_value']:.2f}")
            lines.append(f"  充值: ${l['deposits']:.2f}")
            lines.append(f"  提现: ${l['withdraws']:.2f}")
            lines.append(f"  资金净额: ${l['net_flow']:.2f} ({l['flow_count']}次)")
            lines.append(f"  毛收益: ${l['gross_pnl']:.2f}")
            lines.append(f"  净收益: ${l['net_pnl']:.2f} ({l['return_pct']:+.2f}%)")
            if l['best_day']:
                lines.append(f"  最佳日: {l['best_day']['date']} ({l['best_day']['return']:+.2f}%)")
            if l['worst_day']:
                lines.append(f"  最差日: {l['worst_day']['date']} ({l['worst_day']['return']:+.2f}%)")
            lines.append(f"  波动率: {l['volatility']:.2f}%")
            lines.append(f"  采样次数: {l['sample_count']}")
            lines.append("")
        
        if data['follower']:
            f = data['follower']
            lines.append("▎用户（Follower）绩效")
            lines.append(f"  起始净值: ${f['start_value']:.2f}")
            lines.append(f"  结束净值: ${f['end_value']:.2f}")
            lines.append(f"  最高/最低: ${f['high_value']:.2f} / ${f['low_value']:.2f}")
            lines.append(f"  充值: ${f['deposits']:.2f}")
            lines.append(f"  提现: ${f['withdraws']:.2f}")
            lines.append(f"  资金净额: ${f['net_flow']:.2f} ({f['flow_count']}次)")
            lines.append(f"  毛收益: ${f['gross_pnl']:.2f}")
            lines.append(f"  净收益: ${f['net_pnl']:.2f} ({f['return_pct']:+.2f}%)")
            if f['ratio_avg']:
                lines.append(f"  跟单比例: avg={f['ratio_avg']:.4f}, min={f['ratio_min']:.4f}, max={f['ratio_max']:.4f}")
            if f['best_day']:
                lines.append(f"  最佳日: {f['best_day']['date']} ({f['best_day']['return']:+.2f}%)")
            if f['worst_day']:
                lines.append(f"  最差日: {f['worst_day']['date']} ({f['worst_day']['return']:+.2f}%)")
            lines.append(f"  波动率: {f['volatility']:.2f}%")
            lines.append("")
        
        if 'deviation' in data:
            lines.append("▎跟单效果评估")
            lines.append(f"  收益率偏差: {data['deviation']:+.2f}个百分点")
            lines.append(f"  评估等级: {data['evaluation']}")
            # P2修复#12: 显示上期趋势对比
            if data.get('prev_leader_return') is not None:
                lines.append(f"  上期Leader收益: {data['prev_leader_return']:+.2f}%")
            if data.get('prev_follower_return') is not None:
                lines.append(f"  上期用户收益: {data['prev_follower_return']:+.2f}%")
            if data.get('follower_qoq_change') is not None:
                qoq = data['follower_qoq_change']
                arrow = "↑" if qoq > 0 else "↓" if qoq < 0 else "→"
                lines.append(f"  用户收益环比: {qoq:+.2f}个百分点 {arrow}")
            lines.append("")
    
    # 关键事件
    if report.get('key_events'):
        lines.append("=" * 70)
        lines.append("【关键事件汇总】")
        
        # 按类型分组
        event_types = {}
        for e in report['key_events']:
            t = e['type']
            if t not in event_types:
                event_types[t] = []
            event_types[t].append(e)
        
        for event_type, events in event_types.items():
            type_names = {
                'position_open': '开仓',
                'position_close': '平仓',
                'position_add': '加仓',
                'position_reduce': '减仓',
                'large_fund_flow': '大额资金进出'
            }
            type_name = type_names.get(event_type, event_type)
            lines.append(f"\n  {type_name} ({len(events)}次):")
            for e in events[:5]:  # 每种类型最多显示5条
                dt = datetime.fromtimestamp(e['ts']).strftime('%m-%d %H:%M')
                lines.append(f"    [{dt}] {e['message']}")
    
    return '\n'.join(lines)

if __name__ == '__main__':
    report = generate_monthly_report()
    
    Path(REPORT_DIR).mkdir(parents=True, exist_ok=True)
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    json_path = f'{REPORT_DIR}/monthly_{timestamp}.json'
    
    with open(json_path, 'w') as f:
        json.dump(report, f, indent=2)
    
    print(f"Monthly report generated: {json_path}")
    
    text = format_monthly_report(report)
    print("\n" + text)
