#!/usr/bin/env python3
"""
HL跟单效果追踪系统 - 飞书推送报告 v3
适配新版 generate_report.py 输出格式
"""

import json
import sys
import requests
from pathlib import Path
from datetime import datetime

from paths import BASE_DIR, DATA_DIR
REPORT_DIR = str(DATA_DIR / 'reports')
MAX_CONTENT_LENGTH = 3000

sys.path.insert(0, str(BASE_DIR))
from shared_config import get_feishu_webhook

def load_config():
    webhook = get_feishu_webhook()
    if not webhook:
        return None
    return {'webhook_url': webhook}

def get_latest_report(report_type):
    files = sorted(Path(REPORT_DIR).glob(f'{report_type}_*.json'))
    if not files:
        return None
    with open(files[-1], 'r') as f:
        return json.load(f)

def fmt_dollar(v):
    if v is None:
        return 'N/A'
    return f'${v:,.2f}'

def fmt_pct(v):
    if v is None:
        return 'N/A'
    return f'{v:+.2f}%'

def format_feishu_message(report):
    if not report:
        return None

    type_cn = '月报' if report.get('type') == 'monthly' else '周报'
    title = f'📊 HL跟单效果{type_cn}'
    sections = report.get('sections', {})

    lines = []
    lines.append(f'**周期**: {report.get("period", "?")}')
    lines.append(f'**生成**: {report.get("generated_at", "?")[:19]}')

    # === 收益对比 ===
    lines.append('')
    lines.append('**━━ 收益对比 ━━**')

    for iid, name in [('lf', '低频'), ('hf', '高频')]:
        sec = sections.get(iid, {})
        perf = sec.get('performance', {})
        leader = perf.get('leader')
        follower = perf.get('follower')
        labels = sec.get('labels', {})
        leader_name = labels.get('leader', 'Leader')
        follower_name = labels.get('follower', '用户')

        lines.append(f'')
        lines.append(f'**【{name}】**')

        if leader:
            lines.append(
                f'{leader_name}: {fmt_dollar(leader["end_equity"])} '
                f'({fmt_pct(leader["return_pct"])}) '
                f'回撤-{leader["max_drawdown_pct"]:.1f}%'
            )
        else:
            lines.append(f'{leader_name}: 无数据')

        if follower:
            lines.append(
                f'{follower_name}: {fmt_dollar(follower["end_equity"])} '
                f'({fmt_pct(follower["return_pct"])}) '
                f'回撤-{follower["max_drawdown_pct"]:.1f}%'
            )
            if follower.get('ratio_avg'):
                lines.append(f'跟单比例: {follower["ratio_avg"]:.4f}')
        else:
            lines.append(f'{follower_name}: 无数据')

        dev = perf.get('deviation_pct')
        if dev is not None:
            tag = '✅' if abs(dev) < 2 else ('⚠️' if abs(dev) < 5 else '❌')
            lines.append(f'{tag} **偏差: {fmt_pct(dev)}**')

    # === 持仓快照 ===
    lines.append('')
    lines.append('**━━ 持仓快照 ━━**')

    for iid, name in [('lf', '低频'), ('hf', '高频')]:
        sec = sections.get(iid, {})
        pos_data = sec.get('positions', {})
        comparison = pos_data.get('comparison', [])
        missing = pos_data.get('missing', [])
        labels = sec.get('labels', {})

        lines.append(f'')
        lines.append(f'**【{name}】** {labels.get("leader","")} {fmt_dollar(pos_data.get("leader_total_equity",0))} | {labels.get("follower","")} {fmt_dollar(pos_data.get("follower_total_equity",0))}')

        if comparison:
            pos_parts = []
            for c in comparison:
                l_side = c.get('leader_side', 'none')
                f_side = c.get('follower_side', 'none')
                l_s = c.get('leader_size', 0)
                f_s = c.get('follower_size', 0)
                dev = c.get('ratio_deviation')
                dev_str = f'偏差{dev:+.3f}' if dev is not None else ''

                if l_side == 'none' and f_side == 'none':
                    continue
                l_arrow = '多' if l_side == 'long' else ('空' if l_side == 'short' else '-')
                f_arrow = '多' if f_side == 'long' else ('空' if f_side == 'short' else '-')

                pos_parts.append(f'{c["coin"]}: Leader{l_arrow}{l_s:.1f} / 用户{f_arrow}{f_s:.1f} {dev_str}')
            if pos_parts:
                lines.append('  ' + ' | '.join(pos_parts[:6]))

        if missing:
            for m in missing[:3]:
                lines.append(f'  ⚠️ {m["coin"]}: {m["issue"]}')

    # === 资金进出 ===
    has_flows = False
    flow_lines = []
    for iid, name in [('lf', '低频'), ('hf', '高频')]:
        sec = sections.get(iid, {})
        labels = sec.get('labels', {})
        flows = sec.get('fund_flows', {})
        for role, label_key in [('leader', 'leader'), ('follower', 'follower')]:
            fd = flows.get(role, {})
            if fd.get('net', 0) != 0 or fd.get('records'):
                has_flows = True
                rname = labels.get(label_key, role)
                flow_lines.append(
                    f'{rname}{name}: 充{fmt_dollar(fd.get("deposits",0))} '
                    f'提{fmt_dollar(fd.get("withdraws",0))} '
                    f'净{fmt_dollar(fd.get("net",0))} ({len(fd.get("records",[]))}笔)'
                )

    if has_flows:
        lines.append('')
        lines.append('**━━ 资金进出 ━━**')
        for fl in flow_lines:
            lines.append(f'  {fl}')

    # === 执行统计 ===
    lines.append('')
    lines.append('**━━ 执行统计 ━━**')
    for iid, name in [('lf', '低频'), ('hf', '高频')]:
        sec = sections.get(iid, {})
        exe = sec.get('execution', {})
        if exe:
            total_ops = exe.get('opens', 0) + exe.get('adjusts', 0) + exe.get('closes', 0)
            lines.append(
                f'{name}: 开{exe.get("opens",0)} 加{exe.get("adjusts",0)} '
                f'平{exe.get("closes",0)} RECON{exe.get("recons",0)} '
                f'轮询{exe.get("polls",0)}次'
            )

    # === 系统健康 ===
    sys_data = sections.get('system', {})
    alerts = sys_data.get('alerts', {})
    lines.append('')
    lines.append('**━━ 系统 ━━**')
    lines.append(f'告警: 🔴{alerts.get("critical",0)} 🟡{alerts.get("warning",0)} 🔵{alerts.get("info",0)}')

    # === 关键事件 ===
    events = sections.get('events', [])
    if events:
        lines.append('')
        lines.append('**━━ 关键事件 ━━**')
        for e in events[:8]:
            dt = datetime.fromtimestamp(e['ts']).strftime('%m-%d %H:%M')
            lines.append(f'[{dt}] {e.get("message","")}')
        if len(events) > 8:
            lines.append(f'... 共{len(events)}条')

    msg_content = '\n'.join(lines)
    if len(msg_content) > MAX_CONTENT_LENGTH:
        msg_content = msg_content[:MAX_CONTENT_LENGTH] + '\n\n...(内容过长已截断)'

    return {
        'title': title,
        'content': msg_content
    }

def push_to_feishu(message, config):
    webhook_url = config['webhook_url']
    payload = {
        'msg_type': 'interactive',
        'card': {
            'header': {
                'title': {
                    'tag': 'plain_text',
                    'content': message['title']
                },
                'template': 'blue'
            },
            'elements': [
                {
                    'tag': 'markdown',
                    'content': message['content']
                }
            ]
        }
    }
    try:
        resp = requests.post(webhook_url, json=payload, timeout=10)
        if resp.status_code == 200:
            result = resp.json()
            if result.get('code') == 0:
                print('Pushed to Feishu successfully')
                return True
            else:
                print(f'ERROR: {result}')
                return False
        else:
            print(f'ERROR: HTTP {resp.status_code}')
            return False
    except Exception as e:
        print(f'ERROR: {e}')
        return False

def main():
    report_type = 'monthly'
    if len(sys.argv) > 1:
        report_type = sys.argv[1]

    config = load_config()
    if not config:
        print('ERROR: Feishu config not found')
        return

    report = get_latest_report(report_type)
    if not report:
        print(f'No {report_type} report found')
        return

    message = format_feishu_message(report)
    if not message:
        print('Failed to format message')
        return

    print(f'Report: {message["title"]}')
    print(f'Content preview:\n{message["content"][:500]}...')

    success = push_to_feishu(message, config)
    if success:
        print(f'\n✅ {report_type} report pushed to Feishu')
    else:
        print(f'\n❌ Failed to push to Feishu')

if __name__ == '__main__':
    main()
