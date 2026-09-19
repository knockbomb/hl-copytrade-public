#!/usr/bin/env python3
"""
HL跟单效果追踪系统 - 飞书推送报告 v4
增强版：展示完整报告数据（账户概览、风险指标、交易统计）
支持周报/月报/半年报/年报
"""

import json
import sys
import requests
from pathlib import Path
from datetime import datetime

from paths import BASE_DIR, DATA_DIR
REPORT_DIR = str(DATA_DIR / 'reports')
MAX_CONTENT_LENGTH = 3000

CHART_BASE_URL = 'http://YOUR_VPS_IP:19628'
CHART_USER = 'charts'
CHART_PASS = 'pefKa8SXFhEBV6Qe'

sys.path.insert(0, str(BASE_DIR))
from shared_config import get_feishu_webhook


def load_config():
    webhook = get_feishu_webhook()
    if not webhook:
        return None
    return {'webhook_url': webhook}


def get_latest_report(report_type):
    if report_type in ('semiannual', 'semiannual_custom'):
        files = sorted(Path(REPORT_DIR).glob('semiannual/semiannual_*.json'))
    elif report_type == 'annual':
        files = sorted(Path(REPORT_DIR).glob('annual/annual_*.json'))
    else:
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


def fmt_num(v, decimals=2):
    if v is None:
        return 'N/A'
    return f'{v:.{decimals}f}'


def _get_report_type_cn(report):
    rtype = report.get('type', '')
    subtype = report.get('subtype', '')
    if rtype == 'annual' or subtype == 'annual':
        return '年报'
    elif rtype in ('semiannual', 'semiannual_custom') or subtype in ('semiannual', 'custom'):
        return '半年报'
    elif rtype == 'monthly':
        return '月报'
    else:
        return '周报'


def _build_account_overview(sections):
    lines = []
    lines.append('')
    lines.append('**\u2501\u2501 账户概览 \u2501\u2501**')
    for iid, name in [('lf', '低频'), ('hf', '高频')]:
        sec = sections.get(iid, {})
        perf = sec.get('performance', {})
        leader = perf.get('leader')
        follower = perf.get('follower')
        labels = sec.get('labels', {})
        leader_name = labels.get('leader', 'Leader')
        follower_name = labels.get('follower', '用户')
        lines.append('')
        lines.append(f'**【{name}】**')
        if leader:
            start = fmt_dollar(leader.get('start_equity'))
            end = fmt_dollar(leader.get('end_equity'))
            ret = fmt_pct(leader.get('return_pct'))
            pnl = fmt_dollar(leader.get('net_pnl', leader.get('gross_pnl')))
            dd = leader.get('max_drawdown_pct', 0)
            lines.append(f'{leader_name}: 起始 {start} -> 结束 {end} (收益率 {ret})')
            lines.append(f'  盈亏: {pnl} | 回撤 {fmt_pct(-dd)}')
        else:
            lines.append(f'{leader_name}: 暂无数据')
        if follower:
            start = fmt_dollar(follower.get('start_equity'))
            end = fmt_dollar(follower.get('end_equity'))
            ret = fmt_pct(follower.get('return_pct'))
            pnl = fmt_dollar(follower.get('net_pnl', follower.get('gross_pnl')))
            dd = follower.get('max_drawdown_pct', 0)
            lines.append(f'{follower_name}: 起始 {start} -> 结束 {end} (收益率 {ret})')
            lines.append(f'  盈亏: {pnl} | 回撤 {fmt_pct(-dd)}')
            if follower.get('ratio_avg'):
                lines.append(f'  跟单比例: {follower["ratio_avg"]:.4f}')
        else:
            lines.append(f'{follower_name}: 暂无数据')
        dev = perf.get('deviation_pct')
        if dev is not None:
            tag = '\u2705' if abs(dev) < 2 else ('\u26a0\ufe0f' if abs(dev) < 5 else '\u274c')
            lines.append(f'{tag} **偏差: {fmt_pct(dev)}**')
    return lines


def _build_risk_metrics(sections):
    lines = []
    has_any = False
    for iid in ['lf', 'hf']:
        sec = sections.get(iid, {})
        adv = sec.get('advanced_metrics', {})
        if adv:
            has_any = True
            break
    if not has_any:
        return []
    lines.append('')
    lines.append('**\u2501\u2501 风险指标 \u2501\u2501**')
    for iid, name in [('lf', '低频'), ('hf', '高频')]:
        sec = sections.get(iid, {})
        adv = sec.get('advanced_metrics', {})
        if not adv:
            continue
        leader_m = adv.get('leader', {})
        follower_m = adv.get('follower', {})
        parts = []
        if leader_m:
            sharpe = fmt_num(leader_m.get('sharpe_ratio'))
            sortino = fmt_num(leader_m.get('sortino_ratio'))
            calmar = fmt_num(leader_m.get('calmar_ratio'))
            vol = fmt_num(leader_m.get('annualized_volatility_pct'), 1) + '%'
            dd_dur = fmt_num(leader_m.get('max_drawdown_duration_days'), 1) + '天'
            parts.append(f'Leader: 夏普{sharpe} 索提诺{sortino} 卡尔马{calmar}')
            parts.append(f'  波动率{vol} | 回撤持续{dd_dur}')
        if follower_m:
            sharpe = fmt_num(follower_m.get('sharpe_ratio'))
            sortino = fmt_num(follower_m.get('sortino_ratio'))
            calmar = fmt_num(follower_m.get('calmar_ratio'))
            parts.append(f'用户: 夏普{sharpe} 索提诺{sortino} 卡尔马{calmar}')
        if parts:
            lines.append('')
            lines.append(f'**【{name}】**')
            for p in parts:
                lines.append(p)
    return lines


def _build_trade_stats(sections):
    lines = []
    has_any = False
    for iid in ['lf', 'hf']:
        sec = sections.get(iid, {})
        ts = sec.get('trade_stats', {})
        if ts and ts.get('total_trades', 0) > 0:
            has_any = True
            break
    if not has_any:
        return []
    lines.append('')
    lines.append('**\u2501\u2501 交易统计 \u2501\u2501**')
    for iid, name in [('lf', '低频'), ('hf', '高频')]:
        sec = sections.get(iid, {})
        ts = sec.get('trade_stats', {})
        if not ts or ts.get('total_trades', 0) == 0:
            continue
        total = ts.get('total_trades', 0)
        wr = ts.get('win_rate')
        win_rate = f'{wr:.0f}%' if wr is not None else 'N/A'
        wlr = ts.get('avg_win_loss_ratio')
        pnl_ratio = f'{wlr:.1f}' if wlr is not None else 'N/A'
        pf = ts.get('profit_factor')
        profit_factor = f'{pf:.1f}' if pf is not None else 'N/A'
        max_win = ts.get('max_consecutive_wins', 'N/A')
        max_lose = ts.get('max_consecutive_losses', 'N/A')
        avg_hold = ts.get('avg_hold_time_hours')
        hold_str = f'{avg_hold:.1f}h' if avg_hold is not None else 'N/A'
        lines.append('')
        lines.append(f'**【{name}】**')
        lines.append(f'总交易: {total}笔 | 胜率 {win_rate} | 盈亏比 {pnl_ratio} | 利润因子 {profit_factor}')
        lines.append(f'最大连胜: {max_win} | 最大连亏: {max_lose} | 平均持仓: {hold_str}')
    return lines



def _build_risk_metrics_v2(sections):
    """风险指标区块 - 支持 risk_metrics 和 advanced_metrics"""
    lines = []
    has_any = False
    for iid in ['lf', 'hf']:
        sec = sections.get(iid, {})
        if sec.get('risk_metrics') or sec.get('advanced_metrics'):
            has_any = True
            break
    if not has_any:
        return []
    lines.append('')
    lines.append('**━━ 风险指标 ━━**')
    for iid, name in [('lf', '低频'), ('hf', '高频')]:
        sec = sections.get(iid, {})
        rm = sec.get('risk_metrics', {})
        am = sec.get('advanced_metrics', {})
        parts = []
        for role, label in [('leader', 'Leader'), ('follower', '用户')]:
            m = rm.get(role, {}) if rm else {}
            if m and isinstance(m, dict) and m.get('sharpe_ratio') is not None:
                sharpe = fmt_num(m.get('sharpe_ratio'))
                sortino = fmt_num(m.get('sortino_ratio'))
                calmar = fmt_num(m.get('calmar_ratio'))
                dd = fmt_num(m.get('current_drawdown_pct'), 1) + '%'
                dd_dur = fmt_num(m.get('drawdown_duration_days'), 0) + '天'
                vol = fmt_num(m.get('annualized_volatility_pct'), 1) + '%'
                parts.append(f'{label}: 夏普{sharpe} 索提诺{sortino} 卡尔马{calmar}')
                parts.append(f'  回撤{dd} 持续{dd_dur} 波动率{vol}')
            elif am and isinstance(am, dict):
                adv = am.get(role, {})
                if adv and adv.get('sharpe_ratio') is not None:
                    sharpe = fmt_num(adv.get('sharpe_ratio'))
                    sortino = fmt_num(adv.get('sortino_ratio'))
                    calmar = fmt_num(adv.get('calmar_ratio'))
                    vol = fmt_num(adv.get('annualized_volatility_pct'), 1) + '%'
                    dd_dur = fmt_num(adv.get('max_drawdown_duration_days'), 0) + '天'
                    parts.append(f'{label}: 夏普{sharpe} 索提诺{sortino} 卡尔马{calmar}')
                    parts.append(f'  波动率{vol} | 回撤持续{dd_dur}')
        if parts:
            lines.append(f'**【{name}】**')
            for p in parts:
                lines.append(p)
    return lines


def _build_position_concentration(sections):
    """持仓分析区块"""
    lines = []
    has_any = False
    for iid in ['lf', 'hf']:
        sec = sections.get(iid, {})
        pc = sec.get('position_concentration', {})
        if pc and (pc.get('leader') or pc.get('follower')):
            has_any = True
            break
    if not has_any:
        return []
    lines.append('')
    lines.append('**━━ 持仓分析 ━━**')
    for iid, name in [('lf', '低频'), ('hf', '高频')]:
        sec = sections.get(iid, {})
        pc = sec.get('position_concentration', {})
        if not pc:
            continue
        parts = []
        for role, label in [('leader', 'Leader'), ('follower', '用户')]:
            m = pc.get(role, {})
            if m and isinstance(m, dict) and m.get('hhi_index') is not None:
                hhi = fmt_num(m.get('hhi_index'))
                top1_v = m.get('top1_weight')
                top1 = f'{top1_v * 100:.0f}%' if top1_v is not None else 'N/A'
                long_pct = fmt_num(m.get('long_pct'), 0) + '%'
                short_pct = fmt_num(m.get('short_pct'), 0) + '%'
                ls = m.get('long_short_ratio')
                ls_str = f'{ls:.1f}:1' if ls is not None else 'N/A'
                parts.append(f'{label}: HHI {hhi} 最大持仓 {top1}')
                parts.append(f'  多{long_pct} 空{short_pct} 比{ls_str}')
        if parts:
            lines.append(f'**【{name}】**')
            for p in parts:
                lines.append(p)
    return lines


def _build_utilization_display(sections):
    """有效杠杆区块"""
    lines = []
    has_any = False
    for iid in ['lf', 'hf']:
        sec = sections.get(iid, {})
        ut = sec.get('utilization', {})
        if ut and (ut.get('leader') or ut.get('follower')):
            has_any = True
            break
    if not has_any:
        return []
    lines.append('')
    lines.append('**━━ 有效杠杆 ━━**')
    lines.append('_有效杠杆 = 持仓名义价值 / 账户净值 × 100%，>100%表示使用了合约杠杆_')
    for iid, name in [('lf', '低频'), ('hf', '高频')]:
        sec = sections.get(iid, {})
        ut = sec.get('utilization', {})
        if not ut:
            continue
        parts = []
        for role, label in [('leader', 'Leader'), ('follower', '用户')]:
            m = ut.get(role, {})
            if m and isinstance(m, dict):
                avg = m.get('avg_utilization_pct', m.get('avg_pct'))
                mn = m.get('min_utilization_pct', m.get('min_pct'))
                mx = m.get('max_utilization_pct', m.get('max_pct'))
                if avg is not None:
                    parts.append(f'{label}: 均值 {fmt_num(avg, 1)}% 区间[{fmt_num(mn, 1)}%, {fmt_num(mx, 1)}%]')
        if parts:
            lines.append(f'**【{name}】**')
            for p in parts:
                lines.append(p)
    return lines


def _build_mom_growth(sections):
    """环比变化区块"""
    lines = []
    has_any = False
    for iid in ['lf', 'hf']:
        sec = sections.get(iid, {})
        mg = sec.get('mom_growth', {})
        if mg and mg.get('mom_growth_pct') is not None:
            has_any = True
            break
    if not has_any:
        return []
    lines.append('')
    lines.append('**━━ 环比变化 ━━**')
    for iid, name in [('lf', '低频'), ('hf', '高频')]:
        sec = sections.get(iid, {})
        mg = sec.get('mom_growth', {})
        if mg and mg.get('mom_growth_pct') is not None:
            prev = fmt_pct(mg.get('prev_return_pct'))
            curr = fmt_pct(mg.get('current_return_pct'))
            mom = fmt_pct(mg.get('mom_growth_pct'))
            lines.append(f'**【{name}】** 上期 {prev} → 本期 {curr} (环比 {mom})')
    return lines


def _build_monthly_returns_display(sections):
    """月度收益区块"""
    lines = []
    has_any = False
    for iid in ['lf', 'hf']:
        sec = sections.get(iid, {})
        mr = sec.get('monthly_returns', {})
        if mr and (mr.get('leader') or mr.get('follower')):
            has_any = True
            break
    if not has_any:
        return []
    lines.append('')
    lines.append('**━━ 月度收益 ━━**')
    for iid, name in [('lf', '低频'), ('hf', '高频')]:
        sec = sections.get(iid, {})
        mr = sec.get('monthly_returns', {})
        if not mr:
            continue
        for role, label in [('leader', 'Leader'), ('follower', '用户')]:
            data = mr.get(role, {})
            if not data:
                continue
            if isinstance(data, dict) and data:
                parts = [f'{m}: {fmt_pct(v)}' for m, v in sorted(data.items())]
                lines.append(f'{label}({name}): {" | ".join(parts[:6])}')
            elif isinstance(data, list) and data:
                parts = [f'{item["month"]}: {fmt_pct(item.get("return_pct"))}' for item in data[:6]]
                lines.append(f'{label}({name}): {" | ".join(parts)}')
    return lines


def format_feishu_message(report):
    if not report:
        return None
    type_cn = _get_report_type_cn(report)
    test_tag = ' [TEST]' if report.get('test_mode') else ''
    title = f'\U0001f4ca HL跟单效果{type_cn}{test_tag}'
    sections = report.get('sections', {})
    lines = []
    lines.append(f'**周期**: {report.get("period", "?")}')
    lines.append(f'**生成**: {report.get("generated_at", "?")[:19]}')
    try:
        lines.extend(_build_account_overview(sections))
    except Exception as e:
        lines.append(f'[账户概览异常: {e}]')
    try:
        lines.extend(_build_risk_metrics_v2(sections))
    except Exception:
        pass
    try:
        lines.extend(_build_trade_stats(sections))
    except Exception:
        pass
    try:
        lines.extend(_build_position_concentration(sections))
    except Exception:
        pass
    try:
        lines.extend(_build_utilization_display(sections))
    except Exception:
        pass
    try:
        lines.extend(_build_mom_growth(sections))
    except Exception:
        pass
    try:
        lines.extend(_build_monthly_returns_display(sections))
    except Exception:
        pass
    lines.append('')
    lines.append('**\u2501\u2501 持仓快照 \u2501\u2501**')
    for iid, name in [('lf', '低频'), ('hf', '高频')]:
        sec = sections.get(iid, {})
        pos_data = sec.get('positions', {})
        comparison = pos_data.get('comparison', [])
        missing = pos_data.get('missing', [])
        labels = sec.get('labels', {})
        lines.append('')
        le = labels.get('leader', '')
        fe = labels.get('follower', '')
        lte = fmt_dollar(pos_data.get('leader_total_equity', 0))
        fte = fmt_dollar(pos_data.get('follower_total_equity', 0))
        lines.append(f'**【{name}】** {le} {lte} | {fe} {fte}')
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
                coin = c['coin']
                pos_parts.append(f'{coin}: Leader{l_arrow}{l_s:.1f} / 用户{f_arrow}{f_s:.1f} {dev_str}')
            if pos_parts:
                lines.append('  ' + ' | '.join(pos_parts[:6]))
        if missing:
            for m in missing[:3]:
                lines.append(f'  \u26a0\ufe0f {m["coin"]}: {m["issue"]}')
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
                dep = fmt_dollar(fd.get('deposits', 0))
                wd = fmt_dollar(fd.get('withdraws', 0))
                net = fmt_dollar(fd.get('net', 0))
                cnt = len(fd.get('records', []))
                flow_lines.append(f'{rname}{name}: 充{dep} 提{wd} 净{net} ({cnt}笔)')
    if has_flows:
        lines.append('')
        lines.append('**\u2501\u2501 资金进出 \u2501\u2501**')
        for fl in flow_lines:
            lines.append(f'  {fl}')
    lines.append('')
    lines.append('**\u2501\u2501 执行统计 \u2501\u2501**')
    for iid, name in [('lf', '低频'), ('hf', '高频')]:
        sec = sections.get(iid, {})
        exe = sec.get('execution', {})
        if exe:
            lines.append(
                f'{name}: 开{exe.get("opens",0)} 加{exe.get("adjusts",0)} '
                f'平{exe.get("closes",0)} RECON{exe.get("recons",0)} '
                f'轮询{exe.get("polls",0)}次'
            )
    sys_data = sections.get('system', {})
    alerts = sys_data.get('alerts', {})
    lines.append('')
    lines.append('**\u2501\u2501 系统 \u2501\u2501**')
    c_val = alerts.get("critical", 0)
    w_val = alerts.get("warning", 0)
    i_val = alerts.get("info", 0)
    lines.append(f'告警: \U0001f534{c_val} \U0001f7e1{w_val} \U0001f535{i_val}')
    events = sections.get('events', [])
    if events:
        lines.append('')
        lines.append('**\u2501\u2501 关键事件 \u2501\u2501**')
        for e in events[:5]:
            dt = datetime.fromtimestamp(e['ts']).strftime('%m-%d %H:%M')
            lines.append(f'[{dt}] {e.get("message","")}')
        if len(events) > 5:
            lines.append(f'... 共{len(events)}条')
    msg_content = '\n'.join(lines)
    if len(msg_content) > MAX_CONTENT_LENGTH:
        msg_content = msg_content[:MAX_CONTENT_LENGTH] + '\n\n...(内容过长已截断)'
    return {'title': title, 'content': msg_content}


def push_to_feishu(message, config):
    webhook_url = config['webhook_url']
    payload = {
        'msg_type': 'interactive',
        'card': {
            'header': {
                'title': {'tag': 'plain_text', 'content': message['title']},
                'template': 'blue'
            },
            'elements': [
                {'tag': 'markdown', 'content': message['content']},
                {'tag': 'hr'},
                {
                    'tag': 'action',
                    'actions': [{
                        'tag': 'button',
                        'text': {'tag': 'plain_text', 'content': '\U0001f4ca 查看交互式图表'},
                        'type': 'primary',
                        'url': f'{CHART_BASE_URL}/charts_latest.html'
                    }]
                },
                {
                    'tag': 'note',
                    'elements': [{'tag': 'plain_text', 'content': f'图表访问: 用户名 {CHART_USER} | 密码 {CHART_PASS}'}]
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



def send_to_feishu(message, config=None):
    """统一的飞书推送函数"""
    if config is None:
        config = load_config()
    if not config:
        print('ERROR: Feishu config not found')
        return False
    return push_to_feishu(message, config)


def main():
    report_type = 'monthly'
    test_mode = '--test' in sys.argv
    args_list = [a for a in sys.argv[1:] if a != '--test']
    if args_list:
        report_type = args_list[0]
    config = load_config()
    if not config:
        print('ERROR: Feishu config not found')
        return
    report = get_latest_report(report_type)
    if not report:
        print(f'No {report_type} report found')
        return
    if test_mode:
        report['test_mode'] = True
    message = format_feishu_message(report)
    if not message:
        print('Failed to format message')
        return
    print(f'Report: {message["title"]}')
    print(f'Content preview:\n{message["content"][:800]}...')
    success = push_to_feishu(message, config)
    if success:
        print(f'\n\u2705 {report_type} report pushed to Feishu')
    else:
        print(f'\n\u274c Failed to push to Feishu')


if __name__ == '__main__':
    main()
