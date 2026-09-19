#!/usr/bin/env python3
"""
HL跟单系统 - 统一飞书推送器
从 alert_queue.json 读取待发送告警，推送到飞书webhook。
这是系统唯一的飞书推送出口。

用法:
    ./push_alerts_to_feishu.py           # 推送队列中的告警
    ./push_alerts_to_feishu.py --status  # 查看队列状态（不推送）

调度：cron每5分钟执行一次
"""

import json
import os
import sys
import time
import fcntl
import traceback
from datetime import datetime
from pathlib import Path

import requests

# 路径
from paths import BASE_DIR
import shared_config
from heartbeat_writer import write_heartbeat
QUEUE_FILE = BASE_DIR / 'data' / 'alert_queue.json'
HISTORY_FILE = BASE_DIR / 'data' / 'alert_history.json'
STATUS_FILE = BASE_DIR / 'data' / 'push_status.json'

# 限制
MAX_RETRIES = 3

# webhook: 通过 shared_config 统一获取（消除硬编码路径）
WEBHOOK = shared_config.get_feishu_webhook() or ''


def _lock(f):
    try:
        fcntl.flock(f.fileno(), fcntl.LOCK_EX)
        return True
    except Exception:
        return False


def _unlock(f):
    try:
        fcntl.flock(f.fileno(), fcntl.LOCK_UN)
    except Exception:
        pass




def write_status(sent: int = 0, failed: int = 0, error: str = None):
    """写入push脚本运行状态（供V3队列堆积检测使用）"""
    try:
        status = {}
        if STATUS_FILE.exists():
            with open(STATUS_FILE, 'r') as f:
                status = json.load(f)
        
        now_str = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        status['last_run_time'] = now_str
        status['last_run_timestamp'] = time.time()
        status['last_sent_count'] = sent
        status['last_fail_count'] = failed
        
        if sent > 0:
            status['last_success_time'] = now_str
        
        if error:
            status['last_error'] = error
            status['last_fail_time'] = now_str
        else:
            status['last_error'] = None
        
        # 累计
        status['total_sent'] = status.get('total_sent', 0) + sent
        
        os.makedirs(STATUS_FILE.parent, exist_ok=True)
        with open(STATUS_FILE, 'w') as f:
            json.dump(status, f, ensure_ascii=False, indent=2)
    except Exception as e:
        print(f'[PUSH] 写状态文件失败: {e}')


def append_to_history(alert: dict):
    """将已发送的告警追加到历史文件，供Coze拉取"""
    try:
        # 转换为V3 JSON格式（兼容Coze拉取脚本）
        history_entry = {
            "type": f"INSPECTION_{alert.get('level', 'WARNING')}_{alert.get('source', 'unknown').replace('巡检-', '').upper()}",
            "coin": "ALL",
            "message": f"[{alert.get('source', '')}] {alert.get('title', '')}\n{alert.get('detail', '')}",
            "timestamp": alert.get('timestamp', ''),
            "poll_count": 0
        }
        
        # 读取现有历史
        if HISTORY_FILE.exists():
            with open(HISTORY_FILE, 'r') as f:
                history = json.load(f)
        else:
            history = []
        
        # 追加并保留最近500条
        history.append(history_entry)
        if len(history) > 500:
            history = history[-500:]
        
        with open(HISTORY_FILE, 'w') as f:
            json.dump(history, f, ensure_ascii=False, indent=2)
    except Exception as e:
        print(f'[PUSH] 写历史文件失败: {e}')


def send_card(level: str, title: str, detail: str, source: str = '') -> bool:
    """发送飞书卡片消息"""
    if not WEBHOOK:
        print('[PUSH] 错误: 飞书webhook未配置！')
        return False

    emoji = {'CRITICAL': '🔴', 'WARNING': '🟡', 'INFO': '🟢'}.get(level, '⚪')
    color = {'CRITICAL': 'red', 'WARNING': 'orange', 'INFO': 'green'}.get(level, 'grey')
    ts = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

    header_title = f'{emoji} [{level}] {title}'
    if source:
        header_title = f'{emoji} [{source}][{level}] {title}'

    content = detail if detail else title

    card = {
        'msg_type': 'interactive',
        'card': {
            'header': {
                'title': {'tag': 'plain_text', 'content': header_title},
                'template': color,
            },
            'elements': [
                {'tag': 'div', 'text': {'tag': 'lark_md', 'content': content}},
                {'tag': 'note', 'elements': [
                    {'tag': 'plain_text', 'content': f'⏰ {ts}'}
                ]},
            ],
        }
    }

    try:
        resp = requests.post(WEBHOOK, json=card, timeout=10,
                             headers={'Content-Type': 'application/json'})
        if resp.status_code == 200:
            data = resp.json()
            code = data.get('StatusCode', data.get('code', -1))
            if code == 0:
                return True
        print(f'[PUSH] 飞书返回异常: {resp.status_code} {resp.text[:200]}')
        return False
    except requests.exceptions.Timeout:
        print('[PUSH] 飞书请求超时')
        return False
    except Exception as e:
        print(f'[PUSH] 飞书推送异常: {e}')
        return False


def process_queue():
    """处理队列：推送所有待发送告警"""
    if not QUEUE_FILE.exists():
        return

    with open(QUEUE_FILE, 'r') as f:
        content = f.read().strip()

    if not content:
        return

    try:
        queue = json.loads(content)
    except (json.JSONDecodeError, ValueError):
        print('[PUSH] 队列文件损坏，重建')
        write_status(error='队列文件损坏')
        with open(QUEUE_FILE, 'w') as f:
            f.write('[]')
        return

    if not queue:
        write_status(sent=0, failed=0)
        return

    _lock(f) if False else None  # placeholder
    remaining = []
    sent = 0
    failed = 0
    exhausted = 0

    for alert in queue:
        level = alert.get('level', 'WARNING')
        title = alert.get('title', '')
        detail = alert.get('detail', '')
        source = alert.get('source', '')
        retry_count = alert.get('retry_count', 0)
        expires_at = alert.get('expires_at', 0)
        alert_id = alert.get('id', '?')

        # 跳过过期
        if time.time() > expires_at:
            print(f'[PUSH] 过期丢弃: [{alert_id}] {title}')
            continue

        # INSPECTION_WARNING 去重：24h内同类告警只推送一次
        # 根据 type 字段（如 INSPECTION_WARNING_高频）+ 时间戳判断
        if 'INSPECTION' in title or '24h交易净盈利' in title:
            if HISTORY_FILE.exists():
                try:
                    with open(HISTORY_FILE, 'r') as hf:
                        history = json.load(hf)
                    # 确定本告警的类型标识
                    if '高频' in title:
                        alert_type_key = 'INSPECTION_WARNING_高频'
                    elif '低频' in title:
                        alert_type_key = 'INSPECTION_WARNING_低频'
                    else:
                        alert_type_key = None
                    if alert_type_key:
                        now_ts = time.time()
                        recent = []
                        for h in history:
                            if h.get('type') != alert_type_key:
                                continue
                            try:
                                ht = datetime.strptime(h['timestamp'], '%Y-%m-%d %H:%M:%S')
                                ht = ht.replace(tzinfo=None)
                                age_h = (now_ts - ht.timestamp()) / 3600
                                if age_h < 24:
                                    recent.append(h)
                            except (ValueError, KeyError):
                                continue
                        if recent:
                            print(f'[PUSH] 去重跳过: [{alert_id}] {alert_type_key} 24h内已推送')
                            continue
                except Exception:
                    pass

        # 推送
        if send_card(level, title, detail, source):
            sent += 1
            append_to_history(alert)
            print(f'[PUSH] ✅ [{alert_id}] {title}')
        else:
            alert['retry_count'] = retry_count + 1
            if alert['retry_count'] >= MAX_RETRIES:
                exhausted += 1
                print(f'[PUSH] ❌ 重试{MAX_RETRIES}次放弃: [{alert_id}] {title}')
            else:
                remaining.append(alert)
                failed += 1
                print(f'[PUSH] ⏳ 第{alert[retry_count]}次失败待重试: [{alert_id}] {title}')

    # 写回队列
    with open(QUEUE_FILE, 'w') as f:
        json.dump(remaining, f, ensure_ascii=False, indent=2)

    print(f'[PUSH] 完成: 发送{sent} 失败{failed} 过期丢弃{exhausted}')
    write_status(sent=sent, failed=failed)


def show_status():
    """显示队列状态"""
    if not QUEUE_FILE.exists():
        print('队列文件不存在')
        return

    with open(QUEUE_FILE, 'r') as f:
        content = f.read().strip()

    if not content:
        print('队列为空')
        return

    try:
        queue = json.loads(content)
    except Exception:
        print('队列文件损坏')
        return

    now = time.time()
    pending = [a for a in queue if a.get('expires_at', 0) > now]
    expired = [a for a in queue if a.get('expires_at', 0) <= now]

    print(f'队列状态: {len(pending)}条待发送, {len(expired)}条已过期')
    for a in pending:
        aid=a.get('id','?'); alevel=a.get('level','?'); atitle=a.get('title','?'); aretry=a.get('retry_count',0); print(f'  [{aid}] {alevel} {atitle} (重试{aretry}次)')


def main():
    if '--status' in sys.argv:
        show_status()
        return

    if not WEBHOOK:
        print('[PUSH] 致命错误: 飞书webhook未配置！告警无法推送！')
        print('[PUSH] 请检查 config_v4.yaml 和 .orchestrator/*/ 下的 .env 文件')
        sys.exit(1)

    process_queue()



if __name__ == '__main__':
    _ec = 0
    try:
        main()
    except SystemExit as _se:
        _ec = _se.code if isinstance(_se.code, int) else 1
    except Exception as _e:
        _ec = 1
        print(f"执行异常: {_e}")
    finally:
        try:
            write_heartbeat("push_alerts", exit_code=_ec)
        except Exception:
            pass
    sys.exit(_ec)
