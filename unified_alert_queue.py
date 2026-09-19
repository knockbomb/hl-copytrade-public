#!/usr/bin/env python3
"""
HL跟单系统 - 统一告警队列

所有告警统一写入队列文件，由 push_alerts_to_feishu.py 定时消费推送。
这是系统唯一的告警出口，消除多通道冗余导致的静默故障。

用法:
    from unified_alert_queue import enqueue_alert
    enqueue_alert("WARNING", "保证金超标", "使用率130%", source="低频")
"""

import json
import os
import fcntl
import uuid
import time
from datetime import datetime

try:
    import requests
except ImportError:
    requests = None
from typing import Optional

from paths import BASE_DIR, DATA_DIR
import shared_config
QUEUE_FILE = str(DATA_DIR / 'alert_queue.json')
MAX_QUEUE_SIZE = 500  # 队列最大长度，防止无限膨胀


def _acquire_lock(f):
    """获取文件排他锁"""
    try:
        fcntl.flock(f.fileno(), fcntl.LOCK_EX)
        return True
    except Exception:
        return False


def _release_lock(f):
    """释放文件锁"""
    try:
        fcntl.flock(f.fileno(), fcntl.LOCK_UN)
    except Exception:
        pass


def enqueue_alert(
    level: str,
    title: str,
    detail: str = '',
    source: str = '',
    ttl_hours: int = 72,
) -> bool:
    """
    将告警写入统一队列（线程安全/进程安全）

    Args:
        level: CRITICAL / WARNING / INFO
        title: 告警标题
        detail: 告警详情
        source: 来源标识（低频/高频/巡检/钱包/VPS等）
        ttl_hours: 过期时间（小时），超时未推送则丢弃

    Returns:
        True=入队成功, False=入队失败
    """
    alert = {
        'id': str(uuid.uuid4())[:8],
        'timestamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
        'level': level.upper(),
        'title': title,
        'detail': detail,
        'source': source,
        'retry_count': 0,
        'max_retries': 3,
        'expires_at': int(time.time()) + ttl_hours * 3600,
    }

    try:
        os.makedirs(os.path.dirname(QUEUE_FILE), exist_ok=True)

        with open(QUEUE_FILE, 'a+') as f:
            _acquire_lock(f)
            try:
                f.seek(0)
                content = f.read().strip()
                queue = json.loads(content) if content else []
            except (json.JSONDecodeError, ValueError):
                queue = []

            # 清理过期告警
            now = time.time()
            queue = [a for a in queue if a.get('expires_at', 0) > now]

            queue.append(alert)

            # 防止无限膨胀
            if len(queue) > MAX_QUEUE_SIZE:
                queue = queue[-MAX_QUEUE_SIZE:]

            f.seek(0)
            f.truncate()
            json.dump(queue, f, ensure_ascii=False, indent=2)
            _release_lock(f)

        return True

    except Exception as e:
        print(f'[QUEUE] 入队失败: {e}')
        return False


# webhook: 从实例.env读取
_WEBHOOK_CACHE = {'url': None, 'loaded': False}

def _get_webhook():
    """获取飞书webhook URL（通过 shared_config 统一接口）"""
    if not _WEBHOOK_CACHE['loaded']:
        _WEBHOOK_CACHE['url'] = shared_config.get_feishu_webhook()
        _WEBHOOK_CACHE['loaded'] = True
    return _WEBHOOK_CACHE['url']


def direct_push_critical(level: str, title: str, detail: str = '', source: str = '') -> bool:
    """
    CRITICAL告警直推飞书（绕过队列延迟）
    
    用于最紧急的告警（崩溃、资金风险等），不等5分钟cron，立即推送。
    失败时静默降级到队列，由正常流程重试。
    
    Returns:
        True=推送成功, False=推送失败（调用方应继续走队列）
    """
    if not requests:
        return False
    
    webhook = _get_webhook()
    if not webhook:
        return False
    
    emoji = {'CRITICAL': '🔴', 'WARNING': '🟡', 'INFO': '🟢'}.get(level, '⚪')
    color = {'CRITICAL': 'red', 'WARNING': 'orange', 'INFO': 'green'}.get(level, 'grey')
    ts = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    
    header_title = f'{emoji} [{source}][{level}] {title}' if source else f'{emoji} [{level}] {title}'
    
    card = {
        'msg_type': 'interactive',
        'card': {
            'header': {
                'title': {'tag': 'plain_text', 'content': header_title},
                'template': color,
            },
            'elements': [
                {'tag': 'div', 'text': {'tag': 'lark_md', 'content': detail or title}},
                {'tag': 'note', 'elements': [
                    {'tag': 'plain_text', 'content': f'⚡ 直推 | {ts}'}
                ]},
            ],
        }
    }
    
    try:
        resp = requests.post(webhook, json=card, timeout=10,
                             headers={'Content-Type': 'application/json'})
        if resp.status_code == 200:
            data = resp.json()
            code = data.get('StatusCode', data.get('code', -1))
            if code == 0:
                return True
        return False
    except Exception:
        return False

