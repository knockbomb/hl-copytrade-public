#!/usr/bin/env python3
"""
心跳写入模块 - 三层观测体系第一层
每个cron脚本执行完毕后写入心跳文件，记录运行状态

用法:
    from heartbeat_writer import write_heartbeat
    write_heartbeat("script_name", exit_code=0)
    write_heartbeat("script_name", exit_code=1, error="something failed")
    write_heartbeat("script_name", exit_code=0, stats={"items_processed": 42})
"""
import json
import os
import time
from datetime import datetime
from pathlib import Path

HEARTBEAT_DIR = Path(__file__).parent / 'data' / 'heartbeats'

def write_heartbeat(script_name: str, exit_code: int = 0,
                    stats: dict = None, error: str = None):
    """写入脚本心跳文件"""
    HEARTBEAT_DIR.mkdir(parents=True, exist_ok=True)
    
    data = {
        'script': script_name,
        'last_run': datetime.now().isoformat(timespec='seconds'),
        'timestamp': int(time.time()),
        'exit_code': exit_code,
        # exit_code=0: success, exit_code=1: warning(脚本正常运行但发现告警), >=2: failed
        'status': 'success' if exit_code == 0 else ('warning' if exit_code == 1 else 'failed'),
        'stats': stats or {},
    }
    if error:
        data['error'] = str(error)[:500]
    
    hb_path = HEARTBEAT_DIR / f'{script_name}.json'
    tmp_path = HEARTBEAT_DIR / f'{script_name}.tmp'
    try:
        with open(tmp_path, 'w') as f:
            json.dump(data, f, indent=2, ensure_ascii=False)
        os.replace(tmp_path, hb_path)
    except Exception:
        pass
