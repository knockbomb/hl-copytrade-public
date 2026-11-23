#!/usr/bin/env python3
"""
HL跟单系统 - 统一配置读取模块

单一数据源：
- 实例注册中心：从 config_v4.yaml 动态读取所有启用实例
- 账户地址：从 .orchestrator/*/ 的 .env 文件读取
- Webhook URL：统一获取飞书告警 webhook

新增跟单实例时只需更新 config_v4.yaml + .orchestrator/ 下的 .env，
所有监控/审计/报告脚本自动发现新实例，无需修改代码。
"""

import yaml
from pathlib import Path

from paths import BASE_DIR, ORCH_DIR

CONFIG_V4_PATH = BASE_DIR / 'config_v4.yaml'


def _short_id(full_name):
    """
    从完整实例名派生短ID（向后兼容）。
    'lowfreq' -> 'lf', 'highfreq' -> 'hf'
    """
    idx = full_name.find('freq')
    if idx > 1:
        return full_name[0] + full_name[idx]
    return full_name[:2]


def get_instances():
    """
    获取所有启用实例的完整信息（实例注册中心）。
    从 config_v4.yaml 动态读取。

    Returns:
        dict: {
            'lf': {
                'name': 'lowfreq',
                'display': '低频跟单',
                'display_short': '低频',
                'env_file': '...',
                'audit_dir': '...',
                'audit_prefix': 'hl_copytrade_v3_lowfreq_audit_',
                'alert_bot_name': '...',
                'health_port': 8997,
            }, ...
        }
    """
    with open(CONFIG_V4_PATH) as f:
        config = yaml.safe_load(f)

    instances_cfg = config.get('instances', {})
    if not instances_cfg:
        raise ValueError("config_v4.yaml 中未找到 instances 配置")

    result = {}
    for full_name, cfg in instances_cfg.items():
        if not cfg.get('enabled', False):
            continue
        sid = _short_id(full_name)
        file_prefix = cfg.get('file_prefix', full_name)
        inst_dir = ORCH_DIR / full_name
        display = cfg.get('display_name', full_name)

        result[sid] = {
            'name': full_name,
            'display': display,
            'display_short': display.replace('跟单', ''),
            'env_file': str(inst_dir / f'.env.{file_prefix}'),
            'audit_dir': str(inst_dir),
            'audit_prefix': f'hl_copytrade_v3_{file_prefix}_audit_',
            'alert_bot_name': cfg.get('alert_bot_name', ''),
            'health_port': cfg.get('health_port', 0),
        }
    return result


def _load_env(filepath):
    """从 .env 文件读取环境变量为 dict"""
    env = {}
    path = Path(filepath)
    if not path.exists():
        return env
    with open(path, 'r') as f:
        for line in f:
            line = line.strip()
            if line and not line.startswith('#') and '=' in line:
                key, _, value = line.partition('=')
                env[key.strip()] = value.strip()
    return env


# 向后兼容：INSTANCES 由 get_instances() 动态构建
INSTANCES = {
    sid: {'name': info['name'], 'env_file': Path(info['env_file'])}
    for sid, info in get_instances().items()
}


def get_accounts():
    """
    读取所有实例的账户地址。
    Returns: {'lf': {'leader': '0x...', 'follower': '0x...'}, ...}
    """
    accounts = {}
    for inst_id, inst_cfg in INSTANCES.items():
        env = _load_env(inst_cfg['env_file'])
        leader = env.get('HL_LEADER_ADDR', '')
        follower = env.get('HL_USER_MAIN_ADDR', '')
        if not leader or not follower:
            print(f"WARNING: {inst_id} .env 缺少 HL_LEADER_ADDR 或 HL_USER_MAIN_ADDR")
        accounts[inst_id] = {'leader': leader, 'follower': follower}
    return accounts


def get_feishu_webhook():
    """
    获取飞书webhook URL。
    依次尝试各实例 .env，返回第一个有效值。
    """
    for inst_cfg in INSTANCES.values():
        env = _load_env(inst_cfg['env_file'])
        webhook = env.get('HL_FEISHU_WEBHOOK', '')
        if webhook:
            return webhook
    print("WARNING: 所有实例 .env 中均未找到 HL_FEISHU_WEBHOOK")
    return None
