#!/usr/bin/env python3
"""
HL跟单系统 - 统一配置读取模块

单一数据源：从 .orchestrator/*/ 的 .env 文件读取账户地址和webhook
消除 key_events_logger / net_value_tracker / vps_expiry_checker / push_report 等脚本中的硬编码

新增跟单实例时只需更新 .orchestrator/ 下的 .env 和本文件 INSTANCES 映射，
无需再逐个修改各监控/审计脚本。
"""

from pathlib import Path

from paths import BASE_DIR

# 实例ID → Orchestrator实例名 + .env路径
# 新增实例时只需在此添加一行
INSTANCES = {
    'lf': {
        'name': 'lowfreq',
        'env_file': BASE_DIR / '.orchestrator' / 'lowfreq' / '.env.lowfreq',
    },
    'hf': {
        'name': 'highfreq',
        'env_file': BASE_DIR / '.orchestrator' / 'highfreq' / '.env.highfreq',
    },
}


def _load_env(filepath):
    """从 .env 文件加载环境变量为 dict"""
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


def get_accounts():
    """
    读取所有实例的账户地址。
    返回格式: {'lf': {'leader': '0x...', 'follower': '0x...'}, 'hf': {...}}
    与原硬编码 ACCOUNTS 结构完全兼容。
    """
    accounts = {}
    for inst_id, inst_cfg in INSTANCES.items():
        env = _load_env(inst_cfg['env_file'])
        leader = env.get('HL_LEADER_ADDR', '')
        follower = env.get('HL_USER_MAIN_ADDR', '')
        if not leader or not follower:
            print(f"WARNING: {inst_id} .env 缺少 HL_LEADER_ADDR 或 HL_USER_MAIN_ADDR")
        accounts[inst_id] = {
            'leader': leader,
            'follower': follower,
        }
    return accounts


def get_feishu_webhook():
    """
    获取飞书webhook URL。
    依次尝试各实例 .env，返回第一个有效值。
    不再硬依赖特定实例。
    """
    for inst_cfg in INSTANCES.values():
        env = _load_env(inst_cfg['env_file'])
        webhook = env.get('HL_FEISHU_WEBHOOK', '')
        if webhook:
            return webhook
    print("WARNING: 所有实例 .env 中均未找到 HL_FEISHU_WEBHOOK")
    return None
