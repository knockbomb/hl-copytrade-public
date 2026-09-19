#!/usr/bin/env python3
"""
钱包生命周期自治系统 - VPS原生调度脚本
零依赖Coze，全自动检测/轮转/告警

状态机：
  idle           → days>7，正常
  auto_rotated   → 自动rotate成功，防重复
  auto_failed    → 自动rotate失败，等待手动
  manual_alert   → D-5~D-1每天告警

cron: 30 10 * * *
"""

import json
import os
import subprocess
import sys
import time
from datetime import datetime, timezone, timedelta
from shared_config import get_instances
from pathlib import Path

import yaml

# ======================== 常量 ========================

from paths import BASE_DIR
from heartbeat_writer import write_heartbeat
CONFIG_V4_PATH = BASE_DIR / "config_v4.yaml"
STATE_FILE_NAME = "wallet_lifecycle_state.json"
LOG_FILE = BASE_DIR / "wallet_lifecycle.log"

# 自动rotate窗口
AUTO_ROTATE_DAYS = [6, 7]  # D-7, D-6
# 手动告警窗口
MANUAL_ALERT_DAYS = [1, 2, 3, 4, 5]  # D-5 ~ D-1

# 实例列表（与wallet_manager_config.yaml一致）
INSTANCES = [info["name"] for info in get_instances().values()]


# ======================== 工具函数 ========================

def log(msg: str):
    """带时间戳的日志"""
    ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    line = f"[{ts}] {msg}"
    print(line)
    with open(LOG_FILE, "a") as f:
        f.write(line + "\n")


def load_config_v4() -> dict:
    """读取config_v4.yaml"""
    with open(CONFIG_V4_PATH) as f:
        return yaml.safe_load(f)


def get_instance_expiry(config: dict, instance: str) -> str | None:
    """从config_v4获取指定实例的api_wallet_expiry
    config_v4.yaml中instances是字典: {lowfreq: {...}, highfreq: {...}}
    """
    instances = config.get("instances", {})
    if instance in instances:
        return instances[instance].get("api_wallet_expiry")
    return None


def get_instance_display_name_from_config(config: dict, instance: str) -> str:
    """从config_v4获取实例显示名称"""
    instances = config.get("instances", {})
    if instance in instances:
        return instances[instance].get("display_name", instance)
    return instance


def load_state(instance: str) -> dict:
    """加载实例状态文件"""
    state_file = BASE_DIR / ".orchestrator" / instance / STATE_FILE_NAME
    if state_file.exists():
        with open(state_file) as f:
            return json.load(f)
    return {
        "instance": instance,
        "tracked_expiry": None,
        "state": "idle",
        "last_rotate_success": None,
        "last_rotate_failure": None,
        "last_alert_date": None,
        "failure_count": 0,
        "history": []
    }


def save_state(instance: str, state: dict):
    """保存实例状态文件"""
    state_file = BASE_DIR / ".orchestrator" / instance / STATE_FILE_NAME
    with open(state_file, "w") as f:
        json.dump(state, f, indent=2, ensure_ascii=False)


def calc_days_remaining(expiry_str: str) -> int:
    """计算距离到期还有多少天"""
    expiry = datetime.strptime(expiry_str, "%Y-%m-%d").date()
    today = datetime.now().date()
    return (expiry - today).days


def send_feishu_alert(title: str, content: str):
    """发送飞书告警（通过统一队列）"""
    try:
        sys.path.insert(0, str(BASE_DIR))
        from unified_alert_queue import enqueue_alert
        level = "CRITICAL" if ("失败" in title or "过期" in title) else "WARNING"
        ok = enqueue_alert(level=level, title=title, detail=content, source="钱包管理")
        if ok:
            log(f"✅ 告警已入队: {title}")
        else:
            log(f"⚠️ 告警入队失败: {title}")
    except Exception as e:
        log(f"⚠️ 告警入队异常: {e}")


def do_auto_rotate(instance: str) -> tuple[bool, str]:
    """
    执行自动rotate
    返回: (成功与否, 消息)
    """
    log(f"🔄 [{instance}] 开始自动rotate...")
    
    # 读取密码文件
    pw_file = BASE_DIR / ".orchestrator" / instance / ".wm_password"
    if not pw_file.exists():
        return False, f"密码文件不存在: {pw_file}"
    
    password = pw_file.read_text().strip()
    
    # 构建环境变量
    env = os.environ.copy()
    env["WALLET_PASSWORD"] = password
    
    # 执行wallet_manager rotate
    cmd = [
        str(BASE_DIR / "venv" / "bin" / "python3"),
        str(BASE_DIR / "wallet_manager" / "wallet_manager.py"),
        "rotate",
        "--instance", instance,
        "--yes"  # 跳过确认
    ]
    
    try:
        result = subprocess.run(
            cmd,
            env=env,
            capture_output=True,
            text=True,
            timeout=300,  # 5分钟超时
            cwd=str(BASE_DIR)
        )
        
        output = result.stdout + result.stderr
        log(f"[{instance}] wallet_manager输出:\n{output}")
        
        if result.returncode == 0:
            # 验证新钱包生效
            success, verify_msg = verify_wallet_active(instance)
            if success:
                return True, f"自动rotate成功\n{verify_msg}"
            else:
                return False, f"rotate执行成功但验证失败: {verify_msg}"
        else:
            return False, f"wallet_manager返回码{result.returncode}\n{output[-500:]}"
            
    except subprocess.TimeoutExpired:
        return False, "wallet_manager执行超时(>5分钟)"
    except Exception as e:
        return False, f"执行异常: {e}"


def verify_wallet_active(instance: str) -> tuple[bool, str]:
    """验证新钱包已生效"""
    # 重新读取config_v4获取最新expiry
    config = load_config_v4()
    new_expiry = get_instance_expiry(config, instance)
    
    if not new_expiry:
        return False, "无法从config_v4读取expiry"
    
    days = calc_days_remaining(new_expiry)
    if days > 80:  # 新钱包应该有~90天
        return True, f"新钱包到期日: {new_expiry} (剩余{days}天)"
    else:
        return False, f"expiry未更新: {new_expiry} (剩余{days}天)"


def get_instance_display_name(instance: str, config: dict = None) -> str:
    """获取实例显示名称（优先从config_v4读取）"""
    if config:
        name = get_instance_display_name_from_config(config, instance)
        if name:
            return name
    # fallback
    names = {info["name"]: info["display"] for info in get_instances().values()}
    return names.get(instance, instance)


# ======================== 主逻辑 ========================

def process_instance(instance: str, config: dict):
    """处理单个实例的钱包生命周期"""
    log(f"\n{'='*50}")
    log(f"📋 处理实例: {instance}")
    log(f"{'='*50}")
    
    # 1. 读取当前expiry
    current_expiry = get_instance_expiry(config, instance)
    if not current_expiry:
        log(f"❌ [{instance}] config_v4中未找到api_wallet_expiry")
        return
    
    # 2. 加载状态
    state = load_state(instance)
    tracked_expiry = state.get("tracked_expiry")
    current_state = state.get("state", "idle")
    today = datetime.now().date().isoformat()
    days = calc_days_remaining(current_expiry)
    display_name = get_instance_display_name(instance, config)
    
    log(f"  当前expiry: {current_expiry} (剩余{days}天)")
    log(f"  跟踪expiry: {tracked_expiry}")
    log(f"  当前状态: {current_state}")
    
    # 3. 检测expiry变化（新周期开始）
    if tracked_expiry and tracked_expiry != current_expiry:
        log(f"🔄 [{instance}] 检测到expiry变化: {tracked_expiry} → {current_expiry}")
        log(f"  重置状态为idle（新周期开始）")
        state["tracked_expiry"] = current_expiry
        state["state"] = "idle"
        state["failure_count"] = 0
        state["history"].append({
            "date": today,
            "action": "expiry_changed",
            "old_expiry": tracked_expiry,
            "new_expiry": current_expiry
        })
        save_state(instance, state)
        # 重新计算状态
        current_state = "idle"
    
    # 首次运行，初始化tracked_expiry
    if not tracked_expiry:
        state["tracked_expiry"] = current_expiry
        state["state"] = "idle"
        save_state(instance, state)
        current_state = "idle"
        log(f"  初始化tracked_expiry: {current_expiry}")
    
    # 4. 状态机决策
    if days > 7:
        log(f"  ✅ days={days}>7，无需操作")
        return
    
    if days in AUTO_ROTATE_DAYS:  # D-7, D-6
        if current_state == "auto_rotated":
            log(f"  ✅ 本轮已完成自动rotate，跳过")
            return
        
        log(f"  🔄 D-{days}，尝试自动rotate...")
        success, msg = do_auto_rotate(instance)
        
        if success:
            state["state"] = "auto_rotated"
            state["last_rotate_success"] = today
            state["failure_count"] = 0
            state["tracked_expiry"] = current_expiry  # 更新为最新的
            state["history"].append({
                "date": today,
                "action": "auto_rotate",
                "result": "success",
                "message": msg
            })
            save_state(instance, state)
            send_feishu_alert(
                f"{display_name} API钱包自动更新成功",
                f"**实例:** {instance} ({display_name})\n"
                f"**新到期日:** {current_expiry}\n"
                f"**剩余天数:** {days}\n\n"
                f"{msg}"
            )
            log(f"  ✅ 自动rotate成功")
        else:
            state["state"] = "auto_failed"
            state["last_rotate_failure"] = today
            state["failure_count"] = state.get("failure_count", 0) + 1
            state["history"].append({
                "date": today,
                "action": "auto_rotate",
                "result": "failure",
                "message": msg
            })
            save_state(instance, state)
            send_feishu_alert(
                f"🚨 {display_name} API钱包自动更新失败",
                f"**实例:** {instance} ({display_name})\n"
                f"**到期日:** {current_expiry} (D-{days})\n"
                f"**失败次数:** {state['failure_count']}\n\n"
                f"**错误:** {msg}\n\n"
                f"请尽快手动操作！\n"
                f"SSH命令: `wallet_manager.py rotate --instance {instance} --yes`"
            )
            log(f"  ❌ 自动rotate失败: {msg}")
    
    elif days in MANUAL_ALERT_DAYS:  # D-5 ~ D-1
        if current_state == "auto_rotated":
            log(f"  ✅ 本轮已完成，跳过告警")
            return
        
        # 每天只告警一次
        if state.get("last_alert_date") == today:
            log(f"  ⏭️ 今天已告警，跳过")
            return
        
        alert_msg = (
            f"**实例:** {instance} ({display_name})\n"
            f"**到期日:** {current_expiry} (剩余{days}天)\n"
            f"**状态:** 自动更新未成功，需手动操作\n\n"
            f"**操作方式（任选其一）:**\n\n"
            f"━━━━━━━━━━━━━━━━━━━━━━━━\n"
            f"**方法1：通过Coze助手（最简单）**\n"
            f"直接告诉助手：「帮我更换{display_name}API钱包」\n"
            f"助手会自动完成所有操作，你无需任何技术知识\n\n"
            f"━━━━━━━━━━━━━━━━━━━━━━━━\n"
            f"**方法2：通过本地电脑SSH到VPS**\n"
            f"步骤1：打开本地电脑的命令行（Windows: Win+R输入cmd，Mac: 打开终端）\n"
            f"步骤2：输入以下命令连接VPS：\n"
            f"`ssh -p YOUR_SSH_PORT ubuntu@YOUR_VPS_IP`\n"
            f"步骤3：输入密码（输入时不显示，正常）：`635624Fei`\n"
            f"步骤4：进入项目目录并执行替换：\n"
            f"```bash\n"
            f"cd {BASE_DIR}\n"
            f"./venv/bin/python3 wallet_manager/wallet_manager.py {instance} rotate --yes\n"
            f"```\n"
            f"看到 `✅ 钱包轮换完成！` 表示成功\n\n"
            f"⚠️ **常见问题:**\n"
            f"- 如果提示 `Permission denied (publickey)`：说明VPS只允许密钥认证，你的电脑没有配置密钥，请使用方法1或方法3\n"
            f"- 如果提示 `connection refused`：检查网络是否正常，或VPS是否运行中\n\n"
            f"━━━━━━━━━━━━━━━━━━━━━━━━\n"
            f"**方法3：通过云服务器控制台直接操作**\n"
            f"步骤1：登录腾讯云控制台 https://console.cloud.tencent.com/lighthouse\n"
            f"步骤2：找到你的服务器（IP: YOUR_VPS_IP）\n"
            f"步骤3：点击服务器名称进入详情页\n"
            f"步骤4：点击「登录」按钮，选择「免密登录」\n"
            f"步骤5：会打开一个网页终端窗口，输入以下命令：\n"
            f"```bash\n"
            f"cd {BASE_DIR}\n"
            f"./venv/bin/python3 wallet_manager/wallet_manager.py {instance} rotate --yes\n"
            f"```\n"
            f"看到 `✅ 钱包轮换完成！` 表示成功\n\n"
            f"━━━━━━━━━━━━━━━━━━━━━━━━\n"
            f"️ **重要提示:**\n"
            f"1. 钱包即将过期，请尽快处理\n"
            f"2. 操作前请确认实例名称是 `{instance}`\n"
            f"3. 如果操作失败，可以截图发给助手寻求帮助\n"
        )

        
        send_feishu_alert(f"⚠️ {display_name} API钱包即将过期(D-{days})", alert_msg)
        
        state["last_alert_date"] = today
        state["history"].append({
            "date": today,
            "action": "manual_alert",
            "days_remaining": days
        })
        save_state(instance, state)
        log(f"  ⚠️ 已发送D-{days}告警")
    
    elif days <= 0:
        # 紧急告警
        send_feishu_alert(
            f"💀 {display_name} API钱包已过期！",
            f"**实例:** {instance} ({display_name})\n"
            f"**到期日:** {current_expiry}\n"
            f"**状态:** 已过期{abs(days)}天！\n\n"
            f"请立即手动更换钱包！"
        )
        log(f"  💀 钱包已过期{abs(days)}天！")


def main():
    log(f"\n{'#'*60}")
    log(f"# 钱包生命周期检查 - 开始")
    log(f"{'#'*60}")
    
    # 读取config
    try:
        config = load_config_v4()
    except Exception as e:
        log(f"💀 无法读取config_v4.yaml: {e}")
        sys.exit(1)
    
    # 处理每个实例
    for instance in INSTANCES:
        try:
            process_instance(instance, config)
        except Exception as e:
            log(f"💀 [{instance}] 处理异常: {e}")
            import traceback
            traceback.print_exc()
    
    log(f"\n# 钱包生命周期检查 - 完成")



if __name__ == "__main__":
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
            write_heartbeat("wallet_lifecycle", exit_code=_ec)
        except Exception:
            pass
    sys.exit(_ec)
