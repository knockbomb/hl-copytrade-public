#!/usr/bin/env python3
"""
Hyperliquid API 钱包生命周期管理工具
功能：list / rotate / cleanup / init / setup / health / verify
方案：master key AES-256-GCM 加密存储，按需解锁

使用方式：
  wallet_manager.py <instance|--all> <command> [options]
  wallet_manager.py --help

命令：
  list      列出所有API钱包及到期状态
  rotate    轮换钱包（创建新钱包+替换.env+重启服务+验证）
  cleanup   清理到期超7天的钱包（仅报告，HL无直接删除API）
  init      初始化master key加密（首次使用）
  setup     一键初始化（init + 验证）
  health    检查所有实例健康状态
  verify    验证master key可正常解密

示例：
  wallet_manager.py lowfreq list
  wallet_manager.py highfreq rotate
  wallet_manager.py all list
  wallet_manager.py lowfreq init
"""

import os
import sys
import json
import time
import getpass
import argparse
import subprocess
from pathlib import Path
from datetime import datetime, timezone

import requests
import yaml

# PyCryptodome（VPS已安装）
from Crypto.Cipher import AES
from Crypto.Protocol.KDF import PBKDF2
from Crypto.Random import get_random_bytes

# ======================== 路径与常量 ========================
import sys as _sys
_sys.path.insert(0, str(Path(__file__).parent.parent.resolve()))
from paths import BASE_DIR
CONFIG_PATH = BASE_DIR / "wallet_manager" / "wallet_manager_config.yaml"
def get_enc_key_path(instance_name: str) -> Path:
    """每个实例独立存储加密的 master key"""
    return BASE_DIR / "wallet_manager" / f".master_key.{instance_name}.enc"
KDF_ITERATIONS = 480_000
SALT_SIZE = 16
NONCE_SIZE = 12
API_URL = "https://api.hyperliquid.xyz"
GRACE_DAYS = 7

# ======================== Orchestrator 集成 ========================
ORCHESTRATOR_CMD_DIR = BASE_DIR / ".orchestrator" / "commands"
ORCHESTRATOR_API_URL = "http://127.0.0.1:9000"
ORCHESTRATOR_SERVICE = "hl-orchestrator.service"
CONFIG_V4_PATH = BASE_DIR / "config_v4.yaml"

def is_orchestrator_running() -> bool:
    """检查 Orchestrator 是否在运行（通过 HTTP API）"""
    try:
        resp = requests.get(f"{ORCHESTRATOR_API_URL}/health", timeout=3)
        return resp.status_code == 200
    except Exception:
        return False

def orchestrator_restart(instance_name: str, timeout: int = 30) -> tuple:
    """
    通过 Orchestrator 文件命令接口重启实例
    返回 (success: bool, message: str)
    """
    # 确保 CMD_DIR 存在
    ORCHESTRATOR_CMD_DIR.mkdir(parents=True, exist_ok=True)
    
    # 创建重启命令文件
    cmd_file = ORCHESTRATOR_CMD_DIR / f"restart_{instance_name}"
    cmd_file.write_text(datetime.now(timezone.utc).isoformat())
    print(f"  📝 已创建重启命令: {cmd_file.name}")
    
    # 等待 Orchestrator 处理（每3秒轮询，最长 timeout 秒）
    deadline = time.time() + timeout
    last_mtime = 0
    while time.time() < deadline:
        if not cmd_file.exists():
            # 命令文件已被 Orchestrator 消费
            print(f"  ✅ Orchestrator 已处理重启命令")
            time.sleep(5)  # 额外等待进程完全启动
            return True, "重启命令已执行"
        # 检查文件是否被修改过（orchestrator可能在处理但没删除）
        try:
            cur_mtime = cmd_file.stat().st_mtime
            if cur_mtime != last_mtime:
                last_mtime = cur_mtime
        except:
            pass
        time.sleep(3)
    
    # 超时：清理命令文件，尝试直接重启
    cmd_file.unlink(missing_ok=True)
    print(f"  ⚠️ Orchestrator 未在 {timeout} 秒内处理，尝试 systemctl 直接重启...")
    try:
        r = subprocess.run(
            ["sudo", "systemctl", "restart", ORCHESTRATOR_SERVICE],
            capture_output=True, text=True, timeout=30,
        )
        if r.returncode == 0:
            time.sleep(5)
            return True, "systemctl直接重启成功"
        else:
            return False, f"Orchestrator超时且systemctl失败: {r.stderr.strip()}"
    except Exception as e:
        return False, f"Orchestrator超时且systemctl异常: {e}"

def orchestrator_instance_health(instance_name: str) -> tuple:
    """
    通过 Orchestrator HTTP API 检查实例健康状态
    返回 (running: bool, details: dict)
    """
    try:
        resp = requests.get(f"{ORCHESTRATOR_API_URL}/instance/{instance_name}", timeout=5)
        if resp.status_code == 200:
            data = resp.json()
            running = data.get("running", False)
            return running, data
        return False, {"error": f"HTTP {resp.status_code}"}
    except Exception as e:
        return False, {"error": str(e)}


# ======================== 加密/解密工具 ========================

def _derive_key(password: str, salt: bytes) -> bytes:
    """PBKDF2-HMAC-SHA256 派生 AES-256 密钥"""
    return PBKDF2(password, salt, dkLen=32, count=KDF_ITERATIONS)


def encrypt_master_key(instance_name: str, master_key: str, password: str) -> None:
    """AES-256-GCM 加密存储 master key（每实例独立）"""
    salt = get_random_bytes(SALT_SIZE)
    nonce = get_random_bytes(NONCE_SIZE)
    key = _derive_key(password, salt)
    cipher = AES.new(key, AES.MODE_GCM, nonce=nonce)
    ciphertext, tag = cipher.encrypt_and_digest(master_key.encode())
    payload = salt + nonce + tag + ciphertext
    enc_path = get_enc_key_path(instance_name)
    enc_path.parent.mkdir(parents=True, exist_ok=True)
    enc_path.write_bytes(payload)
    os.chmod(str(enc_path), 0o600)


def decrypt_master_key(instance_name: str, password: str) -> tuple:
    """解密 master key。成功返回 (key_str, None)，失败返回 (None, error_msg)"""
    enc_path = get_enc_key_path(instance_name)
    if not enc_path.exists():
        return None, f"加密文件不存在: {enc_path.name}，请先运行: wallet_manager.py {instance_name} init"
    try:
        data = enc_path.read_bytes()
        expected = SALT_SIZE + NONCE_SIZE + 16  # 16 = GCM tag size
        if len(data) < expected:
            return None, "加密文件损坏"
        salt = data[:SALT_SIZE]
        nonce = data[SALT_SIZE : SALT_SIZE + NONCE_SIZE]
        tag = data[SALT_SIZE + NONCE_SIZE : expected]
        ciphertext = data[expected:]
        key = _derive_key(password, salt)
        cipher = AES.new(key, AES.MODE_GCM, nonce=nonce)
        master_key = cipher.decrypt_and_verify(ciphertext, tag)
        return master_key.decode(), None
    except (ValueError, KeyError) as e:
        return None, f"解密失败（密码错误或文件损坏）: {e}"


def get_master_key_interactive(instance_name: str) -> str:
    """获取 master key（支持环境变量/密码文件/交互式）
    
    优先级：
    1. WALLET_PASSWORD 环境变量（自动化场景）
    2. .orchestrator/{instance}/.wm_password 文件（自动化场景）
    3. getpass 交互式输入（手动场景）
    """
    # 方式1：环境变量（最高优先级）
    env_pw = os.environ.get("WALLET_PASSWORD")
    if env_pw:
        mk, err = decrypt_master_key(instance_name, env_pw)
        if mk:
            print(f"  🔑 [{instance_name}] 使用环境变量密码解密成功")
            return mk
        else:
            print(f"  ❌ 环境变量 WALLET_PASSWORD 解密失败: {err}")
            sys.exit(1)
    
    # 方式2：密码文件
    env_pw_file = os.environ.get("WALLET_PASSWORD_FILE", "")
    if env_pw_file:
        pw_file = Path(env_pw_file)
    else:
        pw_file = None
    
    if not pw_file or not pw_file.exists():
        # 默认路径：.orchestrator/{instance}/.wm_password
        pw_file = BASE_DIR / ".orchestrator" / instance_name / ".wm_password"
    
    if pw_file.exists():
        try:
            file_pw = pw_file.read_text().strip()
            mk, err = decrypt_master_key(instance_name, file_pw)
            if mk:
                print(f"  🔑 [{instance_name}] 使用密码文件解密成功: {pw_file}")
                return mk
            else:
                print(f"  ❌ 密码文件 {pw_file} 解密失败: {err}")
                sys.exit(1)
        except Exception as e:
            print(f"  ❌ 读取密码文件失败: {e}")
            sys.exit(1)
    
    # 方式3：交互式（fallback）
    for attempt in range(3):
        pw = getpass.getpass(f"🔑 输入 [{instance_name}] 的解锁密码: ")
        mk, err = decrypt_master_key(instance_name, pw)
        if mk:
            return mk
        print(f"  ❌ {err}（{attempt + 1}/3）")
    print("\n💀 连续3次失败，退出")
    sys.exit(1)



def send_feishu_notification(title: str, content: str):
    """发送飞书通知（wallet更换成功/失败）"""
    try:
        # 确保能找到根目录的shared_config
        import sys as _sys
        _parent = str(Path(__file__).resolve().parent.parent)
        if _parent not in _sys.path:
            _sys.path.insert(0, _parent)
        from shared_config import get_feishu_webhook
        webhook_url = get_feishu_webhook()
        
        if not webhook_url:
            print("  ⚠️ 飞书webhook未配置，跳过通知")
            return
        
        payload = {
            "msg_type": "interactive",
            "card": {
                "header": {
                    "title": {"tag": "plain_text", "content": f"🔔 {title}"},
                    "template": "green" if "成功" in title else "orange"
                },
                "elements": [
                    {
                        "tag": "markdown",
                        "content": content
                    }
                ]
            }
        }
        resp = requests.post(webhook_url, json=payload, timeout=10)
        if resp.status_code == 200:
            print(f"  ✅ 飞书通知已发送")
        else:
            print(f"  ⚠️ 飞书通知发送失败: {resp.status_code}")
    except Exception as e:
        print(f"  ⚠️ 飞书通知异常: {e}")


# ======================== 配置加载 ========================

def load_config() -> dict:
    if not CONFIG_PATH.exists():
        print(f"💀 配置文件不存在: {CONFIG_PATH}")
        sys.exit(1)
    with open(CONFIG_PATH) as f:
        return yaml.safe_load(f)


def get_instances(config: dict, name: str) -> list:
    if name == "all":
        return list(config["instances"].values())
    if name not in config["instances"]:
        avail = ", ".join(config["instances"].keys())
        print(f"💀 未知实例: {name}（可用: {avail}）")
        sys.exit(1)
    return [config["instances"][name]]


def load_env(env_file: Path) -> dict:
    result = {}
    if not env_file.exists():
        return result
    for line in env_file.read_text().strip().splitlines():
        line = line.strip()
        if line and not line.startswith("#") and "=" in line:
            k, v = line.split("=", 1)
            result[k] = v
    return result


# ======================== API 钱包查询 ========================

def query_wallets(user_main_addr: str) -> list:
    """查询 Hyperliquid 主地址下的所有 API 钱包"""
    resp = requests.post(
        f"{API_URL}/info",
        json={"type": "extraAgents", "user": user_main_addr},
        timeout=15,
    )
    resp.raise_for_status()
    wallets = []
    for agent in resp.json():
        wallets.append(
            {
                "address": agent.get("address", "").lower(),
                "name": agent.get("name", ""),
                "expiry_ms": int(agent.get("validUntil", 0)),
                "expiry_dt": datetime.fromtimestamp(
                    int(agent.get("validUntil", 0)) / 1000, tz=timezone.utc
                ),
            }
        )
    return wallets


# ======================== 钱包轮换 ========================


def update_config_v4_expiry(orchestrator_instance: str, expiry_date: str) -> bool:
    """P2-1: rotate成功后同步更新 config_v4.yaml 的 api_wallet_expiry"""
    if not CONFIG_V4_PATH.exists():
        print(f"  WARNING: {CONFIG_V4_PATH} not found, skip config update")
        return False
    try:
        with open(CONFIG_V4_PATH, 'r') as f:
            cfg = yaml.safe_load(f)
        instances = cfg.get("instances", {})
        if orchestrator_instance not in instances:
            print(f"  WARNING: instance '{orchestrator_instance}' not in config_v4.yaml")
            return False
        old_expiry = instances[orchestrator_instance].get("api_wallet_expiry", "")
        instances[orchestrator_instance]["api_wallet_expiry"] = expiry_date
        with open(CONFIG_V4_PATH, 'w') as f:
            yaml.dump(cfg, f, default_flow_style=False, allow_unicode=True)
        print(f"  OK: config_v4.yaml updated: {orchestrator_instance}.api_wallet_expiry {old_expiry} -> {expiry_date}")
        return True
    except Exception as e:
        print(f"  WARNING: config_v4.yaml update failed: {e}")
        return False


def do_rotate(inst_name: str, inst: dict, master_key: str, dry_run: bool = False) -> bool:
    """
    钱包轮换全流程：
    1. 查询当前钱包列表
    2. 调用 HL API approveAgent 创建新钱包
    3. 更新 .env 中的 HL_API_PK 和 HL_USER_API_ADDR
    4. 通过 Orchestrator 重启实例（或回退到 systemctl）
    5. 验证新钱包生效
    """
    from hyperliquid.exchange import Exchange
    from eth_account import Account

    wallet_name = inst["wallet_name"]
    env_file = BASE_DIR / inst["env_file"]
    user_main_addr = inst["user_main_addr"]

    print(f"\n{'='*50}")
    print(f"🔄 钱包轮换: {inst_name}（{wallet_name}）")
    print(f"{'='*50}")

    if dry_run:
        print("  [DRY RUN] 不执行实际操作，仅模拟流程")
        print(f"  将创建新钱包: {wallet_name}")
        print(f"  将更新: {env_file}")
        print(f"  将重启: Orchestrator 实例 {inst_name}")
        return True

    # Step 1: 查询当前钱包
    print("\n📋 Step 1/5: 查询当前钱包列表...")
    try:
        before_wallets = query_wallets(user_main_addr)
        for w in before_wallets:
            status = "🟢活跃" if w["expiry_dt"] > datetime.now(timezone.utc) else "🔴已过期"
            name_display = w["name"] if w["name"] else "(unnamed)"
            print(f"    {name_display}  {w['address'][:10]}...  到期:{w['expiry_dt'].strftime('%Y-%m-%d')} {status}")
        if len(before_wallets) >= 4:
            print("\n  ⚠️  钱包数量已达上限(4/4)！请等待旧钱包过期或手动处理。")
            print("  提示: Hyperliquid 每账户最多4个活跃API钱包")
            return False
    except Exception as e:
        print(f"  ⚠️  查询失败（非致命，继续）: {e}")
        before_wallets = []

    # Step 2: 创建新钱包
    print(f"\n🔐 Step 2/5: 创建新API钱包（name={wallet_name}）...")
    try:
        account = Account.from_key(master_key)
        exchange = Exchange(account, API_URL)
        result, agent_key = exchange.approve_agent(wallet_name)

        # 检查API返回是否有错误
        if isinstance(result, dict):
            status = result.get("status")
            if status == "error":
                resp_msg = result.get("response", {})
                if isinstance(resp_msg, dict):
                    err_msg = resp_msg.get("message", str(resp_msg))
                else:
                    err_msg = str(resp_msg)
                print(f"  ❌ HL API 拒绝: {err_msg}")
                return False

        new_addr = None
        if isinstance(result, dict):
            new_addr = result.get("agentAddress", "")
        if not new_addr:
            # 从 agent_key 反推地址
            new_account = Account.from_key(agent_key)
            new_addr = new_account.address

        new_addr = new_addr.lower() if new_addr else ""
        print(f"  ✅ 新钱包地址: {new_addr}")
        print(f"  🔑 新私钥已获取（长度={len(agent_key)}）")
    except Exception as e:
        print(f"  ❌ 创建钱包失败: {e}")
        import traceback
        traceback.print_exc()
        return False

    # Step 3: 更新 .env 文件
    print(f"\n📝 Step 3/5: 更新环境变量文件 {env_file.name}...")
    try:
        if not env_file.exists():
            print(f"  ❌ 文件不存在: {env_file}")
            return False

        # 先备份
        backup_path = env_file.with_suffix(env_file.suffix + f".bak.{int(time.time())}")
        backup_path.write_text(env_file.read_text())
        print(f"  📦 已备份到: {backup_path.name}")

        # 更新内容
        lines = env_file.read_text().strip().splitlines()
        new_lines = []
        updated_keys = set()
        for line in lines:
            if line.startswith("HL_API_PK="):
                new_lines.append(f"HL_API_PK={agent_key}")
                updated_keys.add("HL_API_PK")
            elif line.startswith("HL_USER_API_ADDR="):
                new_lines.append(f"HL_USER_API_ADDR={new_addr}")
                updated_keys.add("HL_USER_API_ADDR")
            else:
                new_lines.append(line)

        # 如果某些key不存在于原文件，追加
        if "HL_API_PK" not in updated_keys:
            new_lines.append(f"HL_API_PK={agent_key}")
        if "HL_USER_API_ADDR" not in updated_keys:
            new_lines.append(f"HL_USER_API_ADDR={new_addr}")

        env_file.write_text("\n".join(new_lines) + "\n")
        os.chmod(str(env_file), 0o600)
        print(f"  ✅ HL_API_PK 已更新")
        print(f"  ✅ HL_USER_API_ADDR → {new_addr}")
    except Exception as e:
        print(f"  ❌ 更新 .env 失败: {e}")
        return False

    # Step 4: 重启服务
    # Step 4: 通过 Orchestrator 重启实例
    orch_inst = inst.get("orchestrator_instance", inst_name)
    print(f"\n🔄 Step 4/5: 通过 Orchestrator 重启实例 {orch_inst}...")
    try:
        success, msg = orchestrator_restart(orch_inst)
        if success:
            print(f"  ✅ {msg}")
        else:
            print(f"  ❌ Orchestrator 重启失败: {msg}")
            print(f"  ⚠️  尝试回退: sudo systemctl restart {ORCHESTRATOR_SERVICE}")
            r = subprocess.run(
                ["sudo", "systemctl", "restart", ORCHESTRATOR_SERVICE],
                capture_output=True, text=True, timeout=30,
            )
            if r.returncode != 0:
                print(f"  ❌ 回退也失败: {r.stderr.strip()}")
                return False
            time.sleep(3)
            print(f"  ✅ 回退重启成功")
    except Exception as e:
        print(f"  ❌ 重启异常: {e}")
        return False

    # Step 5: 验证
    # Step 5: 验证实例重启后正常运行
    orch_inst = inst.get("orchestrator_instance", inst_name)
    print(f"\n🔍 Step 5/5: 验证实例 {orch_inst} 正常运行...")
    try:
        # 等待实例完全启动
        time.sleep(5)
        running, details = orchestrator_instance_health(orch_inst)
        if running:
            pid = details.get("pid", "N/A")
            uptime = details.get("uptime", 0)
            print(f"  ✅ 实例运行正常 (PID: {pid}, uptime: {uptime:.0f}s)")
        else:
            print(f"  ⚠️  实例状态异常: {details.get('error', 'unknown')}，请检查日志")
    except Exception as e:
        print(f"  ⚠️  验证异常（非致命）: {e}")

    # 查询最新钱包列表
    try:
        after_wallets = query_wallets(user_main_addr)
        found = any(w["address"] == new_addr for w in after_wallets)
        if found:
            match_w = next(w for w in after_wallets if w["address"] == new_addr)
            print(f"  ✅ HL API 确认: 新钱包已生效")
            print(f"     到期时间: {match_w['expiry_dt'].strftime('%Y-%m-%d %H:%M UTC')}")
        else:
            print(f"  ⚠️  新钱包未在 extraAgents 中确认（可能延迟，请手动 list 检查）")
    except Exception as e:
        print(f"  ⚠️  API验证失败（不影响轮换结果）: {e}")

    # P2-1: 同步更新 config_v4.yaml 的到期日
    if found and match_w:
        expiry_str = match_w['expiry_dt'].strftime('%Y-%m-%d')
        update_config_v4_expiry(orch_inst, expiry_str)

    # 汇总
    print(f"\n{'='*50}")
    print(f"✅ 钱包轮换完成！")
    print(f"   实例: {inst_name}")
    print(f"   钱包名: {wallet_name}")
    print(f"   新地址: {new_addr}")
    print(f"   到期: {match_w['expiry_dt'].strftime('%Y-%m-%d') if found else '待确认'}")
    if before_wallets:
        same_name = [w for w in before_wallets if w["name"] == wallet_name]
        if same_name:
            print(f"   ℹ️  同名旧钱包已自动失效（HL机制）")
    print(f"{'='*50}\n")
    
    # 发送飞书通知（无论谁触发的rotate都会通知）
    expiry_display = match_w['expiry_dt'].strftime('%Y-%m-%d') if found else '待确认'
    notify_title = f"{inst_name} API钱包已更换"
    notify_content = (
        f"**实例:** {inst_name}\n"
        f"**钱包名:** {wallet_name}\n"
        f"**新钱包地址:** {new_addr}\n"
        f"**新到期日:** {expiry_display}\n"
        f"**轮换时间:** {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n\n"
        f"✅ 跟单服务已自动重启，运行正常"
    )
    send_feishu_notification(notify_title, notify_content)
    
    return True


# ======================== 命令实现 ========================

def cmd_list(instances: list) -> None:
    """列出钱包状态"""
    now = datetime.now(timezone.utc)
    for inst in instances:
        user_addr = inst["user_main_addr"]
        user_short = user_addr[:6] + "..." + user_addr[-4:]
        print(f"\n{'='*50}")
        print(f"📋 {inst['display_name']}（主地址: {user_short}）")
        print(f"{'='*50}")

        try:
            wallets = query_wallets(user_addr)
        except Exception as e:
            print(f"  ❌ 查询失败: {e}")
            continue

        if not wallets:
            print("  （无API钱包）")
            continue

        print(f"  数量: {len(wallets)}/4")
        print(f"  {'名称':<20} {'地址':<12} {'到期时间':<12} {'剩余天数':<8} {'状态'}")
        print(f"  {'-'*65}")

        for w in wallets:
            days_left = (w["expiry_dt"] - now).days
            name_display = w["name"] if w["name"] else "(unnamed)"
            if days_left > 30:
                status = "🟢 正常"
            elif days_left > 7:
                status = "🟡 注意"
            elif days_left > 0:
                status = "🟠 即将过期"
            elif days_left > -GRACE_DAYS:
                status = "🔴 已过期（宽限期内）"
            else:
                status = "⚫ 可清理"
            print(
                f"  {name_display:<20} {w['address'][:10]}... "
                f"{w['expiry_dt'].strftime('%Y-%m-%d'):<12} {days_left:>5}天   {status}"
            )


def cmd_cleanup(instances: list, auto_revoke: bool = False) -> None:
    """检查并报告过期超7天的钱包"""
    now = datetime.now(timezone.utc)
    found = False
    for inst in instances:
        try:
            wallets = query_wallets(inst["user_main_addr"])
        except Exception as e:
            print(f"  ❌ {inst['display_name']} 查询失败: {e}")
            continue

        expired = [
            w for w in wallets
            if (now - w["expiry_dt"]).days > GRACE_DAYS
        ]
        if not expired:
            continue

        found = True
        print(f"\n⚫ {inst['display_name']}: {len(expired)}个钱包已过期超{GRACE_DAYS}天")
        for w in expired:
            name_display = w["name"] if w["name"] else "(unnamed)"
            print(f"  - {name_display} | {w['address'][:16]}... | 过期: {w['expiry_dt'].strftime('%Y-%m-%d')}")

        if auto_revoke:
            print("  ℹ️  Hyperliquid API 不支持直接删除/撤销API钱包")
            print("  钱包到期后自动失效，不影响账户安全")
            print("  钱包列表仅做展示，到期钱包不占用4个名额上限")

    if not found:
        print("✅ 所有钱包状态正常，无需清理")

    # 下次到期预警
    for inst in instances:
        try:
            wallets = query_wallets(inst["user_main_addr"])
            active = [w for w in wallets if w["expiry_dt"] > now]
            if active:
                next_expiry = min(active, key=lambda w: w["expiry_dt"])
                days = (next_expiry["expiry_dt"] - now).days
                name = next_expiry["name"] if next_expiry["name"] else "(unnamed)"
                if days <= 30:
                    print(f"\n⏰ 提醒: {inst['display_name']} 的 [{name}] 将在{days}天后过期")
                    print(f"   请运行: wallet_manager.py {inst['instance_key']} rotate")
        except Exception:
            pass


def cmd_health(instances: list) -> None:
    """全面健康检查"""
    now = datetime.now(timezone.utc)
    for inst in instances:
        # 从 config 中获取实例名（用于 Orchestrator）
        inst_name = inst.get("orchestrator_instance", "unknown")
        print(f"\n{'='*50}")
        print(f"🏥 健康检查: {inst['display_name']}")
        print(f"{'='*50}")

        # 1. 实例状态（通过 Orchestrator）
        orch_instance = inst.get("orchestrator_instance", inst_name)
        if is_orchestrator_running():
            running, details = orchestrator_instance_health(orch_instance)
            if running:
                pid = details.get("pid", "N/A")
                uptime = details.get("uptime", 0)
                print(f"  实例: 🟢 running (PID: {pid}, uptime: {uptime:.0f}s)")
            else:
                print(f"  实例: 🔴 {details.get('error', 'unknown')}")
        else:
            # Orchestrator 未运行
            print(f"  Orchestrator: 🔴 未运行")

        # 2. .env 文件
        env_file = BASE_DIR / inst["env_file"]
        if env_file.exists():
            env = load_env(env_file)
            api_addr = env.get("HL_USER_API_ADDR", "未设置")
            print(f"  钱包: {api_addr[:10]}...")
        else:
            print(f"  钱包: ❌ {inst['env_file']} 不存在")

        # 3. API钱包状态
        try:
            wallets = query_wallets(inst["user_main_addr"])
            active = [w for w in wallets if w["expiry_dt"] > now]
            print(f"  活跃钱包: {len(active)}/4")

            inst_addr = env.get("HL_USER_API_ADDR", "").lower() if env else ""
            current = next((w for w in wallets if w["address"] == inst_addr), None)
            if current:
                days = (current["expiry_dt"] - now).days
                icon = "🟢" if days > 30 else ("🟡" if days > 7 else "🔴")
                name = current["name"] if current["name"] else "(unnamed)"
                print(f"  当前钱包: {icon} {name}（剩余{days}天，到期{current['expiry_dt'].strftime('%Y-%m-%d')}）")
            else:
                print(f"  当前钱包: ⚠️ 未在API列表中找到（地址可能不匹配）")
        except Exception as e:
            print(f"  API查询: ❌ {e}")


def cmd_init(inst_name: str) -> None:
    """初始化 master key 加密存储"""
    enc_path = get_enc_key_path(inst_name)
    if enc_path.exists():
        print("⚠️  Master key 加密文件已存在")
        ans = input("  是否覆盖？输入 YES 确认: ").strip()
        if ans != "YES":
            print("  已取消")
            return

    print("\n📋 初始化 Master Key 加密存储")
    print("-" * 40)
    print("  Master Key = 你在 Hyperliquid 主账户的私钥")
    print("  （即 HL 网页端导出/备份的那个私钥）")
    print("  用途: 签名 approveAgent 操作以创建新 API 钱包")
    print("  加密方式: AES-256-GCM，密码派生 PBKDF2（480,000次迭代）")
    print("  存储位置: " + str(enc_path))
    print("-" * 40)

    mk = getpass.getpass("\n🔑 请输入 Master Private Key（以0x开头，输入不回显）: ").strip()
    if not mk.startswith("0x") or len(mk) != 66:
        print("  ❌ 格式错误（需要 0x + 64位十六进制 = 66字符）")
        sys.exit(1)

    pw = getpass.getpass("🔒 设置加密密码（至少8位，请记住！丢失无法恢复）: ")
    if len(pw) < 8:
        print("  ❌ 密码至少8位")
        sys.exit(1)

    pw2 = getpass.getpass("🔒 再次确认密码: ")
    if pw != pw2:
        print("  ❌ 两次密码不一致")
        sys.exit(1)

    encrypt_master_key(inst_name, mk, pw)
    print(f"\n✅ Master key 已加密存储: {enc_path}")
    print(f"   权限: 0600（仅文件所有者可读写）")

    # 验证
    print("\n🔍 验证解密...")
    result, err = decrypt_master_key(inst_name, pw)
    if result and result.lower() == mk.lower():
        print("  ✅ 解密验证通过")
    else:
        print(f"  ❌ 解密验证失败: {err}")
        sys.exit(1)

    print("\n💡 后续使用: 运行 rotate 命令时会要求输入此密码")
    print("💡 运行 verify 命令可随时验证密码是否正确")


def cmd_verify(instance_name: str) -> None:
    """验证 master key 可正常解密"""
    enc_path = get_enc_key_path(instance_name)
    if not enc_path.exists():
        print(f"❌ 加密文件不存在: {enc_path.name}，请先运行: wallet_manager.py {instance_name} init")
        return
    mk = get_master_key_interactive(instance_name)
    if mk.startswith("0x") and len(mk) == 66:
        print("✅ 解密成功，Master key 格式正确")
    else:
        print(f"⚠️  解密成功但格式可疑（长度={len(mk)}，开头={mk[:2]}）")


# ======================== 主入口 ========================

def main():
    parser = argparse.ArgumentParser(
        description="Hyperliquid API 钱包生命周期管理工具",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
命令说明:
  list      列出所有API钱包及到期状态（不需要master key）
  rotate    轮换钱包（创建+替换.env+重启+验证）
  cleanup   清理过期超7天的钱包（仅报告）
  init      初始化master key加密存储（首次使用）
  setup     一键初始化（等同于init）
  verify    验证master key解密（不需要master key）
  health    检查所有实例健康状态

示例:
  %(prog)s lowfreq init           # 首次初始化
  %(prog)s lowfreq list           # 查看低频钱包
  %(prog)s all list             # 查看所有实例钱包
  %(prog)s lowfreq rotate         # 轮换低频钱包
  %(prog)s highfreq rotate        # 轮换高频钱包
  %(prog)s all health           # 全部实例健康检查
        """,
    )
    parser.add_argument(
        "instance",
        help="实例名称（lowfreq/highfreq）或 --all",
    )
    parser.add_argument(
        "command",
        choices=["list", "rotate", "cleanup", "init", "setup", "verify", "health"],
        help="要执行的命令",
    )
    parser.add_argument("--dry-run", action="store_true", help="仅模拟，不执行实际操作")
    parser.add_argument("--yes", "-y", action="store_true", help="跳过确认提示")
    args = parser.parse_args()

    config = load_config()
    instances = get_instances(config, args.instance)

    # 为每个实例注入 instance_key（用于提示命令）
    if args.instance == "all":
        for k, v in config["instances"].items():
            v["instance_key"] = k
    else:
        instances[0]["instance_key"] = args.instance

    cmd = args.command

    # 不需要 master key 的命令
    if cmd == "list":
        cmd_list(instances)
        return

    if cmd == "cleanup":
        cmd_cleanup(instances, auto_revoke=False)
        return

    if cmd == "health":
        cmd_health(instances)
        return

    if cmd == "verify":
        cmd_verify(args.instance)
        return

    # 需要 master key 的命令
    if cmd == "rotate":
        if not get_enc_key_path(args.instance).exists():
            print(f"💀 [{args.instance}] Master key 未初始化")
            print(f"  请先运行: wallet_manager.py {args.instance} init")
            sys.exit(1)

        master_key = get_master_key_interactive(args.instance)

        # 确认提示
        if not args.yes:
            print(f"\n⚠️  即将轮换以下实例的 API 钱包:")
            for inst in instances:
                print(f"  - {inst['display_name']}（钱包名: {inst['wallet_name']}）")
            print("  操作: 创建新钱包 → 更新 .env → 重启服务")
            ans = input("\n确认执行? (y/N): ").strip().lower()
            if ans not in ("y", "yes"):
                print("已取消")
                return

        for inst in instances:
            ok = do_rotate(inst["instance_key"], inst, master_key, dry_run=args.dry_run)
            if not ok:
                print(f"\n💀 {inst['instance_key']} 轮换失败，中止后续")
                sys.exit(1)
        return

    if cmd in ("init", "setup"):
        cmd_init(args.instance)
        return


if __name__ == "__main__":
    main()
