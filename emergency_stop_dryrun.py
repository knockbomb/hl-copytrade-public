#!/usr/bin/env python3
"""
紧急制动演练脚本 (Dry Run)
模拟完整紧急制动流程，不实际调用API，零风险
"""
import sys
import os
import time
import json
import logging
from datetime import datetime

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S"
)
logger = logging.getLogger(__name__)

def load_env(env_path):
    """手动加载.env文件"""
    if not os.path.exists(env_path):
        return {}
    env_vars = {}
    with open(env_path) as f:
        for line in f:
            line = line.strip()
            if line and not line.startswith("#") and "=" in line:
                key, value = line.split("=", 1)
                env_vars[key.strip()] = value.strip()
    return env_vars

def main():
    logger.info("=" * 70)
    logger.info("🔴 紧急制动演练 (DRY RUN) - 零风险模式")
    logger.info("=" * 70)
    now_str = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    logger.info(f"演练时间: {now_str}")
    logger.info("")
    
    # 加载依赖
    try:
        from hyperliquid.info import Info
        import yaml
    except ImportError as e:
        logger.error(f"导入依赖失败: {e}")
        return False
    
    # 读取配置
    config_path = os.path.expanduser("~/hl_copytrade/config_v3.yaml")
    env_path = os.path.expanduser("~/hl_copytrade/.env")
    
    if not os.path.exists(config_path):
        logger.error(f"配置文件不存在: {config_path}")
        return False
    
    # 加载环境变量
    env_vars = load_env(env_path)
    for key, value in env_vars.items():
        os.environ[key] = value
    
    with open(config_path) as f:
        config = yaml.safe_load(f)
    
    # 初始化API
    base_url = config.get("api", {}).get("base_url", "https://api.hyperliquid.xyz")
    user_addr = os.environ.get(config["account"]["user_main_addr_env"], "")
    
    if not user_addr:
        logger.error("未配置用户地址")
        return False
    
    logger.info(f"用户地址: {user_addr}")
    logger.info(f"API地址: {base_url}")
    logger.info("")
    
    info = Info(base_url, skip_ws=True)
    
    start_time = time.time()
    
    # 步骤1: 获取当前状态
    logger.info("📊 步骤1: 获取当前状态")
    logger.info("-" * 70)
    
    try:
        # 获取持仓
        state = info.user_state(user_addr)
        positions = {}
        for pos in state.get("assetPositions", []):
            szi = float(pos["position"]["szi"])
            coin = pos["position"]["coin"]
            if szi != 0:
                positions[coin] = {
                    "szi": szi,
                    "side": "多" if szi > 0 else "空"
                }
        
        logger.info(f"当前持仓: {len(positions)} 个")
        for coin, pos in sorted(positions.items()):
            logger.info(f"  {coin}: {pos["side"]} {abs(pos["szi"])}")
        
        # 获取挂单
        open_orders = info.open_orders(user_addr)
        logger.info(f"当前挂单: {len(open_orders)} 个")
        for order in open_orders[:10]:
            coin = order.get("coin", "")
            oid = order.get("oid", "")
            sz = order.get("sz", "")
            side = "买入" if order.get("side") == "B" else "卖出"
            logger.info(f"  {coin}: oid={oid} {side} {sz}")
        if len(open_orders) > 10:
            more = len(open_orders) - 10
            logger.info(f"  ... 还有 {more} 个挂单")
        
    except Exception as e:
        logger.error(f"获取状态失败: {e}")
        import traceback
        traceback.print_exc()
        return False
    
    logger.info("")
    
    # 步骤2: 模拟撤销所有挂单
    logger.info("📋 步骤2: 模拟撤销所有挂单 (DRY RUN)")
    logger.info("-" * 70)
    cancel_count = 0
    for order in open_orders:
        coin = order.get("coin", "")
        oid = order.get("oid", "")
        logger.info(f"  [DRY RUN] 撤销 {coin} oid={oid}")
        cancel_count += 1
        time.sleep(0.01)
    
    logger.info(f"将撤销 {cancel_count} 个挂单")
    logger.info("")
    
    # 步骤3: 模拟平仓
    logger.info("📋 步骤3: 模拟市价平仓 (DRY RUN)")
    logger.info("-" * 70)
    close_count = 0
    for coin, pos in sorted(positions.items()):
        szi = pos["szi"]
        action = "卖出平多" if szi > 0 else "买入平空"
        logger.info(f"  [DRY RUN] {coin}: {action} {abs(szi)}")
        close_count += 1
        time.sleep(0.01)
    
    logger.info(f"将平仓 {close_count} 个仓位")
    logger.info("")
    
    # 步骤4: 停止V3服务
    logger.info("📋 步骤4: 模拟停止V3服务 (DRY RUN)")
    logger.info("-" * 70)
    logger.info("  [DRY RUN] systemctl stop hl-copytrade-v3.service")
    logger.info("  [DRY RUN] V3服务将停止")
    logger.info("")
    
    # 总结
    elapsed = time.time() - start_time
    logger.info("=" * 70)
    logger.info("✅ 演练完成 (零风险，未实际执行任何操作)")
    logger.info("=" * 70)
    logger.info(f"演练耗时: {elapsed:.2f} 秒")
    logger.info("")
    logger.info("📊 演练摘要:")
    logger.info(f"  - 将撤销 {cancel_count} 个挂单")
    logger.info(f"  - 将平仓 {close_count} 个仓位")
    logger.info("  - 将停止 V3 服务")
    logger.info("")
    logger.info("💡 真实紧急制动执行时:")
    logger.info("  1. 创建信号文件: touch ~/hl_copytrade/EMERGENCY_STOP")
    logger.info("  2. V3检测到信号后自动执行上述操作")
    logger.info("  3. V3服务自动停止")
    logger.info("  4. 信号文件自动删除")
    logger.info("")
    
    return True

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)
