#!/usr/bin/env python3
"""
HL跟单系统 - Phase 2 监控模块
功能：
  1. API钱包过期检测 + 递进告警
  2. 保证金使用率监控 + 告警（仅告警不暂停）
  3. 启动配置校验

作者: HL CopyTrade
日期: 2026-07-25
"""

from __future__ import annotations

import json
import logging
import os
import time
from datetime import datetime, timezone, timedelta
from typing import Any, Dict, Optional, Tuple

import requests


# ============================================================
# Phase 2 新增告警类型（补充到 ALERT_LEVEL_MAP）
# ============================================================
PHASE2_ALERT_MAP: Dict[str, str] = {
    "WALLET_EXPIRY_URGENT": "WARNING",
    "MARGIN_WARNING": "WARNING",
    "CONFIG_VALIDATION_FAIL": "WARNING",
}


class Phase2Monitor:
    """Phase 2 监控器"""
    
    WALLET_THRESHOLDS = [
        (30, "INFO", "WALLET_EXPIRY_REMINDER"),
        (14, "WARNING", "WALLET_EXPIRY_URGENT"),
        (7, "WARNING", "WALLET_EXPIRY_URGENT"),
        (3, "CRITICAL", "WALLET_EXPIRED"),
        (1, "CRITICAL", "WALLET_EXPIRED"),
    ]
    
    MARGIN_WARNING_THRESHOLD = 1.50
    MARGIN_CRITICAL_THRESHOLD = 1.80
    
    def __init__(self, config, logger, alert_manager=None):
        self._config = config
        self._logger = logger
        self._alert_manager = alert_manager
        
        self._wallet_check_interval = 6 * 3600  # 6小时
        self._last_wallet_check: float = 0
        self._wallet_status = "valid"  # valid / expired / warning / unknown
        
        self._last_margin_alert_time: float = 0
        self._last_margin_alert_level: str = ""
        self._margin_alert_interval = 4 * 3600  # 4小时
        self._margin_check_interval = 1800  # 30分钟检查一次
        
        self._last_margin_check: float = 0
        
        self._api_wallet_addr = ""
        self._main_wallet_addr = ""
        self._wallet_expiry_ts: Optional[float] = None
        self._wallet_name: str = ""
        
        self._startup_validated: bool = False
        self._logger.info("[Phase2] 监控器初始化完成")
    
    def _send_alert(self, level: str, title: str, detail: str, alert_key: str = "") -> bool:
        if self._alert_manager and self._alert_manager.enabled:
            return self._alert_manager.send(level, title, detail, alert_key=alert_key or f"{level}:{title}")
        return False
    
    # ============================================================
    # 1. API钱包过期检测
    # ============================================================
    
    def query_api_wallet_expiry(self) -> Tuple[Optional[float], str, str]:
        """通过extraAgents API查询API钱包过期时间"""
        try:
            main_addr = self._main_wallet_addr or os.environ.get("HL_USER_MAIN_ADDR", "")
            if not main_addr:
                self._logger.warning("[Phase2] 用户主钱包地址为空")
                return None, "", ""
            
            resp = requests.post(
                f"{self._config.base_url}/info",
                json={"type": "extraAgents", "user": main_addr},
                timeout=self._config.api_timeout,
            )
            resp.raise_for_status()
            agents = resp.json()
            
            api_addr = (self._api_wallet_addr or os.environ.get("HL_USER_API_ADDR", "")).lower()
            if not api_addr:
                self._logger.warning("[Phase2] API钱包地址为空")
                return None, "", ""
            
            for agent in agents:
                if agent.get("address", "").lower() == api_addr:
                    valid_until_ms = agent.get("validUntil", 0)
                    name = agent.get("name", "unknown")
                    expiry_ts = valid_until_ms / 1000.0
                    self._wallet_expiry_ts = expiry_ts
                    self._wallet_name = name
                    expiry_bj = datetime.fromtimestamp(expiry_ts, tz=timezone.utc).astimezone(timezone(timedelta(hours=8)))
                    self._logger.info(f"[Phase2] API钱包 '{name}' 过期时间: {expiry_bj.strftime('%Y-%m-%d %H:%M')} 北京时间")
                    return expiry_ts, name, agent.get("address", "")
            
            self._logger.warning(f"[Phase2] extraAgents中未找到API钱包 {api_addr[:10]}...")
            return None, "", ""
            
        except Exception as e:
            self._logger.warning(f"[Phase2] 查询API钱包过期时间失败: {e}")
            return None, "", ""
    
    def check_wallet_expiry(self, force: bool = False) -> str:
        """检查API钱包过期时间并递进告警"""
        now = time.time()
        if not force and (now - self._last_wallet_check) < self._wallet_check_interval:
            return
        
        self._last_wallet_check = now
        
        expiry_ts, name, addr = self.query_api_wallet_expiry()
        if expiry_ts is None:
            self._logger.warning("[Phase2] 无法获取API钱包过期时间")
            self._wallet_status = "unknown"
            return "unknown"
        
        now_dt = datetime.now(tz=timezone.utc)
        expiry_dt = datetime.fromtimestamp(expiry_ts, tz=timezone.utc)
        remaining = expiry_dt - now_dt
        remaining_days = remaining.total_seconds() / 86400
        remaining_hours = remaining.total_seconds() / 3600
        
        expiry_bj = expiry_dt.astimezone(timezone(timedelta(hours=8)))
        
        if remaining_days < 0:
            self._wallet_status = "expired"
            self._logger.critical(f"[Phase2] API钱包 '{name}' 已过期！")
            self._send_alert(
                "CRITICAL",
                "API钱包已过期",
                f"**钱包名称:** {name}\n"
                f"**钱包地址:** {addr[:10]}...\n"
                f"**过期时间:** {expiry_bj.strftime('%Y-%m-%d %H:%M')} (北京时间)\n"
                f"**状态:** 已过期，跟单已自动暂停\n"
                f"**操作:** 请更新API钱包，系统将自动恢复跟单",
                alert_key="CRITICAL:API钱包已过期",
            )
            return "expired"
        
        for threshold_days, level, alert_type in self.WALLET_THRESHOLDS:
            if remaining_days <= threshold_days:
                if remaining_days <= 1:
                    title = "API钱包即将过期（紧急）"
                    detail = (
                        f"**钱包名称:** {name}\n"
                        f"**过期时间:** {expiry_bj.strftime('%Y-%m-%d %H:%M')} (北京时间)\n"
                        f"**剩余:** {remaining_hours:.1f}小时\n"
                        f"**操作:** 请立即更新API钱包！"
                    )
                elif remaining_days <= 3:
                    title = "API钱包即将过期（紧急）"
                    detail = (
                        f"**钱包名称:** {name}\n"
                        f"**过期时间:** {expiry_bj.strftime('%Y-%m-%d %H:%M')} (北京时间)\n"
                        f"**剩余:** {remaining_days:.1f}天\n"
                        f"**操作:** 请尽快更新API钱包"
                    )
                elif remaining_days <= 7:
                    title = "API钱包即将过期"
                    detail = (
                        f"**钱包名称:** {name}\n"
                        f"**过期时间:** {expiry_bj.strftime('%Y-%m-%d %H:%M')} (北京时间)\n"
                        f"**剩余:** {remaining_days:.1f}天\n"
                        f"**建议:** 请准备更新API钱包"
                    )
                else:
                    title = "API钱包过期提醒"
                    detail = (
                        f"**钱包名称:** {name}\n"
                        f"**过期时间:** {expiry_bj.strftime('%Y-%m-%d %H:%M')} (北京时间)\n"
                        f"**剩余:** {remaining_days:.0f}天"
                    )
                
                self._send_alert(
                    level, title, detail,
                    alert_key=f"{level}:{alert_type}:{threshold_days}d",
                )
                self._wallet_status = "warning"
                self._logger.info(f"[Phase2] 钱包过期告警: {level} (剩余{remaining_days:.1f}天, 阈值{threshold_days}天)")
                return "warning"
                break
        else:
            self._wallet_status = "valid"
            self._logger.info(f"[Phase2] API钱包状态正常: 剩余{remaining_days:.0f}天")
            return "valid"
    
    # ============================================================

    def detect_wallet_rotation(self, current_wallet_addr: str) -> bool:
        """检测是否发生了钱包轮换，返回True表示需要重建TPSL"""
        if not hasattr(self, "_last_known_wallet"):
            self._last_known_wallet = current_wallet_addr
            return False
        if current_wallet_addr != self._last_known_wallet:
            self._logger.warning(f"[Phase2] 检测到钱包轮换: {self._last_known_wallet[:10]}... -> {current_wallet_addr[:10]}...")
            self._last_known_wallet = current_wallet_addr
            return True
        return False

    # 2. 保证金使用率监控
    # ============================================================
    
    def check_margin_usage(self, poll_count: int = 0, cached_user_value: float = None, cached_margin_used: float = None) -> None:
        """检查用户保证金使用率（直接调用API）"""
        now = time.time()
        if (now - self._last_margin_check) < self._margin_check_interval:
            return
        self._last_margin_check = now
        
        try:
            user_addr = self._main_wallet_addr or os.environ.get("HL_USER_MAIN_ADDR", "")
            if not user_addr:
                return
            
            # P2优化: 优先使用poll缓存数据，避免重复API调用
            if cached_user_value is not None and cached_margin_used is not None:
                account_value = cached_user_value
                total_margin_used = cached_margin_used
                self._logger.debug("[Phase2] 使用poll缓存数据计算保证金使用率")
            else:
                perp_data = requests.post(
                    f"{self._config.base_url}/info",
                    json={"type": "clearinghouseState", "user": user_addr},
                    timeout=self._config.api_timeout,
                )
                perp_data.raise_for_status()
                data = perp_data.json()
                margin_summary = data.get("marginSummary", {})
                account_value = float(margin_summary.get("accountValue", 0))
                total_margin_used = float(margin_summary.get("totalMarginUsed", 0))
            
            if account_value <= 0:
                return
            
            margin_ratio = total_margin_used / account_value
            
            
            
            if poll_count % 10 == 0:
                self._logger.info(
                    f"[Phase2] 保证金使用率: {margin_ratio:.1%} "
                    f"(used=${total_margin_used:.2f}, value=${account_value:.2f})"
                )
            
            alert_level = None
            if margin_ratio >= self.MARGIN_CRITICAL_THRESHOLD:
                alert_level = "CRITICAL"
                title = "保证金使用率超过180%（紧急）"
            elif margin_ratio >= self.MARGIN_WARNING_THRESHOLD:
                alert_level = "WARNING"
                title = "保证金使用率超过150%（警告）"
            
            if alert_level:
                if (alert_level != self._last_margin_alert_level or
                    now - self._last_margin_alert_time > self._margin_alert_interval):
                    self._last_margin_alert_time = now
                    self._last_margin_alert_level = alert_level
                    
                    self._send_alert(
                        alert_level, title,
                        f"**保证金使用率:** {margin_ratio:.1%}\n"
                        f"**已用保证金:** ${total_margin_used:.2f}\n"
                        f"**账户净值:** ${account_value:.2f}\n"
                        f"**说明:** 仅告警，不自动暂停。如需降低风险请手动减仓或追加保证金。",
                        alert_key=f"{alert_level}:保证金使用率",
                    )
                    self._logger.warning(f"[Phase2] 保证金告警: {alert_level} (使用率={margin_ratio:.1%})")
            else:
                self._last_margin_alert_level = ""
                
        except Exception as e:
            self._logger.warning(f"[Phase2] 保证金检查异常: {e}")
    
    # ============================================================
    # 3. 启动配置校验
    # ============================================================
    
    def validate_startup_config(self) -> bool:
        """启动时校验关键配置"""
        issues = []
        warnings = []
        
        # 1. 检查.env文件
        env_path = os.path.expanduser(self._config._get("paths", "env_file", default="~/hl_copytrade/.env"))
        if not os.path.exists(env_path):
            issues.append(f".env文件不存在: {env_path}")
        else:
            try:
                env_vars = {}
                with open(env_path, 'r') as f:
                    for line in f:
                        line = line.strip()
                        if line and not line.startswith('#') and '=' in line:
                            key, val = line.split('=', 1)
                            env_vars[key.strip()] = val.strip()
                
                required_keys = ["HL_API_PK", "HL_LEADER_ADDR", "HL_USER_MAIN_ADDR", "HL_USER_API_ADDR"]
                for key in required_keys:
                    if key not in env_vars or not env_vars[key]:
                        issues.append(f"环境变量 {key} 缺失或为空")
                
                self._api_wallet_addr = env_vars.get("HL_USER_API_ADDR", "").strip()
                self._main_wallet_addr = env_vars.get("HL_USER_MAIN_ADDR", "").strip()
                
            except Exception as e:
                warnings.append(f".env文件读取异常: {e}")
        
        # 2. 检查Leader地址
        if not self._config.leader_addr:
            issues.append("Leader地址为空")
        
        # 3. 检查跟单比例
        fund_ratio = self._config.fund_ratio
        if fund_ratio <= 0:
            warnings.append(f"跟单比例 {fund_ratio} <= 0，不会开仓")
        elif fund_ratio > 1:
            warnings.append(f"跟单比例 {fund_ratio} > 1，请确认是否正确")
        
        # 4. 检查API钱包
        expiry_ts, name, addr = self.query_api_wallet_expiry()
        if expiry_ts is not None:
            expiry_dt = datetime.fromtimestamp(expiry_ts, tz=timezone.utc)
            now_dt = datetime.now(tz=timezone.utc)
            remaining_days = (expiry_dt - now_dt).total_seconds() / 86400
            expiry_bj = expiry_dt.astimezone(timezone(timedelta(hours=8)))
            
            if remaining_days < 0:
                issues.append(f"API钱包 '{name}' 已过期 ({expiry_bj.strftime('%Y-%m-%d')})")
            elif remaining_days < 7:
                warnings.append(f"API钱包 '{name}' 将在 {remaining_days:.0f} 天后过期 ({expiry_bj.strftime('%Y-%m-%d')})")
            else:
                self._logger.info(f"[Phase2] API钱包校验通过: '{name}' 有效至 {expiry_bj.strftime('%Y-%m-%d')}")
        else:
            warnings.append("无法查询API钱包过期时间")
        
        # 5. 检查config.yaml参数
        if self._config.api_timeout < 5:
            warnings.append(f"API超时 {self._config.api_timeout}s 过短")
        if self._config.max_retries < 1:
            warnings.append(f"API重试次数 {self._config.max_retries} < 1")
        
        # 输出
        self._logger.info("[Phase2] ===== 启动配置校验 =====")
        if issues:
            for i in issues:
                self._logger.critical(f"  [Phase2] ❌ {i}")
        if warnings:
            for w in warnings:
                self._logger.warning(f"  [Phase2] ⚠️ {w}")
        if not issues and not warnings:
            self._logger.info("  [Phase2] ✅ 所有配置校验通过")
        self._logger.info("[Phase2] =========================")
        
        if issues:
            self._send_alert(
                "CRITICAL", "启动配置校验失败",
                "**严重问题:**\n" + "\n".join(f"- {i}" for i in issues) +
                ("\n\n**警告:**\n" + "\n".join(f"- {w}" for w in warnings) if warnings else ""),
                alert_key="CRITICAL:启动配置校验失败",
            )
        elif warnings:
            self._send_alert(
                "WARNING", "启动配置校验警告",
                "**警告项:**\n" + "\n".join(f"- {w}" for w in warnings),
                alert_key="WARNING:启动配置校验警告",
            )
        
        self._startup_validated = True
        return len(issues) == 0
