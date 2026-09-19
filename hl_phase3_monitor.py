#!/usr/bin/env python3
"""
HL跟单系统 - Phase 3 增强检测模块
功能：
  1. Leader地址变更检测
  2. VPS资源监控（内层自检：CPU/内存/磁盘）
  3. 健康检查端点集成自检
  4. WS长期断连告警
  5. 跟单比例波动告警

作者: HL CopyTrade
日期: 2026-07-25
"""

from __future__ import annotations

import json
import logging
import os
import platform
import time
from datetime import datetime, timezone, timedelta
from pathlib import Path
from typing import Any, Dict, Optional

import requests


# ============================================================
# Phase 3 新增告警类型
# ============================================================
PHASE3_ALERT_MAP: Dict[str, str] = {
    "LEADER_ADDRESS_CHANGED": "CRITICAL",
    "VPS_CPU_CRITICAL": "CRITICAL",
    "VPS_MEMORY_CRITICAL": "CRITICAL",
    "VPS_DISK_CRITICAL": "CRITICAL",
    "VPS_CPU_WARNING": "WARNING",
    "VPS_MEMORY_WARNING": "WARNING",
    "VPS_DISK_WARNING": "WARNING",
    "HEALTH_ENDPOINT_UNHEALTHY": "CRITICAL",
    "WS_LONG_DISCONNECT": "WARNING",
    "WS_LONG_DISCONNECT_CRITICAL": "CRITICAL",
    "COPY_RATIO_DRIFT": "WARNING",
    "COPY_RATIO_DRIFT_CRITICAL": "CRITICAL",
}


class Phase3Monitor:
    """Phase 3 增强检测监控器"""

    # Leader地址变更检测间隔：10分钟
    LEADER_ADDR_CHECK_INTERVAL = 600

    # VPS资源检查间隔：5分钟
    RESOURCE_CHECK_INTERVAL = 300

    # 健康端点检查间隔：3分钟
    HEALTH_CHECK_INTERVAL = 180

    # WS断连告警阈值
    WS_DISCONNECT_WARNING_SECONDS = 300    # 5分钟断连 → WARNING
    WS_DISCONNECT_CRITICAL_SECONDS = 1800  # 30分钟断连 → CRITICAL

    # 跟单比例波动告警阈值
    COPY_RATIO_DRIFT_WARNING_PCT = 0.15    # 偏离15% → WARNING
    COPY_RATIO_DRIFT_CRITICAL_PCT = 0.30   # 偏离30% → CRITICAL
    COPY_RATIO_SAMPLE_COUNT = 10           # 用最近N个采样计算均值
    COPY_RATIO_CHECK_INTERVAL = 300        # 5分钟检查一次

    # VPS资源阈值
    CPU_WARNING_THRESHOLD = 85.0       # CPU使用率 > 85% → WARNING
    CPU_CRITICAL_THRESHOLD = 95.0      # CPU使用率 > 95% → CRITICAL
    MEMORY_WARNING_THRESHOLD = 85.0    # 内存使用率 > 85% → WARNING
    MEMORY_CRITICAL_THRESHOLD = 95.0   # 内存使用率 > 95% → CRITICAL
    DISK_WARNING_THRESHOLD = 85.0      # 磁盘使用率 > 85% → WARNING
    DISK_CRITICAL_THRESHOLD = 95.0     # 磁盘使用率 > 95% → CRITICAL

    def __init__(self, config, logger, alert_manager=None):
        self._config = config
        self._logger = logger
        self._alert_manager = alert_manager

        # Leader地址变更检测
        self._last_leader_addr_check: float = 0
        self._configured_leader_addr: str = ""
        self._leader_addr_changed: bool = False

        # VPS资源监控
        self._last_resource_check: float = 0
        self._last_resource_alert_time: Dict[str, float] = {}
        self._resource_alert_interval = 43200  # 同级别告警12小时去重

        # 健康端点自检
        self._last_health_check: float = 0
        self._consecutive_health_failures: int = 0
        self._health_check_threshold = 3  # 连续3次失败才告警

        # WS长期断连
        self._ws_disconnect_start: Optional[float] = None
        self._last_ws_alert_level: str = ""
        self._last_was_connected: bool = True  # 初始假设连接

        # 跟单比例波动
        self._copy_ratio_samples: list = []
        self._last_ratio_check: float = 0
        self._last_ratio_alert_time: float = 0
        self._last_ratio_alert_level: str = ""

        self._logger.info("[Phase3] 增强检测监控器初始化完成")

    def _send_alert(self, level: str, title: str, detail: str, alert_key: str = "") -> bool:
        """发送告警"""
        if self._alert_manager and self._alert_manager.enabled:
            return self._alert_manager.send(level, title, detail, alert_key=alert_key or f"{level}:{title}")
        return False

    def _should_dedup(self, key: str) -> bool:
        """检查是否应该去重（同级别告警在去重间隔内不重复发送）"""
        now = time.time()
        last_time = self._last_resource_alert_time.get(key, 0)
        if now - last_time < self._resource_alert_interval:
            return True
        self._last_resource_alert_time[key] = now
        return False

    # ============================================================
    # 1. Leader地址变更检测
    # ============================================================

    def check_leader_address_change(self) -> None:
        """检查Leader地址是否发生变更（通过重新读取.env文件）"""
        now = time.time()
        if (now - self._last_leader_addr_check) < self.LEADER_ADDR_CHECK_INTERVAL:
            return
        self._last_leader_addr_check = now

        try:
            # 重新读取.env文件获取当前配置的Leader地址
            env_path = os.path.expanduser(
                self._config._get("paths", "env_file", default="~/hl_copytrade/.env")
            )
            if not os.path.exists(env_path):
                return

            env_vars = {}
            with open(env_path, 'r') as f:
                for line in f:
                    line = line.strip()
                    if line and not line.startswith('#') and '=' in line:
                        key, val = line.split('=', 1)
                        env_vars[key.strip()] = val.strip()

            current_leader = env_vars.get("HL_LEADER_ADDR", "").strip()
            if not current_leader:
                return

            # 首次记录
            if not self._configured_leader_addr:
                self._configured_leader_addr = current_leader
                self._logger.info(f"[Phase3] Leader地址已记录: {current_leader[:10]}...")
                return

            # 检测变更
            if current_leader != self._configured_leader_addr:
                if not self._leader_addr_changed:
                    self._leader_addr_changed = True
                    self._logger.critical(
                        f"[Phase3] Leader地址变更检测: "
                        f"{self._configured_leader_addr[:10]}... → {current_leader[:10]}..."
                    )
                    self._send_alert(
                        "CRITICAL", "Leader地址变更",
                        f"**原地址:** {self._configured_leader_addr}\n"
                        f"**新地址:** {current_leader}\n"
                        f"**影响:** .env文件中的Leader地址已被修改，跟单目标可能发生变化\n"
                        f"**操作:** 请确认是否为主动修改。如果是误操作，请立即恢复原地址并重启V3服务。",
                        alert_key="CRITICAL:Leader地址变更",
                    )
        except Exception as e:
            self._logger.warning(f"[Phase3] Leader地址变更检测异常: {e}")

    def set_leader_address(self, addr: str) -> None:
        """设置初始Leader地址（启动时调用）"""
        if not self._configured_leader_addr:
            self._configured_leader_addr = addr
            self._logger.info(f"[Phase3] Leader地址已设置: {addr[:10]}...")

    # ============================================================
    # 2. VPS资源监控（内层自检）
    # ============================================================

    def _get_system_resources(self) -> Dict[str, float]:
        """获取当前系统资源使用情况"""
        result = {}

        try:
            # CPU使用率（两次采样差值法，精确计算实时CPU使用率）
            def _read_cpu_stat():
                with open('/proc/stat', 'r') as f:
                    line = f.readline()
                vals = [int(x) for x in line.split()[1:]]
                total = sum(vals)
                idle = vals[3] if len(vals) > 3 else 0
                return total, idle

            t1_total, t1_idle = _read_cpu_stat()
            import time as _time_mod
            _time_mod.sleep(0.5)  # 500ms采样间隔
            t2_total, t2_idle = _read_cpu_stat()
            d_total = t2_total - t1_total
            d_idle = t2_idle - t1_idle
            if d_total > 0:
                result['cpu_percent'] = (1.0 - d_idle / d_total) * 100.0
            else:
                result['cpu_percent'] = 0.0
        except Exception:
            result['cpu_percent'] = -1

        try:
            # 内存使用率
            with open('/proc/meminfo', 'r') as f:
                meminfo = {}
                for line in f:
                    parts = line.split(':')
                    if len(parts) == 2:
                        key = parts[0].strip()
                        val = int(parts[1].strip().split()[0])  # kB
                        meminfo[key] = val
            total_mem = meminfo.get('MemTotal', 0)
            available_mem = meminfo.get('MemAvailable', 0)
            if total_mem > 0:
                result['memory_percent'] = ((total_mem - available_mem) / total_mem) * 100.0
                result['memory_total_mb'] = total_mem / 1024
                result['memory_used_mb'] = (total_mem - available_mem) / 1024
            else:
                result['memory_percent'] = -1
        except Exception:
            result['memory_percent'] = -1

        try:
            # 磁盘使用率（根分区）
            statvfs = os.statvfs('/')
            total_disk = statvfs.f_frsize * statvfs.f_blocks
            free_disk = statvfs.f_frsize * statvfs.f_bavail
            if total_disk > 0:
                result['disk_percent'] = ((total_disk - free_disk) / total_disk) * 100.0
                result['disk_total_gb'] = total_disk / (1024**3)
                result['disk_free_gb'] = free_disk / (1024**3)
            else:
                result['disk_percent'] = -1
        except Exception:
            result['disk_percent'] = -1

        return result

    def check_vps_resources(self) -> None:
        """检查VPS系统资源"""
        now = time.time()
        if (now - self._last_resource_check) < self.RESOURCE_CHECK_INTERVAL:
            return
        self._last_resource_check = now

        resources = self._get_system_resources()

        # CPU告警已静默（健康检查端点覆盖，无需单独通知）
        cpu = resources.get('cpu_percent', -1)
        if cpu >= 0:
            if cpu >= self.CPU_CRITICAL_THRESHOLD:
                self._logger.warning(f"[Phase3] CPU使用率极高: {cpu:.1f}%（已静默，由健康检查覆盖）")
            elif cpu >= self.CPU_WARNING_THRESHOLD:
                self._logger.info(f"[Phase3] CPU使用率偏高: {cpu:.1f}%")

        # 内存告警已静默（健康检查端点会覆盖）
        mem = resources.get('memory_percent', -1)
        if mem >= 0:
            mem_total = resources.get('memory_total_mb', 0)
            mem_used = resources.get('memory_used_mb', 0)
            if mem >= self.MEMORY_CRITICAL_THRESHOLD:
                self._logger.warning(f"[Phase3] 内存使用率极高: {mem:.1f}% ({mem_used:.0f}/{mem_total:.0f}MB)（已静默）")
            elif mem >= self.MEMORY_WARNING_THRESHOLD:
                self._logger.info(f"[Phase3] 内存使用率偏高: {mem:.1f}%")

        # 磁盘
        disk = resources.get('disk_percent', -1)
        if disk >= 0:
            disk_total = resources.get('disk_total_gb', 0)
            disk_free = resources.get('disk_free_gb', 0)
            if disk >= self.DISK_CRITICAL_THRESHOLD:
                key = "disk_critical"
                if not self._should_dedup(key):
                    self._send_alert(
                        "CRITICAL", "VPS磁盘空间即将耗尽",
                        f"**磁盘使用率:** {disk:.1f}%\n"
                        f"**剩余/总计:** {disk_free:.1f}GB / {disk_total:.1f}GB\n"
                        f"**阈值:** {self.DISK_CRITICAL_THRESHOLD}%\n"
                        f"**操作:** 请立即清理磁盘空间，否则日志写入可能失败",
                        alert_key=f"CRITICAL:VPS_DISK_CRITICAL",
                    )
            elif disk >= self.DISK_WARNING_THRESHOLD:
                key = "disk_warning"
                if not self._should_dedup(key):
                    self._send_alert(
                        "WARNING", "VPS磁盘空间不足",
                        f"**磁盘使用率:** {disk:.1f}%\n"
                        f"**剩余/总计:** {disk_free:.1f}GB / {disk_total:.1f}GB\n"
                        f"**阈值:** {self.DISK_WARNING_THRESHOLD}%",
                        alert_key=f"WARNING:VPS_DISK_WARNING",
                    )

        # 周期性记录资源状态到日志
        if cpu >= 0 or mem >= 0 or disk >= 0:
            parts = []
            if cpu >= 0:
                parts.append(f"CPU={cpu:.1f}%")
            if mem >= 0:
                parts.append(f"MEM={mem:.1f}%")
            if disk >= 0:
                parts.append(f"DISK={disk:.1f}%")
            self._logger.debug(f"[Phase3] 系统资源: {' | '.join(parts)}")

    # ============================================================
    # 3. 健康检查端点集成自检
    # ============================================================

    def check_health_endpoint(self) -> None:
        """自检健康检查端点是否正常工作"""
        now = time.time()
        if (now - self._last_health_check) < self.HEALTH_CHECK_INTERVAL:
            return
        self._last_health_check = now

        try:
            health_port = self._config.health_http_port if hasattr(self._config, 'health_http_port') else 8998
            url = f"http://127.0.0.1:{health_port}/health"
            resp = requests.get(url, timeout=5)
            resp.raise_for_status()
            data = resp.json()

            status = data.get("status", "unknown")
            if status == "unhealthy":
                self._consecutive_health_failures += 1
                if self._consecutive_health_failures >= self._health_check_threshold:
                    self._send_alert(
                        "CRITICAL", "健康检查端点报告不健康",
                        f"**状态:** {status}\n"
                        f"**连续失败次数:** {self._consecutive_health_failures}\n"
                        f"**上次成功:** {data.get('last_success', '无记录')}\n"
                        f"**最后错误:** {data.get('last_error', '未知')}\n"
                        f"**操作:** 请检查V3服务运行状态和API连接",
                        alert_key="CRITICAL:HEALTH_ENDPOINT_UNHEALTHY",
                    )
            elif status == "degraded":
                self._consecutive_health_failures += 1
                if self._consecutive_health_failures >= self._health_check_threshold * 2:
                    self._logger.warning(f"[Phase3] 健康检查持续降级: {status} (连续{self._consecutive_health_failures}次)")
            else:
                if self._consecutive_health_failures > 0:
                    self._logger.info(f"[Phase3] 健康检查恢复正常: {status}")
                self._consecutive_health_failures = 0

        except requests.exceptions.ConnectionError:
            self._consecutive_health_failures += 1
            if self._consecutive_health_failures >= self._health_check_threshold:
                self._send_alert(
                    "CRITICAL", "健康检查端点无法连接",
                    f"**端口:** {health_port}\n"
                    f"**连续失败次数:** {self._consecutive_health_failures}\n"
                    f"**操作:** 健康检查HTTP服务可能已停止，请检查V3服务状态",
                    alert_key="CRITICAL:HEALTH_ENDPOINT_UNHEALTHY",
                )
        except Exception as e:
            self._consecutive_health_failures += 1
            self._logger.warning(f"[Phase3] 健康检查端点自检异常: {e}")

    # ============================================================
    # 4. WS长期断连告警
    # ============================================================

    def check_ws_connection(self, is_connected: bool) -> None:
        """检查WebSocket连接状态，长期断连则递进告警"""
        now = time.time()

        if is_connected:
            # 连接正常
            if self._ws_disconnect_start is not None:
                # 从断连恢复
                disconnect_duration = now - self._ws_disconnect_start
                self._logger.info(
                    f"[Phase3] WebSocket已恢复连接 (断连持续 {disconnect_duration:.0f}秒)"
                )
            self._ws_disconnect_start = None
            self._last_ws_alert_level = ""
            self._last_was_connected = True
            return

        # WS断连
        if self._last_was_connected:
            # 刚开始断连
            self._ws_disconnect_start = now
            self._last_was_connected = False
            self._logger.warning("[Phase3] WebSocket连接断开，开始计时")

        if self._ws_disconnect_start is None:
            return

        disconnect_duration = now - self._ws_disconnect_start

        # 递进告警
        if disconnect_duration >= self.WS_DISCONNECT_WARNING_SECONDS and self._last_ws_alert_level != "WARNING":
            if self._last_ws_alert_level != "CRITICAL":
                self._last_ws_alert_level = "WARNING"
                self._send_alert(
                    "WARNING", "WebSocket断连超过5分钟",
                    f"**断连时长:** {disconnect_duration:.0f}秒\n"
                    f"**影响:** 系统降级为纯轮询模式，跟单延迟增加\n"
                    f"**状态:** 自动重连中，超过30分钟将升级为CRITICAL",
                    alert_key="WARNING:WS_LONG_DISCONNECT",
                )

        if disconnect_duration >= self.WS_DISCONNECT_CRITICAL_SECONDS:
            if self._last_ws_alert_level != "CRITICAL":
                self._last_ws_alert_level = "CRITICAL"
                self._send_alert(
                    "CRITICAL", "WebSocket长期断连（严重）",
                    f"**断连时长:** {disconnect_duration:.0f}秒 ({disconnect_duration/60:.1f}分钟)\n"
                    f"**阈值:** {self.WS_DISCONNECT_CRITICAL_SECONDS}秒\n"
                    f"**影响:** 系统已降级到纯轮询模式，跟单延迟显著增加\n"
                    f"**操作:** 请检查VPS网络连接和Hyperliquid WebSocket服务状态",
                    alert_key="CRITICAL:WS_LONG_DISCONNECT_CRITICAL",
                )


    # ============================================================
    # 5. 跟单比例波动告警
    # ============================================================

    def check_copy_ratio_drift(self, current_ratio: float) -> None:
        """检查跟单比例是否偏离配置值"""
        now = time.time()
        if (now - self._last_ratio_check) < self.COPY_RATIO_CHECK_INTERVAL:
            return
        self._last_ratio_check = now

        if current_ratio <= 0:
            return

        # 记录采样
        self._copy_ratio_samples.append(current_ratio)
        if len(self._copy_ratio_samples) > self.COPY_RATIO_SAMPLE_COUNT:
            self._copy_ratio_samples.pop(0)

        # 至少需要3个采样才判断
        if len(self._copy_ratio_samples) < 3:
            return

        # 计算配置的期望比例
        try:
            fund_ratio = self._config.fund_ratio
        except Exception:
            return

        if fund_ratio <= 0:
            return

        # 计算当前均值
        avg_ratio = sum(self._copy_ratio_samples) / len(self._copy_ratio_samples)

        # 计算偏离百分比（相对于fund_ratio）
        # 实际跟单比例 = avg_ratio / fund_ratio 应该接近1.0
        # 偏离度 = |avg_ratio - fund_ratio| / fund_ratio
        drift_pct = abs(avg_ratio - fund_ratio) / fund_ratio

        # 告警
        if drift_pct >= self.COPY_RATIO_DRIFT_CRITICAL_PCT:
            if self._last_ratio_alert_level != "CRITICAL":
                self._last_ratio_alert_level = "CRITICAL"
                self._last_ratio_alert_time = now
                self._send_alert(
                    "CRITICAL", "跟单比例严重偏离",
                    f"**配置比例:** {fund_ratio:.4f}\n"
                    f"**实际均值:** {avg_ratio:.4f} (最近{len(self._copy_ratio_samples)}次采样)\n"
                    f"**偏离度:** {drift_pct:.1%}\n"
                    f"**阈值:** {self.COPY_RATIO_DRIFT_CRITICAL_PCT:.0%}\n"
                    f"**操作:** 跟单比例严重偏离，可能导致资金利用不足或超额，请检查",
                    alert_key="CRITICAL:COPY_RATIO_DRIFT_CRITICAL",
                )
        elif drift_pct >= self.COPY_RATIO_DRIFT_WARNING_PCT:
            # WARNING级别也做去重
            if now - self._last_ratio_alert_time > self._resource_alert_interval:
                if self._last_ratio_alert_level != "WARNING":
                    self._last_ratio_alert_level = "WARNING"
                    self._last_ratio_alert_time = now
                    self._send_alert(
                        "WARNING", "跟单比例偏离",
                        f"**配置比例:** {fund_ratio:.4f}\n"
                        f"**实际均值:** {avg_ratio:.4f} (最近{len(self._copy_ratio_samples)}次采样)\n"
                        f"**偏离度:** {drift_pct:.1%}\n"
                        f"**阈值:** {self.COPY_RATIO_DRIFT_WARNING_PCT:.0%}\n"
                        f"**说明:** 可能是市场波动导致的价格差异，持续关注",
                        alert_key="WARNING:COPY_RATIO_DRIFT",
                    )
        else:
            # 偏离恢复正常
            if self._last_ratio_alert_level:
                self._logger.info(f"[Phase3] 跟单比例恢复正常: drift={drift_pct:.1%}")
            self._last_ratio_alert_level = ""

    # ============================================================
    # 统一入口
    # ============================================================

    def periodic_check(self, ws_connected: bool = True, current_ratio: float = 0.0) -> None:
        """统一周期性检查入口，在_poll中调用"""
        self.check_leader_address_change()
        self.check_vps_resources()
        self.check_health_endpoint()
        self.check_ws_connection(ws_connected)
        if current_ratio > 0:
            self.check_copy_ratio_drift(current_ratio)

    def startup_validate(self, leader_addr: str) -> None:
        """启动时调用，设置初始状态"""
        self.set_leader_address(leader_addr)
        # 启动时也做一次资源基线检查
        resources = self._get_system_resources()
        parts = []
        for k, v in resources.items():
            if v >= 0 and 'percent' in k:
                parts.append(f"{k}={v:.1f}%")
        if parts:
            self._logger.info(f"[Phase3] 系统资源基线: {' | '.join(parts)}")
