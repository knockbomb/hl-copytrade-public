#!/usr/bin/env python3
"""
HL跟单系统 - Phase 4 统计增强模块
功能：
  1. 滑点统计：追踪每笔订单的预期价格vs实际成交价
  2. 资金费率监控：监控持仓币种的资金费率，高费率时告警

作者: HL CopyTrade
日期: 2026-07-25
"""

from __future__ import annotations

import json
import logging
import os
import time
from datetime import datetime, timezone, timedelta
from typing import Any, Dict, List, Optional, Tuple

import requests


# ============================================================
# Phase 4 新增告警类型
# ============================================================
PHASE4_ALERT_MAP: Dict[str, str] = {
    "HIGH_SLIPPAGE": "WARNING",
    "EXTREME_SLIPPAGE": "CRITICAL",
    "HIGH_FUNDING_RATE": "WARNING",
    "EXTREME_FUNDING_RATE": "CRITICAL",
    "FUNDING_RATE_NEGATIVE_ALARM": "WARNING",
}


class Phase4Stats:
    """Phase 4 统计增强监控器"""

    # 滑点阈值
    SLIPPAGE_WARNING_PCT = 0.30      # 0.30% → WARNING（0.10%在HL上属于正常波动）
    SLIPPAGE_CRITICAL_PCT = 0.50     # 0.50% → CRITICAL
    SLIPPAGE_LOG_INTERVAL = 50       # 每50笔记录一次统计摘要

    # 资金费率阈值
    FUNDING_WARNING_RATE = 0.0005    # 0.05% per hour → WARNING
    FUNDING_CRITICAL_RATE = 0.001    # 0.10% per hour → CRITICAL
    FUNDING_CHECK_INTERVAL = 3600    # 每小时检查一次
    FUNDING_ALERT_INTERVAL = 7200    # 同级别告警2小时去重

    # 滑点数据持久化文件
    # SLIPPAGE_DATA_FILE 已改为实例变量，在__init__中从config.state_file派生（系统隔离）

    def __init__(self, config, logger, alert_manager=None):
        self._config = config
        self._logger = logger
        self._alert_manager = alert_manager

        # 系统隔离: 从state_file派生滑点数据文件名
        _state_stem = os.path.splitext(os.path.basename(config.state_file))[0].removesuffix("_state")
        self._slippage_data_file = f"{_state_stem}_slippage_stats.json"

        # 滑点统计
        self._slippage_records: List[Dict] = []  # 最近N条滑点记录
        self._slippage_by_coin: Dict[str, List[float]] = {}  # 按币种统计
        self._total_trades = 0
        self._avg_slippage = 0.0
        self._max_slippage = 0.0
        self._max_slippage_coin = ""
        self._load_slippage_data()

        # 资金费率
        self._last_funding_check: float = 0
        self._last_funding_alert_time: Dict[str, float] = {}  # coin -> last alert time
        self._last_funding_alert_level: Dict[str, str] = {}   # coin -> level

        self._logger.info("[Phase4] 统计增强模块初始化完成")

    def _send_alert(self, level: str, title: str, detail: str, alert_key: str = "") -> bool:
        """发送告警"""
        if self._alert_manager and self._alert_manager.enabled:
            return self._alert_manager.send(level, title, detail, alert_key=alert_key or f"{level}:{title}")
        return False

    def _load_slippage_data(self):
        """从文件加载滑点统计数据"""
        try:
            path = os.path.join(os.path.dirname(self._config.state_file), self._slippage_data_file)
            if os.path.exists(path):
                with open(path, 'r') as f:
                    data = json.load(f)
                self._slippage_records = data.get("records", [])[-200:]  # 保留最近200条
                self._total_trades = data.get("total_trades", len(self._slippage_records))
                self._avg_slippage = data.get("avg_slippage", 0.0)
                self._max_slippage = data.get("max_slippage", 0.0)
                self._max_slippage_coin = data.get("max_slippage_coin", "")
                self._slippage_by_coin = data.get("by_coin", {})
                self._logger.info(f"[Phase4] 加载滑点统计: {self._total_trades}笔交易, 平均滑点={self._avg_slippage:.4f}%")
        except Exception as e:
            self._logger.warning(f"[Phase4] 加载滑点数据失败: {e}")

    def _save_slippage_data(self):
        """保存滑点统计数据到文件"""
        try:
            path = os.path.join(os.path.dirname(self._config.state_file), self._slippage_data_file)
            data = {
                "records": self._slippage_records[-200:],
                "total_trades": self._total_trades,
                "avg_slippage": self._avg_slippage,
                "max_slippage": self._max_slippage,
                "max_slippage_coin": self._max_slippage_coin,
                "by_coin": self._slippage_by_coin,
                "updated_at": datetime.now().isoformat(),
            }
            tmp_path = path + ".tmp"
            with open(tmp_path, 'w') as f:
                json.dump(data, f, ensure_ascii=False, indent=2)
            os.replace(tmp_path, path)
        except Exception as e:
            self._logger.warning(f"[Phase4] 保存滑点数据失败: {e}")

    # ============================================================
    # 1. 滑点统计
    # ============================================================

    def record_slippage(self, coin: str, expected_price: float, actual_fill_price: float,
                        size: float, action: str, timestamp: Optional[float] = None) -> float:
        """记录一笔交易的滑点

        Args:
            coin: 币种
            expected_price: 预期价格（下单时的mid价格）
            actual_fill_price: 实际成交价格
            size: 成交数量
            action: 操作类型（open/adjust/close）
            timestamp: 时间戳

        Returns:
            slippage_pct: 滑点百分比
        """
        if expected_price <= 0 or actual_fill_price <= 0:
            return 0.0

        # 计算滑点百分比
        # 买入：实际价格 > 预期价格 = 正滑点（不利）
        # 卖出：实际价格 < 预期价格 = 正滑点（不利）
        if action == "close":
            # 平仓时，买入平仓（空头平仓）希望低价，卖出平仓（多头平仓）希望高价
            slippage_pct = (expected_price - actual_fill_price) / expected_price * 100
        else:
            # 开仓/调仓时，买入希望低价，卖出希望高价
            slippage_pct = (actual_fill_price - expected_price) / expected_price * 100

        now = timestamp or time.time()

        # 记录
        record = {
            "coin": coin,
            "expected_px": expected_price,
            "actual_px": actual_fill_price,
            "slippage_pct": round(slippage_pct, 4),
            "size": size,
            "action": action,
            "time": datetime.fromtimestamp(now, tz=timezone.utc).isoformat(),
        }
        self._slippage_records.append(record)
        if len(self._slippage_records) > 200:
            self._slippage_records = self._slippage_records[-200:]

        # 更新统计
        self._total_trades += 1
        if coin not in self._slippage_by_coin:
            self._slippage_by_coin[coin] = []
        self._slippage_by_coin[coin].append(slippage_pct)
        if len(self._slippage_by_coin[coin]) > 50:
            self._slippage_by_coin[coin] = self._slippage_by_coin[coin][-50:]

        # 更新平均和最大滑点
        all_slippages = [r["slippage_pct"] for r in self._slippage_records]
        self._avg_slippage = sum(all_slippages) / len(all_slippages) if all_slippages else 0.0
        if abs(slippage_pct) > abs(self._max_slippage):
            self._max_slippage = slippage_pct
            self._max_slippage_coin = coin

        # 日志
        self._logger.info(
            f"[Phase4] 滑点记录: {coin} {action} | "
            f"预期={expected_price:.6f} 实际={actual_fill_price:.6f} | "
            f"滑点={slippage_pct:+.4f}% | 总均={self._avg_slippage:+.4f}%"
        )

        # 定期保存
        if self._total_trades % 10 == 0:
            self._save_slippage_data()

        # 高滑点告警
        abs_slippage = abs(slippage_pct)
        if abs_slippage >= self.SLIPPAGE_CRITICAL_PCT:
            self._send_alert(
                "CRITICAL", f"极端滑点: {coin}",
                f"**币种:** {coin}\n"
                f"**操作:** {action}\n"
                f"**预期价格:** {expected_price:.6f}\n"
                f"**成交价格:** {actual_fill_price:.6f}\n"
                f"**滑点:** {slippage_pct:+.4f}%\n"
                f"**阈值:** {self.SLIPPAGE_CRITICAL_PCT}%\n"
                f"**影响:** 流动性不足或市场剧烈波动，大额订单可能遭受较大损失",
                alert_key=f"CRITICAL:EXTREME_SLIPPAGE:{coin}",
            )
        elif abs_slippage >= self.SLIPPAGE_WARNING_PCT:
            self._send_alert(
                "WARNING", f"滑点偏高: {coin}",
                f"**币种:** {coin}\n"
                f"**操作:** {action}\n"
                f"**预期价格:** {expected_price:.6f}\n"
                f"**成交价格:** {actual_fill_price:.6f}\n"
                f"**滑点:** {slippage_pct:+.4f}%\n"
                f"**阈值:** {self.SLIPPAGE_WARNING_PCT}%",
                alert_key=f"WARNING:HIGH_SLIPPAGE:{coin}",
            )

        return slippage_pct

    def get_slippage_summary(self) -> Dict[str, Any]:
        """获取滑点统计摘要"""
        summary = {
            "total_trades": self._total_trades,
            "avg_slippage_pct": round(self._avg_slippage, 4),
            "max_slippage_pct": round(self._max_slippage, 4),
            "max_slippage_coin": self._max_slippage_coin,
            "by_coin": {},
        }
        for coin, slips in self._slippage_by_coin.items():
            if slips:
                summary["by_coin"][coin] = {
                    "count": len(slips),
                    "avg": round(sum(slips) / len(slips), 4),
                    "max": round(max(slips, key=abs), 4),
                }
        return summary

    def check_recent_fills_for_slippage(self, user_addr: str) -> None:
        """查询最近的成交记录，计算滑点（用于事后分析）"""
        try:
            resp = requests.post(
                f"{self._config.base_url}/info",
                json={"type": "userFills", "user": user_addr},
                timeout=self._config.api_timeout,
            )
            resp.raise_for_status()
            fills = resp.json()

            if not isinstance(fills, list):
                return

            # 只处理最近1小时的成交
            one_hour_ago = (time.time() - 3600) * 1000
            recent_fills = [f for f in fills if f.get("time", 0) > one_hour_ago]

            # 获取当前mid价格作为参考（简化版：用oraclePx近似）
            meta_resp = requests.post(
                f"{self._config.base_url}/info",
                json={"type": "metaAndAssetCtxs"},
                timeout=self._config.api_timeout,
            )
            meta_resp.raise_for_status()
            meta_data = meta_resp.json()
            asset_ctxs = meta_data[1]  # list of contexts
            universe = meta_data[0]["universe"]  # list of coin metadata
            coin_to_idx = {u["name"]: i for i, u in enumerate(universe)}

            for fill in recent_fills:
                coin = fill.get("coin", "")
                fill_px = float(fill.get("px", 0))
                fill_time = fill.get("time", 0)

                if coin in coin_to_idx:
                    ctx = asset_ctxs[coin_to_idx[coin]]
                    mid_px = float(ctx.get("midPx", 0))
                    if mid_px > 0 and fill_px > 0:
                        # 简单滑点计算（注意：这不如用下单时的mid精确，但可作为参考）
                        slippage = (fill_px - mid_px) / mid_px * 100
                        if abs(slippage) > self.SLIPPAGE_WARNING_PCT:
                            self._logger.info(
                                f"[Phase4] 近期成交滑点: {coin} fill={fill_px:.6f} mid={mid_px:.6f} slip={slippage:+.4f}%"
                            )

        except Exception as e:
            self._logger.debug(f"[Phase4] 查询成交记录失败: {e}")

    # ============================================================
    # 2. 资金费率监控
    # ============================================================

    def get_current_funding_rates(self) -> Dict[str, float]:
        """查询所有币种当前资金费率"""
        try:
            resp = requests.post(
                f"{self._config.base_url}/info",
                json={"type": "metaAndAssetCtxs"},
                timeout=self._config.api_timeout,
            )
            resp.raise_for_status()
            data = resp.json()

            asset_ctxs = data[1]  # list of contexts
            universe = data[0]["universe"]

            rates = {}
            for i, asset_meta in enumerate(universe):
                coin = asset_meta["name"]
                if i < len(asset_ctxs):
                    ctx = asset_ctxs[i]
                    funding_str = ctx.get("funding", "0")
                    try:
                        funding_rate = float(funding_str)
                        rates[coin] = funding_rate
                    except (ValueError, TypeError):
                        pass
            return rates

        except Exception as e:
            self._logger.warning(f"[Phase4] 查询资金费率失败: {e}")
            return {}

    def check_funding_rates(self, user_positions: Dict[str, dict], poll_count: int = 0) -> None:
        """检查持仓币种的资金费率

        Args:
            user_positions: 用户当前持仓 {coin: {szi, ...}}
        """
        now = time.time()
        if (now - self._last_funding_check) < self.FUNDING_CHECK_INTERVAL:
            return
        self._last_funding_check = now

        funding_rates = self.get_current_funding_rates()
        if not funding_rates:
            return

        held_coins = [coin for coin, pos in user_positions.items() if float(pos.get("szi", 0)) != 0]

        for coin in held_coins:
            if coin not in funding_rates:
                continue

            rate = funding_rates[coin]
            position = user_positions.get(coin, {})
            szi = float(position.get("szi", 0))
            is_long = szi > 0

            # 计算年化费率
            hourly_rate = abs(rate)
            daily_rate = hourly_rate * 24
            annual_rate = hourly_rate * 24 * 365

            # 判断方向：正费率多头付钱，负费率空头付钱
            paying_side = "多头" if (rate > 0 and is_long) or (rate < 0 and not is_long) else "空头"

            if hourly_rate >= self.FUNDING_CRITICAL_RATE:
                last_time = self._last_funding_alert_time.get(coin, 0)
                last_level = self._last_funding_alert_level.get(coin, "")
                if last_level != "CRITICAL" or (now - last_time) > self.FUNDING_ALERT_INTERVAL:
                    self._last_funding_alert_time[coin] = now
                    self._last_funding_alert_level[coin] = "CRITICAL"
                    self._send_alert(
                        "CRITICAL", f"资金费率极高: {coin}",
                        f"**币种:** {coin}\n"
                        f"**方向:** {'做多' if is_long else '做空'}\n"
                        f"**当前费率:** {rate:.6f}/小时 ({rate*100:.4f}%)\n"
                        f"**年化费率:** {annual_rate:.1%}\n"
                        f"**付费方:** {paying_side}（你正在付费）\n"
                        f"**日成本:** 约 {daily_rate:.2%}/天\n"
                        f"**操作:** 费率极高，持仓成本显著增加，建议评估是否继续持有",
                        alert_key=f"CRITICAL:EXTREME_FUNDING_RATE:{coin}",
                    )
                    self._logger.warning(f"[Phase4] 资金费率告警 CRITICAL: {coin} rate={rate:.6f}/h")

            elif hourly_rate >= self.FUNDING_WARNING_RATE:
                last_time = self._last_funding_alert_time.get(coin, 0)
                last_level = self._last_funding_alert_level.get(coin, "")
                if last_level != "WARNING" or (now - last_time) > self.FUNDING_ALERT_INTERVAL:
                    self._last_funding_alert_time[coin] = now
                    self._last_funding_alert_level[coin] = "WARNING"
                    self._send_alert(
                        "WARNING", f"资金费率偏高: {coin}",
                        f"**币种:** {coin}\n"
                        f"**方向:** {'做多' if is_long else '做空'}\n"
                        f"**当前费率:** {rate:.6f}/小时 ({rate*100:.4f}%)\n"
                        f"**年化费率:** {annual_rate:.1%}\n"
                        f"**付费方:** {paying_side}\n"
                        f"**说明:** 费率偏高，持仓成本增加",
                        alert_key=f"WARNING:HIGH_FUNDING_RATE:{coin}",
                    )

            # 记录日志
            if poll_count % 20 == 0:
                self._logger.info(
                    f"[Phase4] {coin} 资金费率: {rate:.6f}/h "
                    f"(年化{annual_rate:.1%}, {'付费' if paying_side == ('多头' if is_long else '空头') else '收费'})"
                )

    def get_funding_summary(self, user_positions: Dict[str, dict]) -> Dict[str, Any]:
        """获取资金费率摘要"""
        rates = self.get_current_funding_rates()
        summary = {"coins": {}}

        held_coins = [coin for coin, pos in user_positions.items() if float(pos.get("szi", 0)) != 0]

        for coin in held_coins:
            if coin in rates:
                rate = rates[coin]
                szi = float(user_positions[coin].get("szi", 0))
                summary["coins"][coin] = {
                    "rate_per_hour": rate,
                    "rate_pct": f"{rate*100:.4f}%",
                    "annual_rate": f"{rate*24*365:.1%}",
                    "position": "long" if szi > 0 else "short",
                    "paying": (rate > 0 and szi > 0) or (rate < 0 and szi < 0),
                }

        return summary

    # ============================================================
    # 统一入口
    # ============================================================

    def periodic_check(self, user_positions: Dict[str, dict] = None, poll_count: int = 0) -> None:
        """统一周期性检查入口"""
        if user_positions:
            self.check_funding_rates(user_positions, poll_count=poll_count)

    def on_order_filled(self, coin: str, expected_price: float, actual_price: float,
                        size: float, action: str) -> None:
        """订单成交后调用，记录滑点"""
        self.record_slippage(coin, expected_price, actual_price, size, action)
