#!/usr/bin/env python3
"""
HL跟单系统 - 告警管理器 (AlertManager) v2

redesigned 2026-08-06: 告警规格注册表 (Alert Spec Registry)
- 每个告警代码自动翻译为中文：发生了什么/原因/如何处理
- 结构化飞书卡片：标题 + 发生了什么 + 原因 + 如何处理 + 具体数据
- 未注册的告警类型自动回退到原始格式（向后兼容）
- 支持 extra_data 传入具体诊断数据

redesigned 2026-07-30: 统一告警出口
- 所有告警写入 unified_alert_queue，由 push_alerts_to_feishu.py 统一推送
- 去除直接 webhook 推送，消除多通道冗余
- 保留文件写入（alerts_unread.json）作为巡检可读副本
"""

from __future__ import annotations

import json
import logging
import os
import threading
import time
from datetime import datetime
from typing import Any, Dict, Optional

from unified_alert_queue import enqueue_alert, direct_push_critical


# ============================================================
# 告警规格注册表 (Alert Spec Registry)
# ============================================================
# 每个告警代码 → 人类可读的完整解释
# 字段:
#   title:        中文标题（替代英文代码）
#   what:         发生了什么（通用描述，不含具体数字）
#   cause:        可能的原因
#   action:       如何处理（含详细步骤或"无需操作"）
#   intervention: "action_required" | "monitor" | "info"
#   level:        默认级别 (CRITICAL/WARNING/INFO)，V3可覆盖
#
# 新增告警类型：只需在此字典中加一个条目即可

ALERT_SPEC: Dict[str, Dict[str, str]] = {

    # ==================== 安全检查 ====================

    "SAFETY_2_VERIFIED": {
        "title": "安全检查：大变化经复核确认属实",
        "what": "系统检测到Leader仓位大幅变化，经二次查询复核，确认变化属实，已正常执行跟单。",
        "cause": "Leader进行了大幅调仓（开仓/平仓/调整），数据准确无误。",
        "action": "✅ 无需操作 — 跟单已正常完成。",
        "intervention": "info",
        "level": "WARNING",
    },
    "SAFETY_2_INCONSISTENT": {
        "title": "安全检查：大变化复核不一致，已安全跳过",
        "what": "系统检测到仓位大幅变化，但二次查询发现变化并不属实，判定为数据异常，已安全跳过本轮跟单。",
        "cause": "HELIUM DEX节点响应超时或部分节点返回了过期数据，导致首次查询结果不准确。",
        "action": "✅ 无需操作 — 系统下次轮询会自动恢复正常。这是安全机制在正确工作。",
        "intervention": "monitor",
        "level": "WARNING",
    },
    "SAFETY_2_VERIFY_ERROR": {
        "title": "安全检查：复核查询失败",
        "what": "系统检测到大变化后进行二次复核，但复核查询本身失败了，已跳过本轮大变化检查。",
        "cause": "网络抖动、DEX节点不可用或API请求超时。",
        "action": "✅ 无需操作 — 下次轮询会重新检测。如频繁出现请检查VPS网络。",
        "intervention": "monitor",
        "level": "WARNING",
    },
    "SAFETY_2_CIRCUIT_BREAKER_VERIFIED": {
        "title": "安全检查：熔断机制复核确认",
        "what": "仓位变化触发了熔断检查阈值，经复核确认变化属实。",
        "cause": "Leader进行了大幅调仓，属于正常交易行为。",
        "action": "✅ 无需操作 — 熔断检查已通过，跟单正常执行。",
        "intervention": "info",
        "level": "WARNING",
    },
    "SAFETY_2_CIRCUIT_BREAKER_INCONSISTENT": {
        "title": "安全检查：熔断机制复核不一致",
        "what": "仓位变化触发熔断检查，但复核结果与首次不一致，已跳过本轮。",
        "cause": "API数据不稳定，两次查询返回了不同结果。",
        "action": "✅ 无需操作 — 系统安全机制正常运作，下次轮询恢复。",
        "intervention": "monitor",
        "level": "WARNING",
    },
    "SAFETY_2_CIRCUIT_BREAKER_VERIFY_FAILED": {
        "title": "安全检查：熔断复核失败",
        "what": "熔断检查期间复核操作完全失败，系统采取了保守策略。",
        "cause": "严重的网络问题或API不可用。",
        "action": "⚠️ 请保持关注 — 如果连续出现，需要检查VPS网络连接和DEX状态。",
        "intervention": "monitor",
        "level": "CRITICAL",
    },
    "SAFETY_3_ALL_DEX_FAILED": {
        "title": "安全机制：所有DEX节点查询失败",
        "what": "查询Leader仓位时，所有HELIUM DEX节点都返回失败，无法获取最新数据。",
        "cause": "HELIUM网络暂时不可用、VPS网络故障、或所有已知节点都下线了。",
        "action": "⚠️ 请保持关注 — 通常几轮内会自动恢复。如连续超过10轮失败，需要登录VPS检查网络和DEX状态。",
        "intervention": "monitor",
        "level": "WARNING",
    },
    "SAFETY_3_DEX_QUERY_INCOMPLETE": {
        "title": "安全机制：DEX查询不完整",
        "what": "查询Leader仓位时，部分DEX节点成功但部分失败，数据可能不完整。",
        "cause": "部分DEX节点响应超时或暂时不可用。",
        "action": "✅ 无需操作 — 系统会使用成功节点的数据继续运行。",
        "intervention": "monitor",
        "level": "WARNING",
    },
    "SAFETY_4_OPEN_CANCELLED": {
        "title": "安全检查：取消开仓（二次确认不一致）",
        "what": "系统准备跟单开仓时，二次确认发现Leader仓位状态已变化，取消了本次开仓。",
        "cause": "Leader在系统执行期间快速平仓，或市场条件快速变化。",
        "action": "✅ 无需操作 — 这是安全保护机制在正常工作，避免了可能的错误跟单。",
        "intervention": "info",
        "level": "WARNING",
    },
    "SAFETY_4_CLOSE_CANCELLED": {
        "title": "安全检查：取消平仓（二次确认不一致）",
        "what": "系统准备跟单平仓时，二次确认发现Leader仓位状态已变化，取消了本次平仓。",
        "cause": "Leader在系统执行期间重新开仓，或仓位已被其他操作修改。",
        "action": "✅ 无需操作 — 安全保护机制正常工作。",
        "intervention": "info",
        "level": "WARNING",
    },
    "SAFETY_4_WS_OPEN_CANCELLED": {
        "title": "安全检查：取消开仓（WebSocket确认不一致）",
        "what": "通过WebSocket二次确认时发现Leader仓位不存在，取消了开仓。",
        "cause": "WebSocket实时数据显示Leader已无该仓位，可能是快速开平操作。",
        "action": "✅ 无需操作 — WebSocket确认更及时，避免了过时数据导致的错误跟单。",
        "intervention": "info",
        "level": "WARNING",
    },
    "SAFETY_4_WS_CLOSE_CANCELLED": {
        "title": "安全检查：取消平仓（WebSocket确认不一致）",
        "what": "通过WebSocket二次确认时发现Leader仓位仍然存在，取消了平仓。",
        "cause": "WebSocket实时数据与REST查询不一致，系统选择了保守策略。",
        "action": "✅ 无需操作 — 下一轮会重新评估。",
        "intervention": "info",
        "level": "WARNING",
    },

    # ==================== 平仓操作 ====================

    "FLIP_MAX_RETRY": {
        "title": "🚨 平仓重试上限：{coin}",
        "what": "系统尝试翻转（FLIP）{coin}仓位，已重试多次仍然失败，已放弃自动重试。",
        "cause": "可能是Hyperliquid API持续报错、市场流动性不足、或账户保证金不足。",
        "action": "⚠️ 需要手动处理\n1. 打开Orcaterm → 选择你的VPS连接\n2. 输入: cd /opt/hl-copytrade && tail -50 logs/v3_highfreq.log | grep FLIP\n3. 查看具体失败原因\n4. 检查Hyperliquid账户保证金是否充足\n5. 确认后重启: supervisorctl restart highfreq",
        "intervention": "action_required",
        "level": "CRITICAL",
    },
    "FLIP_CLOSE_FAILED": {
        "title": "FLIP平仓失败：{coin}",
        "what": "系统尝试翻转{coin}仓位（先平旧仓再开新仓），平仓步骤失败。",
        "cause": "通常是API报错（保证金不足、订单被拒、或市场暂停）。",
        "action": "⚠️ 系统会自动重试 — 如连续失败多次会升级为CRITICAL告警。",
        "intervention": "monitor",
        "level": "CRITICAL",
    },
    "FLIP_RESOLVED_OPEN_FAILED": {
        "title": "FLIP后续开仓失败：{coin}",
        "what": "FLIP平仓成功后，开新仓位失败。旧仓已平但新仓未建立。",
        "cause": "平仓后市场条件变化导致新仓被拒绝，或价格已大幅变动。",
        "action": "⚠️ 需要手动检查\n1. 登录Orcaterm查看V3日志\n2. 检查{coin}当前仓位状态\n3. 如需手动跟单，可在Hyperliquid界面操作",
        "intervention": "action_required",
        "level": "CRITICAL",
    },
    "FLIP_RETRY_FAILED": {
        "title": "FLIP平仓重试中：{coin}",
        "what": "FLIP平仓{coin}再次失败，系统将继续按退避策略重试。",
        "cause": "API暂时不可用或市场条件暂不满足。",
        "action": "✅ 保持关注 — 系统会自动重试，达到上限后会通知你。",
        "intervention": "monitor",
        "level": "WARNING",
    },
    "CLOSE_MAX_RETRY": {
        "title": "🚨 平仓重试上限：{coin}",
        "what": "系统尝试平仓{coin}，多次重试均失败，已放弃自动重试。",
        "cause": "可能是保证金不足、市场暂停交易、或API持续报错。",
        "action": "⚠️ 需要手动处理\n1. 打开Orcaterm → 连接VPS\n2. 输入: cd /opt/hl-copytrade && tail -50 logs/v3_highfreq.log | grep CLOSE\n3. 查看失败原因\n4. 检查账户保证金\n5. 问题解决后重启: supervisorctl restart highfreq",
        "intervention": "action_required",
        "level": "CRITICAL",
    },
    "CLOSE_RETRY_FAILED": {
        "title": "平仓重试中：{coin}",
        "what": "平仓{coin}再次失败，系统将继续重试。",
        "cause": "API暂时不可用或订单暂时无法成交。",
        "action": "✅ 保持关注 — 系统会自动重试。",
        "intervention": "monitor",
        "level": "WARNING",
    },
    "CLOSE_FAILED": {
        "title": "平仓失败：{coin}",
        "what": "系统尝试平仓{coin}但未成功，已加入重试队列。",
        "cause": "API报错、市场条件不满足、或订单被拒绝。",
        "action": "✅ 系统会自动重试 — 如持续失败会升级告警。",
        "intervention": "monitor",
        "level": "CRITICAL",
    },

    # ==================== 开仓/调仓 ====================

    "OPEN_FAILED": {
        "title": "开仓确认失败：{coin}",
        "what": "系统执行开仓{coin}后，确认仓位方向与预期不符。",
        "cause": "订单可能未成交或成交方向错误，属于执行异常。",
        "action": "⚠️ 需要关注\n1. 检查V3日志确认订单执行结果\n2. 检查Hyperliquid上的实际仓位\n3. 如仓位方向错误，可能需要手动调整",
        "intervention": "action_required",
        "level": "CRITICAL",
    },
    "ADJUST_FAILED": {
        "title": "调仓确认失败：{coin}",
        "what": "系统调整{coin}仓位后，确认结果与预期不符。",
        "cause": "部分成交或市场快速变化导致实际仓位与目标不一致。",
        "action": "⚠️ 请检查 — 系统下一轮会尝试再次修正。如持续出现需手动介入。",
        "intervention": "monitor",
        "level": "WARNING",
    },
    "ORDER_REJECT_REPEATED": {
        "title": "订单连续被拒：{coin}",
        "what": "对{coin}的下单操作连续多次被Hyperliquid拒绝。",
        "cause": "通常是下单金额低于最小限额，或账户保证金不足。",
        "action": "⚠️ 需要检查\n1. 确认跟单比例（copy_ratio）是否合理\n2. 检查计算出的下单金额是否低于最小限额\n3. 如保证金不足，需要充值",
        "intervention": "action_required",
        "level": "WARNING",
    },
    "CONFIRM_RESIDUAL": {
        "title": "补单后仍有残余差额：{coin}",
        "what": "系统对{coin}进行了补单操作，但仍有部分差额未完全消除。",
        "cause": "订单部分成交或市场变化导致补单未完全覆盖差额。",
        "action": "✅ 保持关注 — 系统会继续尝试修正。",
        "intervention": "monitor",
        "level": "WARNING",
    },

    # ==================== 对账系统 (RECON) ====================

    "RECON_DEVIATION": {
        "title": "对账偏差已自动修正：{coin}",
        "what": "RECON系统检测到你的{coin}持仓与预期存在偏差，已自动触发修正操作。",
        "cause": "通常由之前某次调仓部分成交、或API数据同步延迟导致。",
        "action": "✅ 无需操作 — 已自动修正。偶尔出现属于正常现象。",
        "intervention": "monitor",
        "level": "WARNING",
    },
    "RECON_RECOVERY_FAIL": {
        "title": "对账自动补仓失败",
        "what": "RECON系统检测到仓位缺失，尝试自动补仓但失败了。",
        "cause": "API报错、保证金不足、或查询账户总值失败。",
        "action": "⚠️ 需要检查\n1. 查看V3日志确认具体错误\n2. 检查账户保证金\n3. 如持续失败，重启V3实例",
        "intervention": "action_required",
        "level": "CRITICAL",
    },
    "RECON_ADJUST_FAIL": {
        "title": "偏差修正执行失败：{coin}",
        "what": "RECON系统尝试修正{coin}的仓位偏差，但执行失败。",
        "cause": "API报错或市场条件不满足。",
        "action": "⚠️ 请检查 — 下轮会重新尝试。如持续失败需登录VPS查看日志。",
        "intervention": "monitor",
        "level": "WARNING",
    },
    "RECON_RESIDUAL_CLOSE_FAIL": {
        "title": "残留仓位平仓失败：{coin}",
        "what": "RECON系统尝试平掉{coin}的残留仓位，但操作失败。",
        "cause": "API报错或订单被拒绝。",
        "action": "⚠️ 请检查 — 可能需要手动在Hyperliquid上操作。",
        "intervention": "action_required",
        "level": "WARNING",
    },
    "RECON_QUERY_FAIL": {
        "title": "对账查询失败",
        "what": "RECON系统无法获取你的持仓数据，本轮对账跳过。",
        "cause": "网络问题或API暂时不可用。",
        "action": "✅ 无需操作 — 下次轮询会自动恢复。",
        "intervention": "monitor",
        "level": "WARNING",
    },

    # ==================== 钱包 & 保证金 ====================

    "WALLET_EXPIRED": {
        "title": "🚨 钱包已过期",
        "what": "交易钱包的API密钥或授权已过期，系统无法继续交易。",
        "cause": "钱包密钥到期，需要重新生成或更新。",
        "action": "⚠️ 需要立即处理\n1. 登录Hyperliquid更新钱包API密钥\n2. 更新VPS上的密钥配置\n3. 重启V3跟单实例",
        "intervention": "action_required",
        "level": "CRITICAL",
    },
    "WALLET_EXPIRY_URGENT": {
        "title": "钱包即将过期",
        "what": "交易钱包的授权即将到期，请尽快更新。",
        "cause": "钱包密钥即将到期。",
        "action": "⚠️ 请尽快处理 — 在到期前更新钱包密钥，避免服务中断。",
        "intervention": "action_required",
        "level": "WARNING",
    },
    "WALLET_EXPIRY_REMINDER": {
        "title": "钱包到期提醒",
        "what": "交易钱包授权还有一段时间到期，暂不紧急。",
        "cause": "定期提醒。",
        "action": "✅ 无需立即操作 — 记得到期前更新即可。",
        "intervention": "info",
        "level": "INFO",
    },
    "WALLET_ROTATE_FAIL": {
        "title": "Exchange热重建失败",
        "what": "系统尝试重新建立Exchange连接（钱包轮换），但失败了。",
        "cause": "API密钥问题或网络连接异常。",
        "action": "⚠️ 请检查 — 可能影响后续交易执行。查看VPS日志确认原因。",
        "intervention": "monitor",
        "level": "WARNING",
    },
    "WALLET_ROTATE_TPSL_FAIL": {
        "title": "TPSL全量重建失败",
        "what": "TP/SL（止盈止损）全量重建失败，可能导致止盈止损功能暂时不可用。",
        "cause": "API报错或钱包状态异常。",
        "action": "⚠️ 请检查 — 确认TP/SL功能是否正常。",
        "intervention": "monitor",
        "level": "WARNING",
    },
    "ADDRESS_CHANGED": {
        "title": "🚨 钱包地址变更",
        "what": "检测到钱包地址发生变化，可能导致跟单失效。",
        "cause": "钱包被重新创建或配置被修改。",
        "action": "⚠️ 需要立即处理\n1. 确认地址变更原因\n2. 更新配置文件中的钱包地址\n3. 重启V3实例",
        "intervention": "action_required",
        "level": "CRITICAL",
    },
    "MARGIN_EXCEEDED": {
        "title": "🚨 保证金使用率超标",
        "what": "账户保证金使用率超过安全阈值，存在爆仓风险。",
        "cause": "仓位过大或市场反向波动导致保证金不足。",
        "action": "⚠️ 需要立即处理\n1. 检查Hyperliquid账户保证金\n2. 考虑充值保证金或减少仓位\n3. 确认Leader仓位是否也在调整",
        "intervention": "action_required",
        "level": "CRITICAL",
    },
    "MARGIN_WARNING": {
        "title": "保证金使用率偏高",
        "what": "账户保证金使用率接近警戒线。",
        "cause": "仓位接近上限或市场波动。",
        "action": "⚠️ 请关注 — 如继续上升可能需要充值保证金。",
        "intervention": "monitor",
        "level": "WARNING",
    },

    # ==================== 系统控制 ====================

    "PAUSE_TRADING": {
        "title": "⚠️ 跟单已暂停",
        "what": "跟单系统已暂停，不再执行新的跟单操作。",
        "cause": "触发了暂停条件（如钱包过期、手动暂停等）。",
        "action": "⚠️ 需要处理\n1. 查看暂停原因\n2. 问题解决后恢复跟单",
        "intervention": "action_required",
        "level": "CRITICAL",
    },
    "EMERGENCY_STOP": {
        "title": "🚨 紧急制动已执行",
        "what": "紧急制动信号被触发，系统已执行全部紧急停止步骤。",
        "cause": "检测到严重异常，触发了紧急制动机制。",
        "action": "⚠️ 需要立即处理\n1. 登录Orcaterm检查VPS状态\n2. 确认触发紧急制动的原因\n3. 问题解决后手动重启系统",
        "intervention": "action_required",
        "level": "CRITICAL",
    },
    "BASELINE_FAIL": {
        "title": "启动基准建立失败",
        "what": "V3启动时无法建立初始基准数据（仓位快照），无法开始跟单。",
        "cause": "实时查询和状态文件都返回空数据，可能是API不可用或确实无仓位。",
        "action": "⚠️ 需要检查\n1. 确认Leader是否有持仓\n2. 检查API连接是否正常\n3. 重启V3实例",
        "intervention": "action_required",
        "level": "CRITICAL",
    },
    "PUSH_STALLED": {
        "title": "告警推送停滞",
        "what": "飞书告警推送脚本可能已停止运行，告警队列出现堆积。",
        "cause": "push脚本崩溃、cron任务异常、或webhook配置错误。",
        "action": "⚠️ 需要检查\n1. 登录VPS检查cron任务: crontab -l\n2. 手动执行推送测试: cd /opt/hl-copytrade && python3 push_alerts_to_feishu.py\n3. 检查webhook配置是否正确",
        "intervention": "action_required",
        "level": "CRITICAL",
    },
    "V3_CRASH": {
        "title": "🚨 V3跟单脚本崩溃",
        "what": "V3跟单脚本发生未处理的异常，已崩溃。Orchestrator会尝试自动重启。",
        "cause": "代码bug、API异常、或系统资源不足。",
        "action": "⚠️ 请保持关注 — Orchestrator会自动重启。如反复崩溃需要检查日志。",
        "intervention": "monitor",
        "level": "CRITICAL",
    },
    "CONSECUTIVE_ERROR": {
        "title": "连续错误告警",
        "what": "V3连续多个轮询周期出现相同的错误，系统健康受到影响。",
        "cause": "通常是API持续不可用或某个功能模块出现持续故障。",
        "action": "⚠️ 需要关注 — 查看V3日志确认错误详情。如持续超过10分钟，考虑重启。",
        "intervention": "monitor",
        "level": "CRITICAL",
    },

    # ==================== 连接 ====================

    "WS_DISCONNECTED": {
        "title": "WebSocket连接断开",
        "what": "与HELIUM的WebSocket实时数据连接断开了。",
        "cause": "网络抖动、服务端断开、或VPS网络波动。",
        "action": "✅ 无需操作 — 系统会自动重连。",
        "intervention": "monitor",
        "level": "WARNING",
    },
    "WS_RECONNECTED": {
        "title": "WebSocket已重连",
        "what": "WebSocket连接已自动恢复。",
        "cause": "之前的断开已自动修复。",
        "action": "✅ 无需操作 — 一切恢复正常。",
        "intervention": "info",
        "level": "INFO",
    },

    # ==================== 滑点 & 资金费率 ====================

    "HIGH_SLIPPAGE": {
        "title": "滑点偏高：{coin}",
        "what": "{coin}的交易滑点高于正常水平，执行成本增加。",
        "cause": "市场流动性不足或交易量较大。",
        "action": "✅ 保持关注 — 系统会自动调整滑点容忍度。",
        "intervention": "monitor",
        "level": "WARNING",
    },
    "EXTREME_SLIPPAGE": {
        "title": "🚨 极端滑点：{coin}",
        "what": "{coin}的交易滑点极高，继续执行可能造成较大损失。",
        "cause": "市场流动性严重不足、剧烈波动、或大额交易。",
        "action": "⚠️ 需要关注 — 系统可能暂停该币种的交易。检查市场状况。",
        "intervention": "monitor",
        "level": "CRITICAL",
    },
    "HIGH_FUNDING_RATE": {
        "title": "资金费率偏高：{coin}",
        "what": "{coin}的资金费率高于正常水平，持仓成本增加。",
        "cause": "市场单边情绪严重，多空力量失衡。",
        "action": "✅ 保持关注 — 系统会持续监控。",
        "intervention": "monitor",
        "level": "WARNING",
    },
    "EXTREME_FUNDING_RATE": {
        "title": "🚨 极端资金费率：{coin}",
        "what": "{coin}的资金费率极高，持续持仓将面临显著的资金费用。",
        "cause": "市场极度单边，资金费率达到异常水平。",
        "action": "⚠️ 需要评估 — 考虑是否需要调整跟单策略。",
        "intervention": "action_required",
        "level": "CRITICAL",
    },

    # ==================== 仓位退出 ====================

    "COIN_TPSL_EXITED": {
        "title": "仓位止盈止损退出：{coin}",
        "what": "{coin}的TP/SL（止盈/止损）被触发，仓位已平。系统标记该币种为已退出，等待Leader新信号。",
        "cause": "市场触发了预设的止盈或止损价格。",
        "action": "✅ 无需操作 — 当Leader再次开仓{coin}时，系统会自动跟进。",
        "intervention": "info",
        "level": "WARNING",
    },
    "COIN_MANUAL_EXITED": {
        "title": "仓位消失（手动退出）：{coin}",
        "what": "检测到{coin}仓位消失（Leader手动平仓），系统标记该币种为已退出。",
        "cause": "Leader主动平仓了该仓位。",
        "action": "✅ 无需操作 — 当Leader再次开仓时系统会自动跟进。",
        "intervention": "info",
        "level": "WARNING",
    },

    # ==================== 配置 ====================

    "CONFIG_VALIDATION_FAIL": {
        "title": "配置读写失败",
        "what": "跟单配置数据（position_ratios）读取或保存失败。",
        "cause": "文件权限问题、磁盘空间不足、或文件系统异常。",
        "action": "⚠️ 请检查VPS磁盘空间和文件权限。",
        "intervention": "monitor",
        "level": "WARNING",
    },

    # ==================== TPSL ====================

    "TPSL_RETRY_EXHAUSTED": {
        "title": "TP/SL重试放弃：{coin}",
        "what": "{coin}的TP/SL订单设置重试30分钟后仍失败，已放弃。",
        "cause": "API持续报错或市场条件不满足。",
        "action": "⚠️ 请检查 — 该仓位的TP/SL可能未设置成功。",
        "intervention": "action_required",
        "level": "WARNING",
    },
    "TPSL_SYNC_FAIL": {
        "title": "TP/SL同步失败",
        "what": "TP/SL（止盈止损）数据同步失败。",
        "cause": "API报错或钱包状态异常。",
        "action": "⚠️ 请检查 — 可能影响止盈止损功能。",
        "intervention": "monitor",
        "level": "WARNING",
    },
    "VERIFY_FILL_EXHAUSTED": {
        "title": "成交验证耗尽",
        "what": "订单成交验证重试多次后仍未确认。",
        "cause": "订单可能未成交或成交信息延迟。",
        "action": "⚠️ 请检查VPS日志确认订单实际状态。",
        "intervention": "monitor",
        "level": "WARNING",
    },

    # ==================== 系统资源（巡检脚本） ====================

    "DISK_WARNING": {
        "title": "磁盘空间不足",
        "what": "VPS磁盘使用率超过警戒线。",
        "cause": "日志文件积累、数据库增长、或临时文件未清理。",
        "action": "⚠️ 需要处理\n1. 登录VPS检查磁盘使用: df -h\n2. 清理大文件: du -sh /home/user/* | sort -rh | head -10\n3. 清理旧日志",
        "intervention": "action_required",
        "level": "WARNING",
    },
    "MEMORY_WARNING": {
        "title": "内存使用率偏高",
        "what": "VPS内存使用率超过警戒线。",
        "cause": "进程内存泄漏或负载过高。",
        "action": "⚠️ 请检查: top -bn1 | head -20",
        "intervention": "monitor",
        "level": "WARNING",
    },
}


# ============================================================
# 告警类型 → 级别映射（从 ALERT_SPEC 自动生成，向后兼容）
# ============================================================
ALERT_LEVEL_MAP: Dict[str, str] = {k: v.get("level", "WARNING") for k, v in ALERT_SPEC.items()}

# 补充不在 ALERT_SPEC 中的旧映射（确保向后兼容）
_ALERT_LEVEL_EXTRA = {
    "SLIPPAGE_STATS_UPDATE": "INFO",
    "FUNDING_RATE_NORMAL": "INFO",
}
ALERT_LEVEL_MAP.update(_ALERT_LEVEL_EXTRA)


# ============================================================
# 通知白名单（2026-07-29）
# ============================================================
WARNING_NOTIFY_PATTERNS = [
    "WARNING:WALLET_EXPIRY_URGENT",
    "WARNING:保证金使用率",
    "WARNING:VPS_DISK_WARNING",
    "WARNING:COPY_RATIO_DRIFT",
    "WARNING:WS_LONG_DISCONNECT",
    "WARNING:保证金快速上升",
    "WARNING:HIGH_SLIPPAGE",
    "WARNING:HIGH_FUNDING_RATE",
    "WARNING:ORDER_REJECT_REPEATED",
    "WARNING:FLIP_RETRY_FAILED",
    "WARNING:CLOSE_RETRY_FAILED",
    "WARNING:RECON_ADJUST_FAIL",
    "WARNING:RECON_RESIDUAL_CLOSE_FAIL",
    "WARNING:TPSL_SYNC_FAIL",
    "WARNING:CONFIG_VALIDATION_FAIL",
    "WARNING:SAFETY_2_CIRCUIT_BREAKER_VERIFIED",
    "WARNING:SAFETY_2_CIRCUIT_BREAKER_INCONSISTENT",
    "WARNING:SAFETY_3_DEX_QUERY_INCOMPLETE",
    "WARNING:SAFETY_4_OPEN_CANCELLED",
    "WARNING:SAFETY_4_CLOSE_CANCELLED",
    "WARNING:SAFETY_4_WS_OPEN_CANCELLED",
    "WARNING:SAFETY_4_WS_CLOSE_CANCELLED",
    "WARNING:COIN_TPSL_EXITED",
    "WARNING:COIN_MANUAL_EXITED",
    "WARNING:RECON_DEVIATION",
    "WARNING:ADJUST_FAILED",
    "WARNING:VERIFY_FILL_EXHAUSTED",
    "WARNING:TPSL_RETRY_EXHAUSTED",
    "WARNING:CONFIRM_RESIDUAL",
    "WARNING:SAFETY_2_VERIFY_ERROR",
    "WARNING:SAFETY_2_VERIFIED",
    "WARNING:SAFETY_2_INCONSISTENT",
    "CRITICAL:V3_CRASH",
    "CRITICAL:SAFETY_3_ALL_DEX_FAILED",
    "CRITICAL:PAUSE_TRADING",
    "CRITICAL:EMERGENCY_STOP",
    "CRITICAL:BASELINE_FAIL",
    "WARNING:RECON_QUERY_FAIL",
    "WARNING:WALLET_ROTATE_FAIL",
    "WARNING:WALLET_ROTATE_TPSL_FAIL",
    "CRITICAL:PUSH_STALLED",
]


def should_notify(alert_key: str) -> bool:
    """检查告警是否应该通知用户"""
    if alert_key.startswith("CRITICAL:"):
        return True
    if alert_key.startswith("WARNING:"):
        for pattern in WARNING_NOTIFY_PATTERNS:
            if alert_key.startswith(pattern):
                return True
        return False
    alert_type = alert_key.split(":")[0] if ":" in alert_key else alert_key
    level = get_alert_level(alert_type)
    if level == "CRITICAL":
        return True
    elif level == "WARNING":
        warning_key = f"WARNING:{alert_type}"
        for pattern in WARNING_NOTIFY_PATTERNS:
            if warning_key.startswith(pattern):
                return True
        return False
    return False


def get_alert_level(alert_type: str) -> str:
    """根据告警类型确定级别"""
    if alert_type in ALERT_LEVEL_MAP:
        return ALERT_LEVEL_MAP[alert_type]
    for prefix, level in ALERT_LEVEL_MAP.items():
        if alert_type.startswith(prefix):
            return level
    return "WARNING"


class AlertManager:
    """告警管理器 - 写入统一队列

    v2 (2026-08-06): 告警规格注册表，自动翻译英文代码为中文解释
    公开API保持不变，V3主体代码无需修改（extra_data为可选增强）。
    线程安全，异常绝不影响主程序。
    """

    def __init__(
        self,
        webhook_url: str = "",
        logger: logging.Logger = None,
        enabled: bool = True,
        dedup_hours: int = 24,
        bot_name: str = "V3跟单",
        alert_file: str = "",
        system_name: str = "",
    ):
        self._logger = logger or logging.getLogger(__name__)
        self._enabled = enabled
        self._dedup_seconds = dedup_hours * 3600
        self._bot_name = bot_name
        self._system_name = system_name
        self._last_alert: Dict[str, float] = {}
        self._lock = threading.Lock()
        self._send_count: int = 0
        self._skip_count: int = 0

        # alerts_unread.json 路径（巡检脚本可读的副本）
        if alert_file:
            self._alert_file = alert_file
        else:
            self._alert_file = os.path.join(
                os.path.dirname(os.path.abspath(__file__)), "alerts_unread.json"
            )

        self._logger.info(
            f"[ALERT] AlertManager v2 已启用 | 注册表={len(ALERT_SPEC)}个告警规格 | 去重={dedup_hours}h | bot={bot_name}"
        )

    @property
    def enabled(self) -> bool:
        return self._enabled

    def _format_title(self, title: str) -> str:
        """添加系统前缀"""
        if self._system_name:
            if "高频" in self._system_name or "highfreq" in self._system_name:
                return f"[高频] {title}"
            elif "低频" in self._system_name or "main" in self._system_name:
                return f"[低频] {title}"
        if self._bot_name:
            if "高频" in self._bot_name:
                return f"[高频] {title}"
            elif "低频" in self._bot_name:
                return f"[低频] {title}"
        return title

    def _enrich_alert(self, alert_type: str, coin: str, message: str,
                      extra_data: Optional[Dict] = None, poll_count: int = 0,
                      level: str = "WARNING") -> tuple:
        """
        使用注册表将告警翻译为结构化格式。
        返回 (enriched_title, enriched_detail, is_enriched)
        """
        spec = ALERT_SPEC.get(alert_type)

        # 前缀匹配（处理 CONSECUTIVE_ERROR_xxx 等动态类型）
        if not spec:
            for code, s in ALERT_SPEC.items():
                if alert_type.startswith(code):
                    spec = s
                    break

        if not spec:
            # 未注册：回退到原始格式
            return (f"跟单告警: {alert_type}",
                    f"**币种:** {coin}\n**详情:** {message}\n**轮询:** #{poll_count}",
                    False)

        # 格式化标题（支持 {coin} 变量）
        title = spec["title"].format(coin=coin)

        # 格式化 what（支持 {coin} 变量）
        what_text = spec["what"].format(coin=coin)

        # 格式化 cause
        cause_text = spec["cause"].format(coin=coin)

        # 格式化 action
        action_text = spec["action"].format(coin=coin)

        # intervention 标记
        intervention = spec.get("intervention", "monitor")
        if intervention == "action_required":
            action_header = "⚠️ **如何处理**（需要手动处理）"
        elif intervention == "monitor":
            action_header = "⚙️ **如何处理**"
        else:
            action_header = "✅ **如何处理**"

        # 构建结构化 detail
        parts = [
            f"📖 **发生了什么**\n{what_text}",
            f"🔍 **原因**\n{cause_text}",
            f"{action_header}\n{action_text}",
        ]

        # 添加具体数据
        data_parts = []
        if extra_data:
            for k, v in extra_data.items():
                data_parts.append(f"{k}: {v}")
        if message:
            data_parts.append(message)
        data_parts.append(f"#{poll_count}")

        detail = "\n\n".join(parts)
        detail += "\n\n─────────────────────\n"
        detail += f"📊 {' | '.join(data_parts)}"

        return (title, detail, True)

    def send(
        self,
        level: str,
        title: str,
        detail: str,
        alert_key: Optional[str] = None,
    ) -> bool:
        """发送告警 → 写入统一队列 + alerts_unread.json 副本"""
        if not self._enabled:
            return False

        key = alert_key or f"{level}:{title}"

        # 去重（所有级别统一去重）
        with self._lock:
            now = time.time()
            last = self._last_alert.get(key, 0)
            elapsed = now - last
            if elapsed < self._dedup_seconds:
                self._skip_count += 1
                self._logger.debug(
                    "[ALERT] 去重跳过: [%s] %s (距上次%.1fh)", level, title, elapsed / 3600
                )
                return False
            self._last_alert[key] = now

        formatted_title = self._format_title(title)
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        try:
            # 写入统一队列
            ok = enqueue_alert(
                level=level,
                title=formatted_title,
                detail=detail,
                source=self._system_name or self._bot_name,
            )
            # 同时写入 alerts_unread.json 副本
            self._write_alert_to_file(level, formatted_title, detail, timestamp, alert_key=key)

            if ok:
                with self._lock:
                    self._send_count += 1
                self._logger.info("[ALERT] ✅ 已入队: [%s] %s", level, formatted_title)

                # CRITICAL直推：绕过队列延迟，立即推送飞书
                if level == "CRITICAL":
                    try:
                        dp_ok = direct_push_critical(level, formatted_title, detail, self._system_name or self._bot_name)
                        if dp_ok:
                            self._logger.info("[ALERT] ⚡ CRITICAL直推成功: %s", formatted_title)
                    except Exception:
                        pass  # 直推失败不影响，队列正常流程兜底

            return ok

        except Exception as e:
            self._logger.warning("[ALERT] 入队异常: %s", e)
            return False

    # ============================================================
    # 便捷方法（签名向后兼容，extra_data为可选增强）
    # ============================================================

    def send_alert(
        self,
        alert_type: str,
        coin: str,
        message: str,
        poll_count: int = 0,
        extra_data: Optional[Dict] = None,
        level_override: Optional[str] = None,
    ) -> bool:
        """从 _write_alert 参数发送告警（v2: 注册表翻译）"""
        level = level_override or get_alert_level(alert_type)
        title, detail, is_enriched = self._enrich_alert(
            alert_type, coin, message, extra_data=extra_data,
            poll_count=poll_count, level=level,
        )
        return self.send(level, title, detail, alert_key=f"{alert_type}:{coin}")

    def send_health_alert(self, alert_type: str, message: str, poll_count: int = 0,
                          extra_data: Optional[Dict] = None) -> bool:
        """从 _write_health_alert 参数发送告警"""
        level = get_alert_level(alert_type)
        title, detail, is_enriched = self._enrich_alert(
            alert_type, "ALL", message, extra_data=extra_data,
            poll_count=poll_count, level=level,
        )
        if not is_enriched:
            # 健康告警回退格式
            title = f"健康告警: {alert_type}"
            detail = f"**详情:** {message}\n**轮询:** #{poll_count}"
        return self.send(level, title, detail, alert_key=f"{alert_type}:HEALTH")

    def send_startup(self, info: str) -> bool:
        """启动通知"""
        return self.send("INFO", "V3跟单系统启动", info, alert_key="STARTUP")

    def send_shutdown(self, reason: str = "正常退出") -> bool:
        """关闭通知"""
        return self.send("WARNING", "V3跟单系统停止", f"**原因:** {reason}", alert_key=f"SHUTDOWN:{reason}")

    def send_custom(self, level: str, title: str, detail: str) -> bool:
        """自定义告警（供巡检等外部脚本使用）"""
        return self.send(level, title, detail)

    def get_stats(self) -> Dict[str, Any]:
        """获取告警统计"""
        with self._lock:
            return {
                "enabled": self._enabled,
                "send_count": self._send_count,
                "skip_count": self._skip_count,
                "tracked_keys": len(self._last_alert),
                "registry_size": len(ALERT_SPEC),
            }

    def _write_alert_to_file(self, level: str, title: str, detail: str, timestamp: str, alert_key: str = "") -> None:
        """写入 alerts_unread.json 副本（仅白名单告警）"""
        try:
            if alert_key and not should_notify(alert_key):
                self._logger.info("[ALERT] 📝 静默告警(仅日志): [%s] %s", level, title)
                return

            alerts = []
            if os.path.exists(self._alert_file):
                try:
                    with open(self._alert_file, 'r') as f:
                        alerts = json.load(f)
                except Exception:
                    alerts = []

            alerts.append({
                "level": level,
                "title": title,
                "detail": detail,
                "timestamp": timestamp,
                "read": False,
            })

            if len(alerts) > 100:
                alerts = alerts[-100:]

            with open(self._alert_file, 'w') as f:
                json.dump(alerts, f, ensure_ascii=False, indent=2)

        except Exception as e:
            self._logger.warning("[ALERT] 写入副本失败: %s", e)


# 直接运行测试
if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    logger = logging.getLogger("test")
    mgr = AlertManager(logger=logger, bot_name="测试")

    # 测试注册表翻译
    mgr.send_alert("SAFETY_2_INCONSISTENT", "ALL", "大变化复核不一致，跳过",
                   poll_count=50552,
                   extra_data={"首次": "8/9=88.9%", "复核": "0/9=0.0%", "阈值": "50%"})

    # 测试未注册类型回退
    mgr.send_alert("UNKNOWN_CODE", "BTC", "未知告警测试", poll_count=100)

    print(f"\n注册表包含 {len(ALERT_SPEC)} 个告警规格")
    print("测试完成，检查 data/alert_queue.json")
