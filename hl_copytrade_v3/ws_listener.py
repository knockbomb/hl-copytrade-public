"""
WebSocket监听线程 — 订阅Hyperliquid openOrders/userFills实时推送（多DEX）

职责:
  - 连接 wss://api.hyperliquid.xyz/ws
  - 订阅 openOrders（挂单变化，用于即时同步TP/SL）
  - 订阅 userFills（成交通知，用于加速跟单响应）
  - 订阅 allMids（全市场价格，用于快速价格查询）
  - 过滤 isSnapshot=true 的初始快照（跳过）
  - 将变化推送到 queue.Queue（附带dex标识）
  - 自动重连（指数退避：1s→2s→4s→...→最大300s）
  - 心跳超时30秒主动重连
  - 线程安全，daemon线程
  - 提供 is_connected 属性
  - 后台定期REST补充xyz DEX价格（WS allMids不含xyz）

v3.1 变更:
  - 支持多DEX订阅（主DEX + xyz/flx/vntl/hyna/km/cash/para/mkts）
  - 队列数据附带 _dex 字段，标识来源DEX

v3.4 变更 (xyz价格修复):
  - 新增 xyz DEX 价格 REST 补充查询（WS allMids 仅返回主DEX ~950个币种）
  - 后台线程每60秒REST获取 xyz allMids 并缓存
  - get_all_mids() 返回 主DEX WS价格 + xyz REST价格 的合并结果
  - xyz REST失败时降级为仅返回主DEX价格（不影响现有币种跟单）

历史记录:
  - 2026-08-18: 移除 clearinghouseState 订阅。该频道原本用于推送账户清算所状态
    （持仓、保证金、净值等），但后来发现：(1) 多DEX分片推送有误判风险；
    (2) REST API 60秒全量轮询已足够稳定可靠；(3) 真正需要实时的 TP/SL 和成交检测
    已由 openOrders 和 userFills 频道承担。因此 clearinghouseState 成为空转订阅，
    移除以减少不必要的连接开销。
  - 2026-08-27: 新增 xyz DEX 价格 REST 补充。WS allMids 仅包含主DEX加密币种价格，
    xyz DEX 的股票/商品（AAPL/TSLA/SILVER等117个）不在WS推送范围内，导致xyz跟单
    因价格为0而被全部跳过。通过后台线程每60秒REST补充xyz价格，确保跟单正常。
"""

from __future__ import annotations

import hashlib
import asyncio
import json
import logging
import queue
import threading
import time
from typing import Any, Dict, List, Optional

# WS断连告警由Phase3渐进式检测（5分钟WARNING，30分钟CRITICAL），此处不直接告警


# 已知的活跃perp DEX列表（排除空壳abcd）
# 与 hl_monitor.py 的 _KNOWN_DEX_LIST 保持一致
ACTIVE_PERP_DEXES: List[str] = ["xyz"]  # 优化: 仅保留实际使用的xyz DEX，其余7个空闲DEX已移除

# xyz DEX 价格REST补充间隔（秒）
XYZ_MIDS_REFRESH_INTERVAL: float = 60.0


class WsListener:
    """Hyperliquid WebSocket 监听线程（多DEX版本）

    订阅 openOrders 和 userFills 推送（所有活跃DEX），将非快照更新放入队列供主线程消费。
    同时订阅 allMids 获取主DEX实时价格，并通过后台REST线程补充xyz DEX价格。

    Args:
        ws_url: WebSocket端点URL
        leader_addr: 目标用户（Leader）地址
        change_queue: 主线程提供的队列，用于传递变化数据
        logger: 日志记录器
        initial_backoff: 初始重连退避秒数
        max_backoff: 最大重连退避秒数
        pong_timeout: 心跳超时秒数（超时则主动重连）
        perp_dexes: 需要订阅的perp DEX列表（默认全部活跃DEX）
        rest_base_url: REST API基础URL，用于xyz价格补充查询（默认从ws_url推导）
    """

    def __init__(
        self,
        ws_url: str,
        leader_addr: str,
        change_queue: queue.Queue,
        logger: logging.Logger,
        initial_backoff: float = 1.0,
        max_backoff: float = 300.0,
        pong_timeout: float = 30.0,
        perp_dexes: Optional[List[str]] = None,
        wakeup_event=None,
        rest_base_url: Optional[str] = None,
    ) -> None:
        self._ws_url = ws_url
        self._leader_addr = leader_addr
        self._change_queue = change_queue
        self._logger = logger
        self._initial_backoff = initial_backoff
        self._max_backoff = max_backoff
        self._pong_timeout = pong_timeout
        self._perp_dexes = perp_dexes or ACTIVE_PERP_DEXES
        self._wakeup_event = wakeup_event
        # REST基础URL：用于xyz价格补充查询，默认从ws_url推导
        if rest_base_url:
            self._rest_base_url = rest_base_url
        else:
            # wss://api.hyperliquid.xyz/ws -> https://api.hyperliquid.xyz
            self._rest_base_url = ws_url.replace("wss://", "https://").replace("/ws", "")

        self._connected: bool = False
        self._running: bool = False
        self._thread: Optional[threading.Thread] = None
        self._lock = threading.Lock()
        self._last_pong_time: float = 0.0
        self._snapshot_seen: dict = {}  # 跟踪每个频道是否已收到快照 {channel: True}
        self._last_orders_log_time: float = 0  # openOrders日志限流（每30秒最多一条）
        self._last_fills_log_time: float = 0  # userFills日志限流
        self._last_all_mids_log_time: float = 0  # allMids日志限流（每60秒最多一条）
        # allMids WS缓存（线程安全）— 仅含主DEX加密币种价格
        self._latest_all_mids: Dict[str, float] = {}
        self._all_mids_timestamp: float = 0
        # === v3.4: xyz DEX 价格REST补充缓存 ===
        self._latest_xyz_mids: Dict[str, float] = {}  # xyz DEX价格缓存
        self._xyz_mids_timestamp: float = 0  # xyz价格时间戳
        self._xyz_mids_thread: Optional[threading.Thread] = None  # 后台刷新线程
        # === v3.3 WS信号驱动: 活动信号检测 ===
        self._activity_signal: threading.Event = threading.Event()
        self._last_activity_time: float = 0
        self._activity_count: int = 0  # 活动信号触发次数（用于统计）
        # === v3.3 内容指纹对比（替代30秒冷却期）===
        self._scope_fingerprints: Dict[str, str] = {}  # {scope_key: fingerprint}
        self._fills_fingerprint: str = ""  # userFills内容指纹
        self._fp_warmup_until: float = 0  # 连接后warm-up截止时间

    # ------------------------------------------------------------------
    # 公开接口
    # ------------------------------------------------------------------

    @property
    def is_connected(self) -> bool:
        """当前WebSocket连接是否活跃"""
        with self._lock:
            return self._connected

    def start(self) -> None:
        """启动WebSocket监听线程（daemon）+ xyz价格后台刷新线程"""
        if self._running:
            self._logger.warning("[WS] 监听线程已在运行")
            return

        self._running = True
        self._thread = threading.Thread(
            target=self._run_loop,
            daemon=True,
            name="ws-listener",
        )
        self._thread.start()
        self._logger.info("[WS] 监听线程已启动")

        # 启动xyz价格REST补充线程（后台，每60秒刷新一次）
        self._xyz_mids_thread = threading.Thread(
            target=self._xyz_mids_refresh_loop,
            daemon=True,
            name="ws-xyz-mids",
        )
        self._xyz_mids_thread.start()
        self._logger.info(
            f"[WS-XYZ] xyz价格补充线程已启动 (刷新间隔={XYZ_MIDS_REFRESH_INTERVAL:.0f}s, "
            f"REST={self._rest_base_url})"
        )

    def stop(self) -> None:
        """停止WebSocket监听线程 + xyz价格刷新线程"""
        self._running = False
        self._set_connected(False)
        if self._thread and self._thread.is_alive():
            self._thread.join(timeout=5)
        if self._xyz_mids_thread and self._xyz_mids_thread.is_alive():
            self._xyz_mids_thread.join(timeout=5)
        self._logger.info("[WS] 监听线程已停止")

    # ------------------------------------------------------------------
    # xyz DEX 价格REST补充（后台线程）
    # ------------------------------------------------------------------

    def _xyz_mids_refresh_loop(self) -> None:
        """xyz价格刷新循环：每60秒通过REST获取一次xyz allMids

        WS allMids 仅推送主DEX加密币种价格，xyz DEX的股票/商品价格
        需要通过 REST {type: "allMids", dex: "xyz"} 查询获取。
        失败时仅记录日志，不中断主循环，下一次继续尝试。
        """
        import requests

        # 首次启动先快速获取一次（延迟2秒，等WS连接稳定）
        time.sleep(2)

        while self._running:
            try:
                self._fetch_xyz_mids(requests)
            except Exception as e:
                self._logger.warning(f"[WS-XYZ] xyz价格刷新异常: {e}")

            # 等待下一次刷新（可被stop打断）
            wait_end = time.time() + XYZ_MIDS_REFRESH_INTERVAL
            while self._running and time.time() < wait_end:
                time.sleep(1)

        self._logger.debug("[WS-XYZ] xyz价格刷新线程退出")

    def _fetch_xyz_mids(self, requests_module) -> None:
        """单次获取xyz DEX allMids价格并更新缓存

        Args:
            requests_module: requests模块引用（避免在类顶部import）
        """
        url = f"{self._rest_base_url}/info"
        payload = {"type": "allMids", "dex": "xyz"}
        try:
            resp = requests_module.post(url, json=payload, timeout=10)
            resp.raise_for_status()
            data = resp.json()
        except Exception as e:
            self._logger.warning(f"[WS-XYZ] REST获取xyz价格失败: {e}")
            return

        if not isinstance(data, dict):
            self._logger.warning(f"[WS-XYZ] xyz价格返回格式异常: {type(data).__name__}")
            return

        # 转换字符串价格为float
        mids: Dict[str, float] = {}
        for coin, price_str in data.items():
            try:
                mids[coin] = float(price_str)
            except (ValueError, TypeError):
                pass

        if not mids:
            self._logger.warning("[WS-XYZ] xyz价格数据为空，跳过更新")
            return

        self._latest_xyz_mids = mids
        self._xyz_mids_timestamp = time.time()
        self._logger.info(
            f"[WS-XYZ] xyz价格刷新成功: {len(mids)}个产品 "
            f"(示例: {list(mids.keys())[:5]}...)"
        )

    # ------------------------------------------------------------------
    # 内部实现
    # ------------------------------------------------------------------

    def _set_connected(self, value: bool) -> None:
        """线程安全地更新连接状态（不直接告警，由Phase3渐进式检测）"""
        with self._lock:
            self._connected = value

    def _run_loop(self) -> None:
        """主循环：连接 → 监听 → 断开 → 退避重连"""
        backoff = self._initial_backoff

        while self._running:
            try:
                self._connect_and_listen()
            except Exception as e:
                self._logger.warning(f"[WS] 连接异常: {e}")

            self._set_connected(False)

            if not self._running:
                break

            # 退避等待（可被stop打断）
            self._logger.info(f"[WS] {backoff:.1f}秒后重连...")
            wait_end = time.time() + backoff
            while self._running and time.time() < wait_end:
                time.sleep(0.5)
            


            # 长期断连优化：连续失败后重连间隔封顶300秒
            backoff = min(backoff * 2, self._max_backoff)

        self._logger.info("[WS] 主循环退出")

    def _connect_and_listen(self) -> None:
        """建立WebSocket连接并监听消息"""

        # 在独立事件循环中运行（daemon线程自己的循环）
        asyncio.run(self._ws_session())

    async def _ws_session(self) -> None:
        """WebSocket会话：连接、订阅（多DEX）、接收消息"""
        import websockets

        try:
            async with websockets.connect(
                self._ws_url,
                ping_interval=None,  # 我们自行管理心跳
                close_timeout=5,
            ) as ws:
                self._set_connected(True)
                self._snapshot_seen = {}  # 重连时重置快照跟踪
                self._last_orders_log_time = 0
                self._last_fills_log_time = 0
                self._latest_all_mids = {}  # 重连时重置allMids缓存
                self._all_mids_timestamp = 0
                self._scope_fingerprints = {}  # 重连时重置指纹
                self._fills_fingerprint = ""
                self._fp_warmup_until = time.time() + 30  # 30秒warm-up
                if hasattr(self, '_activity_coins_by_dex'):
                    self._activity_coins_by_dex = {}  # 重连时重置DEX币种追踪
                self._logger.info(f"[WS] 已连接 {self._ws_url}")

                # === 订阅allMids (全市场价格，用于快速价格查询) ===
                await ws.send(json.dumps({
                    "method": "subscribe",
                    "subscription": {
                        "type": "allMids",
                    },
                }))
                self._logger.info("[WS] 已订阅allMids")

                # === 新增: 订阅openOrders (挂单变化，含TP/SL) ===
                # 主DEX openOrders
                await ws.send(json.dumps({
                    "method": "subscribe",
                    "subscription": {
                        "type": "openOrders",
                        "user": self._leader_addr,
                    },
                }))
                self._logger.info(f"[WS] 已订阅主DEX openOrders (user={self._leader_addr[:8]}...)")

                # 各perp DEX的openOrders
                for dex in self._perp_dexes:
                    await ws.send(json.dumps({
                        "method": "subscribe",
                        "subscription": {
                            "type": "openOrders",
                            "user": self._leader_addr,
                            "dex": dex,
                        },
                    }))
                    self._logger.info(f"[WS] 已订阅{dex} openOrders")

                # === 新增: 订阅userFills (成交通知) ===
                # 主DEX userFills
                await ws.send(json.dumps({
                    "method": "subscribe",
                    "subscription": {
                        "type": "userFills",
                        "user": self._leader_addr,
                    },
                }))
                self._logger.info(f"[WS] 已订阅主DEX userFills (user={self._leader_addr[:8]}...)")

                # 各perp DEX的userFills
                for dex in self._perp_dexes:
                    await ws.send(json.dumps({
                        "method": "subscribe",
                        "subscription": {
                            "type": "userFills",
                            "user": self._leader_addr,
                            "dex": dex,
                        },
                    }))
                    self._logger.info(f"[WS] 已订阅{dex} userFills")

                # 启动心跳检查协程
                pong_check_task = asyncio.create_task(self._pong_check_loop(ws))

                # 接收消息循环
                while self._running:
                    try:
                        raw_message = await asyncio.wait_for(ws.recv(), timeout=5)
                    except asyncio.TimeoutError:
                        # 超时继续循环，检查running状态
                        continue
                    except Exception as e:
                        self._logger.warning(f"[WS] 接收消息异常: {e}")
                        break

                    # 解析JSON
                    try:
                        msg = json.loads(raw_message)
                    except (json.JSONDecodeError, TypeError):
                        self._logger.debug(f"[WS] 非JSON消息: {raw_message[:100]}")
                        continue

                    # 类型检查：HL WS某些推送可能是非dict格式
                    if not isinstance(msg, dict):
                        self._logger.debug(f"[WS] 非dict消息(类型={type(msg).__name__}): {str(msg)[:100]}")
                        continue

                    # 处理pong响应
                    if msg.get("channel") == "pong" or msg.get("method") == "pong":
                        self._last_pong_time = time.time()
                        continue

                    # 处理订阅确认
                    if msg.get("method") == "subscribe":
                        self._logger.debug(f"[WS] 订阅确认: {msg}")
                        continue

                    # 处理数据推送
                    await self._handle_data_message(msg)

        except Exception as e:
            import traceback
            self._logger.warning(f"[WS] 接收循环异常: {e}\n{traceback.format_exc()}")
        finally:
            pong_check_task.cancel()
            try:
                await pong_check_task
            except asyncio.CancelledError:
                pass

    async def _handle_data_message(self, msg: Dict[str, Any]) -> None:
        """处理数据推送消息

        支持两种频道:
        - openOrders: 挂单变化（含TP/SL调整）
        - userFills: 成交通知

        过滤 isSnapshot=true 的初始快照，将增量更新推入队列。
        队列数据附带 _dex、_channel、_coins 字段，标识来源和涉及币种。
        
        注：clearinghouseState 频道已于2026-08-18移除（原因见文件顶部历史记录）。
        """
        try:
            await self._handle_data_message_inner(msg)
        except Exception as e:
            # 保护性兜底：任何异常都不应导致WS断连
            self._logger.warning(
                f"[WS] 消息处理异常(已跳过): {e} | "
                f"msg_keys={list(msg.keys()) if isinstance(msg, dict) else type(msg).__name__}"
            )

    async def _handle_data_message_inner(self, msg: Dict[str, Any]) -> None:
        """内部实现：处理数据推送消息"""
        if not isinstance(msg, dict):
            self._logger.debug(f"[WS] 非dict消息({type(msg).__name__})，跳过")
            return

        channel = msg.get("channel", "")

        # 过滤掉订阅确认消息
        if "method" in msg and "data" not in msg:
            return

        # === openOrders 频道 ===
        if channel == "openOrders":
            await self._handle_open_orders(msg)
            return

        # === userFills 频道 ===
        if channel == "userFills":
            await self._handle_user_fills(msg)
            return

        # === allMids 频道（全市场价格推送）===
        if channel == "allMids":
            await self._handle_all_mids(msg)
            return

        # clearinghouseState 频道已于2026-08-18移除（原因见文件顶部历史记录）
        # 如果收到该频道的消息，仅记录调试日志
        self._logger.debug(f"[WS] 收到未订阅频道消息: {msg.get('channel', 'unknown')}")

    async def _handle_open_orders(self, msg: Dict[str, Any]) -> None:
        """处理openOrders推送：提取挂单币种，推入队列（带日志限流+内容去重）"""
        data = msg.get("data")
        dex_tag = msg.get("dex", "")
        channel_key = f"openOrders_{dex_tag or 'main'}"

        # 跳过每个频道的第一条消息（初始快照）
        if not self._snapshot_seen.get(channel_key):
            self._snapshot_seen[channel_key] = True
            self._logger.info(f"[WS] 跳过openOrders初始快照 (dex={dex_tag or 'main'})")
            return

        # 解析data格式
        if isinstance(data, dict):
            orders = data.get("orders", data.get("openOrders", []))
            if not isinstance(orders, list):
                return
            data = orders
        elif not isinstance(data, list):
            return

        if len(data) == 0:
            return

        # === v3.3 内容指纹对比（按scope独立追踪）===
        # HL的main订阅交替推送不同DEX的挂单数据（如RENDER和xyz RWA），
        # 每次都带dex=""，无法用dex_tag区分。改用内容指纹按scope独立追踪。
        # scope_key = 本推送包含的币种集合（排序后拼接）
        _coins_set = set()
        for order in data:
            if isinstance(order, dict):
                _c = order.get("coin", "")
                if _c:
                    _coins_set.add(_c)
        _scope_key = ",".join(sorted(_coins_set)) if _coins_set else "__EMPTY__"

        # 计算本推送内容指纹（leader意图字段）
        _fp_parts = []
        for _o in data:
            if isinstance(_o, dict) and _o.get("oid"):
                _fp_parts.append(
                    f"{_o.get('oid')}|{_o.get('coin')}|{_o.get('side')}|"
                    f"{_o.get('sz')}|{_o.get('limitPx')}|{_o.get('triggerPx')}|"
                    f"{_o.get('isTrigger')}"
                )
        _push_fp = hashlib.md5("|".join(sorted(_fp_parts)).encode()).hexdigest() if _fp_parts else "EMPTY"

        # 与本scope上次的指纹对比
        _last_fp = self._scope_fingerprints.get(_scope_key)
        if _push_fp == _last_fp:
            return  # 内容完全相同，HL周期性刷新，静默跳过
        self._scope_fingerprints[_scope_key] = _push_fp

        # Warm-up期间只学习不触发
        if time.time() < self._fp_warmup_until:
            return

        # === 指纹变化 = leader真实操作 ===
        coins = _coins_set

        # 推入队列
        queue_data = {
            "_channel": "openOrders",
            "_dex": dex_tag,
            "_order_count": len(data),
            "_coins": list(coins),
        }
        try:
            self._change_queue.put_nowait(queue_data)
        except queue.Full:
            self._logger.warning("[WS] 队列已满，丢弃openOrders消息")

        # 触发活动信号
        self._activity_signal.set()
        self._activity_count += 1
        self._last_activity_time = time.time()

        # 日志限流：每30秒最多一条
        import time as _time
        now = _time.time()
        if now - self._last_orders_log_time > 30:
            self._last_orders_log_time = now
            self._logger.info(
                f"[WS-ORDERS] openOrders推送 (dex={dex_tag or 'main'}, "
                f"orders={len(data)}, coins={coins})"
            )


    async def _handle_user_fills(self, msg: Dict[str, Any]) -> None:
        """处理userFills推送：提取成交币种，推入队列（带日志限流）"""
        data = msg.get("data")
        dex_tag = msg.get("dex", "")
        channel_key = f"userFills_{dex_tag or 'main'}"

        # 跳过每个频道的第一条消息（初始快照）
        if not self._snapshot_seen.get(channel_key):
            self._snapshot_seen[channel_key] = True
            self._logger.info(f"[WS] 跳过userFills初始快照 (dex={dex_tag or 'main'})")
            return

        # 解析data格式
        if isinstance(data, dict):
            fills = data.get("fills", data.get("userFills", []))
            if not isinstance(fills, list):
                return
            data = fills
        elif not isinstance(data, list):
            return

        if len(data) == 0:
            return

        # 提取币种
        coins = set()
        for fill in data:
            if isinstance(fill, dict):
                coin = fill.get("coin", "")
                if coin:
                    coins.add(coin)

        # 推入队列
        queue_data = {
            "_channel": "userFills",
            "_dex": dex_tag,
            "_fill_count": len(data),
            "_coins": list(coins),
        }
        try:
            self._change_queue.put_nowait(queue_data)
            # 唤醒主循环立即处理
            if self._wakeup_event:
                self._wakeup_event.set()
        except queue.Full:
            self._logger.warning("[WS] 队列已满，丢弃userFills消息")

        # === v3.3: userFills指纹对比（检测新成交）===
        # 计算fills指纹：基于fill的唯一标识
        _fill_parts = []
        for _f in data:
            if isinstance(_f, dict):
                _fid = _f.get("tid", _f.get("txSid", ""))
                _fcoin = _f.get("coin", "")
                _fsz = _f.get("sz", "")
                _fpx = _f.get("px", "")
                _fdir = _f.get("dir", "")
                _fill_parts.append(f"{_fid}|{_fcoin}|{_fsz}|{_fpx}|{_fdir}")
        _fills_fp = hashlib.md5("|".join(sorted(_fill_parts)).encode()).hexdigest() if _fill_parts else "EMPTY"
        
        if _fills_fp == self._fills_fingerprint:
            return  # 相同fills，HL重复推送，跳过
        self._fills_fingerprint = _fills_fp
        
        # Warm-up期间不触发信号
        if time.time() < self._fp_warmup_until:
            return

        # 有新成交，触发信号
        self._activity_signal.set()
        self._activity_count += 1
        self._last_activity_time = time.time()

        # 日志限流：每30秒最多一条
        import time as _time
        now = _time.time()
        if now - self._last_fills_log_time > 30:
            self._last_fills_log_time = now
            self._logger.info(
                f"[WS-FILL] userFills推送 (dex={dex_tag or 'main'}, "
                f"fills={len(data)}, coins={coins})"
            )


    async def _handle_all_mids(self, msg: Dict[str, Any]) -> None:
        """处理allMids推送：缓存全市场中间价（不需要推入change_queue）
        
        allMids的data格式是 {"mids": {"BTC": "50000.5", "ETH": "3000.2", ...}}
        第一条消息就是完整的价格快照（非增量），直接接受。
        
        注意：WS allMids 仅包含主DEX加密币种价格，xyz DEX价格
        通过后台REST线程（_xyz_mids_refresh_loop）补充。
        """
        data = msg.get("data")
        if not isinstance(data, dict):
            return

        # 提取mids子字典（WS格式: data.mids = {coin: price_str, ...}）
        mids_data = data.get("mids", data)
        if not isinstance(mids_data, dict):
            return

        # 转换字符串值为float
        mids: Dict[str, float] = {}
        for coin, price_str in mids_data.items():
            try:
                mids[coin] = float(price_str)
            except (ValueError, TypeError):
                pass

        if mids:
            self._latest_all_mids = mids
            self._all_mids_timestamp = time.time()

            # 日志限流：每60秒最多一条
            import time as _time
            now = _time.time()
            if now - self._last_all_mids_log_time > 60:
                self._last_all_mids_log_time = now
                # 如果xyz价格也已缓存，合并后显示总数
                xyz_count = len(self._latest_xyz_mids) if self._latest_xyz_mids else 0
                total = len(mids) + xyz_count
                self._logger.info(
                    f"[WS-MIDS] allMids缓存更新: 主DEX={len(mids)}个, "
                    f"xyz={xyz_count}个, 合计={total}个"
                )

    def get_all_mids(self) -> Optional[Dict[str, float]]:
        """获取全市场最新中间价（主DEX WS价格 + xyz DEX REST价格合并结果）

        如果主DEX WS数据超过30秒未更新则返回None（数据过期）。
        xyz价格为REST补充，60秒刷新一次；若xyz数据不可用，
        仍然返回主DEX价格（降级，不影响现有加密币种跟单）。
        """
        if not self._latest_all_mids:
            return None
        if time.time() - self._all_mids_timestamp > 30:
            return None  # 主DEX数据过期

        result = dict(self._latest_all_mids)  # 主DEX价格（副本）

        # 合并xyz DEX价格（如果有缓存且未过期）
        if self._latest_xyz_mids and self._xyz_mids_timestamp > 0:
            # xyz价格有效期设为5分钟（远大于60秒刷新间隔）
            if time.time() - self._xyz_mids_timestamp < 300:
                result.update(self._latest_xyz_mids)

        return result

    # ------------------------------------------------------------------
    # v3.3: WS信号驱动接口
    # ------------------------------------------------------------------

    def wait_for_activity(self, timeout: float) -> bool:
        """等待WS活动信号（openOrders或userFills内容变化）。
        
        用于事件驱动主循环：阻塞等待直到leader有操作 或 超时。
        返回True表示有活动（应触发按需查询），False表示超时（应触发对账）。
        """
        result = self._activity_signal.wait(timeout=timeout)
        if result:
            self._activity_signal.clear()
        return result

    def is_healthy(self) -> bool:
        """WS连接是否健康（已连接 且 最近有数据推送）。"""
        if not self.is_connected:
            return False
        # 60秒内无任何推送视为不健康（allMids应每几秒推送一次）
        if self._last_activity_time > 0 and time.time() - self._last_activity_time > 60:
            return False
        return True

    def get_activity_stats(self) -> Dict[str, Any]:
        """获取活动统计（用于日志和监控）"""
        return {
            "total_signals": self._activity_count,
            "last_activity_ago": round(time.time() - self._last_activity_time, 1) if self._last_activity_time > 0 else None,
            "connected": self.is_connected,
            "healthy": self.is_healthy(),
            "xyz_mids_count": len(self._latest_xyz_mids) if self._latest_xyz_mids else 0,
            "xyz_mids_age": round(time.time() - self._xyz_mids_timestamp, 1) if self._xyz_mids_timestamp > 0 else None,
        }

    async def _pong_check_loop(self, ws: Any) -> None:
        """心跳监控协程：定期发送ping，检测pong超时

        Hyperliquid WS要求客户端主动发送ping保持连接。
        如果pong超时则关闭连接触发重连。
        """

        ping_interval = 10  # 每10秒发送一次ping
        while self._running:
            try:
                # 发送ping
                ping_msg = json.dumps({"method": "ping"})
                await ws.send(ping_msg)

                # 检查pong超时
                elapsed = time.time() - self._last_pong_time
                if elapsed > self._pong_timeout:
                    self._logger.warning(
                        f"[WS] 心跳超时({elapsed:.1f}s > {self._pong_timeout}s)，主动重连"
                    )
                    await ws.close()
                    return

            except Exception as e:
                self._logger.debug(f"[WS] ping发送异常: {e}")
                return

            await asyncio.sleep(ping_interval)
