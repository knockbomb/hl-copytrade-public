"""
WebSocket监听线程 — 订阅Hyperliquid clearinghouseState实时推送（多DEX）

职责:
  - 连接 wss://api.hyperliquid.xyz/ws
  - 订阅 clearinghouseState (目标用户地址，所有活跃DEX)
  - 过滤 isSnapshot=true 的初始快照（跳过）
  - 将变化推送到 queue.Queue（附带dex标识）
  - 自动重连（指数退避：1s→2s→4s→...→最大300s）
  - 心跳超时30秒主动重连
  - 线程安全，daemon线程
  - 提供 is_connected 属性

v3.1 变更:
  - 支持多DEX订阅（主DEX + xyz/flx/vntl/hyna/km/cash/para/mkts）
  - 队列数据附带 _dex 字段，标识来源DEX
"""

from __future__ import annotations

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


class WsListener:
    """Hyperliquid WebSocket 监听线程（多DEX版本）

    订阅 clearinghouseState 推送（所有活跃DEX），将非快照更新放入队列供主线程消费。

    Args:
        ws_url: WebSocket端点URL
        leader_addr: 目标用户（Leader）地址
        change_queue: 主线程提供的队列，用于传递变化数据
        logger: 日志记录器
        initial_backoff: 初始重连退避秒数
        max_backoff: 最大重连退避秒数
        pong_timeout: 心跳超时秒数（超时则主动重连）
        perp_dexes: 需要订阅的perp DEX列表（默认全部活跃DEX）
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

        self._connected: bool = False
        self._running: bool = False
        self._thread: Optional[threading.Thread] = None
        self._lock = threading.Lock()
        self._last_pong_time: float = 0.0
        self._snapshot_seen: dict = {}  # 跟踪每个频道是否已收到快照 {channel: True}
        self._last_orders_log_time: float = 0  # openOrders日志限流（每30秒最多一条）
        self._last_fills_log_time: float = 0  # userFills日志限流

    # ------------------------------------------------------------------
    # 公开接口
    # ------------------------------------------------------------------

    @property
    def is_connected(self) -> bool:
        """当前WebSocket连接是否活跃"""
        with self._lock:
            return self._connected

    def start(self) -> None:
        """启动WebSocket监听线程（daemon）"""
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

    def stop(self) -> None:
        """停止WebSocket监听线程"""
        self._running = False
        self._set_connected(False)
        if self._thread and self._thread.is_alive():
            self._thread.join(timeout=5)
        self._logger.info("[WS] 监听线程已停止")

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
            


            # 指数退避
            backoff = min(backoff * 2, self._max_backoff)

        self._logger.info("[WS] 主循环退出")

    def _connect_and_listen(self) -> None:
        """建立WebSocket连接并监听消息"""
        import asyncio

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
                self._logger.info(f"[WS] 已连接 {self._ws_url}")

                # 订阅主DEX clearinghouseState（无dex参数）
                subscribe_msg = {
                    "method": "subscribe",
                    "subscription": {
                        "type": "clearinghouseState",
                        "user": self._leader_addr,
                    },
                }
                await ws.send(json.dumps(subscribe_msg))
                self._logger.info(
                    f"[WS] 已订阅主DEX clearinghouseState (user={self._leader_addr[:8]}...)"
                )

                # 订阅所有活跃perp DEX的clearinghouseState
                subscribed_dexes = []
                for dex in self._perp_dexes:
                    dex_subscribe_msg = {
                        "method": "subscribe",
                        "subscription": {
                            "type": "clearinghouseState",
                            "user": self._leader_addr,
                            "dex": dex,
                        },
                    }
                    await ws.send(json.dumps(dex_subscribe_msg))
                    subscribed_dexes.append(dex)
                self._logger.info(
                    f"[WS] 已订阅 {len(subscribed_dexes)} 个perp DEX: {', '.join(subscribed_dexes)}"
                )

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

                # 重置退避（连接成功）
                self._last_pong_time = time.time()

                # 消息接收循环
                await self._receive_loop(ws)

        except Exception as e:
            self._set_connected(False)
            self._logger.warning(f"[WS] 会话异常: {e}")
            raise

    async def _receive_loop(self, ws: Any) -> None:
        """持续接收WebSocket消息，处理心跳和超时"""
        import asyncio

        # 启动心跳监控协程
        pong_check_task = asyncio.create_task(self._pong_check_loop(ws))

        try:
            async for raw_message in ws:
                if not self._running:
                    break

                try:
                    msg = json.loads(raw_message)
                except json.JSONDecodeError:
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

        支持三种频道:
        - clearinghouseState: 仓位变化（原有）
        - openOrders: 挂单变化（含TP/SL调整）
        - userFills: 成交通知

        过滤 isSnapshot=true 的初始快照，将增量更新推入队列。
        队列数据附带 _dex、_channel、_coins 字段，标识来源和涉及币种。
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

        # === clearinghouseState 频道（原有逻辑） ===
        await self._handle_clearinghouse_state(msg)

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

        # 内容去重：对比orderId+triggerPx签名，挂单未变则跳过
        order_sig = frozenset(
            (str(o.get("oid", "")), str(o.get("triggerPx", "")))
            for o in data if isinstance(o, dict) and o.get("oid")
        )
        sig_attr = f"_last_order_sig_{dex_tag or 'main'}"
        if order_sig == getattr(self, sig_attr, None):
            return  # 挂单内容完全相同，跳过
        setattr(self, sig_attr, order_sig)

        # 提取币种
        coins = set()
        for order in data:
            if isinstance(order, dict):
                coin = order.get("coin", "")
                if coin:
                    coins.add(coin)

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

        # 日志限流：每30秒最多一条
        import time as _time
        now = _time.time()
        if now - self._last_fills_log_time > 30:
            self._last_fills_log_time = now
            self._logger.info(
                f"[WS-FILL] userFills推送 (dex={dex_tag or 'main'}, "
                f"fills={len(data)}, coins={coins})"
            )


    async def _handle_clearinghouse_state(self, msg: Dict[str, Any]) -> None:
        """处理clearinghouseState推送（原有逻辑）"""
        data = msg.get("data")
        if data is None:
            if "clearinghouseState" in msg:
                data = msg
            else:
                return

        if not isinstance(data, dict):
            self._logger.debug(f"[WS] clearinghouseState data非dict({type(data).__name__})，跳过")
            return

        is_snapshot = data.get("isSnapshot", False)
        if is_snapshot:
            self._logger.debug("[WS] 跳过初始快照")
            return

        # 提取dex标识并注入到data中，供下游处理
        # WS推送的data中可能已有dex字段，如果没有则根据subscription推断
        # 主DEX的push没有dex字段，其他DEX的push有dex字段
        if "dex" not in data:
            # 检查subscription中是否有dex信息
            subscription = msg.get("subscription", {})
            dex = subscription.get("dex", "")
            if dex:
                data["_dex"] = dex
            else:
                data["_dex"] = ""  # 主DEX

        # 推入队列供主线程消费
        try:
            self._change_queue.put_nowait(data)
            # 唤醒主循环立即处理
            if self._wakeup_event:
                self._wakeup_event.set()
            dex_tag = data.get("_dex", data.get("dex", ""))
            ch_state = data.get("clearinghouseState", {})
            if isinstance(ch_state, dict) and ch_state:
                ap_list = ch_state.get("assetPositions", [])
            else:
                ap_list = data.get("assetPositions", [])
            coins = []
            for ap in (ap_list or []):
                pos = ap.get("position", {})
                c = pos.get("coin", "?")
                coins.append(c)
            if ap_list:
                self._logger.debug(f"[WS] 推送变化到队列 (dex={dex_tag or 'main'}, coins={coins}, ap_count={len(ap_list)})")
        except queue.Full:
            self._logger.warning("[WS] 队列已满，丢弃本次变化")

    async def _pong_check_loop(self, ws: Any) -> None:
        """心跳监控协程：定期发送ping，检测pong超时

        Hyperliquid WS要求客户端主动发送ping保持连接。
        如果pong超时则关闭连接触发重连。
        """
        import asyncio

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
