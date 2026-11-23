"""
仓位变化去抖器 — 延迟触发避免重复处理

职责:
  - 收到变化后延迟指定窗口时间（默认500ms）
  - 窗口期内如有同coin的新变化则覆盖（只保留最新）
  - 窗口期结束无新变化后触发 flush_callback
  - 线程安全
  - 支持配置窗口时间

设计原理:
  WebSocket推送可能短时间内连续到达（如仓位调整过程中连续多个增量），
  去抖器确保只在变化"稳定"后才触发后续处理，避免对中间状态的过度响应。
"""

from __future__ import annotations

import logging
import threading
import time
from typing import Any, Callable, Dict, Optional


class Debouncer:
    """仓位变化去抖器

    对每个coin独立去抖：收到变化后等待窗口期，窗口期内新变化覆盖旧值，
    窗口期结束后触发回调。

    Args:
        window_ms: 去抖窗口时间（毫秒），默认500ms
        flush_callback: 去抖完成后的回调函数，接收 (coin, change_data) 参数
        logger: 日志记录器
    """

    def __init__(
        self,
        window_ms: int = 500,
        flush_callback: Optional[Callable[[str, Dict[str, Any]], None]] = None,
        logger: Optional[logging.Logger] = None,
    ) -> None:
        self._window_ms = window_ms
        self._flush_callback = flush_callback
        self._logger = logger or logging.getLogger(__name__)

        # 每个coin的去抖状态: {coin: {"data": ..., "deadline": ...}}
        self._pending: Dict[str, Dict[str, Any]] = {}
        self._lock = threading.Lock()
        self._running: bool = False
        self._checker_thread: Optional[threading.Thread] = None

    # ------------------------------------------------------------------
    # 公开接口
    # ------------------------------------------------------------------

    def start(self) -> None:
        """启动去抖检查线程"""
        if self._running:
            return
        self._running = True
        self._checker_thread = threading.Thread(
            target=self._check_loop,
            daemon=True,
            name="debouncer-checker",
        )
        self._checker_thread.start()
        self._logger.debug(f"[DEBOUNCE] 去抖器已启动 (窗口={self._window_ms}ms)")

    def stop(self) -> None:
        """停止去抖器"""
        self._running = False
        if self._checker_thread and self._checker_thread.is_alive():
            self._checker_thread.join(timeout=2)

    def feed(self, coin: str, change_data: Dict[str, Any]) -> None:
        """输入变化数据

        如果该coin已有待处理变化，覆盖之并重置窗口计时。
        如果没有，创建新的待处理条目。

        Args:
            coin: 币种名称
            change_data: 变化数据字典
        """
        deadline = time.time() + (self._window_ms / 1000.0)

        with self._lock:
            if coin in self._pending:
                self._logger.debug(
                    f"[DEBOUNCE] {coin} 更新待处理数据（窗口重置）"
                )
            else:
                self._logger.debug(f"[DEBOUNCE] {coin} 新增待处理数据")

            self._pending[coin] = {
                "data": change_data,
                "deadline": deadline,
            }

    def flush_all(self) -> None:
        """立即刷新所有待处理的变化（不等待窗口结束）

        用于程序关闭前或降级切换时确保不丢失数据。
        """
        with self._lock:
            pending_items = list(self._pending.items())
            self._pending.clear()

        for coin, entry in pending_items:
            self._invoke_callback(coin, entry["data"])

    @property
    def pending_count(self) -> int:
        """当前待处理的coin数量"""
        with self._lock:
            return len(self._pending)

    # ------------------------------------------------------------------
    # 内部实现
    # ------------------------------------------------------------------

    def _check_loop(self) -> None:
        """后台循环：检查到期的去抖条目并触发回调"""
        while self._running:
            self._check_and_flush()
            # 检查间隔：窗口时间的1/5，确保及时响应
            time.sleep(self._window_ms / 5000.0)

    def _check_and_flush(self) -> None:
        """检查并刷新到期的条目"""
        now = time.time()
        expired: list = []

        with self._lock:
            for coin, entry in list(self._pending.items()):
                if now >= entry["deadline"]:
                    expired.append((coin, entry["data"]))

            for coin, _ in expired:
                del self._pending[coin]

        for coin, data in expired:
            self._invoke_callback(coin, data)

    def _invoke_callback(self, coin: str, data: Dict[str, Any]) -> None:
        """安全调用回调函数"""
        if self._flush_callback is None:
            self._logger.warning(f"[DEBOUNCE] {coin} 无回调函数，丢弃数据")
            return

        try:
            self._flush_callback(coin, data)
        except Exception as e:
            self._logger.error(f"[DEBOUNCE] {coin} 回调异常: {e}")
