"""
变化去重器 — 基于哈希和时间窗口去重

职责:
  - 用仓位哈希（coin+szi+leverage+entryPx的MD5）去重
  - 时间窗口5分钟（可配置）
  - should_process(hash) -> bool
  - 定期清理过期记录
  - 可选持久化到JSON文件（重启后恢复去重状态）

设计原理:
  WS推送和轮询可能对同一个变化产生重复通知，去重器确保
  同一个变化在时间窗口内只处理一次，避免重复下单。

  哈希基于coin+szi+leverage+entryPx组合，确保仓位关键参数
  相同的变化被视为同一个变化。

  持久化机制:
  通过state_file参数启用持久化，_seen字典在cleanup和mark_failed时
  原子写入磁盘，重启后自动加载并过滤已过期记录。
"""

from __future__ import annotations

import hashlib
import json
import logging
import os
import threading
import time
from typing import Dict, Optional, Set


class Deduplicator:
    """变化去重器

    基于仓位哈希和时间窗口去重，避免同一变化被重复处理。

    Args:
        window_seconds: 去重时间窗口（秒），默认300秒（5分钟）
        logger: 日志记录器
        state_file: 持久化文件路径，None则不启用持久化
    """

    def __init__(
        self,
        window_seconds: int = 300,
        logger: Optional[logging.Logger] = None,
        state_file: Optional[str] = None,
    ) -> None:
        self._window_seconds = window_seconds
        self._logger = logger or logging.getLogger(__name__)
        self._state_file = state_file  # 持久化文件路径

        # 已处理的哈希记录: {hash_str: timestamp}
        self._seen: Dict[str, float] = {}
        self._lock = threading.Lock()
        self._last_cleanup: float = time.time()
        self._cleanup_interval: float = 60.0  # 每60秒清理一次过期记录

        # 启动时加载持久化记录
        self._load()

    # ------------------------------------------------------------------
    # 持久化
    # ------------------------------------------------------------------

    def _load(self) -> None:
        """从state_file加载_seen字典，过滤已过期记录"""
        if not self._state_file:
            return
        try:
            if not os.path.exists(self._state_file):
                return
            with open(self._state_file, "r", encoding="utf-8") as f:
                data = json.load(f)
            if not isinstance(data, dict):
                self._logger.warning(f"[DEDUP] 持久化文件格式错误，忽略加载")
                return
            # 过滤已过期的记录，只加载窗口内有效的
            now = time.time()
            loaded = 0
            skipped = 0
            for key, timestamp in data.items():
                if not isinstance(timestamp, (int, float)):
                    skipped += 1
                    continue
                if now - timestamp < self._window_seconds:
                    self._seen[key] = float(timestamp)
                    loaded += 1
            self._logger.info(
                f"[DEDUP] 从持久化文件加载{loaded}条有效记录"
                + (f"，跳过{skipped}条过期/无效" if skipped > 0 else "")
            )
        except (json.JSONDecodeError, ValueError) as e:
            self._logger.warning(f"[DEDUP] 持久化文件解析失败，忽略: {e}")
        except Exception as e:
            self._logger.warning(f"[DEDUP] 加载持久化文件异常，忽略: {e}")

    def _save(self) -> None:
        """原子写入_seen字典到state_file"""
        if not self._state_file:
            return
        try:
            tmp_path = self._state_file + ".tmp"
            with open(tmp_path, "w", encoding="utf-8") as f:
                json.dump(self._seen, f, ensure_ascii=False)
            os.replace(tmp_path, self._state_file)
        except Exception as e:
            # 持久化失败只warning，不影响主流程
            self._logger.warning(f"[DEDUP] 持久化写入失败: {e}")

    # ------------------------------------------------------------------
    # 公开接口
    # ------------------------------------------------------------------

    @staticmethod
    def compute_hash(
        coin: str,
        szi: str,
        leverage: str,
        entry_px: str,
    ) -> str:
        """计算仓位变化的唯一哈希

        基于coin+szi+leverage+entryPx组合的MD5哈希。

        Args:
            coin: 币种名称
            szi: 仓位数
            leverage: 杠杆值
            entry_px: 入场价格

        Returns:
            32字符的MD5十六进制字符串
        """
        raw = f"{coin}|{szi}|{leverage}|{entry_px}"
        return hashlib.md5(raw.encode("utf-8")).hexdigest()

    def should_process(
        self,
        change_hash: str,
    ) -> bool:
        """判断该变化是否应该处理

        如果该哈希在时间窗口内已处理过，返回False（重复）。
        否则记录该哈希并返回True（允许处理）。

        Args:
            change_hash: 变化的哈希值

        Returns:
            True=应处理（新变化），False=跳过（重复变化）
        """
        now = time.time()

        with self._lock:
            # 检查是否已存在
            if change_hash in self._seen:
                last_seen = self._seen[change_hash]
                if now - last_seen < self._window_seconds:
                    self._logger.debug(
                        f"[DEDUP] 跳过重复变化 hash={change_hash[:8]}... "
                        f"(距上次{now - last_seen:.0f}s < {self._window_seconds}s)"
                    )
                    return False

            # 记录本次（不每次save，太频繁，等cleanup时统一持久化）
            self._seen[change_hash] = now

            # 定期清理过期记录
            if now - self._last_cleanup >= self._cleanup_interval:
                self._cleanup(now)

        return True

    def should_process_change(
        self,
        coin: str,
        szi: str,
        leverage: str,
        entry_px: str,
    ) -> bool:
        """便捷方法：直接传入变化参数，计算哈希并判断

        Args:
            coin: 币种名称
            szi: 仓位数
            leverage: 杠杆值
            entry_px: 入场价格

        Returns:
            True=应处理，False=跳过
        """
        change_hash = self.compute_hash(coin, szi, leverage, entry_px)
        return self.should_process(change_hash)

    def reset(self) -> None:
        """清空所有去重记录"""
        with self._lock:
            self._seen.clear()
        self._save()  # 清空后也持久化
        self._logger.debug("[DEDUP] 去重记录已清空")

    @property
    def record_count(self) -> int:
        """当前去重记录数量"""
        with self._lock:
            return len(self._seen)

    # ------------------------------------------------------------------
    # 内部实现
    # ------------------------------------------------------------------

    def mark_failed(
        self,
        coin: str,
        szi: str,
        leverage: str,
        entry_px: str = "0",
    ) -> None:
        """处理失败时撤销去重标记，允许下次重试

        当某个仓位变化处理失败（如API报错、类型异常等）时，
        将其从去重记录中移除，确保下次轮询/WS推送时能重新尝试处理。

        Args:
            coin: 币种名称
            szi: 仓位数
            leverage: 杠杆值
            entry_px: 入场价格
        """
        change_hash = self.compute_hash(coin, szi, leverage, entry_px)
        with self._lock:
            if change_hash in self._seen:
                del self._seen[change_hash]
                self._logger.info(f"[DEDUP] 撤销失败操作的去重标记: {coin}")
        # 删除记录后立即持久化，确保下次重试不被误判
        self._save()

    def _cleanup(self, now: float) -> None:
        """清理过期的去重记录

        删除超过时间窗口的记录，避免内存无限增长。

        Args:
            now: 当前时间戳
        """
        expired_keys: list = []
        for key, timestamp in self._seen.items():
            if now - timestamp >= self._window_seconds:
                expired_keys.append(key)

        for key in expired_keys:
            del self._seen[key]

        self._last_cleanup = now

        if expired_keys:
            self._logger.debug(
                f"[DEDUP] 清理{len(expired_keys)}条过期记录, "
                f"剩余{len(self._seen)}条"
            )

        # 清理后持久化
        self._save()
