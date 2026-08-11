"""
状态对比器 — 检测持仓变化

职责:
  - 维护"上次已知的Leader持仓状态"
  - 接收新的持仓快照（从clearinghouseState的assetPositions中提取）
  - 对比出变化：open（新增）、close（关闭）、modify（数量/杠杆变化）
  - 首次调用建立基线，不产生变化
  - 对比关键字段：szi, entryPx, leverage.value
  - 线程安全

设计原理:
  - v2的detect_position_changes函数是无状态的（需要外部传入old/new）
  - v3的StateComparator封装了状态维护，使调用方无需自行管理上次状态
  - 支持从WS推送和轮询两个数据源更新，统一对比逻辑
"""

from __future__ import annotations

import logging
import threading
from copy import deepcopy
from typing import Any, Dict, List, Optional


class PositionChange:
    """持仓变化记录

    Attributes:
        change_type: 变化类型 "open" / "close" / "modify"
        coin: 币种名称
        old_data: 变化前的持仓数据（新增时为None）
        new_data: 变化后的持仓数据（关闭时为None）
    """

    def __init__(
        self,
        change_type: str,
        coin: str,
        old_data: Optional[Dict[str, Any]] = None,
        new_data: Optional[Dict[str, Any]] = None,
    ) -> None:
        self.change_type = change_type
        self.coin = coin
        self.old_data = old_data
        self.new_data = new_data

    def to_dict(self) -> Dict[str, Any]:
        """转换为字典格式，兼容v2的detect_position_changes输出格式"""
        result: Dict[str, Any] = {
            "type": self.change_type,
            "coin": self.coin,
        }

        if self.change_type == "open":
            new = self.new_data or {}
            result.update({
                "old_szi": "0",
                "new_szi": new.get("szi", "0"),
                "leverage": new.get("leverage_value", 1),
                "leverage_type": new.get("leverage_type", "cross"),
                "positionValue": new.get("positionValue", "0"),
            })
        elif self.change_type == "close":
            old = self.old_data or {}
            result.update({
                "old_szi": old.get("szi", "0"),
                "new_szi": "0",
                "leverage": old.get("leverage_value", 1),
                "leverage_type": old.get("leverage_type", "cross"),
            })
        elif self.change_type == "modify":
            old = self.old_data or {}
            new = self.new_data or {}
            result.update({
                "old_szi": old.get("szi", "0"),
                "new_szi": new.get("szi", "0"),
                "leverage": new.get("leverage_value", 1),
                "leverage_type": new.get("leverage_type", "cross"),
                "positionValue": new.get("positionValue", "0"),
                "old_lev": old.get("leverage_value", 1),
                "new_lev": new.get("leverage_value", 1),
            })
            # 细分modify类型
            old_szi = float(old.get("szi", "0"))
            new_szi = float(new.get("szi", "0"))
            old_lev = old.get("leverage_value")
            new_lev = new.get("leverage_value")

            if old_szi != new_szi and old_lev != new_lev:
                pass  # 两者都变，保持modify
            elif old_szi != new_szi:
                # 纯数量变化 → v2兼容: adjust
                if new_szi == 0:
                    result["type"] = "close"
                elif old_szi == 0:
                    result["type"] = "open"
                else:
                    # 检查方向翻转
                    if (old_szi > 0 and new_szi < 0) or (old_szi < 0 and new_szi > 0):
                        result["type"] = "adjust"  # 翻转也是adjust，由上层处理FLIP
                    else:
                        result["type"] = "adjust"
            elif old_lev != new_lev:
                # 纯杠杆变化
                result["type"] = "leverage_change"

        return result

    def __repr__(self) -> str:
        return (
            f"PositionChange({self.change_type}, {self.coin}, "
            f"old_szi={self.old_data.get('szi') if self.old_data else None}, "
            f"new_szi={self.new_data.get('szi') if self.new_data else None})"
        )


class StateComparator:
    """持仓状态对比器

    维护上次已知的Leader持仓状态，接收新快照后对比出变化。
    首次调用仅建立基线，不产生变化。

    Args:
        logger: 日志记录器
    """

    def __init__(self, logger: Optional[logging.Logger] = None) -> None:
        self._logger = logger or logging.getLogger(__name__)
        self._last_state: Dict[str, Dict[str, Any]] = {}
        self._initialized: bool = False
        self._lock = threading.Lock()

    # ------------------------------------------------------------------
    # 公开接口
    # ------------------------------------------------------------------

    def set_baseline(self, positions: Dict[str, Dict[str, Any]]) -> None:
        """手动设置基线状态（用于冷启动时从API获取的初始状态）

        Args:
            positions: 持仓字典 {coin: {szi, leverage_value, ...}}
        """
        with self._lock:
            self._last_state = deepcopy(positions)
            self._initialized = True
        self._logger.info(
            f"[COMPARATOR] 基线已设置: {len(positions)}个持仓"
        )

    def compare(
        self, new_positions: Dict[str, Dict[str, Any]], update_state: bool = True
    ) -> List[PositionChange]:
        """对比新持仓与上次状态，返回变化列表

        首次调用仅建立基线，返回空列表。

        Args:
            new_positions: 新的持仓快照 {coin: {szi, leverage_value, ...}}
            update_state: 是否将 _last_state 更新为 new_positions。
                          多DEX场景下，WS推送是分DEX到达的，
                          调用方应先合并所有DEX的持仓再调用本方法，
                          并传入 update_state=True 以确保 _last_state 反映完整状态。
                          若仅需对比而不更新状态，可传 False。

        Returns:
            变化列表，可能为空
        """
        with self._lock:
            if not self._initialized:
                self._last_state = deepcopy(new_positions)
                self._initialized = True
                self._logger.info(
                    f"[COMPARATOR] 首次快照建立基线: {len(new_positions)}个持仓"
                )
                return []

            changes = self._do_compare(self._last_state, new_positions)
            if update_state:
                self._last_state = deepcopy(new_positions)

        if changes:
            self._logger.debug(
                f"[COMPARATOR] 检测到{len(changes)}个变化: "
                + ", ".join(f"{c.coin}({c.change_type})" for c in changes)
            )

        return changes

    def force_update(self, positions: Dict[str, Dict[str, Any]]) -> None:
        """强制更新当前状态而不产生变化（用于轮询同步后避免重复触发）"""
        with self._lock:
            self._last_state = deepcopy(positions)
            self._initialized = True

    # ------------------------------------------------------------------
    # 内部实现
    # ------------------------------------------------------------------

    def _do_compare(
        self,
        old_state: Dict[str, Dict[str, Any]],
        new_state: Dict[str, Dict[str, Any]],
    ) -> List[PositionChange]:
        """执行实际的对比逻辑

        对比关键字段: szi, entryPx, leverage.value
        变化类型: open, close, modify
        """
        changes: List[PositionChange] = []
        old_coins = set(old_state.keys())
        new_coins = set(new_state.keys())

        # 新增持仓
        for coin in new_coins - old_coins:
            new_pos = new_state[coin]
            szi = float(new_pos.get("szi", 0))
            if szi != 0:
                changes.append(PositionChange(
                    change_type="open",
                    coin=coin,
                    new_data=new_pos,
                ))

        # 关闭持仓
        for coin in old_coins - new_coins:
            old_pos = old_state[coin]
            szi = float(old_pos.get("szi", 0))
            if szi != 0:
                changes.append(PositionChange(
                    change_type="close",
                    coin=coin,
                    old_data=old_pos,
                ))

        # 持仓变化（对比关键字段）
        for coin in old_coins & new_coins:
            old_pos = old_state[coin]
            new_pos = new_state[coin]

            if self._is_significant_change(old_pos, new_pos):
                changes.append(PositionChange(
                    change_type="modify",
                    coin=coin,
                    old_data=old_pos,
                    new_data=new_pos,
                ))

        return changes

    def _is_significant_change(
        self,
        old_pos: Dict[str, Any],
        new_pos: Dict[str, Any],
    ) -> bool:
        """判断是否为显著变化

        对比关键字段: szi, entryPx, leverage.value
        仅当关键字段之一发生变化时视为显著变化。
        """
        # szi变化
        old_szi = float(old_pos.get("szi", 0))
        new_szi = float(new_pos.get("szi", 0))
        if old_szi != new_szi:
            return True

        # leverage.value变化
        old_lev = old_pos.get("leverage_value")
        new_lev = new_pos.get("leverage_value")
        if old_lev != new_lev:
            return True

        # entryPx变化（可能因为加仓导致均价变化）
        old_entry = old_pos.get("entryPx", "0")
        new_entry = new_pos.get("entryPx", "0")
        if old_entry != new_entry:
            # 精度问题：浮点字符串可能因微小差异不同
            try:
                if abs(float(old_entry) - float(new_entry)) > 1e-10:
                    return True
            except (ValueError, TypeError):
                if old_entry != new_entry:
                    return True

        return False

    @staticmethod
    def extract_positions_from_ws(
        ws_data: Dict[str, Any],
    ) -> Dict[str, Dict[str, Any]]:
        """从WebSocket推送的clearinghouseState数据中提取持仓

        HL API返回的coin名已自带DEX前缀（如 xyz:GOLD），无需手动添加。
        主DEX的coin无前缀（如 SUI），其他DEX的coin有前缀（如 xyz:GOLD）。

        Args:
            ws_data: WS推送的data字段（clearinghouseState内容）

        Returns:
            持仓字典 {coin: {szi, leverage_value, leverage_type, entryPx, positionValue, unrealizedPnl, dex}}
        """
        result: Dict[str, Dict[str, Any]] = {}

        # 提取DEX标识（WS推送data中可能含dex字段）
        dex = ws_data.get("dex", ws_data.get("_dex", ""))

        # WS推送结构可能是嵌套的：data.clearinghouseState.assetPositions
        # 也可能是扁平的：data.assetPositions
        ch_state = ws_data.get("clearinghouseState")
        if ch_state and isinstance(ch_state, dict):
            asset_positions = ch_state.get("assetPositions", [])
        else:
            asset_positions = ws_data.get("assetPositions", [])

        for ap in asset_positions:
            pos = ap.get("position", {})
            coin = pos.get("coin", "")
            if not coin:
                continue

            szi = pos.get("szi", "0")
            lev = pos.get("leverage", {})

            result[coin] = {
                "szi": szi,
                "leverage_value": lev.get("value", 1),
                "leverage_type": lev.get("type", "cross"),
                "entryPx": pos.get("entryPx", "0"),
                "positionValue": pos.get("positionValue", "0"),
                "unrealizedPnl": pos.get("unrealizedPnl", "0"),
                "dex": dex,
            }

        return result
