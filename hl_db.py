#!/usr/bin/env python3
"""
统一的数据库访问模块
- 管理SQLite连接（WAL模式）
- trades表的CRUD操作
- 所有操作都用try/except包裹，失败返回None或空列表
"""

import sqlite3
import logging
from pathlib import Path
from typing import Optional, List, Dict
from datetime import datetime

logger = logging.getLogger(__name__)

DB_PATH = Path(__file__).parent / 'data' / 'net_value_history.db'


class TradeDB:
    def __init__(self, db_path: str = None):
        self.db_path = db_path or str(DB_PATH)
        self._init_tables()
        self._enable_wal()

    def _get_conn(self):
        """获取数据库连接（每次操作新建，避免多线程问题）"""
        conn = sqlite3.connect(self.db_path, timeout=10)
        conn.row_factory = sqlite3.Row
        return conn

    def _enable_wal(self):
        """开启WAL模式（解决多进程锁）"""
        try:
            conn = self._get_conn()
            conn.execute("PRAGMA journal_mode=WAL")
            conn.close()
            logger.info("[TradeDB] WAL mode enabled")
        except Exception as e:
            logger.warning(f"[TradeDB] Failed to enable WAL: {e}")

    def _init_tables(self):
        """初始化trades表"""
        try:
            conn = self._get_conn()
            conn.execute('''
                CREATE TABLE IF NOT EXISTS trades (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    -- 业务唯一键（幂等性）
                    instance_id TEXT NOT NULL,
                    coin TEXT NOT NULL,
                    side TEXT NOT NULL,
                    open_ts INTEGER NOT NULL,

                    -- 开仓信息
                    entry_px REAL NOT NULL,
                    size REAL NOT NULL,
                    copy_ratio REAL,

                    -- 平仓信息
                    exit_px REAL,
                    close_ts INTEGER,
                    close_size REAL,

                    -- 计算字段
                    pnl REAL,
                    pnl_pct REAL,
                    slippage_pct REAL,

                    -- 状态
                    status TEXT DEFAULT 'open',
                    close_reason TEXT,

                    -- 元数据
                    leader_entry_px REAL,
                    leader_size REAL,
                    created_at TEXT DEFAULT (datetime('now')),
                    updated_at TEXT DEFAULT (datetime('now')),

                    UNIQUE(instance_id, coin, side, open_ts)
                )
            ''')
            conn.execute('CREATE INDEX IF NOT EXISTS idx_trades_instance ON trades(instance_id)')
            conn.execute('CREATE INDEX IF NOT EXISTS idx_trades_coin ON trades(coin)')
            conn.execute('CREATE INDEX IF NOT EXISTS idx_trades_status ON trades(status)')
            conn.execute('CREATE INDEX IF NOT EXISTS idx_trades_open_ts ON trades(open_ts)')
            conn.commit()
            conn.close()
            logger.info("[TradeDB] trades table initialized")
        except Exception as e:
            logger.error(f"[TradeDB] Failed to init tables: {e}")

    def record_open(self, instance_id: str, coin: str, side: str, entry_px: float,
                    size: float, open_ts: int, copy_ratio: float = None,
                    leader_entry_px: float = None, leader_size: float = None) -> Optional[int]:
        """
        记录开仓。返回trade_id，失败返回None。
        幂等性：如果(instance_id, coin, side, open_ts)已存在，返回已存在的id。
        """
        try:
            conn = self._get_conn()
            c = conn.cursor()
            c.execute('''
                INSERT OR IGNORE INTO trades
                (instance_id, coin, side, entry_px, size, open_ts, copy_ratio,
                 leader_entry_px, leader_size, status)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, 'open')
            ''', (instance_id, coin, side, entry_px, size, open_ts, copy_ratio,
                  leader_entry_px, leader_size))
            conn.commit()

            if c.rowcount == 0:
                # 已存在，查询id
                c.execute('''
                    SELECT id FROM trades
                    WHERE instance_id=? AND coin=? AND side=? AND open_ts=?
                ''', (instance_id, coin, side, open_ts))
                row = c.fetchone()
                trade_id = row['id'] if row else None
            else:
                trade_id = c.lastrowid

            conn.close()
            logger.info(f"[TradeDB] Recorded open: trade_id={trade_id}, {instance_id} {side} {coin} {size}@{entry_px}")
            return trade_id
        except Exception as e:
            logger.error(f"[TradeDB] record_open failed: {e}")
            return None

    def record_close(self, instance_id: str, coin: str, side: str, exit_px: float,
                     close_ts: int, close_size: float = None, close_reason: str = 'leader_close') -> bool:
        """
        记录平仓。成功返回True，失败返回False。
        - 查找匹配的open记录（同一instance_id, coin, side, status='open'）
        - 如果找不到，创建一条"孤儿"记录
        - 如果close_size < open的size，标记为partial_closed
        """
        try:
            conn = self._get_conn()
            c = conn.cursor()

            # 查找匹配的open记录
            c.execute('''
                SELECT id, entry_px, size, status FROM trades
                WHERE instance_id=? AND coin=? AND side=? AND status IN ('open', 'partial_closed')
                ORDER BY open_ts DESC LIMIT 1
            ''', (instance_id, coin, side))
            open_trade = c.fetchone()

            if not open_trade:
                # 孤儿记录：创建一条没有open的close记录
                c.execute('''
                    INSERT INTO trades
                    (instance_id, coin, side, entry_px, size, exit_px, open_ts, close_ts,
                     close_size, pnl, pnl_pct, status, close_reason)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, 0, 0, 'orphan_close', ?)
                ''', (instance_id, coin, side, exit_px, close_size or 0, exit_px,
                      close_ts, close_ts, close_size, close_reason))
                conn.commit()
                conn.close()
                logger.warning(f"[TradeDB] Orphan close recorded: {instance_id} {side} {coin} {close_size or 0}@{exit_px}")
                return True

            # 计算pnl
            open_size = open_trade['size']
            entry_px = open_trade['entry_px']
            actual_close_size = close_size or open_size

            pnl = (exit_px - entry_px) * actual_close_size * (1 if side == 'long' else -1)
            pnl_pct = (exit_px / entry_px - 1) * 100 if side == 'long' else (entry_px / exit_px - 1) * 100

            # 判断是否部分平仓
            remaining = open_size - actual_close_size
            new_status = 'closed' if remaining <= 0.001 else 'partial_closed'

            if new_status == 'closed':
                # 完全平仓：更新原记录
                c.execute('''
                    UPDATE trades SET
                        exit_px=?, close_ts=?, close_size=?, pnl=?, pnl_pct=?,
                        status=?, close_reason=?, updated_at=datetime('now')
                    WHERE id=?
                ''', (exit_px, close_ts, actual_close_size, pnl, pnl_pct,
                      new_status, close_reason, open_trade['id']))
            else:
                # 部分平仓：更新原记录为partial_closed，并创建新的partial记录
                c.execute('''
                    UPDATE trades SET
                        size=?, status='partial_closed', updated_at=datetime('now')
                    WHERE id=?
                ''', (remaining, open_trade['id']))

                c.execute('''
                    INSERT INTO trades
                    (instance_id, coin, side, entry_px, size, exit_px, open_ts, close_ts,
                     close_size, pnl, pnl_pct, status, close_reason, leader_entry_px, leader_size)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'closed', ?, ?, ?)
                ''', (instance_id, coin, side, entry_px, actual_close_size, exit_px,
                      open_trade['open_ts'] if 'open_ts' in open_trade.keys() else close_ts,
                      close_ts, actual_close_size, pnl, pnl_pct, close_reason,
                      open_trade['leader_entry_px'] if 'leader_entry_px' in open_trade.keys() else None,
                      open_trade['leader_size'] if 'leader_size' in open_trade.keys() else None))

            conn.commit()
            conn.close()
            logger.info(f"[TradeDB] Recorded close: {instance_id} {side} {coin} {actual_close_size}@{exit_px}, pnl={pnl:.2f}")
            return True
        except Exception as e:
            logger.error(f"[TradeDB] record_close failed: {e}")
            return False

    def get_open_trades(self, instance_id: str = None, coin: str = None) -> List[Dict]:
        """查询未平仓交易"""
        try:
            conn = self._get_conn()
            c = conn.cursor()
            query = "SELECT * FROM trades WHERE status IN ('open', 'partial_closed')"
            params = []
            if instance_id:
                query += " AND instance_id=?"
                params.append(instance_id)
            if coin:
                query += " AND coin=?"
                params.append(coin)
            query += " ORDER BY open_ts DESC"
            c.execute(query, params)
            rows = [dict(row) for row in c.fetchall()]
            conn.close()
            return rows
        except Exception as e:
            logger.error(f"[TradeDB] get_open_trades failed: {e}")
            return []

    def get_trade_stats(self, instance_id: str = None, days: int = 30) -> Dict:
        """计算交易统计（胜率、盈亏比等）"""
        try:
            conn = self._get_conn()
            c = conn.cursor()

            query = '''
                SELECT
                    COUNT(*) as total_trades,
                    SUM(CASE WHEN pnl > 0 THEN 1 ELSE 0 END) as win_count,
                    SUM(CASE WHEN pnl < 0 THEN 1 ELSE 0 END) as loss_count,
                    AVG(CASE WHEN pnl > 0 THEN pnl END) as avg_win,
                    AVG(CASE WHEN pnl < 0 THEN ABS(pnl) END) as avg_loss,
                    SUM(CASE WHEN pnl > 0 THEN pnl ELSE 0 END) as total_win,
                    SUM(CASE WHEN pnl < 0 THEN ABS(pnl) ELSE 0 END) as total_loss
                FROM trades
                WHERE status = 'closed' AND close_ts > ?
            '''
            params = [int(datetime.now().timestamp()) - days * 86400]
            if instance_id:
                query += " AND instance_id=?"
                params.append(instance_id)

            c.execute(query, params)
            row = c.fetchone()
            conn.close()

            if not row or row['total_trades'] == 0:
                return {'total_trades': 0}

            win_rate = row['win_count'] / row['total_trades'] * 100 if row['total_trades'] > 0 else 0
            profit_factor = row['total_win'] / row['total_loss'] if row['total_loss'] > 0 else 0
            payoff_ratio = row['avg_win'] / row['avg_loss'] if row['avg_loss'] and row['avg_loss'] > 0 else 0

            return {
                'total_trades': row['total_trades'],
                'win_count': row['win_count'],
                'loss_count': row['loss_count'],
                'win_rate': win_rate,
                'avg_win': row['avg_win'] or 0,
                'avg_loss': row['avg_loss'] or 0,
                'profit_factor': profit_factor,
                'payoff_ratio': payoff_ratio,
                'total_pnl': (row['total_win'] or 0) - (row['total_loss'] or 0),
            }
        except Exception as e:
            logger.error(f"[TradeDB] get_trade_stats failed: {e}")
            return {'total_trades': 0, 'error': str(e)}


# 单例模式（可选）
_trade_db_instance = None

def get_trade_db() -> TradeDB:
    global _trade_db_instance
    if _trade_db_instance is None:
        _trade_db_instance = TradeDB()
    return _trade_db_instance


if __name__ == '__main__':
    # 测试
    logging.basicConfig(level=logging.INFO)
    db = TradeDB()

    # 测试开仓
    trade_id = db.record_open(
        instance_id='lf', coin='BTC', side='long',
        entry_px=50000, size=0.1, open_ts=int(datetime.now().timestamp()),
        copy_ratio=0.15
    )
    print(f"Open trade: {trade_id}")

    # 测试平仓
    success = db.record_close(
        instance_id='lf', coin='BTC', side='long',
        exit_px=51000, close_ts=int(datetime.now().timestamp())
    )
    print(f"Close trade: {success}")

    # 测试统计
    stats = db.get_trade_stats(instance_id='lf', days=30)
    print(f"Stats: {stats}")
