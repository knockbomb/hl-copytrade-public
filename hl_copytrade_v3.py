#!/usr/bin/env python3
"""
Hyperliquid 永续合约跟单机器人 v3.0
WebSocket保守混合架构版

在v2基础上增加WebSocket实时通道作为辅助数据源，实现
"WebSocket优先 + 轮询兜底"的保守混合架构。

与v2完全向后兼容，所有跟单逻辑、TP/SL同步、紧急制动等功能保持不变。

新增特性:
  - WebSocket实时监听Leader持仓变化（独立daemon线程）
  - 去抖器(Debouncer)避免短时间内重复处理
  - 状态对比器(StateComparator)统一检测持仓变化
  - 去重器(Deduplicator)防止WS和轮询双重触发
  - WS断开时自动降级到纯轮询模式
  - 健康检查增加ws_connected字段

用法:
  python hl_copytrade_v3.py                          # dry-run模式
  python hl_copytrade_v3.py --live                   # live模式
  python hl_copytrade_v3.py --interval 30            # 30秒轮询间隔
  python hl_copytrade_v3.py --config ./config_v3.yaml

变更日志 (v3.0):
  Phase 1: WebSocket保守混合架构
    - 新增WsListener: WebSocket监听线程
    - 新增Debouncer: 仓位变化去抖器
    - 新增StateComparator: 状态对比器
    - 新增Deduplicator: 变化去重器
    - CopyTradeBot增加WS组件生命周期管理
    - _poll()前处理WS队列中的变化
    - WS变化和轮询变化统一经过: 对比器 → 去抖器 → 去重器 → 跟单引擎
    - WebSocket断开时自动降级到纯轮询
    - 健康检查文件增加ws_connected字段
"""

from __future__ import annotations

import concurrent.futures
import json
import logging
import logging.handlers
import os
import queue
import signal
import sys
import threading
import time
import traceback
from datetime import datetime
from http.server import HTTPServer, BaseHTTPRequestHandler
from pathlib import Path
from typing import Any, Dict, List, Optional, Set, Tuple

# Windows GBK兼容：强制stdout/stderr使用UTF-8
if sys.platform == "win32":
    os.environ.setdefault("PYTHONIOENCODING", "utf-8")
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    sys.stderr.reconfigure(encoding="utf-8", errors="replace")

import requests
from hl_alert_manager import AlertManager
from hl_phase2_monitor import Phase2Monitor
from hl_phase3_monitor import Phase3Monitor
from hl_phase4_stats import Phase4Stats

# HTTP Session for connection pooling (2026-07-02)
_http_session = requests.Session()
_http_session.headers.update({"Content-Type": "application/json"})
import yaml

# v3新增模块
from hl_copytrade_v3 import WsListener, Debouncer, StateComparator, Deduplicator

# ============================================================
# 自定义异常 (P0-3: 明确区分网络超时和API拒绝)
# ============================================================

class ApiError(Exception):
    """Hyperliquid API调用基础异常"""
    pass

class NetworkError(ApiError):
    """网络层错误（超时、连接失败、DNS错误等），可重试"""
    pass

class ApiRejectedError(ApiError):
    """API明确拒绝（参数错误、余额不足等），不应重试"""
    pass


def _classify_api_error(e: Exception) -> Tuple[str, bool]:
    """分类API异常，返回 (error_type: str, should_retry: bool)"""
    if isinstance(e, requests.exceptions.Timeout):
        return "network_timeout", True
    elif isinstance(e, requests.exceptions.ConnectionError):
        return "network_connection", True
    elif isinstance(e, requests.exceptions.HTTPError):
        status_code = getattr(e.response, "status_code", 0) if e.response else 0
        if status_code == 429:
            return "rate_limited", True
        elif 400 <= status_code < 500:
            return "api_rejected", False
        elif status_code >= 500:
            return "server_error", True
        return "http_error", True
    elif isinstance(e, (json.JSONDecodeError, ValueError)):
        return "parse_error", True
    elif isinstance(e, ApiRejectedError):
        return "api_rejected", False
    elif isinstance(e, NetworkError):
        return "network_error", True
    else:
        return "unknown", True


# ============================================================
# JSON格式日志 (P1-8: 结构化日志)
# ============================================================

class JsonFormatter(logging.Formatter):
    """结构化JSON日志格式器，便于ELK/Grafana等分析"""

    def format(self, record: logging.LogRecord) -> str:
        log_entry = {
            "timestamp": datetime.fromtimestamp(record.created).isoformat(),
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
            "module": record.module,
            "func": record.funcName,
            "line": record.lineno,
        }
        for attr in ("coin", "action", "dex", "poll_count", "alert_type"):
            val = getattr(record, attr, None)
            if val is not None:
                log_entry[attr] = val
        if record.exc_info and record.exc_info[0]:
            log_entry["exception"] = self.formatException(record.exc_info)
        return json.dumps(log_entry, ensure_ascii=False)


class TextFormatter(logging.Formatter):
    """兼容人类可读的文本格式"""

    def format(self, record: logging.LogRecord) -> str:
        return super().format(record)


def setup_logger(
    log_file: str,
    level: str = "DEBUG",
    console_level: str = "INFO",
    max_bytes: int = 10 * 1024 * 1024,
    backup_count: int = 5,
    json_format: bool = True,
    log_mode: str = "both",
) -> logging.Logger:
    """配置日志系统，支持JSON结构化输出"""
    logger = logging.getLogger("hl_copytrade_v3")
    logger.setLevel(getattr(logging, level.upper(), logging.DEBUG))
    logger.handlers.clear()

    Path(log_file).parent.mkdir(parents=True, exist_ok=True)

    file_fmt = JsonFormatter() if json_format else TextFormatter(
        "%(asctime)s [%(levelname)s] %(message)s", datefmt="%Y-%m-%d %H:%M:%S"
    )
    console_fmt = TextFormatter(
        "%(asctime)s [%(levelname)s] %(message)s", datefmt="%H:%M:%S"
    )

    if log_mode in ("both", "file"):
        fh = logging.handlers.RotatingFileHandler(
            log_file, maxBytes=max_bytes, backupCount=backup_count, encoding="utf-8"
        )
        fh.setLevel(getattr(logging, level.upper(), logging.DEBUG))
        fh.setFormatter(file_fmt)
        logger.addHandler(fh)

    if log_mode in ("both", "console"):
        ch = logging.StreamHandler(sys.stdout)
        ch.setLevel(getattr(logging, console_level.upper(), logging.INFO))
        ch.setFormatter(console_fmt)
        logger.addHandler(ch)

    # ===== 事件日志文件 =====
    events_log_file = str(Path(log_file).parent / (Path(log_file).stem + "_events.log"))
    events_fh = logging.handlers.RotatingFileHandler(
        events_log_file, maxBytes=max_bytes, backupCount=backup_count, encoding="utf-8"
    )
    events_fh.setLevel(logging.INFO)
    events_fh.setFormatter(TextFormatter(
        "%(asctime)s | %(message)s", datefmt="%Y-%m-%d %H:%M:%S"
    ))
    # 只记录包含 [EVENT] 标记的日志
    class EventFilter(logging.Filter):
        def filter(self, record):
            return "[EVENT]" in record.getMessage()
    events_fh.addFilter(EventFilter())
    logger.addHandler(events_fh)

    return logger


# ============================================================
# 配置管理 (v3扩展: 新增websocket/debounce/deduplication配置)
# ============================================================

class ConfigManager:
    """集中管理所有配置项，支持从YAML文件加载和热重载"""

    _DEFAULTS: Dict[str, Any] = {
        "account": {
            "leader_addr_env": "HL_LEADER_ADDR",
            "user_main_addr_env": "HL_USER_MAIN_ADDR",
            "user_api_addr_env": "HL_USER_API_ADDR",
            "private_key_env": "HL_API_PK",
        },
        "api": {
            "base_url": "https://api.hyperliquid.xyz",
            "timeout": 15,
            "max_retries": 2,
            "tpsl_max_retries": 10,
            "retry_interval": 5,
            "concurrent_max_workers": 4,
            "concurrent_timeout": 15,
            "dex_query_max_workers": 5,
            "dex_query_timeout": 20,
        },
        "strategy": {
            "fund_ratio": 0.95,
            "max_leverage": 40,
            "confirm_wait_seconds": 3,
            "deviation_tolerance": 0.25,
            "tpsl_size_tolerance": 0.05,
            "price_change_tolerance": 0.001,
            "ioc_price_offset": 0.005,
            "tpsl_limit_offset": 0.005,
            "position_min_value": 0,
        },
        "polling": {
            "interval": 60,
            "status_interval": 600,
            "reconciliation_interval": 30,
            "emergency_stop_check_interval": 5,
            "tpsl_sync_interval": 10,
            "tpsl_sync_time_seconds": 900,
        },
        "flip": {
            "max_retries": 30,
            "base_interval": 5,
            "max_interval": 300,
            "notify_interval": 300,
            "backoff_base": 3,
        },
        "paths": {
            "state_file": "hl_copytrade_state.json",
            "log_file": "hl_copytrade.log",
            "emergency_stop_file": "~/hl_copytrade/EMERGENCY_STOP",
            "alert_file": "critical_alerts.json",
            "health_file": "health.json",
            "env_file": "~/hl_copytrade/.env",
        },
        "health": {
            "enabled": True,
            "http_port": 8998,
            "max_consecutive_failures": 5,
        },
        "logging": {
            "level": "DEBUG",
            "console_level": "INFO",
            "max_bytes": 10485760,
            "backup_count": 5,
            "json_format": True,
            "log_mode": "both",
        },
        "runtime": {
            "max_restarts": 100,
            "restart_delay": 30,
            "hot_reload_config": True,
        },
        # ===== v3新增配置 =====
        "websocket": {
            "enabled": True,
            "url": "wss://api.hyperliquid.xyz/ws",
            "subscription_type": "clearinghouseState",
            "reconnect": {
                "initial_backoff": 1,
                "max_backoff": 300,
                "pong_timeout": 30,
            },
        },
        "debounce": {
            "window_ms": 500,
        },
        "deduplication": {
            "window_seconds": 300,
        },
        "sz_decimals": {
            "BTC": 5, "ETH": 4, "SOL": 2, "INJ": 1, "LINK": 1,
            "RENDER": 1, "HYPE": 2, "ATOM": 2, "AVAX": 2, "BNB": 3,
            "DOGE": 0, "OP": 1, "ARB": 1, "LTC": 2, "MATIC": 1,
            "DYDX": 1, "APE": 1, "CRV": 1, "LDO": 1, "STX": 1,
            "SUI": 1, "kPEPE": 0,
        },
        # ===== 告警推送配置 (Phase 1: 2026-07-25) =====
        "alert": {
            "enabled": True,
            "webhook_url": "",
            "dedup_hours": 24,
            "bot_name": "V3跟单",
        },
    }

    def __init__(self, config_path: Optional[str] = None, script_dir: str = ""):
        self._config_path = config_path
        self._script_dir = script_dir or os.path.dirname(os.path.abspath(__file__))
        self._raw: Dict[str, Any] = {}
        self._last_load_time: float = 0
        self._load_config()
        self._resolve_paths()
        self._load_secrets()

    def _deep_merge(self, base: Dict, override: Dict) -> Dict:
        """深度合并字典，override中的值覆盖base"""
        result = dict(base)
        for k, v in override.items():
            if k in result and isinstance(result[k], dict) and isinstance(v, dict):
                result[k] = self._deep_merge(result[k], v)
            else:
                result[k] = v
        return result

    def _load_config(self) -> None:
        """加载配置文件，与默认值深度合并"""
        defaults = self._DEFAULTS.copy()
        if self._config_path and os.path.exists(self._config_path):
            try:
                with open(self._config_path, "r", encoding="utf-8") as f:
                    file_config = yaml.safe_load(f) or {}
                self._raw = self._deep_merge(defaults, file_config)
                self._last_load_time = time.time()
            except Exception as e:
                print(f"[WARN] 加载配置文件失败，使用默认值: {e}")
                self._raw = defaults
        else:
            self._raw = defaults

    def _resolve_paths(self) -> None:
        """解析相对路径为绝对路径"""
        paths = self._raw.get("paths", {})
        for key in ("state_file", "log_file", "alert_file", "health_file"):
            if key in paths and not os.path.isabs(paths[key]):
                paths[key] = os.path.join(self._script_dir, paths[key])
        for key in ("emergency_stop_file", "env_file"):
            if key in paths:
                paths[key] = os.path.expanduser(paths[key])

    def _load_secrets(self) -> None:
        """从环境变量或.env文件加载敏感配置"""
        account = self._raw.setdefault("account", {})
        env_cfg = self._load_env_config()
        for key in ("HL_LEADER_ADDR", "HL_USER_MAIN_ADDR", "HL_USER_API_ADDR"):
            if key in env_cfg:
                account[key] = env_cfg[key]
        account["private_key"] = self._load_private_key()

    def _load_env_config(self) -> Dict[str, str]:
        """从环境变量或.env文件加载敏感配置"""
        config: Dict[str, str] = {}
        for key in ("HL_LEADER_ADDR", "HL_USER_MAIN_ADDR", "HL_USER_API_ADDR"):
            val = os.environ.get(key)
            if val:
                config[key] = val
        env_path = self._raw.get("paths", {}).get("env_file", "")
        if not env_path:
            env_path = os.path.expanduser("~/hl_copytrade/.env")
        if os.path.exists(env_path):
            try:
                with open(env_path, "r", encoding="utf-8") as f:
                    for line in f:
                        line = line.strip()
                        if "=" in line and not line.startswith("#"):
                            k, v = line.split("=", 1)
                            k = k.strip()
                            v = v.strip().strip('"').strip("'")
                            if k in ("HL_LEADER_ADDR", "HL_USER_MAIN_ADDR", "HL_USER_API_ADDR") and k not in config:
                                config[k] = v
            except Exception:
                pass
        return config

    def _load_private_key(self) -> str:
        """按优先级加载API私钥：环境变量 > .env文件"""
        pk_env = self._raw.get("account", {}).get("private_key_env", "HL_API_PK")
        env_pk = os.environ.get(pk_env)
        if env_pk and env_pk.startswith("0x") and len(env_pk) == 66:
            return env_pk
        env_path = self._raw.get("paths", {}).get("env_file", "")
        if not env_path:
            env_path = os.path.expanduser("~/hl_copytrade/.env")
        if os.path.exists(env_path):
            try:
                with open(env_path, "r", encoding="utf-8") as f:
                    for line in f:
                        line = line.strip()
                        if line.startswith(f"{pk_env}="):
                            pk = line[len(f"{pk_env}="):].strip().strip('"').strip("'")
                            if pk and pk.startswith("0x") and len(pk) == 66:
                                return pk
            except Exception:
                pass
        raise RuntimeError(f"未找到API私钥：请设置环境变量{pk_env}或配置.env文件")

    def reload_if_needed(self) -> bool:
        """热加载：检查配置文件修改时间，如有变化则重新加载"""
        if not self._config_path or not os.path.exists(self._config_path):
            return False
        if not self._raw.get("runtime", {}).get("hot_reload_config", True):
            return False
        try:
            mtime = os.path.getmtime(self._config_path)
            if mtime > self._last_load_time:
                self._load_config()
                self._resolve_paths()
                self._load_secrets()
                return True
        except Exception:
            pass
        return False

    # ---- 便捷属性访问 ----

    def _get(self, *keys: str, default: Any = None) -> Any:
        """嵌套字典取值"""
        d = self._raw
        for k in keys:
            if isinstance(d, dict) and k in d:
                d = d[k]
            else:
                return default
        return d

    @property
    def leader_addr(self) -> str:
        val = self._get("account", "HL_LEADER_ADDR")
        if not val:
            raise RuntimeError("未配置HL_LEADER_ADDR")
        return val

    @property
    def user_main_addr(self) -> str:
        val = self._get("account", "HL_USER_MAIN_ADDR")
        if not val:
            raise RuntimeError("未配置HL_USER_MAIN_ADDR")
        return val

    @property
    def user_api_addr(self) -> str:
        """API钱包地址（钱包轮换时会变化）"""
        val = self._get("account", "HL_USER_API_ADDR")
        if not val:
            raise RuntimeError("未配置HL_USER_API_ADDR")
        return val

    @property
    def private_key(self) -> str:
        return self._get("account", "private_key", default="")

    @property
    def base_url(self) -> str:
        return self._get("api", "base_url", default="https://api.hyperliquid.xyz")

    @property
    def api_timeout(self) -> int:
        return self._get("api", "timeout", default=15)

    @property
    def max_retries(self) -> int:
        return self._get("api", "max_retries", default=2)

    @property
    def tpsl_max_retries(self) -> int:
        return self._get("api", "tpsl_max_retries", default=10)

    @property
    def retry_interval(self) -> int:
        return self._get("api", "retry_interval", default=5)

    @property
    def concurrent_max_workers(self) -> int:
        return self._get("api", "concurrent_max_workers", default=4)

    @property
    def concurrent_timeout(self) -> int:
        return self._get("api", "concurrent_timeout", default=15)

    @property
    def dex_query_max_workers(self) -> int:
        return self._get("api", "dex_query_max_workers", default=5)

    @property
    def dex_query_timeout(self) -> int:
        return self._get("api", "dex_query_timeout", default=20)

    @property
    def fund_ratio(self) -> float:
        return self._get("strategy", "fund_ratio", default=0.95)

    @property
    def max_leverage(self) -> int:
        return self._get("strategy", "max_leverage", default=40)

    @property
    def confirm_wait_seconds(self) -> int:
        return self._get("strategy", "confirm_wait_seconds", default=3)

    @property
    def deviation_tolerance(self) -> float:
        return self._get("strategy", "deviation_tolerance", default=0.25)

    @property
    def position_min_value(self) -> float:
        return self._get("strategy", "position_min_value", default=100.0)

    @property
    def tpsl_size_tolerance(self) -> float:
        return self._get("strategy", "tpsl_size_tolerance", default=0.05)

    @property
    def price_change_tolerance(self) -> float:
        return self._get("strategy", "price_change_tolerance", default=0.001)

    @property
    def ioc_price_offset(self) -> float:
        return self._get("strategy", "ioc_price_offset", default=0.005)

    @property
    def tpsl_limit_offset(self) -> float:
        return self._get("strategy", "tpsl_limit_offset", default=0.005)

    @property
    def poll_interval(self) -> int:
        return self._get("polling", "interval", default=60)

    @property
    def status_interval(self) -> int:
        return self._get("polling", "status_interval", default=600)

    @property
    def reconciliation_interval(self) -> int:
        return self._get("polling", "reconciliation_interval", default=30)

    @property
    def emergency_stop_check_interval(self) -> int:
        return self._get("polling", "emergency_stop_check_interval", default=5)

    @property
    def tpsl_sync_interval(self) -> int:
        return self._get("polling", "tpsl_sync_interval", default=10)

    @property
    def tpsl_sync_time_seconds(self) -> int:
        """定时TP/SL同步间隔（秒），默认900秒=15分钟，不受轮询频率影响"""
        return self._get("polling", "tpsl_sync_time_seconds", default=900)

    @property
    def flip_max_retries(self) -> int:
        return self._get("flip", "max_retries", default=30)

    @property
    def flip_base_interval(self) -> int:
        return self._get("flip", "base_interval", default=5)

    @property
    def flip_max_interval(self) -> int:
        return self._get("flip", "max_interval", default=300)

    @property
    def flip_notify_interval(self) -> int:
        return self._get("flip", "notify_interval", default=300)

    @property
    def flip_backoff_base(self) -> int:
        return self._get("flip", "backoff_base", default=3)

    @property
    def state_file(self) -> str:
        return self._get("paths", "state_file", default="hl_copytrade_state.json")

    @property
    def log_file(self) -> str:
        return self._get("paths", "log_file", default="hl_copytrade.log")

    @property
    def emergency_stop_file(self) -> str:
        return self._get("paths", "emergency_stop_file", default=os.path.expanduser("~/hl_copytrade/EMERGENCY_STOP"))

    @property
    def pause_file(self) -> str:
        """暂停跟单信号文件路径：存在时暂停跟单但保持运行，不执行新交易"""
        es = self.emergency_stop_file
        es_dir = os.path.dirname(es)
        es_base = os.path.basename(es)
        # EMERGENCY_STOP -> PAUSE, EMERGENCY_STOP_HIGHFREQ -> PAUSE_HIGHFREQ
        pause_name = es_base.replace("EMERGENCY_STOP", "PAUSE")
        return os.path.join(es_dir, pause_name)

    @property
    def alert_file(self) -> str:
        return self._get("paths", "alert_file", default="critical_alerts.json")

    @property
    def health_file(self) -> str:
        return self._get("paths", "health_file", default="health.json")

    @property
    def health_enabled(self) -> bool:
        return self._get("health", "enabled", default=True)

    @property
    def health_http_port(self) -> int:
        return self._get("health", "http_port", default=8998)

    @property
    def max_consecutive_failures(self) -> int:
        return self._get("health", "max_consecutive_failures", default=5)

    @property
    def max_restarts(self) -> int:
        return self._get("runtime", "max_restarts", default=100)

    @property
    def restart_delay(self) -> int:
        return self._get("runtime", "restart_delay", default=30)

    @property
    def sz_decimals_defaults(self) -> Dict[str, int]:
        return dict(self._get("sz_decimals", default={}))

    # ---- v3新增属性 ----

    @property
    def ws_enabled(self) -> bool:
        """WebSocket是否启用"""
        return self._get("websocket", "enabled", default=True)

    @property
    def ws_url(self) -> str:
        """WebSocket端点URL"""
        return self._get("websocket", "url", default="wss://api.hyperliquid.xyz/ws")

    @property
    def ws_subscription_type(self) -> str:
        """WebSocket订阅类型"""
        return self._get("websocket", "subscription_type", default="clearinghouseState")

    @property
    def ws_reconnect_initial_backoff(self) -> float:
        """WS重连初始退避秒数"""
        return float(self._get("websocket", "reconnect", "initial_backoff", default=1))

    @property
    def ws_reconnect_max_backoff(self) -> float:
        """WS重连最大退避秒数"""
        return float(self._get("websocket", "reconnect", "max_backoff", default=300))

    @property
    def ws_pong_timeout(self) -> float:
        """WS心跳超时秒数"""
        return float(self._get("websocket", "reconnect", "pong_timeout", default=30))

    @property
    def debounce_window_ms(self) -> int:
        """去抖窗口毫秒数"""
        return self._get("debounce", "window_ms", default=500)

    @property
    def dedup_window_seconds(self) -> int:
        """去重时间窗口秒数"""
        return self._get("deduplication", "window_seconds", default=300)

    # ---- 安全护栏属性 (2026-07-01) ----

    @property
    def safety_enabled(self) -> bool:
        """安全护栏总开关"""
        return self._get("safety", "enabled", default=True)

    @property
    def change_ratio_circuit_breaker(self) -> float:
        """护栏2: 变化幅度熔断阈值(0-1)"""
        return self._get("safety", "change_ratio_circuit_breaker", default=0.5)

    @property
    def require_all_dex_success(self) -> bool:
        """护栏3: 是否要求所有DEX查询成功"""
        return self._get("safety", "require_all_dex_success", default=True)

    @property
    def double_confirm_enabled(self) -> bool:
        """护栏4: 关键操作二次确认是否启用"""
        return self._get("safety", "double_confirm_enabled", default=True)

    # ---- 告警推送属性 (Phase 1: 2026-07-25) ----

    @property
    def alert_enabled(self) -> bool:
        """告警推送总开关"""
        return self._get("alert", "enabled", default=True)

    @property
    def alert_webhook_url(self) -> str:
        """飞书Webhook URL（从环境变量读取，遵循敏感信息零容忍原则）"""
        env_key = self._get("alert", "webhook_url_env", default="HL_FEISHU_WEBHOOK")
        return os.getenv(env_key, "")

    @property
    def alert_dedup_hours(self) -> int:
        """告警去重时间窗口(小时)"""
        return self._get("alert", "dedup_hours", default=24)

    @property
    def alert_bot_name(self) -> str:
        """告警机器人名称"""
        return self._get("alert", "bot_name", default="V3跟单")


# ============================================================
# DEX缓存 (P1-6)
# ============================================================

class DexCache:
    """封装perp DEX列表缓存和sz精度映射，线程安全"""

    def __init__(self, config: ConfigManager, logger: logging.Logger):
        self._config = config
        self._logger = logger
        self._perp_dex_cache: Optional[List[str]] = None
        self._sz_decimals_map: Dict[str, int] = dict(config.sz_decimals_defaults)
        self._lock = threading.Lock()

    def get_perp_dex_list(self, base_url: str, timeout: int = 15) -> List[str]:
        """获取所有perp DEX列表，结果缓存"""
        with self._lock:
            if self._perp_dex_cache is not None:
                return self._perp_dex_cache
        try:
            resp = requests.post(
                f"{base_url}/info", json={"type": "perpDexs"}, timeout=timeout,
            )
            resp.raise_for_status()
            data = resp.json()
            result = [""]
            for d in data:
                if d is not None and isinstance(d, dict):
                    name = d.get("name", "")
                    assets = d.get("assetToStreamingOiCap", [])
                    if name and len(assets) > 0:
                        result.append(name)
            with self._lock:
                self._perp_dex_cache = result
            return result
        except Exception as e:
            self._logger.warning(f"获取perp DEX列表失败: {e}")
            return [""]

    def invalidate_dex_cache(self) -> None:
        with self._lock:
            self._perp_dex_cache = None

    @property
    def sz_decimals_map(self) -> Dict[str, int]:
        with self._lock:
            return dict(self._sz_decimals_map)

    def update_sz_decimals(self, updates: Dict[str, int]) -> None:
        with self._lock:
            self._sz_decimals_map.update(updates)

    def get_sz_decimals(self, coin: str) -> int:
        with self._lock:
            return self._sz_decimals_map.get(coin, 4)

    def init_from_api(self, base_url: str, timeout: int = 15) -> None:
        dexes = self.get_perp_dex_list(base_url, timeout)
        updates: Dict[str, int] = {}
        for dex in dexes:
            try:
                payload = {"type": "meta"}
                if dex:
                    payload["dex"] = dex
                resp = _http_session.post(f"{base_url}/info", json=payload, timeout=timeout)
                resp.raise_for_status()
                meta = resp.json()
                universe = meta.get("universe", [])
                for asset in universe:
                    name = asset.get("name", "")
                    sz_dec = asset.get("szDecimals", 4)
                    if name:
                        updates[name] = sz_dec
            except Exception as e:
                self._logger.warning(f"获取{dex or '主DEX'}meta失败: {e}")
        if updates:
            self.update_sz_decimals(updates)
            self._logger.info(f"已加载 {len(self.sz_decimals_map)} 个币种的sz精度（含HIP-3）")
        else:
            self._logger.warning("未能从API获取任何sz精度，使用已知默认值")


# ============================================================
# 健康检查 (v3扩展: ws_connected字段)
# ============================================================

class HealthChecker:
    """写入health.json并开HTTP端口供监控查询"""

    def __init__(self, config: ConfigManager, logger: logging.Logger):
        self._config = config
        self._logger = logger
        self._health_data: Dict[str, Any] = {
            "status": "starting",
            "last_poll": None,
            "poll_count": 0,
            "consecutive_failures": 0,
            "last_success": None,
            "last_error": None,
            "pid": os.getpid(),
            "started_at": datetime.now().isoformat(),
            "ws_connected": False,  # v3新增
            # P3#9: 运行统计增强
            "copy_ratio": 0.0,
            "leader_positions_count": 0,
            "user_positions_count": 0,
            "today_opens": 0,
            "today_adjusts": 0,
            "today_closes": 0,
            "today_recons": 0,
            "dedup_records": 0,
        }
        self._lock = threading.Lock()
        self._server: Optional[HTTPServer] = None
        self._server_thread: Optional[threading.Thread] = None

    def update(
        self,
        *,
        poll_count: Optional[int] = None,
        success: bool = True,
        error: Optional[str] = None,
        ws_connected: Optional[bool] = None,
    ) -> None:
        """更新健康状态"""
        with self._lock:
            if poll_count is not None:
                self._health_data["poll_count"] = poll_count
            self._health_data["last_poll"] = datetime.now().isoformat()
            if ws_connected is not None:
                self._health_data["ws_connected"] = ws_connected  # v3新增
            if success:
                self._health_data["consecutive_failures"] = 0
                self._health_data["last_success"] = datetime.now().isoformat()
                self._health_data["status"] = "healthy"
            else:
                self._health_data["consecutive_failures"] += 1
                self._health_data["last_error"] = error or "unknown"
                if self._health_data["consecutive_failures"] >= self._config.max_consecutive_failures:
                    self._health_data["status"] = "unhealthy"
                else:
                    self._health_data["status"] = "degraded"
            self._write_health_file()

    def _write_health_file(self) -> None:
        health_path = self._config.health_file
        try:
            Path(health_path).parent.mkdir(parents=True, exist_ok=True)
            tmp_path = health_path + ".tmp"
            with open(tmp_path, "w", encoding="utf-8") as f:
                json.dump(self._health_data, f, indent=2, ensure_ascii=False)
            os.replace(tmp_path, health_path)
        except Exception as e:
            self._logger.warning(f"写入health.json失败: {e}")

    def start_http_server(self) -> None:
        if not self._config.health_enabled:
            self._logger.info("[HEALTH] HTTP健康检查已禁用")
            return
        port = self._config.health_http_port
        health_path = self._config.health_file
        logger = self._logger

        class HealthHandler(BaseHTTPRequestHandler):
            def do_GET(self) -> None:
                if self.path in ("/health", "/"):
                    try:
                        with open(health_path, "r", encoding="utf-8") as f:
                            data = f.read()
                        self.send_response(200)
                        self.send_header("Content-Type", "application/json")
                        self.end_headers()
                        self.wfile.write(data.encode("utf-8"))
                    except FileNotFoundError:
                        self.send_response(503)
                        self.send_header("Content-Type", "application/json")
                        self.end_headers()
                        self.wfile.write(b'{"status": "unavailable"}')
                    except Exception as e:
                        self.send_response(500)
                        self.end_headers()
                        self.wfile.write(f'{{"error": "{e}"}}'.encode())
                else:
                    self.send_response(404)
                    self.end_headers()

            def log_message(self, format: str, *args: Any) -> None:
                logger.debug(f"[HEALTH-HTTP] {format % args}")

        try:
            self._server = HTTPServer(("0.0.0.0", port), HealthHandler)
            self._server_thread = threading.Thread(
                target=self._server.serve_forever, daemon=True, name="health-http-server",
            )
            self._server_thread.start()
            self._logger.info(f"[HEALTH] HTTP健康检查已启动: http://0.0.0.0:{port}/health")
        except Exception as e:
            self._logger.warning(f"[HEALTH] HTTP服务启动失败: {e}，仅使用health.json文件")

    def update_stats(
        self,
        *,
        copy_ratio: Optional[float] = None,
        leader_positions_count: Optional[int] = None,
        user_positions_count: Optional[int] = None,
        today_opens: Optional[int] = None,
        today_adjusts: Optional[int] = None,
        today_closes: Optional[int] = None,
        today_recons: Optional[int] = None,
        dedup_records: Optional[int] = None,
    ) -> None:
        """更新运行统计数据（P3#9），只更新非None字段"""
        with self._lock:
            if copy_ratio is not None:
                self._health_data["copy_ratio"] = copy_ratio
            if leader_positions_count is not None:
                self._health_data["leader_positions_count"] = leader_positions_count
            if user_positions_count is not None:
                self._health_data["user_positions_count"] = user_positions_count
            if today_opens is not None:
                self._health_data["today_opens"] = today_opens
            if today_adjusts is not None:
                self._health_data["today_adjusts"] = today_adjusts
            if today_closes is not None:
                self._health_data["today_closes"] = today_closes
            if today_recons is not None:
                self._health_data["today_recons"] = today_recons
            if dedup_records is not None:
                self._health_data["dedup_records"] = dedup_records
            self._write_health_file()

    def stop(self) -> None:
        if self._server:
            self._server.shutdown()


# ============================================================
# API 工具函数
# ============================================================

def hl_post(payload: dict, base_url: str, timeout: int = 15) -> dict:
    """直接POST到Hyperliquid API，带明确异常分类"""
    try:
        resp = requests.post(f"{base_url}/info", json=payload, timeout=timeout)
        resp.raise_for_status()
        return resp.json()
    except requests.exceptions.Timeout as e:
        raise NetworkError(f"API超时: {e}") from e
    except requests.exceptions.ConnectionError as e:
        raise NetworkError(f"连接失败: {e}") from e
    except requests.exceptions.HTTPError as e:
        status = e.response.status_code if e.response else 0
        if 400 <= status < 500 and status != 429:
            raise ApiRejectedError(f"API拒绝({status}): {e}") from e
        raise NetworkError(f"HTTP错误({status}): {e}") from e
    except (json.JSONDecodeError, ValueError) as e:
        raise NetworkError(f"响应解析失败: {e}") from e


def get_account_value(address: str, config: ConfigManager, logger: Optional[logging.Logger] = None) -> float:
    """获取HL账户真实总权益（直接使用 spotClearinghouseState USDC total）
    
    Hyperliquid 的统一账户架构中，合约保证金通过 hold 机制体现在现货 USDC 中，
    因此 spotClearinghouseState 的 USDC total 即为账户总权益，无需再加减合约数据。
    原公式 spot_usdc + accountValue - marginUsed 存在概念混淆，已统一修正。
    """
    try:
        spot_data = hl_post(
            {"type": "spotClearinghouseState", "user": address},
            base_url=config.base_url, timeout=config.api_timeout,
        )
        for b in spot_data.get("balances", []):
            if b.get("coin") == "USDC":
                return float(b.get("total", 0))
        return 0.0
    except NetworkError as e:
        if logger:
            logger.warning(f"获取账户总值网络错误({address[:8]}...): {e}")
        raise
    except ApiRejectedError as e:
        if logger:
            logger.error(f"获取账户总值API拒绝({address[:8]}...): {e}")
        raise
    except Exception as e:
        if logger:
            logger.error(f"获取账户总值异常({address[:8]}...): {e}")
        raise



# ============================================================
# 安全护栏1: Fills API交叉验证 (2026-07-01)
# ============================================================

def get_margin_used(address: str, config: ConfigManager, logger: Optional[logging.Logger] = None) -> float:
    """获取账户当前已占用的保证金总额（汇总所有 DEX：加密 perp + xyz 代币化股票/商品）"""
    total = 0.0
    any_ok = False
    for dex in ("", "xyz"):
        try:
            payload = {"type": "clearinghouseState", "user": address}
            if dex:
                payload["dex"] = dex
            data = hl_post(
                payload,
                base_url=config.base_url, timeout=config.api_timeout,
            )
            margin_summary = data.get("marginSummary", {})
            total += float(margin_summary.get("totalMarginUsed", 0))
            any_ok = True
        except Exception as e:
            if logger:
                logger.warning(f"获取保证金占用异常(dex={dex or 'perp'}, {address[:8]}...): {e}")
    return total if any_ok else 0.0  # 全部失败才返回0，降级为原有逻辑




# ============================================================
# 安全护栏3: 带状态返回的持仓查询 (2026-07-01)
# ============================================================

def get_positions_with_status(address: str, config: ConfigManager, dex_cache: DexCache, logger: Optional[logging.Logger] = None, prev_positions: Optional[Dict[str, dict]] = None) -> Tuple[Dict[str, dict], bool, List[str]]:
    """获取持仓，同时返回查询状态

    Args:
        prev_positions: 上一轮Leader持仓，用于分级容忍（空DEX失败不触发SAFETY_3）

    Returns:
        (positions, all_success, failed_dexes)
        - positions: 持仓字典
        - all_success: 关键DEX查询是否成功（分级容忍后，空DEX失败不算失败）
        - failed_dexes: 失败的关键DEX列表
    """
    dexes = dex_cache.get_perp_dex_list(config.base_url, config.api_timeout)
    result: Dict[str, dict] = {}
    failed_dexes: List[str] = []
    all_success = True

    if len(dexes) <= 1:
        try:
            result = _query_positions_for_dex(address, "", config.base_url, config.api_timeout)
        except Exception as e:
            if logger:
                logger.warning(f"[SAFETY-3] 查询主DEX持仓失败: {e}")
            failed_dexes.append("")
            all_success = False
        return result, all_success, failed_dexes

    max_workers = min(config.dex_query_max_workers, len(dexes))
    with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_to_dex = {
            executor.submit(_query_positions_for_dex, address, dex, config.base_url, config.api_timeout): dex
            for dex in dexes
        }
        done, not_done = concurrent.futures.wait(future_to_dex.keys(), timeout=config.dex_query_timeout)
        for future in done:
            dex = future_to_dex[future]
            try:
                dex_result = future.result()
                result.update(dex_result)
            except Exception as e:
                if logger:
                    logger.warning(f"[SAFETY-3] 查询{dex or '主DEX'}持仓失败: {e}")
                failed_dexes.append(dex or "")
                all_success = False
        for future in not_done:
            dex = future_to_dex[future]
            if logger:
                logger.warning(f"[SAFETY-3] 查询{dex or '主DEX'}持仓超时({config.dex_query_timeout}s)")
            failed_dexes.append(dex or "")
            all_success = False

    # ===== 多数派表决 (2026-07-30): 只要有DEX成功就继续，全部失败才跳过 =====
    total_dex = len(dexes)
    success_count = total_dex - len(failed_dexes)
    if success_count > 0:
        all_success = True
        if failed_dexes:
            if logger:
                for fd in failed_dexes:
                    logger.warning(f"[SAFETY-3] DEX查询失败，但多数派({success_count}/{total_dex})成功，继续执行")
    else:
        all_success = False
        if logger:
            logger.critical(f"[SAFETY-3] 所有DEX查询失败({total_dex}个)，本轮跳过")

    return result, all_success, failed_dexes


# ============================================================
# 安全护栏4: 关键操作二次确认 (2026-07-01)
# ============================================================

def double_confirm_position_exists(leader_addr: str, coin: str, config: ConfigManager, dex_cache: DexCache, logger: Optional[logging.Logger] = None) -> bool:
    """护栏4: 二次确认Leader某仓位是否仍然存在

    通过独立API调用重新查询Leader持仓，确认该币种仓位确实存在。

    Returns:
        True=仓位存在, False=仓位不存在
    """
    try:
        current_positions = get_positions(leader_addr, config, dex_cache, logger)
        pos = current_positions.get(coin, {})
        szi = float(pos.get("szi", 0))
        exists = abs(szi) > 1e-8
        if logger:
            logger.info(f"[SAFETY-4] 二次确认Leader{coin}仓位: szi={szi}, 存在={exists}")
        return exists
    except Exception as e:
        if logger:
            logger.warning(f"[SAFETY-4] 二次确认查询失败({coin}): {e}")
        # 查询失败时保守处理：不确认
        return False


def double_confirm_position_closed(leader_addr: str, coin: str, config: ConfigManager, dex_cache: DexCache, logger: Optional[logging.Logger] = None) -> bool:
    """护栏4: 二次确认Leader某仓位是否已经平掉

    通过独立API调用重新查询Leader持仓，确认该币种仓位确实不存在。

    Returns:
        True=仓位已平, False=仓位仍存在
    """
    try:
        current_positions = get_positions(leader_addr, config, dex_cache, logger)
        pos = current_positions.get(coin, {})
        szi = float(pos.get("szi", 0))
        closed = abs(szi) < 1e-8
        if logger:
            logger.info(f"[SAFETY-4] 二次确认Leader{coin}仓位已平: szi={szi}, 已平={closed}")
        return closed
    except Exception as e:
        if logger:
            logger.warning(f"[SAFETY-4] 二次确认查询失败({coin}): {e}")
        # 查询失败时保守处理：不确认已平
        return False

def _query_positions_for_dex(address: str, dex: str, base_url: str, timeout: int, max_retries: int = 2, retry_interval: float = 2.0) -> Dict[str, dict]:
    """查询单个DEX的持仓，带重试 (2026-07-02)"""
    import time
    payload = {"type": "clearinghouseState", "user": address}
    if dex:
        payload["dex"] = dex
    last_error = None
    for attempt in range(max_retries + 1):
        try:
            data = hl_post(payload, base_url=base_url, timeout=timeout)
            result: Dict[str, dict] = {}
            for ap in data.get("assetPositions", []):
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
        except Exception as e:
            last_error = e
            if attempt < max_retries:
                time.sleep(retry_interval)
    raise last_error


def get_positions(address: str, config: ConfigManager, dex_cache: DexCache, logger: Optional[logging.Logger] = None) -> Dict[str, dict]:
    """获取持仓（含所有perp DEX），DEX并行查询"""
    dexes = dex_cache.get_perp_dex_list(config.base_url, config.api_timeout)
    result: Dict[str, dict] = {}
    if len(dexes) <= 1:
        try:
            result = _query_positions_for_dex(address, "", config.base_url, config.api_timeout)
        except Exception as e:
            if logger:
                logger.warning(f"查询主DEX持仓失败: {e}")
        return result
    max_workers = min(config.dex_query_max_workers, len(dexes))
    with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_to_dex = {
            executor.submit(_query_positions_for_dex, address, dex, config.base_url, config.api_timeout): dex
            for dex in dexes
        }
        done, not_done = concurrent.futures.wait(future_to_dex.keys(), timeout=config.dex_query_timeout)
        for future in done:
            dex = future_to_dex[future]
            try:
                dex_result = future.result()
                result.update(dex_result)
            except Exception as e:
                if logger:
                    logger.warning(f"查询{dex or '主DEX'}持仓失败: {e}")
        for future in not_done:
            dex = future_to_dex[future]
            if logger:
                logger.warning(f"查询{dex or '主DEX'}持仓超时({config.dex_query_timeout}s)")
    # Fix 1 (2026-07-03): 部分DEX失败时记录警告，不静默丢弃
    if failed_count := len([f for f in future_to_dex if f not in done or f in not_done]):
        if logger:
            failed_dex_names = [future_to_dex[f] for f in not_done] +                                [future_to_dex[f] for f in done if f.exception() is not None]
            logger.warning(f"[INCOMPLETE-DATA] get_positions: {len(failed_dex_names)}个DEX查询失败/超时: {failed_dex_names}，"
                          f"已返回{len(result)}个仓位（可能不完整）")
    return result


def _query_orders_for_dex(address: str, dex: str, base_url: str, timeout: int) -> List[dict]:
    payload = {"type": "openOrders", "user": address}
    if dex:
        payload["dex"] = dex
    data = hl_post(payload, base_url=base_url, timeout=timeout)
    return data if isinstance(data, list) else []


def get_open_orders(address: str, config: ConfigManager, dex_cache: DexCache, logger: Optional[logging.Logger] = None) -> List[dict]:
    """获取挂单列表（含所有perp DEX）"""
    dexes = dex_cache.get_perp_dex_list(config.base_url, config.api_timeout)
    all_orders: List[dict] = []
    if len(dexes) <= 1:
        try:
            all_orders = _query_orders_for_dex(address, "", config.base_url, config.api_timeout)
        except Exception as e:
            if logger:
                logger.warning(f"查询主DEX挂单失败: {e}")
        return all_orders
    max_workers = min(config.dex_query_max_workers, len(dexes))
    with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_to_dex = {
            executor.submit(_query_orders_for_dex, address, dex, config.base_url, config.api_timeout): dex
            for dex in dexes
        }
        done, not_done = concurrent.futures.wait(future_to_dex.keys(), timeout=config.dex_query_timeout)
        for future in done:
            dex = future_to_dex[future]
            try:
                orders = future.result()
                all_orders.extend(orders)
            except Exception as e:
                if logger:
                    logger.warning(f"查询{dex or '主DEX'}挂单失败: {e}")
        for future in not_done:
            dex = future_to_dex[future]
            if logger:
                logger.warning(f"查询{dex or '主DEX'}挂单超时({config.dex_query_timeout}s)")
    return all_orders


def get_frontend_open_orders(address: str, base_url: str, timeout: int = 15) -> List[dict]:
    """获取frontendOpenOrders（含triggerPx等完整信息），用于TP/SL同步"""
    try:
        data = hl_post({"type": "frontendOpenOrders", "user": address}, base_url=base_url, timeout=timeout)
        return data if isinstance(data, list) else []
    except Exception:
        return []


def _query_mids_for_dex(dex: str, base_url: str, timeout: int) -> Dict[str, float]:
    payload = {"type": "allMids"}
    if dex:
        payload["dex"] = dex
    data = hl_post(payload, base_url=base_url, timeout=timeout)
    result: Dict[str, float] = {}
    if isinstance(data, dict):
        for k, v in data.items():
            try:
                result[k] = float(v)
            except (ValueError, TypeError):
                pass
    return result


def get_all_mids(config: ConfigManager, dex_cache: DexCache, logger: Optional[logging.Logger] = None) -> Dict[str, float]:
    """获取所有币种最新中间价"""
    dexes = dex_cache.get_perp_dex_list(config.base_url, config.api_timeout)
    result: Dict[str, float] = {}
    if len(dexes) <= 1:
        try:
            result = _query_mids_for_dex("", config.base_url, config.api_timeout)
        except Exception as e:
            if logger:
                logger.warning(f"查询主DEX价格失败: {e}")
        return result
    max_workers = min(config.dex_query_max_workers, len(dexes))
    with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_to_dex = {
            executor.submit(_query_mids_for_dex, dex, config.base_url, config.api_timeout): dex
            for dex in dexes
        }
        done, not_done = concurrent.futures.wait(future_to_dex.keys(), timeout=config.dex_query_timeout)
        for future in done:
            dex = future_to_dex[future]
            try:
                dex_result = future.result()
                result.update(dex_result)
            except Exception as e:
                if logger:
                    logger.warning(f"查询{dex or '主DEX'}价格失败: {e}")
        for future in not_done:
            dex = future_to_dex[future]
            if logger:
                logger.warning(f"查询{dex or '主DEX'}价格超时({config.dex_query_timeout}s)")
    return result


# ============================================================
# SDK下单工具
# ============================================================

def create_exchange(config: ConfigManager, dex_cache: DexCache, logger: Optional[logging.Logger] = None):
    """创建Exchange实例用于下单"""
    from hyperliquid.exchange import Exchange
    from eth_account import Account
    try:
        perp_dexs = dex_cache.get_perp_dex_list(config.base_url, config.api_timeout)
    except Exception:
        perp_dexs = None
    wallet = Account.from_key(config.private_key)
    exchange = Exchange(wallet=wallet, base_url=config.base_url, account_address=config.user_main_addr, perp_dexs=perp_dexs)
    return exchange


def round_sz(sz: float, coin: str, sz_decimals_map: Dict[str, int], _logger=None) -> float:
    # Fix 6 (2026-07-03): 零值防护，防止round_sz=0导致仓位丢失
    if sz == 0:
        if _logger:
            _logger.warning(f"[ROUND-SZ] round_sz收到零值: coin={coin}, sz={sz}，保持零值")
        return 0.0
    decimals = sz_decimals_map.get(coin, 4)
    if decimals == 0:
        result = float(int(sz))
    else:
        factor = 10 ** decimals
        result = float(int(sz * factor)) / factor
    if result == 0 and sz != 0:
        if _logger:
            _logger.warning(f"[ROUND-SZ] round_sz结果为零: coin={coin}, sz={sz}, decimals={decimals}，使用最小精度值")
        # 返回最小精度值（保持原始符号）
        min_val = 10 ** (-decimals) if decimals > 0 else 1
        return min_val if sz > 0 else -min_val
    return result


def format_price(price: float, sz_decimals: int, is_spot: bool = False) -> float:
    max_decimals = 8 if is_spot else 6
    if price >= 100_000:
        return round(price)
    return round(float(f"{price:.5g}"), max_decimals - sz_decimals)


# ============================================================
# 状态持久化
# ============================================================

def save_state(leader_positions: Dict[str, dict], leader_orders: List[dict], user_positions: Dict[str, dict], state_file: str) -> None:
    Path(state_file).parent.mkdir(parents=True, exist_ok=True)
    state = {
        "timestamp": datetime.now().isoformat(),
        "leader_positions": leader_positions,
        "leader_orders": leader_orders,
        "user_positions": user_positions,
    }
    tmp_path = state_file + ".tmp"
    try:
        with open(tmp_path, "w", encoding="utf-8") as f:
            json.dump(state, f, indent=2, ensure_ascii=False)
        os.replace(tmp_path, state_file)
    except Exception:
        try:
            os.unlink(tmp_path)
        except OSError:
            pass
        raise


def load_state(state_file: str) -> Tuple[Dict[str, dict], List[dict], Dict[str, dict]]:
    if not os.path.exists(state_file):
        return {}, [], {}
    try:
        with open(state_file, "r", encoding="utf-8") as f:
            state = json.load(f)
        return (
            state.get("leader_positions", {}),
            state.get("leader_orders", []),
            state.get("user_positions", {}),
        )
    except Exception:
        return {}, [], {}


# ============================================================
# 变化检测 (v2原有)
# ============================================================

def detect_position_changes(old_pos: Dict[str, dict], new_pos: Dict[str, dict]) -> List[dict]:
    """检测持仓变化（v2原有函数，保留用于轮询路径兼容）"""
    changes: List[dict] = []
    old_coins = set(old_pos.keys())
    new_coins = set(new_pos.keys())
    for coin in new_coins - old_coins:
        pos = new_pos[coin]
        if float(pos["szi"]) != 0:
            changes.append({"type": "open", "coin": coin, "old_szi": "0", "new_szi": str(float(pos["szi"])), "leverage": pos["leverage_value"], "leverage_type": pos["leverage_type"], "positionValue": str(float(pos.get("positionValue", 0)))})
    for coin in old_coins - new_coins:
        pos = old_pos[coin]
        if float(pos["szi"]) != 0:
            changes.append({"type": "close", "coin": coin, "old_szi": str(float(pos["szi"])), "new_szi": "0", "leverage": pos["leverage_value"], "leverage_type": pos["leverage_type"]})
    for coin in old_coins & new_coins:
        old_szi = str(float(old_pos[coin]["szi"]))
        new_szi = str(float(new_pos[coin]["szi"]))
        old_lev = old_pos[coin]["leverage_value"]
        new_lev = new_pos[coin]["leverage_value"]
        old_val = float(old_szi)
        new_val = float(new_szi)
        if old_val != new_val:
            if new_val == 0:
                changes.append({"type": "close", "coin": coin, "old_szi": old_szi, "new_szi": new_szi, "leverage": new_pos[coin]["leverage_value"], "leverage_type": new_pos[coin]["leverage_type"]})
            elif old_val == 0:
                changes.append({"type": "open", "coin": coin, "old_szi": old_szi, "new_szi": new_szi, "leverage": new_pos[coin]["leverage_value"], "leverage_type": new_pos[coin]["leverage_type"], "positionValue": new_pos[coin]["positionValue"]})
            else:
                changes.append({"type": "adjust", "coin": coin, "old_szi": old_szi, "new_szi": new_szi, "leverage": new_pos[coin]["leverage_value"], "leverage_type": new_pos[coin]["leverage_type"], "positionValue": new_pos[coin]["positionValue"]})
        elif old_lev != new_lev:
            changes.append({"type": "leverage_change", "coin": coin, "old_szi": old_szi, "new_szi": new_szi, "old_lev": old_lev, "new_lev": new_lev, "leverage": new_lev, "leverage_type": new_pos[coin]["leverage_type"]})
    return changes


def detect_order_changes(old_orders: List[dict], new_orders: List[dict]) -> Tuple[List[dict], List[dict]]:
    """检测挂单变化"""
    def order_key(o: dict) -> Tuple:
        return (o.get("coin", ""), o.get("side", ""), o.get("limitPx", ""), o.get("oid"))
    old_set = {order_key(o): o for o in old_orders if o.get("reduceOnly")}
    new_set = {order_key(o): o for o in new_orders if o.get("reduceOnly")}
    added = [o for k, o in new_set.items() if k not in old_set]
    removed = [o for k, o in old_set.items() if k not in new_set]
    return added, removed


# ============================================================
# 交易执行
# ============================================================

class TradeExecutor:
    """封装交易执行逻辑，支持dry-run和live模式"""

    def __init__(self, live_mode: bool, logger: logging.Logger, config: ConfigManager, dex_cache: DexCache) -> None:
        self.live_mode = live_mode
        self.logger = logger
        self.config = config
        self.dex_cache = dex_cache
        self.exchange = None
        self._quota_exhausted = False  # 配额耗尽标志
        # ===== 最小下单金额兜底 =====
        self.MIN_ORDER_VALUE_USD = 10.0  # HL交易所最低下单金额$10
        self._order_reject_counts = {}  # 连续下单被拒计数器 {coin: count}
        
        if live_mode:
            self.exchange = create_exchange(config, dex_cache, logger)
            self.logger.info("=== LIVE模式已启用，将实际下单 ===")
        else:
            self.logger.info("=== DRY-RUN模式，只监控不下单 ===")

    def rebuild_exchange(self) -> None:
        """热重建Exchange对象（钱包轮换后使用，无需重启进程）"""
        if not self.live_mode:
            self.logger.info("[REBUILD] DRY-RUN模式，跳过Exchange重建")
            return
        self.logger.info("[REBUILD] 重新读取配置并创建Exchange...")
        # 重新加载配置（从.env读取新的API钱包信息）
        self.config._load_secrets()
        self.exchange = create_exchange(self.config, self.dex_cache, self.logger)
        self.logger.info("[REBUILD] Exchange重建完成")

    def pause_trading(self, reason: str) -> None:
        """暂停跟单（钱包过期等紧急情况）"""
        if self.live_mode:
            self.live_mode = False
            self.logger.critical(f"[PAUSE] 跟单已暂停: {reason}")
            self._write_alert("PAUSE_TRADING", "ALL", f"跟单已暂停: {reason}")

    def resume_trading(self) -> None:
        """恢复跟单"""
        if not self.live_mode:
            self.live_mode = True
            self.rebuild_exchange()  # 恢复时重建Exchange确保用最新配置
            self.logger.info("[RESUME] 跟单已恢复")

    def _ensure_min_order_value(self, coin: str, sz: float, price: float) -> float:
        """确保下单金额满足交易所最低要求($10)
        
        如果 sz*price < $10，向上取整到满足最低要求的sz。
        Returns: 调整后的sz（可能不变）
        """
        if price <= 0 or sz <= 0:
            return sz
        order_value = sz * price
        if order_value >= self.MIN_ORDER_VALUE_USD:
            return sz
        # 向上取整到满足最低金额
        import math
        min_sz = self.MIN_ORDER_VALUE_USD / price
        sz_dec = self.dex_cache.get_sz_decimals(coin)
        factor = 10 ** sz_dec
        min_sz = math.ceil(min_sz * factor) / factor
        self.logger.warning(
            f"[MIN-ORDER] {coin}: 下单金额${order_value:.2f}低于最低${self.MIN_ORDER_VALUE_USD}，"
            f"向上取整 sz={sz} → {min_sz} (金额≈${min_sz * price:.2f})"
        )
        return min_sz

    def track_order_reject(self, coin: str) -> int:
        """记录下单被拒，返回连续被拒次数"""
        self._order_reject_counts[coin] = self._order_reject_counts.get(coin, 0) + 1
        return self._order_reject_counts[coin]
    
    def clear_order_reject(self, coin: str) -> None:
        """清除下单被拒计数（成功下单后调用）"""
        self._order_reject_counts.pop(coin, None)

    def _retry_action(self, action_fn: Any, description: str, exponential_backoff: bool = False, max_retries_override: int = None) -> Tuple[bool, str]:
        max_retries = max_retries_override if max_retries_override is not None else self.config.max_retries
        retry_interval = self.config.retry_interval
        for attempt in range(max_retries + 1):
            try:
                result = action_fn()
                if isinstance(result, dict):
                    status = result.get("status", "")
                    order_error: Optional[str] = None
                    try:
                        statuses = result.get("response", {}).get("data", {}).get("statuses", [])
                        if statuses and isinstance(statuses[0], dict) and "error" in statuses[0]:
                            order_error = statuses[0]["error"]
                    except (AttributeError, IndexError, TypeError):
                        pass
                    if status == "ok" and not order_error:
                        self.logger.info(f"  ✓ {description} 成功: {result}")
                        return True, ""
                    # 检测累计配额耗尽错误，立即停止重试避免浪费配额
                    resp_str = str(result.get("response", ""))
                    if "Too many cumulative" in resp_str:
                        self.logger.error(f"  ✗✗ {description} 配额耗尽，停止重试: {resp_str[:80]}...")
                        self._quota_exhausted = True
                        return False, "quota_exhausted"
                    
                    elif order_error:
                        self.logger.warning(f"  ✗ {description} 下单被拒(尝试{attempt+1}): {order_error}")
                        return False, "rejected"
                    else:
                        self.logger.warning(f"  ✗ {description} 失败(尝试{attempt+1}): {result}")
                elif isinstance(result, str) and "success" in result.lower():
                    self.logger.info(f"  ✓ {description} 成功: {result}")
                    return True, ""
                else:
                    self.logger.warning(f"  ✗ {description} 返回异常(尝试{attempt+1}): {result}")
            except NetworkError as e:
                error_type, should_retry = _classify_api_error(e)
                self.logger.warning(f"  ✗ {description} 网络错误({error_type})(尝试{attempt+1}): {e}")
                if not should_retry or attempt == max_retries:
                    return False, "network"
            except ApiRejectedError as e:
                self.logger.warning(f"  ✗ {description} API拒绝(尝试{attempt+1}): {e}")
                return False, "rejected"
            except Exception as e:
                error_type, should_retry = _classify_api_error(e)
                self.logger.warning(f"  ✗ {description} 异常({error_type})(尝试{attempt+1}): {e}")
                if attempt == max_retries:
                    return False, "exception"
            if attempt < max_retries:
                if exponential_backoff:
                    backoff_base = self.config.flip_backoff_base
                    wait = min(retry_interval * (backoff_base ** attempt), 60)
                    self.logger.info(f"  [退避] {wait}秒后重试...")
                    time.sleep(wait)
                else:
                    time.sleep(retry_interval)
        self.logger.error(f"  ✗✗ {description} 最终失败，已重试{max_retries}次")
        return False, "network"

    def set_leverage(self, coin: str, leverage: int, is_cross: bool = True) -> bool:
        desc = f"设置{coin}杠杆={leverage}x cross={is_cross}"
        self.logger.info(f"  [操作] {desc}")
        if not self.live_mode:
            self.logger.info(f"  [DRY-RUN] {desc}")
            return True
        def action():
            return self.exchange.update_leverage(leverage, coin, is_cross=is_cross)
        ok, _ = self._retry_action(action, desc)
        return ok

    def open_position(self, coin: str, is_buy: bool, sz: float, leverage: int, price: float, leverage_type: str = "cross") -> bool:
        direction = "做多" if is_buy else "做空"
        margin_type = "isolated" if leverage_type in ("isolated", "strictIsolated") else "cross"
        desc = f"{coin} {direction} sz={sz} lev={leverage}x {margin_type}"
        self.logger.info(f"  [操作] 开仓: {desc}")
        if not self.live_mode:
            self.logger.info(f"  [DRY-RUN] 开仓: {desc}")
            return True
        is_cross = leverage_type not in ("isolated", "strictIsolated")
        self.set_leverage(coin, leverage, is_cross=is_cross)
        from hyperliquid.utils.signing import OrderType
        offset = 1 + self.config.ioc_price_offset if is_buy else 1 - self.config.ioc_price_offset
        raw_px = price * offset
        sz_dec = self.dex_cache.get_sz_decimals(coin)
        limit_px = format_price(raw_px, sz_dec)
        order_type = OrderType(limit={"tif": "Ioc"})
        # 最小下单金额兜底
        sz = self._ensure_min_order_value(coin, sz, price)
        def action():
            return self.exchange.order(coin, is_buy, sz, limit_px, order_type)
        ok, _ = self._retry_action(action, desc)
        if ok:
            self.clear_order_reject(coin)
        return ok

    def close_position(self, coin: str, szi: str, exponential_backoff: bool = False) -> Any:
        direction = "做多" if float(szi) > 0 else "做空"
        desc = f"平仓 {coin} {direction}"
        self.logger.info(f"  [操作] {desc}")
        if not self.live_mode:
            self.logger.info(f"  [DRY-RUN] {desc}")
            return True
        def action():
            return self.exchange.market_close(coin)
        ok, fail_reason = self._retry_action(action, desc, exponential_backoff=exponential_backoff)
        return ok if ok else fail_reason

    def adjust_position(self, coin: str, is_buy: bool, delta_sz: float, price: float, max_abs_delta: float = None) -> bool:
        desc = f"调仓 {coin} {'加' if is_buy else '减'} delta={delta_sz}"
        self.logger.info(f"  [操作] {desc}")
        if not self.live_mode:
            self.logger.info(f"  [DRY-RUN] {desc}")
            return True
        from hyperliquid.utils.signing import OrderType
        offset = 1 + self.config.ioc_price_offset if is_buy else 1 - self.config.ioc_price_offset
        raw_px = price * offset
        sz_dec = self.dex_cache.get_sz_decimals(coin)
        limit_px = format_price(raw_px, sz_dec)
        order_type = OrderType(limit={"tif": "Ioc"})
        # 最小下单金额兜底
        delta_sz = self._ensure_min_order_value(coin, delta_sz, price)
        # P0 FIX: 第二道防线——MIN_ORDER兜底后cap到max_abs_delta
        if max_abs_delta is not None and delta_sz > max_abs_delta:
            self.logger.warning(
                f"[ADJUST-CAP] {coin}: MIN_ORDER兜底后delta={delta_sz:.4f}超过上限{max_abs_delta:.4f}，截断"
            )
            delta_sz = max_abs_delta
            if delta_sz <= 0:
                self.logger.info(f"[ADJUST-CAP] {coin}: 截断后delta=0，跳过下单")
                return True
        def action():
            return self.exchange.order(coin, is_buy, delta_sz, limit_px, order_type)
        ok, _ = self._retry_action(action, desc)
        if ok:
            self.clear_order_reject(coin)
        return ok

    def place_tpsl_order(self, coin: str, is_buy: bool, sz: float, trigger_px: str, tpsl: str) -> bool:
        order_type_name = "止盈" if tpsl == "tp" else "止损"
        desc = f"{coin} {order_type_name} trigger={trigger_px} sz={sz}"
        self.logger.info(f"  [操作] {desc}")
        if not self.live_mode:
            self.logger.info(f"  [DRY-RUN] {desc}")
            return True
        from hyperliquid.utils.signing import OrderType
        order_type = OrderType(trigger={"triggerPx": float(trigger_px), "isMarket": True, "tpsl": tpsl})
        tp_px = float(trigger_px)
        sz_dec = self.dex_cache.get_sz_decimals(coin)
        tpsl_offset = self.config.tpsl_limit_offset
        if is_buy:
            limit_px = format_price(tp_px * (1 + tpsl_offset), sz_dec)
        else:
            limit_px = format_price(tp_px * (1 - tpsl_offset), sz_dec)
        def action():
            return self.exchange.order(coin, is_buy, sz, limit_px, order_type, reduce_only=True)
        ok, _ = self._retry_action(action, desc, max_retries_override=self.config.tpsl_max_retries)
        return ok

    def cancel_order(self, coin: str, oid: int) -> bool:
        desc = f"撤单 {coin} oid={oid}"
        self.logger.info(f"  [操作] {desc}")
        if not self.live_mode:
            self.logger.info(f"  [DRY-RUN] {desc}")
            return True
        def action():
            return self.exchange.cancel(coin, oid)
        ok, _ = self._retry_action(action, desc, max_retries_override=self.config.tpsl_max_retries)
        return ok



    def cancel_all_orders(self, coin: Optional[str] = None) -> bool:
        if not self.live_mode:
            if coin:
                self.logger.info(f"  [DRY-RUN] 撤销{coin}所有挂单")
            else:
                self.logger.info(f"  [DRY-RUN] 撤销所有挂单")
            return True
        try:
            user_orders = get_open_orders(self.config.user_main_addr, self.config, self.dex_cache, self.logger)
            cancelled = 0
            for o in user_orders:
                o_coin = o.get("coin", "")
                if coin and o_coin != coin:
                    continue
                oid = o.get("oid", 0)
                if oid:
                    self.cancel_order(o_coin, oid)
                    cancelled += 1
            self.logger.info(f"  已撤销 {cancelled} 个挂单" + (f" ({coin})" if coin else ""))
            return True
        except Exception as e:
            self.logger.error(f"  撤销所有挂单失败: {e}")
            return False

    def market_close_all(self) -> bool:
        if not self.live_mode:
            self.logger.info(f"  [DRY-RUN] 市价平掉所有仓位")
            return True
        try:
            user_positions = get_positions(self.config.user_main_addr, self.config, self.dex_cache, self.logger)
            closed = 0
            for coin, pos in user_positions.items():
                szi = float(pos.get("szi", 0))
                if szi != 0:
                    self.close_position(coin, pos["szi"])
                    closed += 1
            self.logger.info(f"  已平仓 {closed} 个仓位")
            return True
        except Exception as e:
            self.logger.error(f"  市价平掉所有仓位失败: {e}")
            return False


# ============================================================
# 核心跟单逻辑 (v3: 增加WS组件)
# ============================================================

class CopyTradeBot:
    """跟单机器人主类 v3 — WebSocket保守混合架构"""

    def __init__(
        self,
        live_mode: bool,
        interval: int,
        logger: logging.Logger,
        config: ConfigManager,
    ) -> None:
        self.live_mode = live_mode
        self.interval = interval
        self.logger = logger
        self.config = config
        self.running: bool = True

        # v2原有组件
        self.dex_cache = DexCache(config, logger)
        self.health_checker = HealthChecker(config, logger)
        self.sz_decimals_map: Dict[str, int] = dict(config.sz_decimals_defaults)
        self.executor: Optional[TradeExecutor] = None
        self.prev_leader_positions: Dict[str, dict] = {}
        self.position_ratios: Dict[str, float] = {}
        self.prev_leader_orders: List[dict] = []
        self.pending_flips: Dict[str, dict] = {}
        self.pending_closes: Dict[str, dict] = {}
        self.baseline_coins: Set[str] = set()
        # ===== 系统隔离: 从state_file派生系统标识前缀 =====
        _state_stem = Path(self.config.state_file).stem.removesuffix("_state")  # hl_copytrade_v3 / hl_copytrade_v3_highfreq


        # ===== exited_coins 状态机 =====
        self._exited_coins_file: str = os.path.join(
            os.path.dirname(os.path.abspath(self.config.state_file)),
            f"{_state_stem}_exited_coins.json"
        )
        self._exited_coins: Dict[str, float] = {}  # coin -> exit_timestamp
        self._exited_coins_file_mtime: float = 0
        self._recon_residual_count: Dict[str, int] = {}  # RECON残留仓位连续检测次数
        self._safety2_skip_recon: dict = {}  # SAFETY-2跳过后待补检 {coins: [...], time: float}
        self._load_exited_coins()

        # ===== position_ratios 持久化 =====
        self._position_ratios_file: str = os.path.join(
            os.path.dirname(os.path.abspath(self.config.state_file)),
            f"{_state_stem}_position_ratios.json"
        )
        self._load_position_ratios()

        # ===== tpsl_active 状态 (TP/SL活跃感知) =====
        self._tpsl_active_file: str = os.path.join(
            os.path.dirname(os.path.abspath(self.config.state_file)),
            f"{_state_stem}_tpsl_active.json"
        )
        self._tpsl_active: Dict[str, float] = {}  # coin -> last_tpsl_timestamp
        self._load_tpsl_active()
        # P0 FIX: TPSL失败补偿队列
        self._pending_tpsl: List[dict] = []  # [{coin, is_buy, sz, trigger_px, tpsl, fail_count, first_fail_time}]
        self.prev_user_positions: Dict[str, dict] = {}
        self.last_status_time: float = 0
        self.poll_count: int = 0

        # ===== v3新增组件 =====
        self._ws_change_queue: queue.Queue = queue.Queue(maxsize=1000)
        self._ws_wakeup: threading.Event = threading.Event()
        self._last_full_poll_time: float = 0  # 上次完整轮询时间，控制API调用频率
        # === v3.3 WS信号驱动架构 ===
        self._event_driven_mode: bool = True  # 事件驱动模式标记（WS正常时True，断连时切换为False）
        self._last_on_demand_time: float = 0  # 上次按需查询时间（防抖用）
        self._on_demand_debounce: float = 10  # 按需查询防抖秒数
        self._recon_timeout: float = 1800  # 对账超时秒数（30分钟）
        self._degraded_interval: float = 120  # 降级模式轮询间隔（2分钟）
        self._ws_disconnect_grace: float = 30  # WS断连宽限秒数（超过则降级）
        self._ws_disconnect_time: float = 0  # WS断连开始时间
        self._on_demand_count: int = 0  # 按需查询计数
        self._recon_count: int = 0  # 对账查询计数
        self._degraded_count: int = 0  # 降级轮询计数
        self._state_comparator = StateComparator(logger)
        self._debouncer = Debouncer(
            window_ms=config.debounce_window_ms,
            flush_callback=self._on_debounce_flush,
            logger=logger,
        )
        # P3#6: 去重器持久化
        _dedup_state_file = os.path.join(
            os.path.dirname(os.path.abspath(config.state_file)),
            f"{_state_stem}_dedup_state.json"
        )
        self._deduplicator = Deduplicator(
            window_seconds=config.dedup_window_seconds,
            logger=logger,
            state_file=_dedup_state_file,
        )
        self._ws_listener: Optional[WsListener] = None
        self._ws_enabled: bool = config.ws_enabled

        # P3#9: 每日运行统计计数器
        self._today_opens: int = 0
        self._today_adjusts: int = 0
        self._today_closes: int = 0
        self._today_recons: int = 0
        self._stats_date: str = datetime.now().strftime("%Y-%m-%d")  # 跨日重置用
        # ===== 审计数据采集计数器 =====
        self._audit_close_queued: int = 0   # 平仓失败进入pending队列次数
        self._audit_partial_fills: int = 0   # 部分成交检测次数
        self._audit_confirm_retries: int = 0  # 部分成交补单重试次数
        self._tpsl_sync_lock: bool = False  # P1: TP/SL同步锁
        self._ws_tpsl_sync_needed: bool = False  # WS openOrders触发的即时TP/SL同步标记
        self._ws_last_tpsl_sync_time: float = 0  # 上次WS触发TP/SL同步的时间（防抖）
        self._last_periodic_tpsl_sync_time: float = 0  # 上次定时TP/SL同步的时间（时间驱动）
        self._ws_tpsl_coins: set = set()  # openOrders推送涉及的需要TP/SL同步的币种
        self._ws_fill_coins: set = set()  # userFills推送涉及的成交币种（需优先刷新）
        self._ws_recently_synced_coins: dict = {}  # P1: WS近期已同步TP/SL的币种 {coin: timestamp}
        self._poll_cache: dict = {}  # P2: 单次poll内数据缓存，避免重复API查询，防止与全量_sync_user_tpsl_orders并发冲突

        # ===== 智能缓存：降低低频数据刷新频率 (2026-08-27 API优化) =====
        self._cache_account_value = {"leader": {"value": 0, "time": 0}, "user": {"value": 0, "time": 0}}
        self._cache_user_positions = {"data": {}, "time": 0}
        self._cache_user_orders = {"data": [], "time": 0}
        self._cache_margin_used = {"value": 0, "time": 0}
        # 缓存TTL（轮询周期数）
        self._CACHE_ACCOUNT_VALUE_INTERVAL = 5   # 每5轮刷新一次account_value
        self._CACHE_USER_DATA_INTERVAL = 5       # 每5轮刷新一次user_positions/user_orders/margin

        # ===== 健康自检: 连续错误检测 =====
        self._consecutive_errors: int = 0
        self._consecutive_dex_fail_count: int = 0  # SAFETY-3连续失败计数
        self._last_error_signature: str = ""
        self._health_alert_file: str = self.config.alert_file  # 系统隔离: 从config读取
        self._health_alert_threshold: int = 10  # 连续10个周期相同错误触发告警（减少不必要通知）

        # ===== Phase 1: AlertManager飞书告警 (2026-07-25) =====
        # 系统隔离: 从state_file派生alerts_unread文件路径
        _alerts_unread_file = os.path.join(
            os.path.dirname(os.path.abspath(config.state_file)),
            f"{_state_stem}_alerts_unread.json"
        )
        # 系统隔离: 确定 system_name 用于告警前缀
        _sys_name = "高频跟单" if config._config_path and 'highfreq' in os.path.basename(config._config_path) else "低频跟单"
        self._alert_manager = AlertManager(
            webhook_url=config.alert_webhook_url,
            logger=logger,
            enabled=config.alert_enabled,
            dedup_hours=config.alert_dedup_hours,
            bot_name=config.alert_bot_name,
            alert_file=_alerts_unread_file,
            system_name=_sys_name,
        )

        # ===== Phase 2: 增强监控 (2026-07-25) =====
        self._phase2 = Phase2Monitor(config, logger, self._alert_manager)
        self._phase3 = Phase3Monitor(config, logger, self._alert_manager)
        self._phase4 = Phase4Stats(config, logger, self._alert_manager)

        # ===== Phase A: TradeDB交易记录 (2026-08-14) =====
        try:
            from hl_db import TradeDB
            self._trade_db = TradeDB()
            self._instance_id = 'hf' if config._config_path and 'highfreq' in os.path.basename(config._config_path) else 'lf'
        except Exception as _tdb_err:
            logger.warning(f"[Phase A] TradeDB init failed, trades will not be recorded: {_tdb_err}")
            self._trade_db = None
            self._instance_id = 'lf'


        # 信号处理
        if sys.platform != "win32":
            signal.signal(signal.SIGINT, self._signal_handler)
            signal.signal(signal.SIGTERM, self._signal_handler)

    # ----------------------------------------------------------------
    # 健康自检: 连续错误检测与告警
    # ----------------------------------------------------------------
    def _check_consecutive_error(self, error: Exception) -> None:
        """检测连续轮询错误，超过阈值时写入告警文件
        
        判定逻辑:
        - 提取错误签名（异常类型+错误消息前100字符），忽略行号等可变信息
        - 如果连续3个周期出现相同签名的错误，写入告警文件
        - 不同错误会重置计数器
        - 成功轮询会在外层重置计数器
        """
        # 提取错误签名：异常类型 + 错误消息(截断)
        error_type = type(error).__name__
        error_msg = str(error)[:100]
        error_sig = f"{error_type}:{error_msg}"
        
        if error_sig == self._last_error_signature:
            self._consecutive_errors += 1
        else:
            # 不同错误，重新计数
            self._consecutive_errors = 1
            self._last_error_signature = error_sig
        
        self.logger.warning(
            f"[HEALTH-CHECK] 连续错误计数: {self._consecutive_errors}/{self._health_alert_threshold} "
            f"(签名: {error_sig[:80]})"
        )
        
        if self._consecutive_errors >= self._health_alert_threshold:
            self.logger.critical(
                f"[HEALTH-CHECK] 连续{self._consecutive_errors}个周期出现相同错误，写入告警! "
                f"签名: {error_sig}"
            )
            self._write_health_alert(
                alert_type=f"CONSECUTIVE_ERROR_{error_type}",
                message=f"连续{self._consecutive_errors}个轮询周期出现相同错误: {error_msg}"
            )
            # 写入告警后重置，避免重复写入
            self._consecutive_errors = 0
            self._last_error_signature = ""

    def _write_health_alert(self, alert_type: str, message: str) -> None:
        """写入V3健康自检告警文件(critical_alerts_v3.json)
        
        格式与现有告警文件一致: [{"type": "...", "coin": "...", "message": "..."}]
        """
        try:
            alerts: List[dict] = []
            alert_path = self._health_alert_file
            if os.path.exists(alert_path):
                with open(alert_path, "r", encoding="utf-8") as f:
                    alerts = json.load(f)
            alert = {
                "type": alert_type,
                "coin": "ALL",
                "message": message,
                "timestamp": datetime.now().isoformat(),
                "poll_count": self.poll_count,
            }
            alerts.append(alert)
            alerts = alerts[-20:]  # 保留最近20条
            tmp_path = alert_path + ".tmp"
            with open(tmp_path, "w", encoding="utf-8") as f:
                json.dump(alerts, f, ensure_ascii=False, indent=2)
            os.replace(tmp_path, alert_path)
            # Phase 1: 飞书告警推送
            if hasattr(self, '_alert_manager'):
                self._alert_manager.send_health_alert(alert_type, message, self.poll_count)
            self.logger.info(f"[HEALTH-CHECK] 告警已写入: {alert_path}")
        except Exception as e:
            self.logger.warning(f"[HEALTH-CHECK] 写入健康告警文件失败: {e}")

    def _signal_handler(self, signum: int, frame: Any) -> None:
        self.logger.info(f"收到信号 {signum}，准备优雅退出...")
        self.running = False

    # ----------------------------------------------------------------
    # 用户主动操作识别
    # ----------------------------------------------------------------

    def _load_exited_coins(self) -> None:
        """从文件加载退出币种列表"""
        try:
            if os.path.exists(self._exited_coins_file):
                with open(self._exited_coins_file, "r", encoding="utf-8") as f:
                    data = json.load(f)
                if isinstance(data, dict):
                    self._exited_coins = {k: float(v) for k, v in data.items()}
                elif isinstance(data, list):
                    self._exited_coins = {k: 0.0 for k in data}
                else:
                    self._exited_coins = {}
                self._exited_coins_file_mtime = os.path.getmtime(self._exited_coins_file)
                if self._exited_coins:
                    self.logger.info(f"[EXITED] 加载退出币种列表: {sorted(self._exited_coins.keys())}")
            else:
                self.logger.info("[EXITED] 退出币种列表为空")
        except Exception as e:
            self.logger.warning(f"[EXITED] 加载退出币种列表失败: {e}，初始化为空")
            self._exited_coins = {}

    def _load_position_ratios(self):
        """从文件加载 position_ratios"""
        try:
            if os.path.exists(self._position_ratios_file):
                with open(self._position_ratios_file, 'r') as f:
                    data = json.load(f)
                if isinstance(data, dict):
                    self.position_ratios = {k: float(v) for k, v in data.items()}
                    self.logger.info(f"Loaded {len(self.position_ratios)} position ratios from {self._position_ratios_file}")
                else:
                    self.position_ratios = {}
                    self.logger.warning(f"Invalid position_ratios file format, starting with empty dict")
            else:
                self.position_ratios = {}
                self.logger.info(f"No position_ratios file found, starting with empty dict")
        except Exception as e:
            self.logger.error(f"Failed to load position_ratios: {e}")
            self._write_alert("CONFIG_VALIDATION_FAIL", "SYSTEM", f"position_ratios加载失败: {e}")
            self.position_ratios = {}

    def _save_position_ratios(self):
        """持久化 position_ratios 到文件"""
        try:
            with open(self._position_ratios_file, 'w') as f:
                json.dump(self.position_ratios, f, indent=2)
        except Exception as e:
            self.logger.error(f"Failed to save position_ratios: {e}")
            self._write_alert("CONFIG_VALIDATION_FAIL", "SYSTEM", f"position_ratios保存失败: {e}")

    def _save_exited_coins(self) -> None:
        """持久化退出币种列表到文件"""
        try:
            os.makedirs(os.path.dirname(self._exited_coins_file), exist_ok=True)
            with open(self._exited_coins_file, "w", encoding="utf-8") as f:
                json.dump(self._exited_coins, f, ensure_ascii=False)
            self._exited_coins_file_mtime = os.path.getmtime(self._exited_coins_file)
        except Exception as e:
            self.logger.warning(f"[EXITED] 保存退出币种列表失败: {e}")

    def _reload_exited_coins_if_changed(self) -> None:
        """检测exited_coins.json文件变更并热重载"""
        try:
            if not os.path.exists(self._exited_coins_file):
                if self._exited_coins:
                    self._exited_coins.clear()
                    self.logger.info("[EXITED] 文件已删除，清空退出列表")
                return
            current_mtime = os.path.getmtime(self._exited_coins_file)
            if current_mtime != self._exited_coins_file_mtime:
                self._exited_coins_file_mtime = current_mtime
                with open(self._exited_coins_file, "r", encoding="utf-8") as f:
                    data = json.load(f)
                new_dict = {k: float(v) for k, v in data.items()} if isinstance(data, dict) else {}
                if new_dict != self._exited_coins:
                    self.logger.info(f"[EXITED] 检测到文件变更，热重载: {sorted(self._exited_coins.keys())} -> {sorted(new_dict.keys())}")
                    self._exited_coins = new_dict
        except Exception as e:
            self.logger.warning(f"[EXITED] 热重载检测失败: {e}")

    def _mark_coin_exited(self, coin: str, reason: str = "") -> bool:
        """标记币种为已退出，等待Leader新信号再跟单
        
        区分TP/SL触发退出和用户手动退出：
        - TP/SL触发：TP/SL活跃感知检测到 → 标记为tpsl类型退出
        - 手动退出：用户主动平仓或其他原因 → 标记为manual类型退出
        两种类型行为相同（等待Leader新信号），但告警和日志不同。
        
        Returns:
            True = 已标记exited（调用方应跳过该币种）
            False = 未标记（TP/SL活跃，调用方应继续补仓）
        """
        # ===== TP/SL活跃感知：区分退出类型 =====
        is_tpsl_exit = self._is_tpsl_active(coin)
        if is_tpsl_exit:
            self._remove_tpsl_active(coin)
        
        import time as _time_mod
        self._exited_coins[coin] = _time_mod.time()
        self._save_exited_coins()
        
        # 记录退出原因到内存字典
        if not hasattr(self, '_exited_reasons'):
            self._exited_reasons = {}
        self._exited_reasons[coin] = "tpsl" if is_tpsl_exit else "manual"
        
        # 根据退出类型发送不同告警和日志
        if is_tpsl_exit:
            self.logger.info(f"[EXITED-TPSL] {coin} TP/SL触发平仓，标记退出等待Leader新信号（{reason}）")
            self._write_alert("COIN_TPSL_EXITED", coin, f"TP/SL触发平仓，已标记退出等待Leader新信号。原因: {reason}",
                                    extra_data={"退出原因": str(reason)})
        else:
            self.logger.info(f"[EXITED-MANUAL] {coin} 检测到仓位消失，标记退出等待Leader新信号（{reason}）")
            self._write_alert("COIN_MANUAL_EXITED", coin, f"检测到仓位消失（用户手动退出），等待Leader新信号。原因: {reason}",
                                    extra_data={"退出原因": str(reason)})
        
        return True

    def _clear_exited_coin(self, coin: str) -> None:
        """清除退出标记，恢复跟单"""
        if coin in self._exited_coins:
            del self._exited_coins[coin]
            self._save_exited_coins()
            self.logger.info(f"[EXITED] {coin} 退出标记已清除，恢复正常跟单")

    # ----------------------------------------------------------------
    # TP/SL 活跃状态感知 (防止TP/SL触发后被误判为用户退出)
    # ----------------------------------------------------------------
    def _load_tpsl_active(self) -> None:
        """从文件加载TP/SL活跃状态"""
        try:
            if os.path.exists(self._tpsl_active_file):
                with open(self._tpsl_active_file, "r", encoding="utf-8") as f:
                    data = json.load(f)
                if isinstance(data, dict):
                    self._tpsl_active = {k: float(v) for k, v in data.items()}
                else:
                    self._tpsl_active = {}
                if self._tpsl_active:
                    self.logger.info(f"[TPSL-ACTIVE] 加载TP/SL活跃状态: {sorted(self._tpsl_active.keys())}")
            else:
                self.logger.info("[TPSL-ACTIVE] TP/SL活跃状态为空")
        except Exception as e:
            self.logger.warning(f"[TPSL-ACTIVE] 加载TP/SL活跃状态失败: {e}，初始化为空")
            self._tpsl_active = {}

    def _save_tpsl_active(self) -> None:
        """持久化TP/SL活跃状态到文件"""
        try:
            os.makedirs(os.path.dirname(self._tpsl_active_file), exist_ok=True)
            tmp_path = self._tpsl_active_file + ".tmp"
            with open(tmp_path, "w", encoding="utf-8") as f:
                json.dump(self._tpsl_active, f, ensure_ascii=False, indent=2)
            os.replace(tmp_path, self._tpsl_active_file)
        except Exception as e:
            self.logger.warning(f"[TPSL-ACTIVE] 保存TP/SL活跃状态失败: {e}")

    def _add_tpsl_active(self, coin: str) -> None:
        """标记币种有活跃的TP/SL挂单（在下TP/SL时调用）"""
        import time as _time_mod
        self._tpsl_active[coin] = _time_mod.time()
        self._save_tpsl_active()
        self.logger.info(f"[TPSL-ACTIVE] {coin} 标记为TP/SL活跃")

    def _remove_tpsl_active(self, coin: str) -> None:
        """移除币种的TP/SL活跃标记"""
        if coin in self._tpsl_active:
            del self._tpsl_active[coin]
            self._save_tpsl_active()
            self.logger.info(f"[TPSL-ACTIVE] {coin} 移除TP/SL活跃标记")

    def _is_tpsl_active(self, coin: str) -> bool:
        """检查币种是否有活跃的TP/SL挂单"""
        return coin in self._tpsl_active

    # ----------------------------------------------------------------
    # exited_coins 自动过期 (2小时后自动清除)
    # ----------------------------------------------------------------
    def _check_exited_expiry(self, leader_positions: Dict[str, dict]) -> None:
        """检查exited_coins中超过2小时的条目，自动清除过期退出标记
        
        规则：退出时间超过2小时，且Leader仓位无显著变化（变化<20%），自动清除。
        """
        import time as _time_mod
        now = _time_mod.time()
        expired_coins = []
        for coin, exit_ts in list(self._exited_coins.items()):
            elapsed_hours = (now - exit_ts) / 3600
            if elapsed_hours < 2:
                continue  # 不到2小时，跳过
            # 检查Leader仓位变化
            leader_pos = leader_positions.get(coin, {})
            leader_szi = abs(float(leader_pos.get("szi", 0)))
            prev_leader_pos = self.prev_leader_positions.get(coin, {})
            prev_leader_szi = abs(float(prev_leader_pos.get("szi", 0)))
            if prev_leader_szi > 0 and leader_szi > 0:
                change_ratio = abs(leader_szi - prev_leader_szi) / prev_leader_szi
                if change_ratio < 0.2:
                    expired_coins.append((coin, elapsed_hours))
                else:
                    self.logger.info(
                        f"[EXITED-EXPIRY] {coin} 退出已{elapsed_hours:.1f}小时，"
                        f"但Leader仓位变化{change_ratio:.1%}>=20%，保留退出标记"
                    )
            elif leader_szi == 0:
                # Leader也无仓位了，可以清除（用户退出和Leader退出对齐了）
                expired_coins.append((coin, elapsed_hours))
            else:
                # prev=0, curr>0: Leader有新仓位，应已被新信号逻辑捕获
                expired_coins.append((coin, elapsed_hours))
        for coin, elapsed_hours in expired_coins:
            self.logger.info(
                f"[EXITED-EXPIRY] {coin} 退出已{elapsed_hours:.1f}小时，"
                f"Leader仓位无显著变化，自动清除退出标记"
            )
            self._clear_exited_coin(coin)
        
        # ===== tpsl_active 过期清理（超过24小时的条目）=====
        tpsl_expired = []
        for coin, ts in list(self._tpsl_active.items()):
            if now - ts > 86400:  # 24小时
                tpsl_expired.append(coin)
        for coin in tpsl_expired:
            self.logger.info(f"[TPSL-ACTIVE] {coin} 活跃标记超过24小时，自动清理")
            self._remove_tpsl_active(coin)



    # ----------------------------------------------------------------
    # v3新增: WebSocket组件管理
    # ----------------------------------------------------------------

    def _init_ws_components(self) -> None:
        """初始化并启动WebSocket相关组件"""
        if not self._ws_enabled:
            self.logger.info("[WS] WebSocket已禁用，运行纯轮询模式")
            return

        try:
            import websockets  # noqa: F401 检查websockets库是否可用
        except ImportError:
            self.logger.warning("[WS] websockets库未安装，降级到纯轮询模式")
            self._ws_enabled = False
            return

        # 启动去抖器
        self._debouncer.start()

        # 创建并启动WS监听线程
        self._ws_listener = WsListener(
            ws_url=self.config.ws_url,
            leader_addr=self.config.leader_addr,
            change_queue=self._ws_change_queue,
            logger=self.logger,
            initial_backoff=self.config.ws_reconnect_initial_backoff,
            max_backoff=self.config.ws_reconnect_max_backoff,
            pong_timeout=self.config.ws_pong_timeout,
            wakeup_event=self._ws_wakeup,
        )
        self._ws_listener.start()
        self.logger.info("[WS] WebSocket组件已初始化并启动")

    def _stop_ws_components(self) -> None:
        """停止WebSocket相关组件"""
        if self._ws_listener:
            self._ws_listener.stop()
        self._debouncer.stop()
        self._debouncer.flush_all()
        self.logger.info("[WS] WebSocket组件已停止")

    def _process_ws_queue(self) -> None:
        """处理WebSocket队列中的变化数据

        在每次_poll()前调用，将WS推送的变化经过:
        状态对比器 → 去抖器 → 去重器 → 跟单引擎
        """
        if not self._ws_enabled or not self._ws_listener:
            return

        # 更新WS连接状态到健康检查
        ws_connected = self._ws_listener.is_connected
        self.health_checker.update(ws_connected=ws_connected)

        # 降级检测
        if not ws_connected:
            self.logger.debug("[WS] WebSocket未连接，本轮仅使用轮询数据")
            return

        # 从队列取出所有WS变化，合并所有DEX的持仓后再统一比较
        # 关键：HL的WS按DEX分别推送，每次只包含该DEX的持仓。
        # 必须先合并所有DEX的持仓，再与上次状态对比，否则会误判其他DEX的仓位被平掉。
        ws_changes_count = 0
        while not self._ws_change_queue.empty():
            try:
                ws_data = self._ws_change_queue.get_nowait()
            except queue.Empty:
                break

            try:
                # 检查是否为openOrders或userFills频道消息
                ws_channel = ws_data.get("_channel", "")

                if ws_channel == "openOrders":
                    # openOrders推送: Leader挂单变化（含TP/SL调整）
                    # 提取涉及币种，精准触发TP/SL同步
                    coins = ws_data.get("_coins", [])
                    dex_tag = ws_data.get("_dex", "main")
                    order_count = ws_data.get("_order_count", "?")
                    now = time.time()
                    # 内容去重：币种集合未变则跳过（多DEX推送合并）
                    _coins_key = frozenset(coins)
                    if _coins_key == getattr(self, '_ws_last_orders_coins', None):
                        continue
                    self._ws_last_orders_coins = _coins_key
                    if now - self._ws_last_tpsl_sync_time > 30:
                        self._ws_tpsl_sync_needed = True
                        self._ws_tpsl_coins.update(coins)
                        self._ws_last_tpsl_sync_time = now  # 立即更新，防止队列积压时重复输出日志
                        self.logger.info(
                            f"[WS-TPSL] openOrders变化(dex={dex_tag}, orders={order_count}, "
                            f"coins={coins})，标记即时TP/SL同步"
                        )
                    else:
                        # 防抖期间仍收集币种，等防抖结束后一起同步
                        self._ws_tpsl_coins.update(coins)
                        self.logger.debug(
                            f"[WS-TPSL] openOrders变化（防抖中，收集coins={coins}）"
                        )
                    continue

                if ws_channel == "userFills":
                    # userFills推送: Leader成交通知
                    coins = ws_data.get("_coins", [])
                    dex_tag = ws_data.get("_dex", "main")
                    fill_count = ws_data.get("_fill_count", "?")
                    self._ws_fill_coins.update(coins)
                    # 限流：30秒内只输出一次日志
                    if not hasattr(self, '_ws_last_fill_log_time'):
                        self._ws_last_fill_log_time = 0
                    if time.time() - self._ws_last_fill_log_time > 30:
                        self.logger.info(
                            f"[WS-FILL] 检测到成交(dex={dex_tag}, fills={fill_count}, "
                            f"coins={coins})，标记优先刷新"
                        )
                        self._ws_last_fill_log_time = time.time()
                    continue

                # clearinghouseState: 仅记录日志，不参与状态对比和跟单决策
                # WS仓位数据可能存在多DEX分片推送问题，改用REST全量轮询做决策
                # 保留WS用于openOrders(TP/SL同步)和userFills(成交加速)

            except Exception as e:
                self.logger.warning(f"[WS] 处理WS数据异常: {e}")



    def _on_debounce_flush(self, coin: str, change_data: Dict[str, Any]) -> None:
        """去抖器回调：去抖完成后检查去重，然后触发跟单

        Args:
            coin: 币种名称
            change_data: 变化数据字典
        """
        # 去重检查
        szi = change_data.get("new_szi", change_data.get("old_szi", "0"))
        leverage = str(change_data.get("leverage", 1))
        entry_px = "0"  # WS推送变化不含entryPx，用szi+leverage足够去重

        if not self._deduplicator.should_process_change(coin, szi, leverage, entry_px):
            self.logger.debug(f"[DEDUP] 跳过去重变化: {coin}")
            return

        # 触发跟单处理
        self._process_ws_change(coin, change_data)

    def _process_ws_change(self, coin: str, change_data: Dict[str, Any]) -> None:
        """处理经过去抖+去重后的WS变化

        获取实时价格和账户信息，调用跟单引擎执行操作。
        """
        change_type = change_data.get("type", "")
        # ===== 退出标记清除（Leaderopen/adjust → 新信号 → 清除退出标记） =====
        if coin in self._exited_coins and change_type in ("open", "adjust"):
            self._clear_exited_coin(coin)
        self.logger.info(
            f"[WS-TRADE] 处理WS变化: {coin} {change_type} "
            f"szi={change_data.get('old_szi', '?')}→{change_data.get('new_szi', '?')}"
        )

        try:
            # 获取实时价格
            mids = get_all_mids(self.config, self.dex_cache, self.logger)
            price = mids.get(coin, 0)
            # FIX-P0: close操作不依赖mids价格，不跳过
            if change_type != "close" and price <= 0:
                self.logger.warning(f"[WS-TRADE] 跳过{coin}: 无法获取价格")
                return

            # 获取账户信息
            try:
                user_value = get_account_value(self.config.user_main_addr, self.config, self.logger)
                leader_value = get_account_value(self.config.leader_addr, self.config, self.logger)
            except Exception:
                user_value = 0
                leader_value = 0

            copy_ratio = self._calc_copy_ratio(user_value=user_value, leader_value=leader_value)
            if copy_ratio <= 0:
                self.logger.warning("[WS-TRADE] 跟单比例为0，跳过操作")
                return


            # ===== 护栏4: 二次确认 (WS路径) =====
            if self.config.safety_enabled and self.config.double_confirm_enabled:
                if change_type == "close":
                    confirmed_closed = double_confirm_position_closed(
                        self.config.leader_addr, coin, self.config, self.dex_cache, self.logger
                    )
                    if not confirmed_closed:
                        self.logger.warning(
                            f"[SAFETY-4] [WS] 跳过{coin}平仓: 二次确认Leader仓位仍存在"
                        )
                        self._write_alert(
                            "SAFETY_4_WS_CLOSE_CANCELLED",
                            coin,
                            f"WS路径跳过{coin}平仓: 二次确认Leader仓位仍存在"
                        )
                        return
                elif change_type == "open":
                    confirmed_exists = double_confirm_position_exists(
                        self.config.leader_addr, coin, self.config, self.dex_cache, self.logger
                    )
                    if not confirmed_exists:
                        self.logger.warning(
                            f"[SAFETY-4] [WS] 跳过{coin}开仓: 二次确认Leader仓位不存在"
                        )
                        self._write_alert(
                            "SAFETY_4_WS_OPEN_CANCELLED",
                            coin,
                            f"WS路径跳过{coin}开仓: 二次确认Leader仓位不存在"
                        )
                        return

            # 调用v2已有的持仓变化处理逻辑
            self._handle_position_changes([change_data], copy_ratio, mids, user_value)

        except Exception as e:
            self.logger.error(f"[WS-TRADE] 处理WS变化异常 {coin}: {e}")
            # 处理失败，撤销去重标记，允许下次重试
            szi = change_data.get("new_szi", change_data.get("old_szi", "0"))
            leverage = str(change_data.get("leverage", 1))
            if hasattr(self, '_deduplicator'):
                self._deduplicator.mark_failed(coin, str(szi), leverage)

    # ----------------------------------------------------------------
    # 并发API调用
    # ----------------------------------------------------------------
    def _concurrent_get(self, tasks: Dict[str, Any], max_workers: Optional[int] = None, timeout: Optional[int] = None) -> Dict[str, Any]:
        max_workers = max_workers or self.config.concurrent_max_workers
        timeout = timeout or self.config.concurrent_timeout
        results: Dict[str, Any] = {}
        if not tasks:
            return results
        with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor_pool:
            future_to_key = {executor_pool.submit(fn): key for key, fn in tasks.items()}
            done, not_done = concurrent.futures.wait(future_to_key.keys(), timeout=timeout)
            for future in done:
                key = future_to_key[future]
                try:
                    results[key] = future.result()
                except Exception as e:
                    self.logger.warning(f"并发调用 [{key}] 失败: {e}")
                    results[key] = None
            for future in not_done:
                key = future_to_key[future]
                self.logger.warning(f"并发调用 [{key}] 超时({timeout}s)")
                results[key] = None
        return results

    # ----------------------------------------------------------------
    # 紧急制动
    # ----------------------------------------------------------------
    def _check_emergency_stop(self) -> bool:
        if not os.path.exists(self.config.emergency_stop_file):
            return False
        self.logger.critical("[EMERGENCY] 检测到紧急制动信号！开始执行紧急平仓...")
        self.logger.info("[EMERGENCY] 步骤1: 撤掉所有挂单")
        self.executor.cancel_all_orders()
        self.logger.info("[EMERGENCY] 步骤2: 市价平掉所有仓位")
        self.executor.market_close_all()
        self.running = False
        try:
            os.remove(self.config.emergency_stop_file)
            self.logger.info("[EMERGENCY] 步骤3: 已删除信号文件")
        except Exception as e:
            self.logger.warning(f"[EMERGENCY] 删除信号文件失败: {e}")
        self.logger.critical("[EMERGENCY] 紧急制动执行完毕，脚本即将停止")
        self._write_alert("EMERGENCY_STOP", "ALL", "紧急制动已执行完毕")
        return True

    def _check_pause(self) -> bool:
        """检查是否处于暂停跟单状态。暂停时保持运行但不执行新交易。"""
        return os.path.exists(self.config.pause_file)

    # ----------------------------------------------------------------
    # 下单后确认
    # ----------------------------------------------------------------
    def _confirm_order(self, coin: str, action: str, expected_is_buy: bool, expected_sz: float, pre_szi: float, copy_ratio: float = 0.0) -> None:
        if not self.live_mode:
            return
        confirm_wait = self.config.confirm_wait_seconds
        self.logger.info(f"[CONFIRM] 等待{confirm_wait}秒后确认 {coin} {action}...")
        time.sleep(confirm_wait)
        try:
            new_positions = get_positions(self.config.user_main_addr, self.config, self.dex_cache, self.logger)
            post_szi = float(new_positions.get(coin, {}).get("szi", 0))
            actual_delta = abs(post_szi - pre_szi)
            if action == "close":
                if abs(post_szi) > abs(pre_szi) * 0.1:
                    self.logger.error(f"[CONFIRM] {coin} 平仓确认失败! 平仓前szi={pre_szi}, 平仓后szi={post_szi}, 残留仓位={abs(post_szi):.6f}")
                    self._write_alert("CLOSE_FAILED", coin, f"平仓确认失败: szi {pre_szi} → {post_szi}, 残留={abs(post_szi):.6f}")
                else:
                    self.logger.info(f"[CONFIRM] {coin} 平仓确认成功: szi {pre_szi} → {post_szi}")
                    self._log_event("ORDER_CLOSE", coin, f"szi {pre_szi} → {post_szi}")
                    self.position_ratios.pop(coin, None)
                    self._save_position_ratios()
                    self._today_closes += 1  # P3#9: 平仓计数
            elif action == "open":
                expected_sign = 1 if expected_is_buy else -1
                actual_sign = 1 if post_szi > 0 else (-1 if post_szi < 0 else 0)
                if actual_sign != 0 and expected_sign != actual_sign:
                    self.logger.error(f"[CONFIRM] {coin} 开仓方向不符! 预期={'做多' if expected_sign > 0 else '做空'}, 实际szi={post_szi}")
                    self._write_alert("OPEN_FAILED", coin, f"开仓后方向不符: szi={post_szi}")
            elif action == "adjust":
                actual_delta = post_szi - pre_szi
                expected_delta_sign = 1 if expected_is_buy else -1
                actual_delta_sign = 1 if actual_delta > 0 else (-1 if actual_delta < 0 else 0)
                if actual_delta_sign != 0 and expected_delta_sign != actual_delta_sign:
                    self.logger.error(f"[CONFIRM] {coin} 调仓方向不符! 预期{'增' if expected_delta_sign > 0 else '减'}, 实际变化={actual_delta:.6f}")
                    self._write_alert("ADJUST_FAILED", coin, f"调仓后方向不符: pre_szi={pre_szi}, post_szi={post_szi}")
                if expected_sz > 0 and abs(actual_delta) < expected_sz * 0.5:
                    shortfall = expected_sz - abs(actual_delta)
                    self.logger.error(f"[CONFIRM] {coin} 部分成交! 预期delta={expected_sz:.6f}, 实际delta={actual_delta:.6f}, 差额={shortfall:.6f}")
                    # FIX-P2: 部分成交后重试补差额，最多2次
                    remaining_shortfall = shortfall
                    self._audit_partial_fills += 1
                    for _retry_attempt in range(1, 3):
                        self.logger.info(f"[CONFIRM-RETRY] {coin} 第{_retry_attempt}次补单, 差额={remaining_shortfall:.6f}")
                        self._audit_confirm_retries += 1
                        try:
                            mids = get_all_mids(self.config, self.dex_cache, self.logger)
                            price = mids.get(coin, 0)
                            if price <= 0:
                                self.logger.warning(f"[CONFIRM-RETRY] {coin} 无法获取价格，停止补单")
                                break
                            success = self.executor.adjust_position(coin, expected_is_buy, remaining_shortfall, price)
                            if not success:
                                self.logger.warning(f"[CONFIRM-RETRY] {coin} 第{_retry_attempt}次补单失败")
                                break
                            # 等待确认
                            time.sleep(self.config.confirm_wait_seconds)
                            # 检查总成交情况
                            check_pos = get_positions(self.config.user_main_addr, self.config, self.dex_cache, self.logger)
                            check_szi = float(check_pos.get(coin, {}).get("szi", 0))
                            total_delta = abs(check_szi - pre_szi)
                            if total_delta >= expected_sz * 0.9:
                                self.logger.info(f"[CONFIRM-RETRY] {coin} 补单成功! 总delta={total_delta:.6f} (目标{expected_sz:.6f})")
                                break
                            else:
                                remaining_shortfall = expected_sz - total_delta
                                if remaining_shortfall > 0:
                                    self.logger.warning(f"[CONFIRM-RETRY] {coin} 补单后仍有差额={remaining_shortfall:.6f}")
                                    self._write_alert("CONFIRM_RESIDUAL", coin, f"补单后仍有差额={remaining_shortfall:.6f}")
                                else:
                                    self.logger.info(f"[CONFIRM-RETRY] {coin} 补单完成，总delta={total_delta:.6f}")
                                    break
                        except Exception as e:
                            self.logger.error(f"[CONFIRM-RETRY] {coin} 补单异常: {e}")
                            break
                try:
                    user_orders = get_open_orders(self.config.user_main_addr, self.config, self.dex_cache, self.logger)
                    for o in user_orders:
                        if (o.get("coin") == coin and not o.get("reduceOnly") and o.get("side") == ("B" if expected_is_buy else "A")):
                            oid = o.get("oid", 0)
                            self.logger.error(f"[CONFIRM] {coin} 发现IOC残余挂单 oid={oid}, 正在撤销")
                            self.executor.cancel_order(coin, oid)
                except Exception as e:
                    self.logger.warning(f"[CONFIRM] 检查IOC残余挂单失败 {coin}: {e}")
                if actual_delta >= expected_sz * 0.5:
                    self.logger.info(f"[CONFIRM] {coin} {action}确认: szi {pre_szi} → {post_szi}, delta={actual_delta:.6f}")
                    if action in ("open", "adjust") and copy_ratio > 0:
                        ratio_to_store = copy_ratio
                        self.position_ratios[coin] = ratio_to_store
                        self._save_position_ratios()
                        self._log_event(f"ORDER_{action.upper()}", coin, f"szi={post_szi:.6f}, delta={actual_delta:.6f}, ratio={copy_ratio:.4f}")
                        # Phase 4: 滑点追踪
                        if hasattr(self, '_phase4'):
                            try:
                                mids_confirm = get_all_mids(self.config, self.dex_cache, self.logger)
                                current_mid = mids_confirm.get(coin, 0)
                                if current_mid > 0:
                                    self._phase4.on_order_filled(
                                        coin=coin,
                                        expected_price=mids.get(coin, current_mid),
                                        actual_price=current_mid,
                                        size=actual_delta,
                                        action=action,
                                    )
                            except Exception as e:
                                self.logger.debug(f"[Phase4] 滑点记录异常: {e}")
                        # P3#9: 开仓/调仓计数
                        if action == "open":
                            self._today_opens += 1
                        elif action == "adjust":
                            self._today_adjusts += 1
        except Exception as e:
            self.logger.error(f"[CONFIRM] 确认订单异常 {coin}: {e}")

    # ----------------------------------------------------------------
    # 告警
    # ----------------------------------------------------------------
    def _write_alert(self, alert_type: str, coin: str, message: str, *, extra_data: dict = None, level_override: str = None) -> None:
        try:
            alerts: List[dict] = []
            alert_path = self.config.alert_file
            if os.path.exists(alert_path):
                with open(alert_path, "r", encoding="utf-8") as f:
                    alerts = json.load(f)
            alert = {"type": alert_type, "coin": coin, "message": message, "timestamp": datetime.now().isoformat(), "poll_count": self.poll_count}
            alerts.append(alert)
            alerts = alerts[-20:]
            tmp_path = alert_path + ".tmp"
            with open(tmp_path, "w", encoding="utf-8") as f:
                json.dump(alerts, f, ensure_ascii=False, indent=2)
            os.replace(tmp_path, alert_path)
            # Phase 1: 飞书告警推送
            if hasattr(self, '_alert_manager'):
                self._alert_manager.send_alert(alert_type, coin, message, self.poll_count, extra_data=extra_data, level_override=level_override)
        except Exception as e:
            self.logger.warning(f"写入告警文件失败: {e}")


    def _check_push_health(self) -> None:
        """检查push脚本健康状态（队列堆积检测）"""
        try:
            import json as _json
            status_file = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'data', 'push_status.json')
            queue_file = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'data', 'alert_queue.json')
            
            if not os.path.exists(status_file):
                return  # push还没运行过，正常
            
            with open(status_file, 'r') as f:
                status = _json.load(f)
            
            last_run_ts = status.get('last_run_timestamp', 0)
            elapsed_min = (time.time() - last_run_ts) / 60 if last_run_ts else 999
            
            # 检查队列长度
            queue_len = 0
            if os.path.exists(queue_file):
                with open(queue_file, 'r') as f:
                    q = _json.load(f)
                    queue_len = len(q)
            
            # 告警条件: 队列>20条且push超过15分钟未执行
            if queue_len > 20 and elapsed_min > 15:
                self._write_alert(
                    "PUSH_STALLED", "ALL",
                    f"push脚本可能异常: 队列堆积{queue_len}条, 距上次push {elapsed_min:.0f}分钟"
                )
                self.logger.warning(f"[ALERT] push堆积检测: 队列={queue_len}, 距上次push={elapsed_min:.0f}min")
            
        except Exception:
            pass  # 检测失败不影响主流程

    def _log_event(self, event_type: str, coin: str, detail: str) -> None:
        """记录关键业务事件到 events.log"""
        self.logger.info(f"[EVENT] {event_type} | {coin} | {detail}")

    # ----------------------------------------------------------------
    # 平仓后清理TP/SL
    # ----------------------------------------------------------------
    def _cancel_reduce_only_orders(self, coin: str) -> None:
        try:
            user_orders = get_open_orders(self.config.user_main_addr, self.config, self.dex_cache, self.logger)
            cancelled = 0
            for o in user_orders:
                if o.get("coin") == coin and o.get("reduceOnly"):
                    oid = o.get("oid", 0)
                    if oid:
                        self.logger.info(f"  [CLEANUP] 撤销{coin}残余TP/SL oid={oid}")
                        self.executor.cancel_order(coin, oid)
                        cancelled += 1
            if cancelled > 0:
                self.logger.info(f"  [CLEANUP] {coin} 共撤销 {cancelled} 个残余TP/SL")
                self._remove_tpsl_active(coin)  # 清理TP/SL活跃标记
        except Exception as e:
            self.logger.warning(f"  [CLEANUP] 清理{coin}残余TP/SL失败: {e}")

    # ----------------------------------------------------------------
    # 全量对账
    # ----------------------------------------------------------------
    def _reconciliation_check(self, leader_positions: Dict[str, dict], copy_ratio: float, mids: Dict[str, float]) -> None:
        self.logger.info("[RECON] 开始全量对账...")
        self._today_recons += 1  # P3#9: 对账计数
        
        # exited_coins 自动过期检查
        self._check_exited_expiry(leader_positions)
        try:
            user_positions = get_positions(self.config.user_main_addr, self.config, self.dex_cache, self.logger)
        except Exception as e:
            self.logger.error(f"[RECON] 获取用户持仓失败: {e}")
            self._write_alert("RECON_QUERY_FAIL", "ALL", f"RECON获取用户持仓失败: {e}")
            return
        alert_count = 0
        leader_coins = set(leader_positions.keys())
        user_coins = set(user_positions.keys())
        active_coins = leader_coins | user_coins
        self.position_ratios = {k: v for k, v in self.position_ratios.items() if k in active_coins}
        deviation_tolerance = self.config.deviation_tolerance
        recon_min_val = self.config.position_min_value
        recovery_changes: List[dict] = []  # ===== v3 RECON自动补仓收集 =====
        deviation_corrections: List[dict] = []
        residual_close_list: List[dict] = []
        for coin in leader_coins:
            leader_pos = leader_positions[coin]
            leader_szi = float(leader_pos.get("szi", 0))
            if leader_szi == 0:
                continue
            user_pos = user_positions.get(coin, {})
            user_szi = float(user_pos.get("szi", 0))
            leader_direction = "多" if leader_szi > 0 else "空"
            leader_pos_value = abs(float(leader_pos.get("positionValue", 0)))
            stored_ratio = self.position_ratios.get(coin)
            ratio_for_check = stored_ratio if stored_ratio is not None else copy_ratio
            expected_user_value = leader_pos_value * ratio_for_check
            if user_szi == 0:
                # ① 检查退出等待列表
                if coin in self._exited_coins:
                    prev_leader_pos = self.prev_leader_positions.get(coin, {})
                    prev_leader_szi = abs(float(prev_leader_pos.get("szi", 0)))
                    curr_leader_szi = abs(leader_szi)
                    # Leader加仓>20% → 新信号
                    leader_new_signal = (prev_leader_szi > 0 and curr_leader_szi > prev_leader_szi * 1.2)
                    # Leader退出后重新开仓（上一轮Leader无仓位，本轮有了）→ 新信号
                    leader_reentry = (prev_leader_szi == 0 and curr_leader_szi > 0)
                    if leader_new_signal or leader_reentry:
                        reason = f"加仓{prev_leader_szi:.4f}→{curr_leader_szi:.4f}" if leader_new_signal else f"重新开仓({curr_leader_szi:.4f})"
                        self.logger.info(f"[EXITED] {coin} 检测到Leader新信号({reason})，清除退出标记，跟单")
                        self._clear_exited_coin(coin)
                        # fall through to normal recovery below
                    else:
                        self.logger.info(f"[EXITED] {coin} 在退出等待列表中，Leader无新信号(上轮szi={prev_leader_szi:.4f},本轮szi={curr_leader_szi:.4f})，跳过")
                        continue
                # ② 检查是否刚发生退出（上轮有仓位→本轮消失）→ 标记退出，等待新信号
                if coin not in self._exited_coins:
                    prev_user_pos = self.prev_user_positions.get(coin, {})
                    prev_user_szi = float(prev_user_pos.get("szi", 0))
                    if prev_user_szi != 0:
                        if self._mark_coin_exited(coin, f"仓位消失(上轮szi={prev_user_szi})，等待Leader新信号"):
                            continue
                        # TP/SL活跃，不标记exited，不跳过，让RECON继续补仓
                # ③ 触发自动补仓（用户本来就没仓位，非退出场景）
                # 最低下单金额限制：Leader仓位价值×跟单比例<$10时跳过
                user_recovery_value = leader_pos_value * copy_ratio
                if user_recovery_value < self.executor.MIN_ORDER_VALUE_USD:
                    self.logger.info(f"[RECON-RECOVERY] {coin}: 用户目标仓位≈${user_recovery_value:.2f}<${self.executor.MIN_ORDER_VALUE_USD}，跳过补仓（不告警）")
                else:
                    self.logger.warning(f"[RECON-RECOVERY] {coin} 用户无仓位，Leader仍有{leader_direction}仓 价值=${leader_pos_value:.2f}，触发自动补仓")
                    recovery_changes.append({
                    "coin": coin,
                    "type": "open",
                    "new_szi": leader_szi,
                    "positionValue": leader_pos_value,
                    "leverage": leader_pos.get("leverage_value", 1),
                    "leverage_type": leader_pos.get("leverage_type", "cross"),
                    "dex": leader_pos.get("dex", ""),
                })
                continue
            user_direction = "多" if user_szi > 0 else "空"
            if (leader_szi > 0 and user_szi < 0) or (leader_szi < 0 and user_szi > 0):
                self.logger.warning(f"[RECON] 方向相反!! Leader{coin}={leader_direction}, 用户{coin}={user_direction} -> 自动翻转修复")
                # Step 1: 取消该币种所有挂单
                try:
                    open_orders = get_open_orders(self.config.user_main_addr, self.config, self.dex_cache, self.logger)
                    for o in open_orders:
                        if o.get("coin") == coin:
                            self.executor.cancel_order(coin, o["oid"])
                            self.logger.info(f"[RECON-FLIP] 取消{coin}挂单 oid={o['oid']}")
                except Exception as e:
                    self.logger.error(f"[RECON-FLIP] 取消{coin}挂单异常: {e}")
                import time as _time
                _time.sleep(1)
                # Step 2: 平掉错误方向仓位
                try:
                    self.executor.close_position(coin, str(user_szi))
                    self.logger.info(f"[RECON-FLIP] 平仓{coin} szi={user_szi}")
                except Exception as e:
                    self.logger.error(f"[RECON-FLIP] 平仓{coin}异常: {e}")
                _time.sleep(2)
                # Step 3: 加入recovery_changes，后续自动开正确方向仓位
                recovery_changes.append({
                    "coin": coin,
                    "type": "open",
                    "new_szi": leader_szi,
                    "positionValue": leader_pos_value,
                    "leverage": leader_pos.get("leverage_value", 1),
                    "leverage_type": leader_pos.get("leverage_type", "cross"),
                    "dex": leader_pos.get("dex", ""),
                })
                self._log_event("RECON_FLIP", coin, f"方向翻转: {user_direction}->{leader_direction}")
                continue
            user_pos_value = abs(float(user_pos.get("positionValue", 0)))
            if expected_user_value < recon_min_val:
                continue  # 极小仓位跳过RECON偏差检测（最小下单量限制导致永久偏差）
            if expected_user_value > 0:
                deviation = abs(user_pos_value - expected_user_value) / expected_user_value
                if deviation > deviation_tolerance:
                    self.logger.warning(f"[RECON] 仓位偏差 {coin}: 预期≈${expected_user_value:.2f}(ratio={ratio_for_check:.4f}), 实际=${user_pos_value:.2f}, 偏差={deviation:.1%} → 自动修正")
                    self._write_alert("RECON_DEVIATION", coin, f"偏差{deviation:.1%}, 预期≈${expected_user_value:.2f}, 实际=${user_pos_value:.2f}, 已触发自动修正",
                                    extra_data={"偏差": f"{deviation:.1%}", "预期": f"${expected_user_value:.2f}", "实际": f"${user_pos_value:.2f}"})
                    alert_count += 1
                    deviation_corrections.append({
                        "coin": coin,
                        "leader_szi": leader_szi,
                        "user_szi": user_szi,
                        "ratio_for_check": ratio_for_check,
                        "deviation": deviation,
                        "positionValue": leader_pos_value,
                        "leverage": leader_pos.get("leverage_value", 1),
                        "leverage_type": leader_pos.get("leverage_type", "cross"),
                    })
        for coin in user_coins - leader_coins:
            user_pos = user_positions[coin]
            user_szi = float(user_pos.get("szi", 0))
            if user_szi != 0:
                user_pos_value = abs(float(user_pos.get("positionValue", 0)))
                self._recon_residual_count[coin] = self._recon_residual_count.get(coin, 0) + 1
                residual_n = self._recon_residual_count[coin]
                if residual_n >= 3:
                    self.logger.warning(f"[RECON] 用户残留仓位 {coin} {'多' if user_szi > 0 else '空'} 价值=${user_pos_value:.2f}, 连续{residual_n}次 → 自动平仓")
                    residual_close_list.append({
                        "coin": coin,
                        "user_szi": user_szi,
                        "positionValue": user_pos_value,
                    })
                    alert_count += 1
                else:
                    self.logger.warning(f"[RECON] 用户残留仓位 {coin} {'多' if user_szi > 0 else '空'} 价值=${user_pos_value:.2f} (第{residual_n}/3次)")
            else:
                self._recon_residual_count.pop(coin, None)
        # 清理已不存在的币种计数
        for coin in list(self._recon_residual_count.keys()):
            if coin not in (user_coins - leader_coins):
                self._recon_residual_count.pop(coin, None)
        # ===== v3 RECON自动补仓: 执行补仓 =====
        if recovery_changes:
            self.logger.info(f"[RECON-RECOVERY] 需要补仓 {len(recovery_changes)} 个币种: {[c['coin'] for c in recovery_changes]}")
            try:
                self._handle_position_changes(recovery_changes, copy_ratio, mids)
                self.logger.info(f"[RECON-RECOVERY] 补仓指令已发送")
                for rc in recovery_changes:
                    self._log_event("RECON_RECOVERY", rc["coin"], f"自动补仓 type={rc["type"]} szi={rc.get("new_szi", "?")}")
            except Exception as e:
                self.logger.error(f"[RECON-RECOVERY] 补仓执行失败: {e}")
                self._write_alert("RECON_RECOVERY_FAIL", "MULTI", f"自动补仓失败: {e}",
                                    extra_data={"错误": str(e)})
        # ===== 补仓结束 =====

        # ===== RECON 偏差自动修正 =====
        if deviation_corrections:
            deviation_corrections.sort(key=lambda x: x["deviation"], reverse=True)
            self.logger.info(f"[RECON-ADJUST] 开始自动修正 {len(deviation_corrections)} 个偏差仓位")
            for dc in deviation_corrections:
                coin = dc["coin"]
                price = mids.get(coin, 0)
                if price <= 0:
                    self.logger.warning(f"[RECON-ADJUST] 跳过{coin}: 无法获取价格")
                    continue
                leader_szi = dc["leader_szi"]
                user_szi = dc["user_szi"]
                ratio = dc["ratio_for_check"]
                target_szi = leader_szi * ratio
                target_szi_before_round = target_szi
                target_szi = round_sz(target_szi, coin, self.sz_decimals_map)
                self.logger.warning(f"[DEBUG-RECON] {coin}: leader_szi={leader_szi}, ratio={ratio}, before_round={target_szi_before_round}, after_round={target_szi}, sz_decimals={self.sz_decimals_map.get(coin, 4)}")
                delta = target_szi - user_szi
                if abs(delta) < 1e-8:
                    self.logger.info(f"[RECON-ADJUST] {coin}: delta≈0，跳过")
                    continue
                is_buy = delta > 0
                delta_sz = abs(delta)
                # 减仓安全检查
                if not is_buy and abs(user_szi) > 0:
                    try:
                        _chk = get_positions(self.config.user_main_addr, self.config, self.dex_cache, self.logger)
                        _chk_szi = float(_chk.get(coin, {}).get("szi", 0))
                    except Exception:
                        _chk_szi = user_szi
                    if delta_sz > abs(_chk_szi):
                        delta_sz = round_sz(abs(_chk_szi), coin, self.sz_decimals_map)
                        if delta_sz <= 0:
                            self.logger.info(f"[RECON-ADJUST] {coin}: 截断后delta=0，跳过")
                            continue
                        self.logger.info(f"[RECON-ADJUST] {coin}: 减仓截断 delta→{delta_sz:.6f}")
                # 执行修正
                pre_szi = 0.0
                try:
                    pre_pos = get_positions(self.config.user_main_addr, self.config, self.dex_cache, self.logger)
                    pre_szi = float(pre_pos.get(coin, {}).get("szi", 0))
                except Exception:
                    pass
                self.logger.info(f"[RECON-ADJUST] {coin}: 目标szi={target_szi:.6f}, 当前={user_szi:.6f}, delta={delta:.6f}({'加' if is_buy else '减'})")
                success = self.executor.adjust_position(coin, is_buy, delta_sz, price)
                if success:
                    self._confirm_order(coin, "adjust", is_buy, delta_sz, pre_szi, ratio)
                    self.position_ratios[coin] = ratio
                    self._save_position_ratios()
                    self.logger.info(f"[RECON-ADJUST] {coin} 修正完成")
                    # P1: 偏差修正后联动TP/SL同步
                    try:
                        self._sync_single_coin_tpsl(coin, ratio, mids)
                    except Exception as e:
                        self.logger.warning(f"[RECON-ADJUST] {coin} TP/SL联动异常: {e}")
                    self._log_event("RECON_ADJUST", coin, f"target={target_szi:.6f} actual={user_szi:.6f} deviation={dc["deviation"]:.1%}")
                else:
                    self.logger.error(f"[RECON-ADJUST] {coin} 修正失败")
                    self._write_alert("RECON_ADJUST_FAIL", coin, f"偏差修正执行失败")
        # ===== 偏差修正结束 =====

        # ===== RECON 残留自动平仓 =====
        if residual_close_list:
            self.logger.info(f"[RECON-CLOSE] 开始自动平仓 {len(residual_close_list)} 个残留仓位")
            for rc in residual_close_list:
                coin = rc["coin"]
                user_szi = rc["user_szi"]
                try:
                    _pos = get_positions(self.config.user_main_addr, self.config, self.dex_cache, self.logger)
                    _szi = float(_pos.get(coin, {}).get("szi", 0))
                except Exception:
                    _szi = user_szi
                if abs(_szi) < 1e-8:
                    self.logger.info(f"[RECON-CLOSE] {coin}: 用户实际无仓位，跳过")
                    self._recon_residual_count.pop(coin, None)
                    continue
                pre_szi = 0.0
                try:
                    pre_pos = get_positions(self.config.user_main_addr, self.config, self.dex_cache, self.logger)
                    pre_szi = float(pre_pos.get(coin, {}).get("szi", 0))
                except Exception:
                    pass
                self.logger.info(f"[RECON-CLOSE] {coin}: 平仓残留 szi={_szi}")
                close_result = self.executor.close_position(coin, str(_szi))
                success = close_result is True
                if success:
                    self._confirm_order(coin, "close", False, 0, pre_szi, ratio)
                    self._recon_residual_count.pop(coin, None)
                    self.logger.info(f"[RECON-CLOSE] {coin} 残留平仓完成")
                    self._log_event("RECON_RESIDUAL_CLOSE", coin, f"szi={_szi:.6f} value=${rc["positionValue"]:.2f}")
                else:
                    fail_reason = close_result if isinstance(close_result, str) else "unknown"
                    self.logger.error(f"[RECON-CLOSE] {coin} 残留平仓失败(reason={fail_reason})")
                    self._write_alert("RECON_RESIDUAL_CLOSE_FAIL", coin, f"残留平仓失败(reason={fail_reason})")
        # ===== 残留平仓结束 =====

        n_adj = len(deviation_corrections)
        n_close = len(residual_close_list)
        if alert_count == 0 and not recovery_changes and n_adj == 0 and n_close == 0:
            self.logger.info("[RECON] 全量对账通过，无异常")
        elif recovery_changes or n_adj > 0 or n_close > 0:
            parts = []
            if recovery_changes:
                parts.append(f"自动补仓{len(recovery_changes)}个")
            if n_adj > 0:
                parts.append(f"偏差修正{n_adj}个")
            if n_close > 0:
                parts.append(f"残留平仓{n_close}个")
            self.logger.info(f"[RECON] 全量对账完成，{'，'.join(parts)}，另有 {alert_count} 个异常")
        else:
            self.logger.warning(f"[RECON] 全量对账发现 {alert_count} 个异常")

        # 审计数据快照
        self._write_audit_snapshot()

    # ----------------------------------------------------------------
    # 审计数据快照
    # ----------------------------------------------------------------
    def _write_audit_snapshot(self) -> None:
        """每个RECON周期写入审计快照到 audit.json (追加模式，每日一个文件)"""
        try:
            audit_dir = os.path.dirname(os.path.abspath(self.config.state_file))
            _audit_stem = Path(self.config.state_file).stem.removesuffix("_state")
            audit_file = os.path.join(audit_dir, f"{_audit_stem}_audit_{datetime.now().strftime('%Y%m%d')}.json")
            
            snapshot = {
                "ts": datetime.now().isoformat(timespec="seconds"),
                "poll": self.poll_count,
                "leader_count": len(self.prev_leader_positions),
                "user_count": len(self.prev_user_positions),
                "copy_ratio": round(getattr(self, "_last_copy_ratio", 0), 6),
                "pending_closes": len(self.pending_closes),
                "pending_flips": len(self.pending_flips),
                "today": {
                    "opens": self._today_opens,
                    "adjusts": self._today_adjusts,
                    "closes": self._today_closes,
                    "recons": self._today_recons,
                    "close_queued": self._audit_close_queued,
                    "partial_fills": self._audit_partial_fills,
                    "confirm_retries": self._audit_confirm_retries,
                },
                "ws_connected": self._ws_listener.is_connected if self._ws_listener else None,
            }
            
            # 读取现有记录(如果存在)
            records = []
            if os.path.exists(audit_file):
                try:
                    with open(audit_file, "r", encoding="utf-8") as f:
                        records = json.load(f)
                except Exception:
                    records = []
            
            records.append(snapshot)
            # 只保留最近288条(约24小时, 每5分钟一次)
            records = records[-288:]
            
            tmp_path = audit_file + ".tmp"
            with open(tmp_path, "w", encoding="utf-8") as f:
                json.dump(records, f, ensure_ascii=False, indent=1)
            os.replace(tmp_path, audit_file)
        except Exception as e:
            self.logger.debug(f"[AUDIT] 写入审计快照失败: {e}")

    # ----------------------------------------------------------------
    # TP/SL分类辅助
    # ----------------------------------------------------------------
    def _classify_tpsl(self, order: dict, mids: Dict[str, float]) -> Optional[str]:
        order_type_data = order.get("orderType", {})
        if isinstance(order_type_data, dict):
            trigger_info = order_type_data.get("trigger")
            if isinstance(trigger_info, dict):
                tpsl = trigger_info.get("tpsl")
                if tpsl in ("tp", "sl"):
                    return tpsl
        # 支持frontendOpenOrders格式：orderType为string如"Take Profit Market"/"Stop Market"
        order_type_str = order.get("orderType", "")
        if isinstance(order_type_str, str):
            if "Take Profit" in order_type_str:
                return "tp"
            elif "Stop" in order_type_str:
                return "sl"
        coin = order.get("coin", "")
        # 优先使用triggerPx（frontendOpenOrders），其次limitPx（openOrders）
        trigger_or_limit = float(order.get("triggerPx", 0)) or float(order.get("limitPx", 0))
        current_price = mids.get(coin, 0)
        if current_price <= 0 or trigger_or_limit <= 0:
            return None
        # 从挂单side推断仓位方向（TP/SL均为reduceOnly平仓单）
        # side=A（卖出平仓）→ 仓位为多单 → TP在上方，SL在下方
        # side=B（买入平仓）→ 仓位为空单 → TP在下方，SL在上方
        is_long = order.get("side", "") != "B"
        if is_long:
            return "tp" if trigger_or_limit > current_price else "sl"
        else:
            return "tp" if trigger_or_limit < current_price else "sl"

    def _get_trigger_px(self, order: dict) -> str:
        # 优先使用顶层triggerPx字段（frontendOpenOrders格式）
        if "triggerPx" in order and order.get("isTrigger"):
            return str(order["triggerPx"])
        # 兼容openOrders格式（orderType.trigger.triggerPx）
        order_type_data = order.get("orderType", {})
        if isinstance(order_type_data, dict):
            trigger_info = order_type_data.get("trigger")
            if isinstance(trigger_info, dict):
                return str(trigger_info.get("triggerPx", order.get("limitPx", "0")))
        return str(order.get("limitPx", "0"))

    def _init_sz_decimals(self) -> None:
        self.dex_cache.init_from_api(self.config.base_url, self.config.api_timeout)
        self.sz_decimals_map = self.dex_cache.sz_decimals_map

    def _calc_copy_ratio(self, user_value: Optional[float] = None, leader_value: Optional[float] = None) -> float:
        try:
            if user_value is None:
                user_value = get_account_value(self.config.user_main_addr, self.config, self.logger)
            if leader_value is None:
                leader_value = get_account_value(self.config.leader_addr, self.config, self.logger)
        except Exception as e:
            self.logger.error(f"获取账户总值失败: {e}")
            self._write_alert("RECON_RECOVERY_FAIL", "SYSTEM", f"获取账户总值失败: {e}",
                                    extra_data={"错误": str(e)})
            return 0.0
        fund_ratio = self.config.fund_ratio
        user_copy_limit = user_value * fund_ratio
        if leader_value <= 0:
            self.logger.warning("Leader账户总值为0，无法计算跟单比例")
            return 0.0
        ratio = user_copy_limit / leader_value
        self.logger.info(f"跟单比例计算: Leader=${leader_value:.2f}, 用户=${user_value:.2f}, 跟单上限=${user_copy_limit:.2f}, 比例={ratio:.6f}")
        return ratio

    def _update_health_stats(self) -> None:
        """P3#9: 更新运行统计到health_checker"""
        try:
            leader_count = len(self.prev_leader_positions) if self.prev_leader_positions else 0
            user_count = len(self.prev_user_positions) if self.prev_user_positions else 0
            dedup_count = self._deduplicator.record_count if hasattr(self, '_deduplicator') else 0
            # 使用最近一次轮询的跟单比例
            copy_ratio = getattr(self, '_last_copy_ratio', 0.0)
            self.health_checker.update_stats(
                copy_ratio=copy_ratio,
                leader_positions_count=leader_count,
                user_positions_count=user_count,
                today_opens=self._today_opens,
                today_adjusts=self._today_adjusts,
                today_closes=self._today_closes,
                today_recons=self._today_recons,
                dedup_records=dedup_count,
            )
        except Exception as e:
            self.logger.debug(f"[STATS] 更新运行统计失败: {e}")

    def _print_status(self) -> None:
        try:
            leader_positions = get_positions(self.config.leader_addr, self.config, self.dex_cache, self.logger)
            user_positions = get_positions(self.config.user_main_addr, self.config, self.dex_cache, self.logger)
            leader_value = get_account_value(self.config.leader_addr, self.config, self.logger)
            user_value = get_account_value(self.config.user_main_addr, self.config, self.logger)
            ws_status = "已连接" if (self._ws_listener and self._ws_listener.is_connected) else "未连接"
            self.logger.info("=" * 60)
            self.logger.info("[STATUS] 状态摘要")
            self.logger.info(f"  Leader账户: ${leader_value:.2f}")
            self.logger.info(f"  用户账户: ${user_value:.2f}")
            self.logger.info(f"  WebSocket: {ws_status}")  # v3新增
            # TP/SL定时同步倒计时
            _tpsl_interval = getattr(self.config, 'tpsl_sync_time_seconds', 900)
            _tpsl_elapsed = max(0, _tpsl_interval - (time.time() - getattr(self, '_last_periodic_tpsl_sync_time', 0)))
            self.logger.info(f"  下次定时TP/SL同步: {_tpsl_elapsed:.0f}秒后")
            self.logger.info(f"  Leader持仓: {len(leader_positions)} 个")
            for coin, pos in sorted(leader_positions.items()):
                szi = float(pos["szi"])
                direction = "多" if szi > 0 else "空"
                dex_tag = f"[{pos.get('dex','主')}]" if pos.get('dex') else ""
                margin_tag = " isolated" if pos.get('leverage_type', 'cross') in ('isolated', 'strictIsolated') else ""
                self.logger.info(f"    {coin}: {direction} {abs(szi)} lev={pos['leverage_value']}x{margin_tag} {dex_tag} val=${float(pos['positionValue']):.2f} pnl=${float(pos['unrealizedPnl']):.2f}")
            self.logger.info(f"  用户持仓: {len(user_positions)} 个")
            for coin, pos in sorted(user_positions.items()):
                szi = float(pos["szi"])
                direction = "多" if szi > 0 else "空"
                self.logger.info(f"    {coin}: {direction} {abs(szi)} val=${float(pos['positionValue']):.2f} pnl=${float(pos['unrealizedPnl']):.2f}")
            self.logger.info("=" * 60)
        except Exception as e:
            self.logger.error(f"输出状态摘要失败: {e}")

    # ----------------------------------------------------------------
    # FLIP平仓跨轮询重试
    # ----------------------------------------------------------------
    def _retry_pending_flips(self) -> None:
        if self.executor._quota_exhausted:
            self.logger.info("  [FLIP-RETRY] 跳过: API配额耗尽")
            return
        if not self.pending_flips:
            return
        now = time.time()
        coins_to_remove: List[str] = []
        flip_max_retries = self.config.flip_max_retries
        flip_base_interval = self.config.flip_base_interval
        flip_max_interval = self.config.flip_max_interval
        flip_notify_interval = self.config.flip_notify_interval
        flip_backoff_base = self.config.flip_backoff_base
        for coin, flip_info in list(self.pending_flips.items()):
            retry_count = flip_info["retry_count"]
            if now < flip_info["next_retry"]:
                continue
            if retry_count >= flip_max_retries:
                self.logger.error(f"  [FLIP-ABORT] {coin} 已达{flip_max_retries}次重试上限，放弃平仓！请人工介入！")
                self._write_alert("FLIP_MAX_RETRY", coin, f"FLIP平仓{coin}已重试{flip_max_retries}次仍失败，已放弃自动重试",
                                    extra_data={"重试次数": str(flip_max_retries)})
                coins_to_remove.append(coin)
                continue
            try:
                user_pos = get_positions(self.config.user_main_addr, self.config, self.dex_cache, self.logger)
                user_szi = float(user_pos.get(coin, {}).get("szi", 0))
            except Exception:
                user_szi = -999
            if abs(user_szi) < 1e-8:
                # FIX: 用户无仓位，直接开新仓而不是仅清除pending
                new_szi_resolved = float(flip_info["new_szi"])
                leverage_resolved = flip_info["leverage"]
                self.logger.info(f"  [FLIP-RESOLVED] {coin} 用户已无仓位，直接开新仓(目标szi={new_szi_resolved})...")
                try:
                    mids_resolved = get_all_mids(self.config, self.dex_cache, self.logger)
                    price_resolved = mids_resolved.get(coin, 0)
                except Exception:
                    price_resolved = 0
                if price_resolved > 0:
                    cr_resolved = self._calc_copy_ratio()
                    new_pos_val = abs(new_szi_resolved) * price_resolved * cr_resolved
                    new_sz_resolved = new_pos_val / price_resolved
                    new_sz_resolved = round_sz(new_sz_resolved, coin, self.sz_decimals_map)
                    is_buy_resolved = new_szi_resolved > 0
                    open_ok_resolved = self.executor.open_position(coin, is_buy_resolved, new_sz_resolved, leverage_resolved, price_resolved, leverage_type=flip_info.get("leverage_type", "cross"))
                    if open_ok_resolved:
                        self._confirm_order(coin, "open", is_buy_resolved, new_sz_resolved, 0, cr_resolved)
                        self.logger.info(f"  [FLIP-RESOLVED] {coin} 开新仓成功")
                    else:
                        self.logger.error(f"  [FLIP-RESOLVED] {coin} 开新仓失败！请人工介入")
                        self._write_alert("FLIP_RESOLVED_OPEN_FAILED", coin, f"FLIP-RESOLVED开{coin}新仓失败")
                else:
                    self.logger.warning(f"  [FLIP-RESOLVED] {coin} 无法获取价格，跳过开仓")
                coins_to_remove.append(coin)
                continue
            flip_info["retry_count"] = retry_count + 1
            self.logger.info(f"  [FLIP-RETRY] {coin} 第{retry_count+1}次重试平仓 (szi={flip_info['old_szi']})")
            close_result = self.executor.close_position(coin, flip_info["old_szi"], exponential_backoff=True)
            close_ok = close_result is True
            if close_ok:
                self.logger.info(f"  [FLIP-SUCCESS] {coin} 平仓成功！继续开新仓...")
                self._cancel_reduce_only_orders(coin)
                new_szi = float(flip_info["new_szi"])
                leverage = flip_info["leverage"]
                try:
                    mids = get_all_mids(self.config, self.dex_cache, self.logger)
                    price = mids.get(coin, 0)
                except Exception:
                    price = 0
                if price > 0:
                    copy_ratio = self._calc_copy_ratio()
                    new_pos_value = abs(new_szi) * price * copy_ratio
                    new_sz = new_pos_value / price
                    new_sz = round_sz(new_sz, coin, self.sz_decimals_map)
                    is_buy_new = new_szi > 0
                    pre_szi2 = 0.0
                    try:
                        pre_pos2 = get_positions(self.config.user_main_addr, self.config, self.dex_cache, self.logger)
                        pre_szi2 = float(pre_pos2.get(coin, {}).get("szi", 0))
                    except Exception:
                        pass
                    open_ok = self.executor.open_position(coin, is_buy_new, new_sz, leverage, price, leverage_type=flip_info.get("leverage_type", "cross"))
                    if open_ok:
                        self._confirm_order(coin, "open", is_buy_new, new_sz, pre_szi2, copy_ratio)
                coins_to_remove.append(coin)
            else:
                wait = min(flip_base_interval * (flip_backoff_base ** retry_count), flip_max_interval)
                flip_info["next_retry"] = now + wait
                self.logger.warning(f"  [FLIP-RETRY-FAIL] {coin} 第{retry_count+1}次重试仍失败，{wait:.0f}秒后再次尝试")
                if now - flip_info.get("last_notify", 0) >= flip_notify_interval:
                    self._write_alert("FLIP_RETRY_FAILED", coin, f"FLIP平仓{coin}第{retry_count+1}次重试失败，将持续重试",
                                    extra_data={"重试次数": str(retry_count+1)})
                    flip_info["last_notify"] = now
        for coin in coins_to_remove:
            self.pending_flips.pop(coin, None)

    def _retry_pending_closes(self) -> None:
        """FIX-P0: 重试失败的平仓操作"""
        if self.executor._quota_exhausted:
            return

        if not self.pending_closes:
            return
        now = time.time()
        coins_to_remove: List[str] = []
        max_retries = 10  # 最多重试10次
        retry_interval = 30  # 每次间隔30秒
        for coin, close_info in list(self.pending_closes.items()):
            retry_count = close_info["retry_count"]
            if now < close_info["next_retry"]:
                continue
            if retry_count >= max_retries:
                self.logger.error(f"  [CLOSE-ABORT] {coin} 已达{max_retries}次重试上限，放弃平仓！请人工介入！")
                self._write_alert("CLOSE_MAX_RETRY", coin, f"平仓{coin}已重试{max_retries}次仍失败，已放弃自动重试",
                                    extra_data={"重试次数": str(max_retries)})
                coins_to_remove.append(coin)
                continue
            # 先检查用户是否还有该仓位
            try:
                user_pos = get_positions(self.config.user_main_addr, self.config, self.dex_cache, self.logger)
                user_szi = float(user_pos.get(coin, {}).get("szi", 0))
            except Exception:
                user_szi = -999
            if abs(user_szi) < 1e-8:
                self.logger.info(f"  [CLOSE-RETRY] {coin} 用户已无仓位，平仓已完成")
                coins_to_remove.append(coin)
                continue
            # 执行平仓重试
            close_info["retry_count"] = retry_count + 1
            self.logger.info(f"  [CLOSE-RETRY] {coin} 第{retry_count+1}次重试平仓 (old_szi={close_info['old_szi']})")
            close_result = self.executor.close_position(coin, close_info["old_szi"], exponential_backoff=True)
            success = close_result is True
            if success:
                self.logger.info(f"  [CLOSE-RETRY] {coin} 重试平仓成功！")
                self._cancel_reduce_only_orders(coin)
                coins_to_remove.append(coin)
            else:
                fail_reason = close_result if isinstance(close_result, str) else "unknown"
                close_info["next_retry"] = now + retry_interval
                self.logger.warning(f"  [CLOSE-RETRY] {coin} 第{retry_count+1}次重试失败(reason={fail_reason})，{retry_interval}秒后再次尝试")
                if now - close_info.get("last_notify", 0) >= 300:  # 每5分钟通知一次
                    self._write_alert("CLOSE_RETRY_FAILED", coin, f"平仓{coin}第{retry_count+1}次重试失败(reason={fail_reason})，将持续重试",
                                    extra_data={"失败原因": str(fail_reason), "重试次数": str(retry_count+1)})
                    close_info["last_notify"] = now
        for coin in coins_to_remove:
            self.pending_closes.pop(coin, None)

    def _handle_position_changes(self, changes: List[dict], copy_ratio: float, mids: Dict[str, float], user_value: Optional[float] = None) -> None:
        """处理持仓变化（v2逻辑完整保留）"""
        if user_value is None:
            try:
                user_value = get_account_value(self.config.user_main_addr, self.config, self.logger)
            except Exception:
                user_value = 0
        max_leverage = self.config.max_leverage

        affected_coins: List[str] = []  # P1: 收集本轮受影响币种
        for change in changes:
            coin = change["coin"]
            price = mids.get(coin, 0)
            # FIX-P0: close操作使用market_close，不依赖mids价格，不跳过
            if change["type"] != "close" and price <= 0:
                self.logger.warning(f"  跳过{coin}: 无法获取价格")
                continue
            leverage = int(change.get("leverage", 1))
            if leverage > max_leverage:
                self.logger.warning(f"  跳过{coin}: 杠杆{leverage}x超过{max_leverage}x上限")
                continue
            if change["type"] == "leverage_change":
                old_lev = change.get("old_lev", "?")
                new_lev = change.get("new_lev", "?")
                self.logger.info(f"  [LEVERAGE] Leader{coin}杠杆变化: {old_lev}x → {new_lev}x → 用户跟调")
                lev_type = change.get("leverage_type", "cross")
                is_cross_lev = lev_type not in ("isolated", "strictIsolated")
                self.executor.set_leverage(coin, int(float(new_lev)), is_cross=is_cross_lev)
                continue
            if change["type"] == "open":
                leader_pos_value = float(change.get("positionValue", 0))
                user_pos_value = leader_pos_value * copy_ratio
                user_sz = user_pos_value / price
                user_sz = round_sz(user_sz, coin, self.sz_decimals_map)
                is_buy = float(change["new_szi"]) > 0
                if user_sz <= 0:
                    self.logger.info(f"  跳过{coin}: 计算仓位为0")
                    continue
                # 最低下单金额限制：用户目标仓位价值<$10时跳过（低于交易所最低要求）
                if user_pos_value < self.executor.MIN_ORDER_VALUE_USD:
                    self.logger.info(f"  [SKIP-MIN] {coin}: 用户目标仓位=${user_pos_value:.2f}<${self.executor.MIN_ORDER_VALUE_USD}，跳过跟单（不告警）")
                    continue

                # 安全护栏：开仓前检查用户仓位比例，防止重复开仓
                # 逻辑：如果用户已有同方向仓位且比例接近目标copy_ratio，说明是重复信号，跳过
                # 如果用户仓位比例远低于目标（如Leader补仓但用户还没跟上），则允许开仓
                pre_szi = 0.0
                try:
                    pre_pos = get_positions(self.config.user_main_addr, self.config, self.dex_cache, self.logger)
                    pre_szi = float(pre_pos.get(coin, {}).get("szi", 0))
                except Exception:
                    pass
                if pre_szi != 0:
                    same_direction = (pre_szi > 0 and is_buy) or (pre_szi < 0 and not is_buy)
                    if same_direction:
                        # 计算用户当前仓位与目标的比例
                        leader_szi_abs = abs(float(change.get("new_szi", 0)))
                        user_szi_abs = abs(pre_szi)
                        if leader_szi_abs > 0 and copy_ratio > 0:
                            expected_user_szi = leader_szi_abs * copy_ratio
                            # 用户仓位已达到目标的75%以上，视为重复信号
                            if user_szi_abs >= expected_user_szi * 0.75:
                                self.logger.warning(
                                    f"  [SAFETY-OPEN] 跳过{coin}开仓: 用户仓位已接近目标 "
                                    f"(user_szi={user_szi_abs:.4f}, expected≈{expected_user_szi:.4f}, "
                                    f"ratio={user_szi_abs/expected_user_szi:.1%})，判定为重复信号"
                                )
                                continue
                            else:
                                self.logger.info(
                                    f"  [SAFETY-OPEN] {coin}用户仓位不足，允许补仓 "
                                    f"(user_szi={user_szi_abs:.4f}, expected≈{expected_user_szi:.4f}, "
                                    f"ratio={user_szi_abs/expected_user_szi:.1%})"
                                )
                        else:
                            # 无法计算比例时保守处理，跳过
                            self.logger.warning(
                                f"  [SAFETY-OPEN] 跳过{coin}开仓: 用户已有同方向仓位且无法计算比例"
                            )
                            continue

                self.logger.info(f"  [OPEN] Leader开仓 {coin} {'多' if is_buy else '空'} 价值=${leader_pos_value:.2f} → 用户跟单 sz={user_sz} 价值≈${user_sz * price:.2f}")

                leverage_type = change.get("leverage_type", "cross")
                success = self.executor.open_position(coin, is_buy, user_sz, leverage, price, leverage_type=leverage_type)
                if success:
                    self._confirm_order(coin, "open", is_buy, user_sz, pre_szi, copy_ratio)
                    affected_coins.append(coin)  # P1: 收集受影响币种

                    # [Phase A] 记录开仓到trades表
                    try:
                        if self._trade_db:
                            self._trade_db.record_open(
                                instance_id=self._instance_id,
                                coin=coin,
                                side='long' if is_buy else 'short',
                                entry_px=float(price),
                                size=user_sz,
                                open_ts=int(time.time()),
                                copy_ratio=copy_ratio,
                                leader_entry_px=float(change.get('entryPx', 0)) if change.get('entryPx') else None,
                                leader_size=float(change.get('new_szi', 0)) if change.get('new_szi') else None,
                            )
                    except Exception as _tdb_err:
                        self.logger.warning(f"[TradeDB] record_open failed: {_tdb_err}")

                else:
                    # 追踪连续被拒次数，达到阈值发送告警
                    reject_count = self.executor.track_order_reject(coin)
                    if reject_count >= 3:
                        # 如果仓位价值低于最低下单金额，不告警（预期行为）
                        _leader_val = float(change.get("positionValue", 0))
                        _user_val = _leader_val * copy_ratio
                        if _user_val >= self.executor.MIN_ORDER_VALUE_USD:
                            self._write_alert("ORDER_REJECT_REPEATED", coin, f"连续{reject_count}次下单被拒，请检查最小下单金额或其他原因",
                                    extra_data={"连续拒绝次数": str(reject_count)})
                        else:
                            self.logger.info(f"  [OPEN-FAIL] {coin}: 仓位=${_user_val:.2f}<${self.executor.MIN_ORDER_VALUE_USD}，静默跳过告警")
                    self.logger.warning(f"  [OPEN-FAIL] {coin} 开仓失败(连续被拒{reject_count}次)")
            elif change["type"] == "close":
                old_szi = change["old_szi"]
                self.logger.info(f"  [CLOSE] Leader平仓 {coin} (原szi={old_szi}) → 用户跟单平仓")

                try:
                    _chk_pos = get_positions(self.config.user_main_addr, self.config, self.dex_cache, self.logger)
                    _chk_szi = float(_chk_pos.get(coin, {}).get("szi", 0))
                except Exception:
                    _chk_szi = 0.0
                if abs(_chk_szi) < 1e-8:
                    self.logger.info(f"  [保护] 跳过{coin}平仓: 用户无仓位，无需平仓")
                    self._cancel_reduce_only_orders(coin)
                    continue
                pre_szi = 0.0
                try:
                    pre_pos = get_positions(self.config.user_main_addr, self.config, self.dex_cache, self.logger)
                    pre_szi = float(pre_pos.get(coin, {}).get("szi", 0))
                except Exception:
                    pass
                close_result = self.executor.close_position(coin, old_szi)
                success = close_result is True
                if success:
                    self._confirm_order(coin, "close", False, 0, pre_szi, copy_ratio)
                    affected_coins.append(coin)  # P1: 收集受影响币种

                    # [Phase A] 记录平仓到trades表
                    try:
                        if self._trade_db:
                            _close_side = 'long' if float(old_szi) > 0 else 'short'
                            self._trade_db.record_close(
                                instance_id=self._instance_id,
                                coin=coin,
                                side=_close_side,
                                exit_px=float(price),
                                close_ts=int(time.time()),
                                close_size=abs(float(old_szi)),
                                close_reason='leader_close',
                            )
                    except Exception as _tdb_err:
                        self.logger.warning(f"[TradeDB] record_close failed: {_tdb_err}")

                else:
                    fail_reason = close_result if isinstance(close_result, str) else "unknown"
                    self.logger.error(f"  [CLOSE-FAIL] {coin} 平仓失败(reason={fail_reason})")
                    self._write_alert("CLOSE_FAILED", coin, f"平仓{coin}失败(reason={fail_reason})",
                                    extra_data={"失败原因": str(fail_reason)})
                    # FIX-P0: 平仓失败加入pending_closes重试队列
                    self.pending_closes[coin] = {
                        "old_szi": old_szi,
                        "retry_count": 0,
                        "next_retry": time.time(),
                        "last_notify": time.time()
                    }
                    self.logger.info(f"  [CLOSE-RETRY] {coin} 加入pending_closes重试队列")
                    self._audit_close_queued += 1
                self.logger.info(f"  [CLOSE] 清理{coin}残余TP/SL挂单")
                self._cancel_reduce_only_orders(coin)
            elif change["type"] == "adjust":
                old_szi = float(change["old_szi"])
                new_szi = float(change["new_szi"])
                delta = new_szi - old_szi
                user_delta_value = abs(delta) * price * copy_ratio
                user_delta_sz = user_delta_value / price
                user_delta_sz = round_sz(user_delta_sz, coin, self.sz_decimals_map)
                if user_delta_sz <= 0:
                    self.logger.info(f"  跳过{coin}调仓: 计算delta为0")
                    continue
                is_buy = delta > 0
                if (old_szi > 0 and new_szi < 0) or (old_szi < 0 and new_szi > 0):
                    pre_szi = 0.0
                    try:
                        pre_pos = get_positions(self.config.user_main_addr, self.config, self.dex_cache, self.logger)
                        pre_szi = float(pre_pos.get(coin, {}).get("szi", 0))
                    except Exception:
                        pass
                    if abs(pre_szi) < 1e-8:
                        # FIX: 用户无仓位，跳过平仓，直接开新仓（避免6.30遗留仓位导致FLIP失败）
                        self.logger.info(f"  [FLIP-DIRECT] Leader{coin}方向翻转 szi {old_szi}→{new_szi}，用户无仓位，跳过平仓直接开新仓")
                        new_pos_value = abs(new_szi) * price * copy_ratio
                        new_sz = new_pos_value / price
                        new_sz = round_sz(new_sz, coin, self.sz_decimals_map)
                        is_buy_new = new_szi > 0
                        open_ok = self.executor.open_position(coin, is_buy_new, new_sz, leverage, price, leverage_type=change.get("leverage_type", "cross"))
                        if open_ok:
                            self._confirm_order(coin, "open", is_buy_new, new_sz, 0, copy_ratio)
                            affected_coins.append(coin)  # P1: 收集受影响币种

                            # [Phase A] 记录FLIP-DIRECT开仓
                            try:
                                if self._trade_db:
                                    self._trade_db.record_open(
                                        instance_id=self._instance_id,
                                        coin=coin,
                                        side="long" if is_buy_new else "short",
                                        entry_px=float(price),
                                        size=new_sz,
                                        open_ts=int(time.time()),
                                        copy_ratio=copy_ratio,
                                    )
                            except Exception as _tdb_err:
                                self.logger.warning(f"[TradeDB] record_open (flip-direct) failed: {_tdb_err}")
                        continue
                    self.logger.info(f"  [FLIP] Leader{coin}方向翻转 szi {old_szi}→{new_szi} → 先平仓再开仓")
                    close_result = self.executor.close_position(coin, change["old_szi"], exponential_backoff=True)
                    close_ok = close_result is True
                    if close_ok:
                        self._confirm_order(coin, "close", False, 0, pre_szi, copy_ratio)
                        self._cancel_reduce_only_orders(coin)

                        # [Phase A] 记录FLIP平仓
                        try:
                            if self._trade_db:
                                _flip_side = 'long' if float(change.get("old_szi", 0)) > 0 else 'short'
                                self._trade_db.record_close(
                                    instance_id=self._instance_id,
                                    coin=coin,
                                    side=_flip_side,
                                    exit_px=float(price),
                                    close_ts=int(time.time()),
                                    close_size=abs(float(change.get("old_szi", 0))) * copy_ratio if copy_ratio else None,
                                    close_reason='flip_close',
                                )
                        except Exception as _tdb_err:
                            self.logger.warning(f"[TradeDB] record_close (flip) failed: {_tdb_err}")

                    else:
                        fail_reason = close_result if isinstance(close_result, str) else "unknown"
                        self.logger.error(f"  [FLIP-FAIL] {coin}平仓失败(reason={fail_reason})，加入pending_flips")
                        self._write_alert("FLIP_CLOSE_FAILED", coin, f"FLIP平仓{coin}失败(reason={fail_reason})",
                                    extra_data={"失败原因": str(fail_reason)})
                        self.pending_flips[coin] = {"old_szi": change["old_szi"], "new_szi": str(new_szi), "leverage": leverage, "leverage_type": change.get("leverage_type", "cross"), "retry_count": 0, "next_retry": time.time(), "last_notify": time.time()}
                        continue
                    new_pos_value = abs(new_szi) * price * copy_ratio
                    new_sz = new_pos_value / price
                    new_sz = round_sz(new_sz, coin, self.sz_decimals_map)
                    is_buy_new = new_szi > 0
                    pre_szi2 = 0.0
                    try:
                        pre_pos2 = get_positions(self.config.user_main_addr, self.config, self.dex_cache, self.logger)
                        pre_szi2 = float(pre_pos2.get(coin, {}).get("szi", 0))
                    except Exception:
                        pass
                    open_ok = self.executor.open_position(coin, is_buy_new, new_sz, leverage, price, leverage_type=change.get("leverage_type", "cross"))
                    if open_ok:
                        self._confirm_order(coin, "open", is_buy_new, new_sz, pre_szi2, copy_ratio)
                        affected_coins.append(coin)  # P1: 收集受影响币种

                        # [Phase A] 记录FLIP开仓
                        try:
                            if self._trade_db:
                                self._trade_db.record_open(
                                    instance_id=self._instance_id,
                                    coin=coin,
                                    side='long' if is_buy_new else 'short',
                                    entry_px=float(price),
                                    size=new_sz,
                                    open_ts=int(time.time()),
                                    copy_ratio=copy_ratio,
                                )
                        except Exception as _tdb_err:
                            self.logger.warning(f"[TradeDB] record_open (flip) failed: {_tdb_err}")

                else:
                    self.logger.info(f"  [ADJUST] Leader调仓 {coin} szi {old_szi}→{new_szi} delta={delta} → 用户delta={user_delta_sz}")
                    if abs(new_szi) < abs(old_szi):
                        try:
                            _chk_pos = get_positions(self.config.user_main_addr, self.config, self.dex_cache, self.logger)
                            _chk_szi = float(_chk_pos.get(coin, {}).get("szi", 0))
                        except Exception:
                            _chk_szi = 0.0
                        if abs(_chk_szi) < 1e-8:
                            self.logger.info(f"  [保护] 跳过{coin}减仓: 用户无仓位，减仓会变反向开仓")
                            continue
                        _leader_dir = 1 if old_szi > 0 else -1
                        _user_dir = 1 if _chk_szi > 0 else (-1 if _chk_szi < 0 else 0)
                        if _leader_dir != _user_dir:
                            self.logger.info(f"  [保护] 跳过{coin}减仓: 用户仓位方向({_user_dir})与Leader({_leader_dir})不一致")
                            continue
                        if user_delta_sz > abs(_chk_szi):
                            self.logger.info(f"  [保护] {coin}减仓截断: delta={user_delta_sz:.6f} → 用户仓位={abs(_chk_szi):.6f}")
                            user_delta_sz = round_sz(abs(_chk_szi), coin, self.sz_decimals_map)
                    pre_szi = 0.0
                    try:
                        pre_pos = get_positions(self.config.user_main_addr, self.config, self.dex_cache, self.logger)
                        pre_szi = float(pre_pos.get(coin, {}).get("szi", 0))
                    except Exception:
                        pass
                    success = self.executor.adjust_position(coin, is_buy, user_delta_sz, price)
                    if success:
                        self._confirm_order(coin, "adjust", is_buy, user_delta_sz, pre_szi, copy_ratio)
                        affected_coins.append(coin)  # P1: 收集受影响币种

        # P1: 仓位变动后联动TP/SL同步
        if affected_coins:
            try:
                self._batch_sync_tpsl(affected_coins, copy_ratio, mids)
            except Exception as e:
                self.logger.warning(f"  [TPSL-LINKAGE] 仓位变动后TP/SL联动异常: {e}")

    def _handle_order_changes(self, added: List[dict], removed: List[dict], copy_ratio: float, mids: Dict[str, float]) -> None:
        for order in removed:
            coin = order.get("coin", "")
            oid = order.get("oid", 0)
            self.logger.info(f"  [CANCEL] Leader撤止盈止损 {coin} oid={oid}")
            if self.live_mode:
                self._cancel_matching_user_order(coin, order)
        for order in added:
            coin = order.get("coin", "")
            side = order.get("side", "")
            sz = float(order.get("sz", 0))
            is_buy = side == "B"
            if sz <= 0:
                continue
            tpsl = self._classify_tpsl(order, mids)
            if not tpsl:
                continue
            trigger_px = self._get_trigger_px(order)
            user_pos = self.prev_user_positions.get(coin, {})
            user_szi = float(user_pos.get("szi", 0))
            if user_szi == 0:
                continue
            scaled_sz = round_sz(sz * copy_ratio, coin, self.sz_decimals_map)
            user_sz = min(abs(user_szi), scaled_sz)
            if user_sz <= 0:
                continue
            order_type_data = order.get("orderType", {})
            has_trigger = isinstance(order_type_data, dict) and isinstance(order_type_data.get("trigger"), dict)
            tag = "TPSL-TRIGGER" if has_trigger else "TPSL-LIMIT"
            tpsl_name = "TP" if tpsl == "tp" else "SL"
            self.logger.info(f"  [{tag}] Leader new {tpsl_name} {coin} trigger/limit={trigger_px} sz={sz} -> user copy trigger={trigger_px} sz={user_sz}")
            self.executor.place_tpsl_order(coin, is_buy, user_sz, str(trigger_px), tpsl)
            self._add_tpsl_active(coin)  # 标记TP/SL活跃，防止误判为用户退出

    def _cancel_matching_user_order(self, coin: str, leader_order: dict) -> None:
        try:
            user_orders = get_open_orders(self.config.user_main_addr, self.config, self.dex_cache, self.logger)
            side = leader_order.get("side", "")
            leader_trigger_px = self._get_trigger_px(leader_order)
            for uo in user_orders:
                if uo.get("coin") != coin or uo.get("side") != side or not uo.get("reduceOnly"):
                    continue
                uo_trigger_px = self._get_trigger_px(uo)
                if uo_trigger_px == leader_trigger_px:
                    self.executor.cancel_order(coin, uo.get("oid", 0))
                    self._remove_tpsl_active(coin)  # 撤销TP/SL匹配订单，清理活跃标记
                    break
        except Exception as e:
            self.logger.warning(f"  撤单匹配失败 {coin}: {e}")

    def _sync_single_coin_tpsl(self, coin: str, copy_ratio: float, mids: Dict[str, float], user_orders: Optional[List[dict]] = None, user_positions: Optional[Dict[str, dict]] = None) -> None:
        """P1: 单币种TP/SL同步（从_sync_user_tpsl_orders提取核心逻辑）

        用于仓位变动后立即联动TP/SL，而非等待全量同步周期。
        可选传入已有的open_orders和positions数据避免重复查询。
        """
        try:
            if self._tpsl_sync_lock:
                self.logger.info(f"  [TPSL-SYNC-ONE] {coin} 跳过: 全量同步锁已占用")
                return
            self._tpsl_sync_lock = True
            try:
                leader_orders = get_frontend_open_orders(self.config.leader_addr, self.config.base_url, self.config.api_timeout)
                if user_orders is None:
                    user_orders = get_open_orders(self.config.user_main_addr, self.config, self.dex_cache, self.logger)
                if user_positions is None:
                    user_positions = get_positions(self.config.user_main_addr, self.config, self.dex_cache, self.logger)

                leader_tpsl = [o for o in leader_orders if o.get("reduceOnly") and float(o.get("sz", 0)) > 0 and o.get("coin") == coin]
                user_tpsl = [o for o in user_orders if o.get("reduceOnly") and o.get("coin") == coin]

                # 聚合leader的TP/SL
                leader_agg: Dict[Tuple[str, str], dict] = {}
                for o in leader_tpsl:
                    tpsl = self._classify_tpsl(o, mids)
                    if not tpsl:
                        continue
                    trigger_px = float(self._get_trigger_px(o))
                    key = (coin, tpsl)
                    if key not in leader_agg:
                        leader_agg[key] = {"triggerPx": trigger_px, "total_sz": 0.0, "side": o.get("side", "")}
                    else:
                        existing_px = leader_agg[key]["triggerPx"]
                        leader_szi = float(self.prev_leader_positions.get(coin, {}).get("szi", 0))
                        if tpsl == "tp":
                            if leader_szi > 0:
                                leader_agg[key]["triggerPx"] = max(existing_px, trigger_px)
                            elif leader_szi < 0:
                                leader_agg[key]["triggerPx"] = min(existing_px, trigger_px)
                        elif tpsl == "sl":
                            if leader_szi > 0:
                                leader_agg[key]["triggerPx"] = min(existing_px, trigger_px)
                            elif leader_szi < 0:
                                leader_agg[key]["triggerPx"] = max(existing_px, trigger_px)
                    leader_agg[key]["total_sz"] += float(o.get("sz", 0))

                # 聚合user的TP/SL
                user_agg: Dict[Tuple[str, str], List[dict]] = {}
                for o in user_tpsl:
                    tpsl = self._classify_tpsl(o, mids)
                    if not tpsl:
                        continue
                    key = (coin, tpsl)
                    if key not in user_agg:
                        user_agg[key] = []
                    user_agg[key].append(o)

                # 删除用户多余的TP/SL（leader没有的）
                for key, orders in user_agg.items():
                    if key not in leader_agg:
                        c, t = key
                        tpsl_name = "止盈" if t == "tp" else "止损"
                        for o in orders:
                            self.logger.info(f"  [TPSL-SYNC-ONE] 撤销用户多余{tpsl_name} {c} oid={o.get('oid')}")
                            self.executor.cancel_order(c, o.get("oid", 0))

                # 同步leader的TP/SL到用户
                size_tolerance = self.config.tpsl_size_tolerance
                price_tolerance = self.config.price_change_tolerance
                for key, leader_info in leader_agg.items():
                    c, t = key
                    trigger_px = leader_info["triggerPx"]
                    side = leader_info["side"]
                    is_buy = side == "B"
                    tpsl_name = "止盈" if t == "tp" else "止损"
                    user_pos = user_positions.get(c, {})
                    user_szi = float(user_pos.get("szi", 0))
                    if user_szi == 0:
                        if key in user_agg:
                            for o in user_agg[key]:
                                self.logger.info(f"  [TPSL-SYNC-ONE] 用户无持仓，撤销{tpsl_name} {c} oid={o.get('oid')}")
                                self.executor.cancel_order(c, o.get("oid", 0))
                            self._remove_tpsl_active(c)  # 用户无持仓，清理TP/SL活跃标记
                        continue
                    user_sz = abs(user_szi)
                    if user_sz <= 0:
                        continue
                    existing_total = 0.0
                    if key in user_agg:
                        existing_total = sum(float(o.get("sz", 0)) for o in user_agg[key])
                    price_changed = False
                    if key in user_agg:
                        for o in user_agg[key]:
                            user_trigger = float(self._get_trigger_px(o))
                            if abs(user_trigger - trigger_px) / max(trigger_px, 0.001) > price_tolerance:
                                price_changed = True
                                break
                    if not price_changed and existing_total > 0 and abs(existing_total - user_sz) / max(user_sz, 0.001) < size_tolerance:
                        continue
                    if key in user_agg:
                        for o in user_agg[key]:
                            self.logger.info(f"  [TPSL-SYNC-ONE] 撤销旧{tpsl_name} {c} oid={o.get('oid')}")
                            self.executor.cancel_order(c, o.get("oid", 0))
                    self.logger.info(f"  [TPSL-SYNC-ONE] 创建{tpsl_name} {c} trigger={trigger_px} sz={user_sz}")
                    ok = self.executor.place_tpsl_order(c, is_buy, user_sz, str(trigger_px), t)
                    if ok:
                        self._add_tpsl_active(c)  # 标记TP/SL活跃
                    else:
                        self._pending_tpsl.append({
                            "coin": c, "is_buy": is_buy, "sz": user_sz,
                            "trigger_px": float(trigger_px), "tpsl": t,
                            "fail_count": 1, "first_fail_time": time.time()
                        })
                        self.logger.warning(f"  [TPSL-SYNC-ONE] {c} {tpsl_name}失败，加入重试队列")

                self.logger.info(f"  [TPSL-SYNC-ONE] {coin} TP/SL同步完成")
            finally:
                self._tpsl_sync_lock = False
        except Exception as e:
            self.logger.warning(f"  [TPSL-SYNC-ONE] {coin} 单币种TP/SL同步失败: {e}")
            self._tpsl_sync_lock = False

    def _retry_pending_tpsl(self, mids: Dict[str, float]) -> None:
        """P0 FIX: 重试失败的TPSL操作"""
        if not self._pending_tpsl:
            return
        remaining = []
        user_positions = get_positions(self.config.user_main_addr, self.config, self.dex_cache, self.logger)
        # 检查配额是否耗尽
        if self.executor._quota_exhausted:
            self.logger.info("  [TPSL-RETRY] 跳过: API配额耗尽，等待配额恢复")
            return

        for item in self._pending_tpsl:
            coin = item["coin"]
            user_pos = user_positions.get(coin, {})
            user_szi = float(user_pos.get("szi", 0))
            if user_szi == 0:
                self.logger.info(f"  [TPSL-RETRY] {coin} 跳过: 用户已无持仓")
                continue
            if time.time() - item.get("first_fail_time", time.time()) > 1800:
                self.logger.warning(f"  [TPSL-RETRY] {coin} 放弃: 超过30分钟未成功 (失败{item['fail_count']}次)")
                self._write_alert("TPSL_RETRY_EXHAUSTED", coin, f"TPSL重试30分钟放弃，失败{item['fail_count']}次")
                continue
            self.logger.info(f"  [TPSL-RETRY] {coin} 重试 {item['tpsl']} trigger={item['trigger_px']} sz={item['sz']} (第{item['fail_count']}次)")
            ok = self.executor.place_tpsl_order(coin, item["is_buy"], item["sz"], str(item["trigger_px"]), item["tpsl"])
            if ok:
                self.logger.info(f"  [TPSL-RETRY] {coin} 重试成功")
            else:
                item["fail_count"] += 1
                remaining.append(item)
        self._pending_tpsl = remaining

    def _batch_sync_tpsl(self, affected_coins: List[str], copy_ratio: float, mids: Dict[str, float]) -> None:
        """P1: 批量同步指定币种的TP/SL（仓位变动后联动）"""
        if not affected_coins:
            return
        try:
            self.logger.info(f"  [TPSL-BATCH] 仓位变动后联动TP/SL同步: {affected_coins}")
            # 预查询用户挂单和持仓，避免每个币种重复查询
            try:
                user_orders = get_open_orders(self.config.user_main_addr, self.config, self.dex_cache, self.logger)
            except Exception:
                user_orders = None
            try:
                user_positions = get_positions(self.config.user_main_addr, self.config, self.dex_cache, self.logger)
            except Exception:
                user_positions = None
            for coin in affected_coins:
                try:
                    self._sync_single_coin_tpsl(coin, copy_ratio, mids, user_orders=user_orders, user_positions=user_positions)
                except Exception as e:
                    self.logger.warning(f"  [TPSL-BATCH] {coin} 同步异常: {e}")
        except Exception as e:
            self.logger.warning(f"  [TPSL-BATCH] 批量TP/SL同步异常: {e}")

    def _sync_user_tpsl_orders(self, copy_ratio: float, mids: Dict[str, float], force_full: bool = False, cached_leader_orders=None, cached_user_orders=None, cached_user_positions=None, _target_coins: set = None) -> None:
        try:
            # P2优化: 优先使用缓存数据，避免重复API查询
            if cached_leader_orders is not None and cached_user_orders is not None and cached_user_positions is not None:
                leader_orders = cached_leader_orders
                user_orders = cached_user_orders
                user_positions = cached_user_positions
                self.logger.debug("[TPSL-SYNC] 使用poll缓存数据")
            else:
                data = self._concurrent_get({
                    "leader_orders": lambda: get_frontend_open_orders(self.config.leader_addr, self.config.base_url, self.config.api_timeout),
                    "user_orders": lambda: get_open_orders(self.config.user_main_addr, self.config, self.dex_cache, self.logger),
                    "user_positions": lambda: get_positions(self.config.user_main_addr, self.config, self.dex_cache, self.logger),
                })
                leader_orders = data.get("leader_orders") or []
                user_orders = data.get("user_orders") or []
                user_positions = data.get("user_positions") or {}
            leader_tpsl = [o for o in leader_orders if o.get("reduceOnly") and float(o.get("sz", 0)) > 0]
            user_tpsl = [o for o in user_orders if o.get("reduceOnly")]
            leader_agg: Dict[Tuple[str, str], dict] = {}
            for o in leader_tpsl:
                coin = o.get("coin", "")
                tpsl = self._classify_tpsl(o, mids)
                if not tpsl:
                    continue
                trigger_px = float(self._get_trigger_px(o))
                key = (coin, tpsl)
                if key not in leader_agg:
                    leader_agg[key] = {"triggerPx": trigger_px, "total_sz": 0.0, "side": o.get("side", "")}
                else:
                    existing_px = leader_agg[key]["triggerPx"]
                    leader_szi = float(self.prev_leader_positions.get(coin, {}).get("szi", 0))
                    if tpsl == "tp":
                        if leader_szi > 0:
                            leader_agg[key]["triggerPx"] = max(existing_px, trigger_px)
                        elif leader_szi < 0:
                            leader_agg[key]["triggerPx"] = min(existing_px, trigger_px)
                    elif tpsl == "sl":
                        if leader_szi > 0:
                            leader_agg[key]["triggerPx"] = min(existing_px, trigger_px)
                        elif leader_szi < 0:
                            leader_agg[key]["triggerPx"] = max(existing_px, trigger_px)
                leader_agg[key]["total_sz"] += float(o.get("sz", 0))
            user_agg: Dict[Tuple[str, str], List[dict]] = {}
            for o in user_tpsl:
                coin = o.get("coin", "")
                tpsl = self._classify_tpsl(o, mids)
                if not tpsl:
                    continue
                key = (coin, tpsl)
                if key not in user_agg:
                    user_agg[key] = []
                user_agg[key].append(o)
            for key, orders in user_agg.items():
                if key not in leader_agg:
                    coin, tpsl = key
                    tpsl_name = "止盈" if tpsl == "tp" else "止损"
                    for o in orders:
                        self.logger.info(f"  [SYNC] 撤销用户多余{tpsl_name} {coin} oid={o.get('oid')}")
                        self.executor.cancel_order(coin, o.get("oid", 0))
            size_tolerance = self.config.tpsl_size_tolerance
            price_tolerance = self.config.price_change_tolerance
            for key, leader_info in leader_agg.items():
                coin, tpsl = key
                # WS精准同步：只处理目标币种
                if _target_coins and coin not in _target_coins:
                    continue
                trigger_px = leader_info["triggerPx"]
                side = leader_info["side"]
                is_buy = side == "B"
                tpsl_name = "止盈" if tpsl == "tp" else "止损"
                user_pos = user_positions.get(coin, {})
                user_szi = float(user_pos.get("szi", 0))
                if user_szi == 0:
                    if key in user_agg:
                        for o in user_agg[key]:
                            self.logger.info(f"  [SYNC] 用户无持仓，撤销{tpsl_name} {coin} oid={o.get('oid')}")
                            self.executor.cancel_order(coin, o.get("oid", 0))
                    continue
                scaled_sz = round_sz(leader_info["total_sz"] * copy_ratio, coin, self.sz_decimals_map)
                user_sz = abs(user_szi)
                if user_sz <= 0:
                    continue
                existing_total = 0.0
                if key in user_agg:
                    existing_total = sum(float(o.get("sz", 0)) for o in user_agg[key])
                price_changed = False
                if key in user_agg:
                    for o in user_agg[key]:
                        user_trigger = float(self._get_trigger_px(o))
                        if abs(user_trigger - trigger_px) / max(trigger_px, 0.001) > price_tolerance:
                            price_changed = True
                            break
                if not price_changed and existing_total > 0 and abs(existing_total - user_sz) / max(user_sz, 0.001) < size_tolerance:
                    continue
                if key in user_agg:
                    for o in user_agg[key]:
                        self.logger.info(f"  [SYNC] 撤销旧{tpsl_name} {coin} oid={o.get('oid')} (size不匹配: existing={existing_total:.4f} vs desired={user_sz:.4f})")
                        self.executor.cancel_order(coin, o.get("oid", 0))
                self.logger.info(f"  [SYNC] 创建{tpsl_name} {coin} trigger={trigger_px} sz={user_sz}")
                self.executor.place_tpsl_order(coin, is_buy, user_sz, str(trigger_px), tpsl)
        except Exception as e:
            self.logger.warning(f"同步挂单失败: {e}")

    # ----------------------------------------------------------------
    # 主循环 (v3: 增加WS处理)
    # ----------------------------------------------------------------
    def run(self) -> None:
        """主循环"""
        try:
            dexes = self.dex_cache.get_perp_dex_list(self.config.base_url, self.config.api_timeout)
            dex_info = f"{len(dexes)}个DEX: " + ", ".join(d or "主DEX" for d in dexes)
        except Exception:
            dex_info = "仅主DEX（获取DEX列表失败）"

        ws_status = "启用" if self._ws_enabled else "禁用"
        self.logger.info("=" * 60)
        self.logger.info("[START] Hyperliquid 跟单机器人 v3.0 启动")  # v3版本号
        self.logger.info(f"  版本: v3.0 (WebSocket保守混合架构)")
        self.logger.info(f"  模式: {'LIVE' if self.live_mode else 'DRY-RUN'}")
        ws_on = self._ws_listener and self._ws_listener.is_connected
        eff = self.interval * 3 if ws_on else self.interval
        ws_tag = "WS在线(×3)" if ws_on else "WS离线(×1)"
        self.logger.info(f"  轮询间隔: {eff}秒 ({ws_tag}, 基础{self.interval}秒)")
        self.logger.info(f"  架构: v3.3 事件驱动 (WS信号检测 + REST按需确认 + {int(self._recon_timeout//60)}min对账)")
        self.logger.info(f"  WebSocket: {ws_status}")  # v3新增
        self.logger.info(f"  Leader地址: {self.config.leader_addr[:8]}...")
        self.logger.info(f"  用户地址: {self.config.user_main_addr[:8]}...")
        self.logger.info(f"  跟单比例: 账户总值×{self.config.fund_ratio} / Leader账户总值")
        self.logger.info(f"  HIP-3支持: {dex_info}")
        self.logger.info(f"  全量对账间隔: 每{self.config.reconciliation_interval}轮")
        self.logger.info(f"  紧急制动文件: {self.config.emergency_stop_file}")
        self.logger.info(f"  暂停信号文件: {self.config.pause_file}")
        self.logger.info(f"  健康检查: {'启用' if self.config.health_enabled else '禁用'} (端口{self.config.health_http_port})")
        self.logger.info(f"  去抖窗口: {self.config.debounce_window_ms}ms")
        self.logger.info(f"  去重窗口: {self.config.dedup_window_seconds}s")
        self.logger.info(f"  JSON日志: {'启用' if self.config._get('logging', 'json_format', default=True) else '禁用'}")
        self.logger.info(f"  安全护栏: {'启用' if self.config.safety_enabled else '禁用'}")
        if self.config.safety_enabled:
            self.logger.info(f"    护栏2-熔断阈值: {self.config.change_ratio_circuit_breaker:.0%}")
            self.logger.info(f"    护栏3-全量查询: {'启用' if self.config.require_all_dex_success else '禁用'}")
            self.logger.info(f"    护栏4-二次确认: {'启用' if self.config.double_confirm_enabled else '禁用'}")
        self.logger.info(f"  配置文件: {self.config._config_path or '默认值'}")
        self.logger.info("=" * 60)

        # Phase 1: 启动告警通知
        # Phase 2: 启动配置校验
        if hasattr(self, '_phase2'):
            self._phase2.validate_startup_config()
        if hasattr(self, '_phase3'):
            self._phase3.set_leader_address(self.config.leader_addr)

        # 初始化
        self._init_sz_decimals()
        self.executor = TradeExecutor(self.live_mode, self.logger, self.config, self.dex_cache)

        # 启动健康检查
        self.health_checker.start_http_server()

        # ===== v3: 初始化WS组件 =====
        self._init_ws_components()

        # ===== 启动基准快照 =====
        self.logger.info("[BASELINE] 获取实时持仓建立基准快照...")
        baseline_ok = False
        try:
            self.prev_leader_positions = get_positions(self.config.leader_addr, self.config, self.dex_cache, self.logger)
            self.prev_leader_orders = get_open_orders(self.config.leader_addr, self.config, self.dex_cache, self.logger)
            self.prev_user_positions = get_positions(self.config.user_main_addr, self.config, self.dex_cache, self.logger)
            baseline_ok = True
        except Exception as e:
            self.logger.error(f"[BASELINE] 获取实时持仓失败: {e}，降级从状态文件恢复")
            prev_lp, prev_lo, prev_up = load_state(self.config.state_file)
            if prev_lp:
                self.prev_leader_positions = prev_lp
                self.prev_leader_orders = prev_lo
                self.prev_user_positions = prev_up
            else:
                self.logger.error("[BASELINE] 状态文件也为空，无法建立基准!")
                self._write_alert("BASELINE_FAIL", "ALL", "启动基准建立失败: 实时查询和状态文件均为空")

        if baseline_ok:
            save_state(self.prev_leader_positions, self.prev_leader_orders, self.prev_user_positions, self.config.state_file)
            baseline_coins = list(self.prev_leader_positions.keys())
            self.baseline_coins = set(baseline_coins)
            self.logger.info(f"[BASELINE] 基准已建立: Leader{len(baseline_coins)}个持仓")
            for coin, pos in sorted(self.prev_leader_positions.items()):
                szi = float(pos["szi"])
                direction = "多" if szi > 0 else "空"
                dex_tag = f"[{pos.get('dex','主')}]" if pos.get('dex') else ""
                self.logger.info(f"  基准: {coin} {direction} {abs(szi)} {dex_tag}")
            self.logger.info("[BASELINE] 以上仓位均为已有仓位，不会触发跟单。只有未来新开的仓位才会自动跟单。")


            # v3: 设置状态对比器基线
            self._state_comparator.set_baseline(self.prev_leader_positions)

        # 输出初始状态
        self._print_status()
        self.last_status_time = time.time()

        # 冷启动全量sync
        try:
            mids = get_all_mids(self.config, self.dex_cache, self.logger)
            copy_ratio = self._calc_copy_ratio()
            if copy_ratio > 0:
                self.logger.info("[COLD-START] 执行冷启动全量TP/SL同步...")
                self._sync_user_tpsl_orders(copy_ratio, mids)
        except Exception as e:
            self.logger.warning(f"冷启动TP/SL同步失败: {e}")

        # === v3.3: 事件驱动主循环 ===
        ws_for_wait = self._ws_listener if (self._ws_enabled and self._ws_listener) else None
        self.logger.info(f"架构: v3.3 事件驱动 (WS信号检测 + REST按需确认 + {int(self._recon_timeout//60)}min对账兜底)")
        
        while self.running:
            self.poll_count += 1
            
            # === 降级检测: WS断连超时 → 切换REST轮询 ===
            if ws_for_wait and not ws_for_wait.is_connected:
                if self._ws_disconnect_time == 0:
                    self._ws_disconnect_time = time.time()
                elif time.time() - self._ws_disconnect_time > self._ws_disconnect_grace:
                    self.logger.warning(f"降级: WS断连超{int(self._ws_disconnect_grace)}s，切换REST轮询({int(self._degraded_interval)}s)")
                    self._degraded_poll_loop()
                    self._ws_disconnect_time = 0
                    continue
            
            # === 事件驱动等待 ===
            try:
                if ws_for_wait:
                    ws_signal = ws_for_wait.wait_for_activity(timeout=self._recon_timeout)
                else:
                    ws_signal = False
                
                if ws_signal:
                    # WS活动信号 → 按需查询
                    self._on_demand_count += 1
                    self._process_ws_queue()
                    self._on_demand_poll()
                else:
                    # 超时（无信号）→ 定时对账
                    self._recon_count += 1
                    self._poll()
                    self._last_full_poll_time = time.time()
                
                # 更新健康状态
                self.health_checker.update(
                    poll_count=self.poll_count,
                    success=True,
                    ws_connected=(ws_for_wait.is_connected if ws_for_wait else False),
                )
                
                # 跨日重置统计
                today_str = datetime.now().strftime("%Y-%m-%d")
                if today_str != self._stats_date:
                    self._stats_date = today_str
                    self._today_opens = 0
                    self._today_adjusts = 0
                    self._today_closes = 0
                    self._today_recons = 0
                    self._on_demand_count = 0
                    self._recon_count = 0
                    self.logger.info(f"[STATS] 跨日重置统计计数器: {today_str}")
                
                # 重置连续错误
                if self._consecutive_errors > 0:
                    self.logger.info(f"[HEALTH-CHECK] 连续错误已恢复(之前连续{self._consecutive_errors}次)")
                self._consecutive_errors = 0
                self._last_error_signature = ""
                self._consecutive_dex_fail_count = 0
                self._ws_disconnect_time = 0  # WS正常，重置断连计时
                
                # 更新运行统计
                self._update_health_stats()
                
            except Exception as e:
                self.logger.error(f"主循环异常: {e}")
                import traceback
                self.logger.error(traceback.format_exc())
                self.health_checker.update(
                    poll_count=self.poll_count,
                    success=False,
                    error=str(e),
                    ws_connected=(ws_for_wait.is_connected if ws_for_wait else False),
                )
                self._check_consecutive_error(e)
            
            # 配置热加载 + 健康推送
            if self.config.reload_if_needed():
                self.logger.info("[CONFIG] 检测到配置文件变更，已热加载")
            self._check_push_health()

        # 清理
        self._stop_ws_components()  # v3: 停止WS组件
        self.health_checker.stop()
        self.logger.info("跟单机器人已停止")

    def _on_demand_poll(self) -> None:
        """WS信号驱动的按需查询（v3.3新增）
        
        由openOrders/userFills活动信号触发，查询leader全部DEX持仓并执行跟单。
        与_poll()相比：跳过智能缓存（确保数据新鲜），其余逻辑复用_poll()。
        带防抖：10秒内不重复触发。
        """
        # 防抖：10秒内不重复查询
        now = time.time()
        if now - self._last_on_demand_time < self._on_demand_debounce:
            self.logger.debug(f"[ON-DEMAND] 防抖跳过({now - self._last_on_demand_time:.1f}s < {self._on_demand_debounce}s)")
            return
        self._last_on_demand_time = now
        
        self.logger.info(f"--- [按需 #{self._on_demand_count}] WS信号触发 [{datetime.now().strftime('%H:%M:%S')}] ---")
        
        # 紧急制动检查
        if self.poll_count % self.config.emergency_stop_check_interval == 0:
            if self._check_emergency_stop():
                return
        
        # 暂停跟单检查
        if self._check_pause():
            return
        
        try:
            # 1. 查leader全部10个DEX持仓（必须全查，因为不知道在哪个DEX操作了）
            new_leader_positions, leader_all_ok, leader_failed = get_positions_with_status(
                self.config.leader_addr, self.config, self.dex_cache, self.logger,
                prev_positions=self.prev_leader_positions
            )
            
            if not leader_all_ok and self.config.safety_enabled and self.config.require_all_dex_success:
                self.logger.warning("[ON-DEMAND] leader持仓查询部分DEX失败，跳过本次")
                return
            
            # 2. mids: WS优先（零成本）
            ws_mids = None
            if self._ws_enabled and self._ws_listener:
                ws_mids = self._ws_listener.get_all_mids()
            mids = ws_mids if ws_mids else get_all_mids(self.config, self.dex_cache, self.logger)
            
            # 3. leader挂单 + 账户值
            data = self._concurrent_get({
                "leader_orders": lambda: get_open_orders(self.config.leader_addr, self.config, self.dex_cache, self.logger),
                "leader_value": lambda: get_account_value(self.config.leader_addr, self.config, self.logger),
                "user_value": lambda: get_account_value(self.config.user_main_addr, self.config, self.logger),
            })
            new_leader_orders = data.get("leader_orders") or []
            if data.get("leader_value") is not None:
                self._cache_account_value["leader"] = {"value": data["leader_value"], "time": time.time()}
            if data.get("user_value") is not None:
                self._cache_account_value["user"] = {"value": data["user_value"], "time": time.time()}
            leader_value = self._cache_account_value["leader"]["value"]
            user_value = self._cache_account_value["user"]["value"]
            
            # 4. user侧数据（必须新鲜，用于精确对比）
            _mu = get_margin_used(self.config.user_main_addr, self.config, self.logger)
            _up = get_positions(self.config.user_main_addr, self.config, self.dex_cache, self.logger)
            _uo = get_open_orders(self.config.user_main_addr, self.config, self.dex_cache, self.logger)
            
            # 5. 组装数据（复用_poll的缓存结构）
            self._poll_cache = {
                "leader_positions": new_leader_positions,
                "leader_orders": new_leader_orders,
                "mids": mids,
                "leader_value": leader_value,
                "user_value": user_value,
                "margin_used": _mu,
                "user_positions": _up,
                "user_open_orders": _uo,
            }
            
            # 6. 计算跟单比例
            copy_ratio = self._calc_copy_ratio(user_value=user_value, leader_value=leader_value)
            self._last_copy_ratio = copy_ratio
            
            # 7. Phase 2 检查
            self._phase2_check_and_rebuild(copy_ratio, mids)
            
            # 8. 持仓变化检测 + 跟单执行（复用现有逻辑）
            pos_changes = detect_position_changes(self.prev_leader_positions, new_leader_positions)
            if pos_changes:
                self.logger.info(f"[ON-DEMAND] 检测到{len(pos_changes)}个持仓变化: {[c.get('coin','?') for c in pos_changes]}")
                self._handle_position_changes(pos_changes, copy_ratio, mids, user_value)
            else:
                self.logger.info("[ON-DEMAND] 无持仓变化（可能是TP/SL调整或非持仓操作）")
            
            # 9. 全量对账（每次按需都带一次对账，确保完整性）
            self._reconciliation_check(new_leader_positions, copy_ratio, mids)
            
            # 10. 更新基线
            self.prev_leader_positions = new_leader_positions
            self.prev_leader_orders = new_leader_orders
            try:
                new_user_positions = get_positions(self.config.user_main_addr, self.config, self.dex_cache, self.logger)
                self.prev_user_positions = new_user_positions
            except Exception:
                pass
            
            # 11. 更新缓存
            self._cache_margin_used = {"value": _mu, "time": time.time()}
            self._cache_user_positions = {"data": _up, "time": time.time()}
            self._cache_user_orders = {"data": _uo, "time": time.time()}
            
            # 12. 状态持久化 + 清理
            self._state_comparator.set_baseline(new_leader_positions)
            _cutoff = time.time() - 60
            self._ws_recently_synced_coins = {k: v for k, v in self._ws_recently_synced_coins.items() if v > _cutoff}
            save_state(new_leader_positions, new_leader_orders, _up, self.config.state_file)
            
            self.logger.info(f"[ON-DEMAND] 完成 (REST消耗≈15, 总计#{self._on_demand_count})")
            
        except Exception as e:
            self.logger.error(f"[ON-DEMAND] 按需查询异常: {e}")
            import traceback
            self.logger.error(traceback.format_exc())

    def _degraded_poll_loop(self) -> None:
        """WS断连时的降级模式：REST定时轮询（v3.3新增）
        
        复用现有_poll()方法，仅改变轮询间隔。
        持续盔到WS恢复连接。
        长期降级(>30分钟)时自动切换轻量级轮询，降低API消耗。
        """
        self.logger.warning(f"[降级] 进入降级轮询模式 (interval={int(self._degraded_interval)}s)")
        degraded_count = 0
        
        while self.running:
            # 检查WS是否恢复
            if self._ws_listener and self._ws_listener.is_connected:
                self.logger.info(f"[降级] WS恢复连接，退出降级模式 (本次降级共{degraded_count}轮)")
                self._degraded_count += degraded_count
                return
            
            # 执行完整轮询（复用_poll）
            self.poll_count += 1  # FIX: 降级模式下poll_count也要递增
            degraded_count += 1
            self._degraded_count += 1
            # 长期降级优化：>30分钟后切换轻量级轮询
            _lightweight = degraded_count * self._degraded_interval > 1800
            # 降级模式RECON调频：每15轮一次(30分钟)
            _orig_recon = self.config.reconciliation_interval
            self.config.reconciliation_interval = 15
            try:
                self._poll(lightweight=_lightweight)
                self._last_full_poll_time = time.time()
                self.health_checker.update(
                    poll_count=self.poll_count,
                    success=True,
                    ws_connected=False,
                )
            except Exception as e:
                self.logger.error(f"[降级] 轮询异常: {e}")
                self.health_checker.update(
                    poll_count=self.poll_count,
                    success=False,
                    error=str(e),
                    ws_connected=False,
                )
            finally:
                self.config.reconciliation_interval = _orig_recon
            
            # 等待降级间隔（可被_stop打断）
            self._ws_wakeup.wait(timeout=self._degraded_interval)
            self._ws_wakeup.clear()

    def _poll(self, lightweight: bool = False) -> None:
        """单次轮询（P3重构版：拆分为子方法）
        
        Args:
            lightweight: 轻量级模式，跳过非核心操作（TP/SL同步、统计等），仅保留持仓检测和RECON
        """
        self.logger.info(f"--- 轮询 #{self.poll_count} [{datetime.now().strftime('%H:%M:%S')}] ---")

        # 热重载检测
        self._reload_exited_coins_if_changed()

        # WS连接状态日志
        if self._ws_enabled and self._ws_listener:
            ws_ok = self._ws_listener.is_connected
            if not ws_ok:
                self.logger.info("[WS] WebSocket未连接，本轮纯轮询模式")

        # 处理待重试的FLIP平仓
        self._retry_pending_flips()

        # FIX-P0: 处理待重试的平仓
        self._retry_pending_closes()

        # 紧急制动检查
        if self.poll_count % self.config.emergency_stop_check_interval == 0:
            if self._check_emergency_stop():
                return

        # 暂停跟单检查（P4-4优雅降级）
        if self._check_pause():
            if self.poll_count % 10 == 0:
                self.logger.info("[PAUSE] 暂停跟单中 - 检测到PAUSE信号文件，跳过交易操作，保持监控")
            return

        # 并发获取最新数据 (API优化版: WS allMids + 智能缓存)
        try:
            leader_positions_result, leader_positions_all_ok, leader_positions_failed = get_positions_with_status(
                self.config.leader_addr, self.config, self.dex_cache, self.logger,
                prev_positions=self.prev_leader_positions
            )

            # === API优化: mids - WS优先，REST兜底 ===
            ws_mids = None
            if self._ws_enabled and self._ws_listener:
                ws_mids = self._ws_listener.get_all_mids()
            _use_ws_mids = ws_mids is not None
            if _use_ws_mids:
                self.logger.info(f"[API-OPT] mids使用WS缓存({len(ws_mids)}个币种)，节省REST调用")

            # === API优化: 智能缓存判断 ===
            if lightweight:
                # 轻量级模式：强制使用缓存，不刷新account_value和user_data
                _need_leader_value = False
                _need_user_value = False
                _need_user_data = False
            else:
                _need_leader_value = (self.poll_count % self._CACHE_ACCOUNT_VALUE_INTERVAL == 0 or
                                      self._cache_account_value["leader"]["value"] == 0)
                _need_user_value = (self.poll_count % self._CACHE_ACCOUNT_VALUE_INTERVAL == 0 or
                                    self._cache_account_value["user"]["value"] == 0)
                _need_user_data = (self.poll_count % self._CACHE_USER_DATA_INTERVAL == 0 or
                                   not self._cache_user_positions["data"])

            # 构建并发任务（仅包含需要刷新的）
            _concurrent_tasks = {
                "leader_orders": lambda: get_open_orders(self.config.leader_addr, self.config, self.dex_cache, self.logger),
            }
            # mids: 仅当WS不可用时才发起REST
            if not _use_ws_mids:
                _concurrent_tasks["mids"] = lambda: get_all_mids(self.config, self.dex_cache, self.logger)
            # account_value: 按TTL缓存
            if _need_leader_value:
                _concurrent_tasks["leader_value"] = lambda: get_account_value(self.config.leader_addr, self.config, self.logger)
            if _need_user_value:
                _concurrent_tasks["user_value"] = lambda: get_account_value(self.config.user_main_addr, self.config, self.logger)

            data = self._concurrent_get(_concurrent_tasks)

            new_leader_positions = leader_positions_result
            new_leader_orders = data.get("leader_orders") or []
            # mids: WS优先
            mids = ws_mids if _use_ws_mids else (data.get("mids") or {})
            # leader_value: 缓存或REST
            if _need_leader_value and data.get("leader_value") is not None:
                self._cache_account_value["leader"] = {"value": data["leader_value"], "time": time.time()}
            leader_value = self._cache_account_value["leader"]["value"]
            # user_value: 缓存或REST
            if _need_user_value and data.get("user_value") is not None:
                self._cache_account_value["user"] = {"value": data["user_value"], "time": time.time()}
            user_value = self._cache_account_value["user"]["value"]

            self._poll_cache = {
                "leader_positions": new_leader_positions,
                "leader_orders": new_leader_orders,
                "mids": mids,
                "leader_value": leader_value,
                "user_value": user_value,
            }

            # === API优化: user侧数据按TTL缓存 ===
            if lightweight:
                # 轻量级模式：跳过user侧数据刷新，使用缓存
                self._poll_cache["margin_used"] = self._cache_margin_used["value"]
                self._poll_cache["user_positions"] = self._cache_user_positions["data"]
                self._poll_cache["user_open_orders"] = self._cache_user_orders["data"]
                self.logger.info(f"[API-OPT-LIGHT] 轻量级模式：user侧数据使用缓存")
            elif _need_user_data:
                try:
                    _mu = get_margin_used(self.config.user_main_addr, self.config, self.logger)
                    self._cache_margin_used = {"value": _mu, "time": time.time()}
                    self._poll_cache["margin_used"] = _mu
                except Exception:
                    self._poll_cache["margin_used"] = self._cache_margin_used["value"]
                try:
                    _up = get_positions(self.config.user_main_addr, self.config, self.dex_cache, self.logger)
                    self._cache_user_positions = {"data": _up, "time": time.time()}
                    self._poll_cache["user_positions"] = _up
                except Exception:
                    self._poll_cache["user_positions"] = self._cache_user_positions["data"]
                try:
                    _uo = get_open_orders(self.config.user_main_addr, self.config, self.dex_cache, self.logger)
                    self._cache_user_orders = {"data": _uo, "time": time.time()}
                    self._poll_cache["user_open_orders"] = _uo
                except Exception:
                    self._poll_cache["user_open_orders"] = self._cache_user_orders["data"]
                self.logger.info(f"[API-OPT] user侧数据已刷新(margin+positions+orders)")
            else:
                self._poll_cache["margin_used"] = self._cache_margin_used["value"]
                self._poll_cache["user_positions"] = self._cache_user_positions["data"]
                self._poll_cache["user_open_orders"] = self._cache_user_orders["data"]
                self.logger.info(f"[API-OPT] user侧数据使用缓存(本轮跳过margin+positions+orders)")

            # 日志: API调用统计
            _rest_count = len(_concurrent_tasks)
            _saved = 6 - _rest_count  # 6 = mids(1)+leader_value(1)+user_value(1)+margin(1)+positions(1)+orders(1)
            _mids_src = "WS" if _use_ws_mids else "REST"
            self.logger.info(f"[API-OPT] 本轮REST并发: {_rest_count}个, WS mids: {_mids_src}, 节省约{_saved}个调用")
        except Exception as e:
            self.logger.error(f"获取数据失败: {e}")
            return


        # ===== Layer 1: 数据完整性 - 全部DEX失败才跳过（连续计数降噪） =====
        if self.config.safety_enabled and self.config.require_all_dex_success:
            if not leader_positions_all_ok:
                self._consecutive_dex_fail_count += 1
                n = self._consecutive_dex_fail_count
                self.logger.info(f"[SAFETY-3] 所有DEX查询失败 连续第{n}轮，本轮跳过")
                if n == 3:
                    self._write_alert("SAFETY_3_ALL_DEX_FAILED", "ALL",
                                      f"所有DEX连续{n}轮查询失败，请检查网络或API状态", level_override="WARNING",
                                      extra_data={"连续失败轮数": str(n)})
                elif n == 10:
                    self._write_alert("SAFETY_3_ALL_DEX_FAILED", "ALL",
                                      f"所有DEX连续{n}轮查询失败！持续无法跟单", level_override="CRITICAL",
                                      extra_data={"连续失败轮数": str(n)})
                return

        # 计算跟单比例（提前计算，供Phase2和后续模块使用）
        copy_ratio = self._calc_copy_ratio(user_value=user_value, leader_value=leader_value)
        self._last_copy_ratio = copy_ratio

        # ===== Phase 2 + 钱包轮换 + Exchange热重建 + TPSL全量重建 =====
        if not lightweight:
            self._phase2_check_and_rebuild(copy_ratio, mids)

        # ===== Phase 3: 增强检测 =====
        if hasattr(self, '_phase3'):
            ws_connected = self._ws_listener.is_connected if self._ws_listener else False
            self._phase3.periodic_check(ws_connected=ws_connected, current_ratio=0.0)

        # ===== Phase 4: 统计增强 - 资金费率监控 =====
        if hasattr(self, '_phase4'):
            try:
                self._phase4.periodic_check(user_positions=new_leader_positions, poll_count=self.poll_count)
            except Exception as e:
                self.logger.warning(f"[Phase4] 周期性检查异常: {e}")

        # ===== 持仓变化检测 =====
        pos_changes = detect_position_changes(self.prev_leader_positions, new_leader_positions)
        if pos_changes:
            # 去重过滤：仅保留未被WS路径处理过的变化
            filtered_changes = []
            for change in pos_changes:
                coin = change["coin"]
                szi = change.get("new_szi", change.get("old_szi", "0"))
                leverage = str(change.get("leverage", 1))
                if self._deduplicator.should_process_change(coin, szi, leverage, "0"):
                    filtered_changes.append(change)
                else:
                    self.logger.info(f"[DEDUP] 轮询跳过已处理变化: {coin} {change['type']}")

            if filtered_changes:
                self.logger.info(f"[DETECT] 检测到 {len(filtered_changes)} 个新持仓变化 (去重后，原{len(pos_changes)}个):")
                for change in filtered_changes:
                    coin = change["coin"]
                    if change["type"] == "open":
                        self.logger.info(f"  Leader开仓 {coin} szi={change['new_szi']} lev={change['leverage']}x")
                    elif change["type"] == "close":
                        self.logger.info(f"  Leader平仓 {coin} (原szi={change['old_szi']})")
                    elif change["type"] == "adjust":
                        self.logger.info(f"  Leader调仓 {coin} szi: {change['old_szi']} → {change['new_szi']}")
                    elif change["type"] == "leverage_change":
                        self.logger.info(f"  Leader杠杆变化 {coin}: {change.get('old_lev', '?')}x → {change.get('new_lev', '?')}x")

                # Layer 2: 幅度分级验证
                if self.config.safety_enabled:
                    total_positions = len(self.prev_leader_positions)
                    if total_positions > 0:
                        change_count = len([c for c in filtered_changes if c["type"] in ("open", "close", "adjust")])
                        change_ratio = change_count / total_positions
                        threshold = self.config.change_ratio_circuit_breaker
                        if change_ratio >= threshold:
                            self.logger.warning(
                                f"[SAFETY-2] 大变化触发复核! "
                                f"变化仓位{change_count}/{total_positions}={change_ratio:.1%} >= 阈值{threshold:.0%}"
                            )
                            try:
                                verify_positions, verify_ok_flag, _ = get_positions_with_status(
                                    self.config.leader_addr, self.config, self.dex_cache, self.logger
                                )
                                if verify_ok_flag:
                                    verify_changes = detect_position_changes(self.prev_leader_positions, verify_positions)
                                    verify_change_count = len([c for c in verify_changes if c["type"] in ("open", "close", "adjust")])
                                    verify_ratio = verify_change_count / total_positions
                                    if verify_ratio >= threshold:
                                        self.logger.info("[SAFETY-2] 复核确认: 变化属实")
                                        self._write_alert("SAFETY_2_VERIFIED", "ALL", f"大变化{change_count}/{total_positions}={change_ratio:.1%}经复核确认，执行跟单",
                                        extra_data={"变化": f"{change_count}/{total_positions}={change_ratio:.1%}", "复核": f"{verify_change_count}/{total_positions}={verify_ratio:.1%}", "阈值": f"{threshold:.0%}"})
                                    else:
                                        self.logger.warning(f"[SAFETY-2] 复核不一致! 首次={change_ratio:.1%}, 复核={verify_ratio:.1%}，跳过")
                                        self._write_alert("SAFETY_2_INCONSISTENT", "ALL", "大变化复核不一致，跳过",
                                        extra_data={"首次": f"{change_count}/{total_positions}={change_ratio:.1%}", "复核": f"{verify_change_count}/{total_positions}={verify_ratio:.1%}", "阈值": f"{threshold:.0%}"})
                                        return
                                else:
                                    self.logger.warning("[SAFETY-2] 复核查询失败，跳过")
                                    self._write_alert("SAFETY_2_VERIFY_ERROR", "ALL", "复核查询失败，跳过本轮大变化检查",
                                        extra_data={"变化": f"{change_count}/{total_positions}={change_ratio:.1%}"})
                                    return
                            except Exception as e:
                                self.logger.error(f"[SAFETY-2] 复核异常: {e}，跳过")
                                self._write_alert("SAFETY_2_VERIFY_ERROR", "ALL", f"复核异常: {e}，跳过本轮大变化检查")
                                return
                        else:
                            self.logger.info(f"[SAFETY-2] 常规变化{change_count}/{total_positions}={change_ratio:.1%}，直接执行")
                    else:
                        self.logger.info("[SAFETY-2] 之前无仓位记录，跳过熔断检查")

                if copy_ratio > 0:
                    self._handle_position_changes(filtered_changes, copy_ratio, mids, user_value)
                else:
                    self.logger.warning("跟单比例为0，跳过操作")
        else:
            self.logger.info("无变化")

        # ===== P1: userFills成交加速（提取为子方法） =====
        self._ws_fill_acceleration(new_leader_positions, copy_ratio, mids)

        # 更新状态对比器
        if new_leader_positions:
            self._state_comparator.force_update(new_leader_positions)


        # ===== 挂单变化检测 =====
        order_added, order_removed = detect_order_changes(self.prev_leader_orders, new_leader_orders)
        if order_added or order_removed:
            _recent = set(self._ws_recently_synced_coins.keys())
            _filt_added = [o for o in order_added if o.get("coin", "") not in _recent]
            _filt_removed = [o for o in order_removed if o.get("coin", "") not in _recent]
            _skipped = (len(order_added) - len(_filt_added)) + (len(order_removed) - len(_filt_removed))
            if _skipped > 0:
                self.logger.info(f"[DEDUP-TPSL] REST挂单变化跳过{_skipped}个WS已同步币种")
            if _filt_added or _filt_removed:
                self.logger.info(f"[DETECT] 检测到挂单变化: 新增{len(_filt_added)}个, 消失{len(_filt_removed)}个 (过滤后)")
                if copy_ratio > 0:
                    self._handle_order_changes(_filt_added, _filt_removed, copy_ratio, mids)

        # ===== TP/SL同步 + 状态更新（提取为子方法） =====
        if not lightweight:
            self._tpsl_sync_and_state_update(copy_ratio, mids, new_leader_positions, new_leader_orders)
        else:
            # 轻量级模式：仅执行RECON，跳过TP/SL同步
            run_recon = self.poll_count % self.config.reconciliation_interval == 0
            if run_recon:
                self._reconciliation_check(new_leader_positions, copy_ratio, mids)

    # ----------------------------------------------------------------
    # Phase 2 监控 + 钱包轮换 + Exchange热重建 + TPSL全量重建
    # ----------------------------------------------------------------
    def _phase2_check_and_rebuild(self, copy_ratio: float, mids: dict) -> None:
        """Phase2周期性检查、钱包轮换检测、Exchange热重建、TPSL全量重建"""
        if hasattr(self, '_phase2'):
            if self._phase2.check_wallet_expiry() == "expired":
                if self.executor.live_mode:
                    self.executor.pause_trading("API钱包已过期，自动暂停跟单")
            elif not self.executor.live_mode and self._phase2._wallet_status == "valid":
                self.executor.resume_trading()
            self._phase2.check_margin_usage(poll_count=self.poll_count, cached_user_value=self._poll_cache.get("user_value", 0), cached_margin_used=self._poll_cache.get("margin_used", 0))
            current_api_wallet = self.config.user_api_addr
            if self._phase2.detect_wallet_rotation(current_api_wallet):
                self.logger.warning("[WALLET-ROTATE] 检测到API钱包轮换，触发Exchange热重建 + 全量TPSL重建")
                self._exchange_rebuild_needed = True
                self._tpsl_full_resync_needed = True

        # Exchange热重建
        if getattr(self, '_exchange_rebuild_needed', False):
            self.logger.info("[WALLET-ROTATE] 重建Exchange对象（新API钱包）...")
            try:
                self.executor.rebuild_exchange()
                self._exchange_rebuild_needed = False
                self.logger.info("[WALLET-ROTATE] Exchange热重建完成")
            except Exception as e:
                self.logger.error(f"[WALLET-ROTATE] Exchange热重建失败: {e}")
                self._write_alert("WALLET_ROTATE_FAIL", "ALL", f"Exchange热重建失败: {e}")

        # TPSL全量重建
        if getattr(self, '_tpsl_full_resync_needed', False):
            self.logger.info("[WALLET-ROTATE] 执行全量TPSL重建...")
            try:
                self._sync_user_tpsl_orders(copy_ratio, mids, force_full=True,
                    cached_leader_orders=self._poll_cache.get("leader_orders"),
                    cached_user_orders=self._poll_cache.get("user_open_orders"),
                    cached_user_positions=self._poll_cache.get("user_positions"))
                self._tpsl_full_resync_needed = False
                self.logger.info("[WALLET-ROTATE] TPSL重建完成")
            except Exception as e:
                self.logger.error(f"[WALLET-ROTATE] TPSL重建失败: {e}")
                self._write_alert("WALLET_ROTATE_TPSL_FAIL", "ALL", f"TPSL全量重建失败: {e}")

    # ----------------------------------------------------------------
    # WS成交加速：利用userFills推送的币种即时检测仓位变化
    # ----------------------------------------------------------------
    def _ws_fill_acceleration(self, new_leader_positions: dict, copy_ratio: float, mids: dict) -> None:
        """利用WS userFills推送的币种即时检测仓位变化，零额外API调用""",
        ws_fill_coins = self._ws_fill_coins.copy()
        self._ws_fill_coins.clear()
        if not ws_fill_coins:
            return
        try:
            fill_changes = []
            for coin in ws_fill_coins:
                try:
                    old_pos = self.prev_leader_positions.get(coin, {})
                    new_pos = new_leader_positions.get(coin, {})
                    old_szi = float(old_pos.get("szi", 0))
                    new_szi = float(new_pos.get("szi", 0))
                    if old_szi != new_szi:
                        old_dir = "多" if old_szi > 0 else "空" if old_szi < 0 else "无"
                        new_dir = "多" if new_szi > 0 else "空" if new_szi < 0 else "无"
                        if old_szi == 0:
                            change_type = "open"
                        elif new_szi == 0:
                            change_type = "close"
                        else:
                            change_type = "adjust"
                        fill_changes.append({
                            "coin": coin,
                            "type": change_type,
                            "old_szi": str(old_szi),
                            "new_szi": str(new_szi),
                            "leverage": new_pos.get("leverage_value", old_pos.get("leverage_value", 1)),
                            "positionValue": new_pos.get("positionValue", old_pos.get("positionValue", 0)),
                            "dex": new_pos.get("dex", old_pos.get("dex", "")),
                        })
                        self.logger.info(
                            f"[WS-FILL-FAST] 成交加速检测: {coin} {old_dir}→{new_dir} "
                            f"szi:{old_szi}→{new_szi}"
                        )
                except Exception as e:
                    self.logger.warning(f"[WS-FILL-FAST] {coin} 加速检测异常(已跳过): {e}")
            if fill_changes:
                filtered_fill = []
                for ch in fill_changes:
                    _szi = ch.get("new_szi", ch.get("old_szi", "0"))
                    _lev = str(ch.get("leverage", 1))
                    if self._deduplicator.should_process_change(ch["coin"], _szi, _lev, "0"):
                        filtered_fill.append(ch)
                    else:
                        self.logger.info(f"[DEDUP] fill加速跳过已处理: {ch['coin']}")
                if filtered_fill:
                    self.logger.info(f"[WS-FILL-FAST] {len(filtered_fill)}个加速变化执行跟单")
                    if copy_ratio > 0:
                        self._handle_position_changes(
                            filtered_fill, copy_ratio, mids, self._poll_cache.get("user_value", 0)
                        )
                # 更新prev_leader_positions防止后续REST检测重复
                for ch in fill_changes:
                    coin = ch["coin"]
                    if coin in new_leader_positions:
                        self.prev_leader_positions[coin] = new_leader_positions[coin]
                    elif float(ch.get("new_szi", 0)) == 0 and coin in self.prev_leader_positions:
                        del self.prev_leader_positions[coin]
        except Exception as e:
            self.logger.warning(f"[WS-FILL-FAST] 成交加速异常(不影响正常流程): {e}")

    # ----------------------------------------------------------------
    # TP/SL同步（定时+WS触发）+ 状态持久化
    # ----------------------------------------------------------------
    def _tpsl_sync_and_state_update(self, copy_ratio: float, mids: dict,
                                     new_leader_positions: dict, new_leader_orders: list) -> None:
        """定时TP/SL同步、WS触发TP/SL同步、状态更新和持久化"""
        # 定时同步TP/SL（时间驱动，不受降频影响）
        _tpsl_interval = getattr(self.config, 'tpsl_sync_time_seconds', 900)
        if time.time() - self._last_periodic_tpsl_sync_time >= _tpsl_interval and copy_ratio > 0:
            self._last_periodic_tpsl_sync_time = time.time()
            self._sync_user_tpsl_orders(copy_ratio, mids)

        # WS触发的即时TP/SL同步（openOrders变化时）
        self._ws_last_synced_coins = set()
        if self._ws_tpsl_sync_needed and copy_ratio > 0:
            self._ws_tpsl_sync_needed = False
            self._ws_last_tpsl_sync_time = time.time()
            ws_coins = self._ws_tpsl_coins.copy()
            self._ws_tpsl_coins.clear()
            self._ws_last_synced_coins = ws_coins
            if ws_coins:
                self.logger.info(f"[WS-TPSL] 执行WS触发的即时TP/SL同步 (coins={ws_coins})")
                self._sync_user_tpsl_orders(copy_ratio, mids, force_full=False, _target_coins=ws_coins)
            else:
                self.logger.info("[WS-TPSL] 执行WS触发的全量TP/SL同步")
                self._sync_user_tpsl_orders(copy_ratio, mids)

        # 记录WS已同步币种（REST路径跳过，避免双路径冲突）
        ws_coins = getattr(self, '_ws_last_synced_coins', set())
        if ws_coins:
            self._ws_recently_synced_coins.update({c: time.time() for c in ws_coins})
            _cutoff = time.time() - 60
            self._ws_recently_synced_coins = {k: v for k, v in self._ws_recently_synced_coins.items() if v > _cutoff}

        # 全量对账
        run_recon = self.poll_count % self.config.reconciliation_interval == 0
        if not run_recon and getattr(self, '_safety2_skip_recon', None) and self._safety2_skip_recon.get('coins'):
            if time.time() - self._safety2_skip_recon.get('time', 0) < 120:
                run_recon = True
                self.logger.info(f"[SAFETY-2] 触发补检（上轮被跳过）: 币种={self._safety2_skip_recon.get('coins', [])}")
                self._safety2_skip_recon = {}
            else:
                self.logger.info("[SAFETY-2] 补检超时(>120s)，放弃补检")
                self._safety2_skip_recon = {}
        if run_recon:
            self._reconciliation_check(new_leader_positions, copy_ratio, mids)

        # 更新状态
        try:
            new_user_positions = get_positions(self.config.user_main_addr, self.config, self.dex_cache, self.logger)
        except Exception:
            new_user_positions = self.prev_user_positions

        self.prev_leader_positions = new_leader_positions
        self.prev_leader_orders = new_leader_orders
        self.prev_user_positions = new_user_positions

        # 清理过期的WS同步记录
        _cutoff = time.time() - 60
        self._ws_recently_synced_coins = {k: v for k, v in self._ws_recently_synced_coins.items() if v > _cutoff}

        save_state(new_leader_positions, new_leader_orders, new_user_positions, self.config.state_file)

        now = time.time()
        if now - self.last_status_time >= self.config.status_interval:
            self._print_status()
            self.last_status_time = now

def main() -> None:
    import argparse
    parser = argparse.ArgumentParser(description="Hyperliquid 跟单机器人 v3.0 (WebSocket保守混合架构)")
    parser.add_argument("--live", action="store_true", help="启用live模式(实际下单)")
    parser.add_argument("--interval", type=int, default=None, help="轮询间隔秒数(覆盖配置文件)")
    parser.add_argument("--config", type=str, default=None, help="配置文件路径(默认同目录config_v3.yaml)")
    args = parser.parse_args()

    if sys.platform == "win32":
        try:
            sys.stdout.reconfigure(encoding="utf-8", errors="replace")
            sys.stderr.reconfigure(encoding="utf-8", errors="replace")
        except Exception:
            pass
        try:
            os.system("chcp 65001 >nul 2>&1")
        except Exception:
            pass

    script_dir = os.path.dirname(os.path.abspath(__file__))
    config_path = args.config
    if config_path is None:
        candidates = [
            os.path.join(os.getcwd(), "config_v3.yaml"),
            os.path.join(script_dir, "config_v3.yaml"),
            os.path.join(os.getcwd(), "config.yaml"),
            os.path.join(script_dir, "config.yaml"),
            os.path.expanduser("~/hl_copytrade/config_v3.yaml"),
            os.path.expanduser("~/hl_copytrade/config.yaml"),
        ]
        for c in candidates:
            if os.path.exists(c):
                config_path = c
                break

    config = ConfigManager(config_path=config_path, script_dir=script_dir)
    logger = setup_logger(
        log_file=config.log_file,
        level=config._get("logging", "level", default="DEBUG"),
        console_level=config._get("logging", "console_level", default="INFO"),
        max_bytes=config._get("logging", "max_bytes", default=10485760),
        backup_count=config._get("logging", "backup_count", default=5),
        json_format=config._get("logging", "json_format", default=True),
        log_mode=config._get("logging", "log_mode", default="both"),
    )

    interval = args.interval if args.interval is not None else config.poll_interval

    logger.info(f"[CONFIG] 配置文件: {config_path or '无(使用默认值)'}")
    logger.info(f"[CONFIG] API Base URL: {config.base_url}")
    logger.info(f"[CONFIG] 轮询间隔: {interval}s")
    logger.info(f"[CONFIG] WebSocket: {'启用' if config.ws_enabled else '禁用'}")
    logger.info(f"[CONFIG] 跟单比例系数: {config.fund_ratio}")
    logger.info(f"[CONFIG] 最大杠杆: {config.max_leverage}x")

    bot_name = "高频" if config._config_path and "highfreq" in config._config_path else "低频"
    max_restarts = config.max_restarts
    restart_delay = config.restart_delay
    restart_count = 0
    while restart_count < max_restarts:
        try:
            bot = CopyTradeBot(
                live_mode=args.live,
                interval=interval,
                logger=logger,
                config=config,
            )
            bot.run()
            break
        except KeyboardInterrupt:
            logger.info("用户中断，退出")
            break
        except Exception as e:
            restart_count += 1
            logger.error(f"[CRASH] 脚本崩溃({restart_count}/{max_restarts}): {e}\n{traceback.format_exc()}")
            try:
                from unified_alert_queue import enqueue_alert
                enqueue_alert("CRITICAL", "V3跟单脚本崩溃", f"崩溃{restart_count}/{max_restarts}: {e}", source=bot_name)
            except Exception:
                pass
            if restart_count < max_restarts:
                logger.info(f"{restart_delay}秒后自动重启...")
                time.sleep(restart_delay)
            else:
                logger.error("重启次数已达上限，退出")
                try:
                    from unified_alert_queue import enqueue_alert
                    enqueue_alert("CRITICAL", "V3跟单脚本重启次数耗尽", f"已崩溃{restart_count}次，不再自动重启", source=bot_name)
                except Exception:
                    pass

    try:
        input("按回车键关闭窗口...")
    except (EOFError, OSError):
        pass  # Fix 4 (2026-07-03): systemd无stdin时不崩溃


def _crash_excepthook(exc_type, exc_value, exc_tb):
    """P0 FIX: 全局崩溃捕获，写入独立crash日志"""
    import datetime
    crash_file = os.path.join(os.path.dirname(os.path.abspath(__file__)), "crash.log")
    ts = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    tb_text = "".join(traceback.format_exception(exc_type, exc_value, exc_tb))
    with open(crash_file, "a") as f:
        f.write(f"\n{'='*60}\n")
        f.write(f"[{ts}] CRASH: {exc_type.__name__}: {exc_value}\n")
        f.write(tb_text)
        f.write(f"{'='*60}\n")
    try:
        from unified_alert_queue import enqueue_alert
        enqueue_alert("CRITICAL", "V3未捕获异常", f"{exc_type.__name__}: {exc_value}", source="V3-global")
    except Exception:
        pass
    sys.__excepthook__(exc_type, exc_value, exc_tb)

sys.excepthook = _crash_excepthook

if __name__ == "__main__":
    main()

