#!/usr/bin/env python3
"""
Hyperliquid Orchestrator v4.2 - Subprocess Orchestrator
Single process manages multiple V3 copy-trade instances.

v4.2 fixes (2026-07-28 deep audit):
  === Sensitive info isolation ===
  A1: config_v4.yaml no longer stores plaintext addresses/webhooks
      → all secrets loaded from per-instance .env files
  A2: .env duplication eliminated
      → orchestrator manages .orchestrator/{name}/.env.{prefix} as single source
      → no more copying from root .env files
  A3: print_status() hides wallet addresses (shows nothing)
  A4: sensitive files (orchestrator.log, v3_stdout.log) set to 0o600

  === Log management ===
  B1: v3_stdout.log uses SimpleRotatingWriter (5MB × 3 backups)
  B2: auto-cleanup of legacy V2 logs + rotated backups > 7 days
  B3: V3 console_level defaults to WARNING (less noise in stdout)
  B4: orchestrator.log permission set to 0o600

  === Hardcoding elimination ===
  C1: emergency_stop_file → auto-computed as .orchestrator/{name}/EMERGENCY_STOP
  C2: env_file → auto-computed as .orchestrator/{name}/.env.{prefix}
  C3: no path/address/webhook hardcoded in config_v4.yaml
"""
from __future__ import annotations
import json, logging, os, re, requests, signal, subprocess, sys, threading, time, shutil
from datetime import datetime
from http.server import HTTPServer, BaseHTTPRequestHandler
from logging.handlers import RotatingFileHandler
from pathlib import Path
from typing import Any, Dict, Optional
import yaml

from paths import BASE_DIR, ORCH_DIR
CONFIG_V4 = BASE_DIR / "config_v4.yaml"
ORCH_DIR  = ORCH_DIR
PID_FILE  = ORCH_DIR / "orchestrator.pid"
LOG_FILE  = ORCH_DIR / "orchestrator.log"
CMD_DIR   = ORCH_DIR / "commands"

# ---------------------------------------------------------------------------
# SimpleRotatingWriter — for subprocess stdout redirect (B1)
# ---------------------------------------------------------------------------
class SimpleRotatingWriter:
    """File-like object with size-based rotation.
    Replaces plain open() for subprocess stdout to prevent unbounded growth.
    """
    def __init__(self, path: Path, max_bytes: int = 5_242_880, backup_count: int = 3):
        self.path = Path(path)
        self.max_bytes = max_bytes
        self.backup_count = backup_count
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.fh = open(self.path, "a")
        # A4: restrict permission
        os.chmod(str(self.path), 0o600)

    def write(self, data: str):
        self.fh.write(data)
        self.fh.flush()
        try:
            if self.path.stat().st_size > self.max_bytes:
                self._rotate()
        except Exception:
            pass

    def _rotate(self):
        self.fh.close()
        # Shift: .3 → delete, .2 → .3, .1 → .2, current → .1
        for i in range(self.backup_count, 0, -1):
            src = Path(str(self.path) + "." + str(i))
            if i >= self.backup_count:
                src.unlink(missing_ok=True)
            else:
                dst = Path(str(self.path) + "." + str(i + 1))
                if src.exists():
                    src.rename(dst)
        self.path.rename(Path(str(self.path) + ".1"))
        self.fh = open(self.path, "a")
        os.chmod(str(self.path), 0o600)

    def fileno(self):
        return self.fh.fileno()

    def flush(self):
        try:
            self.fh.flush()
        except Exception:
            pass

    def close(self):
        try:
            self.fh.close()
        except Exception:
            pass


# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
def _setup_logger():
    ORCH_DIR.mkdir(parents=True, exist_ok=True)
    lg = logging.getLogger("orchestrator")
    lg.setLevel(logging.DEBUG)
    fmt = logging.Formatter("[%(asctime)s] [%(levelname)s] %(message)s", "%Y-%m-%d %H:%M:%S")
    fh = RotatingFileHandler(str(LOG_FILE), maxBytes=10_485_760, backupCount=5, encoding="utf-8")
    fh.setFormatter(fmt)
    lg.addHandler(fh)
    # A4: restrict log permission
    os.chmod(str(LOG_FILE), 0o600)
    ch = logging.StreamHandler(sys.stdout)
    ch.setLevel(logging.INFO)
    ch.setFormatter(fmt)
    lg.addHandler(ch)
    return lg

log = _setup_logger()

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------
class Cfg:
    def __init__(self):
        with open(CONFIG_V4) as f:
            self.raw = yaml.safe_load(f) or {}

    @property
    def orch(self) -> dict:
        return self.raw.get("orchestrator", {})

    @property
    def instances(self) -> dict:
        return self.raw.get("instances", {})

    @property
    def defaults(self) -> dict:
        return self.raw.get("defaults", {})

    def enabled(self) -> dict:
        return {n: c for n, c in self.instances.items() if c.get("enabled")}

    @property
    def base_url(self) -> str:
        return self.defaults.get("api", {}).get("base_url", "https://api.hyperliquid.xyz")


# ---------------------------------------------------------------------------
# Config Generator — generates per-instance V3 config
# A1: no secrets in config_v4.yaml, all from .env
# C1/C2: paths auto-computed
# ---------------------------------------------------------------------------
class Gen:
    def __init__(self, cfg: Cfg):
        self.cfg = cfg
        self.base = Path(cfg.orch.get("instances_dir", str(ORCH_DIR)))
        self.base.mkdir(parents=True, exist_ok=True)

    def all(self) -> Dict[str, Path]:
        out = {}
        for n, ic in self.cfg.enabled().items():
            out[n] = self.one(n, ic)
        return out

    def one(self, name: str, ic: dict = None) -> Path:
        if ic is None:
            ic = self.cfg.instances.get(name, {})
        d = self.base / name
        d.mkdir(parents=True, exist_ok=True)
        env_path = self._env_path(name, ic, d)
        cfg_path = self._v3cfg(name, ic, d, env_path)
        log.info("[GEN] %s -> %s", name, cfg_path)
        return cfg_path

    @staticmethod
    def _env_path(name: str, ic: dict, d: Path) -> Path:
        """C2: env_file path auto-computed, no config dependency."""
        prefix = ic.get("file_prefix", name)
        return d / (".env." + prefix)

    def _v3cfg(self, name: str, ic: dict, d: Path, env_path: Path) -> Path:
        prefix = ic.get("file_prefix", name)
        out = d / ("config_v3_" + prefix + ".yaml")
        df = self.cfg.defaults
        now_s = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        # C1: emergency_stop_file auto-computed in instance directory
        es = str(d / "EMERGENCY_STOP")

        v3 = {
            "account": {
                "leader_addr_env": "HL_LEADER_ADDR",
                "user_main_addr_env": "HL_USER_MAIN_ADDR",
                "user_api_addr_env": "HL_USER_API_ADDR",
                "private_key_env": "HL_API_PK",
            },
            "api": df.get("api", {}),
            "strategy": dict(
                [("fund_ratio", ic.get("fund_ratio", 0.95)),
                 ("max_leverage", ic.get("max_leverage", 40))]
                + list(df.get("strategy", {}).items())
            ),
            "polling": dict(
                [("interval", ic.get("poll_interval", 60))]
                + list(df.get("polling", {}).items())
            ),
            "flip": df.get("flip", {}),
            "paths": {
                "state_file": str(d / ("hl_copytrade_v3_" + prefix + "_state.json")),
                "log_file": str(d / ("hl_copytrade_v3_" + prefix + ".log")),
                "emergency_stop_file": es,
                "alert_file": str(d / ("critical_alerts_v3_" + prefix + ".json")),
                "health_file": str(d / ("health_v3_" + prefix + ".json")),
                "env_file": str(env_path),
            },
            "health": {
                "enabled": True,
                "http_port": ic.get("health_port", 8997),
                "max_consecutive_failures": 5,
            },
            "logging": dict(
                [("level", ic.get("log_level", "INFO")),
                 # B3: console_level defaults to WARNING to reduce stdout noise
                 ("console_level", ic.get("console_level", "WARNING"))]
                + list(df.get("logging", {}).items())
            ),
            "runtime": {"max_restarts": 1, "restart_delay": 10, "hot_reload_config": True},
            "websocket": dict(
                [("enabled", ic.get("ws_enabled", True))]
                + list(df.get("websocket", {}).items())
            ),
            "debounce": df.get("debounce", {}),
            "deduplication": df.get("deduplication", {}),
            "sz_decimals": df.get("sz_decimals", {}),
            "alert": {
                "enabled": True,
                "webhook_url_env": "HL_FEISHU_WEBHOOK",
                "bot_name": ic.get("alert_bot_name", name),
            },
            "safety": df.get("safety", {}),
            "api_wallet": {
                "expiry_date": ic.get("api_wallet_expiry", "2099-12-31"),
                "description": ic.get("display_name", name),
            },
        }
        hdr = "# Auto-generated for " + name + " at " + now_s + " - DO NOT EDIT\n\n"
        out.write_text(hdr + yaml.dump(v3, default_flow_style=False, allow_unicode=True))
        return out


# ---------------------------------------------------------------------------
# Process wrapper
# ---------------------------------------------------------------------------
class Proc:
    """Manages a single V3 subprocess."""

    def __init__(self, name: str, cfg_path: Path, orch: "Orchestrator"):
        self.name = name
        self.cfg_path = cfg_path
        self.orch = orch
        self.proc: Optional[subprocess.Popen] = None
        self.started: Optional[datetime] = None
        self.restarts = 0
        self._stop = False
        self._log_writer = None  # SimpleRotatingWriter for stdout redirect

    @property
    def alive(self) -> bool:
        return self.proc is not None and self.proc.poll() is None

    @property
    def pid(self) -> Optional[int]:
        return self.proc.pid if self.alive else None

    @property
    def uptime(self) -> float:
        if self.alive and self.started:
            return (datetime.now() - self.started).total_seconds()
        return 0

    def start(self) -> bool:
        if self.alive:
            log.warning("[PROC] %s already running PID=%s", self.name, self.pid)
            return True

        self._stop = False
        orch_cfg = self.orch.cfg.orch
        py = orch_cfg.get("venv_python", str(BASE_DIR / "venv/bin/python3"))
        v3 = orch_cfg.get("v3_script", str(BASE_DIR / "hl_copytrade_v3.py"))
        wd = orch_cfg.get("working_dir", str(BASE_DIR))

        cmd = [py, v3, "--live", "--config", str(self.cfg_path)]
        log.info("[PROC] %s starting (PID will be assigned)", self.name)

        # B1: redirect stdout/stderr to rotating log file instead of plain open()
        inst_dir = ORCH_DIR / self.name
        inst_dir.mkdir(parents=True, exist_ok=True)
        stdout_path = inst_dir / "v3_stdout.log"
        self._log_writer = SimpleRotatingWriter(stdout_path, max_bytes=5_242_880, backup_count=3)

        # P0-4 fix: scrub sensitive env vars before passing to subprocess
        child_env = os.environ.copy()
        child_env.pop("ORCHESTRATOR_WALLET_PASSWORD", None)

        try:
            self.proc = subprocess.Popen(
                cmd, cwd=wd,
                stdout=self._log_writer,
                stderr=self._log_writer,
                env=child_env,
            )
            self.started = datetime.now()
            log.info("[PROC] %s started PID=%d", self.name, self.proc.pid)
            return True
        except Exception as e:
            log.error("[PROC] %s start failed: %s", self.name, e)
            if self._log_writer:
                self._log_writer.close()
                self._log_writer = None
            return False

    def stop(self, force: bool = False, timeout: int = 30) -> bool:
        if not self.alive:
            self._cleanup_writer()
            return True
        self._stop = True
        if force:
            self.proc.kill()
            self.proc.wait(5)
            self._cleanup_writer()
            return True
        self.proc.terminate()
        try:
            self.proc.wait(timeout)
        except subprocess.TimeoutExpired:
            self.proc.kill()
            self.proc.wait(5)
        self._cleanup_writer()
        return True

    def _cleanup_writer(self):
        if self._log_writer:
            try:
                self._log_writer.close()
            except Exception:
                pass
            self._log_writer = None

    def poll(self):
        return self.proc.poll() if self.proc else None


# ---------------------------------------------------------------------------
# Orchestrator
# ---------------------------------------------------------------------------
class Orchestrator:
    def __init__(self):
        self.cfg = Cfg()
        self.gen = Gen(self.cfg)
        self.procs: Dict[str, Proc] = {}
        self._run = False
        self._mon: Optional[threading.Thread] = None
        self._http: Optional[HTTPServer] = None

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------
    def start_all(self):
        # B2: cleanup legacy logs on startup
        self._cleanup_legacy_logs()

        sep = "=" * 60
        log.info(sep)
        log.info("[ORCH] starting all instances")
        log.info(sep)
        paths = self.gen.all()
        for n, cp in paths.items():
            p = Proc(n, cp, self)
            if p.start():
                self.procs[n] = p
        self._write_pid()
        self._run = True
        self._mon = threading.Thread(target=self._monitor, daemon=True, name="mon")
        self._mon.start()
        self._start_http()
        # B2: start periodic log cleanup thread
        self._start_log_cleaner()
        log.info("[ORCH] %d instance(s) running", len(self.procs))

    def stop_all(self, force: bool = False):
        log.info("[ORCH] stopping all force=%s", force)
        self._run = False
        if self._http:
            self._http.shutdown()
        for p in self.procs.values():
            p.stop(force)
        self.procs.clear()
        if PID_FILE.exists():
            PID_FILE.unlink()

    def _check_instance_positions(self, name: str) -> dict:
        """检查实例的用户钱包持仓（用于移除实例前的安全检查）"""
        ic = self.cfg.instances.get(name)
        if not ic:
            return {}
        env = ic.get("env", {})
        user_main = env.get("HL_USER_MAIN_ADDR", "")
        if not user_main:
            return {}
        try:
            resp = requests.post(
                f"{self.cfg.base_url}/info",
                json={"type": "clearinghouseState", "user": user_main},
                timeout=15,
            )
            resp.raise_for_status()
            data = resp.json()
            positions = data.get("assetPositions", [])
            active = {}
            for p in positions:
                pos = p.get("position", {})
                coin = pos.get("coin", "")
                szi = float(pos.get("szi", 0))
                if abs(szi) > 1e-8:
                    active[coin] = {"szi": szi, "side": "LONG" if szi > 0 else "SHORT"}
            return active
        except Exception as e:
            log.warning("[ORCH] 检查实例 %s 持仓失败: %s", name, e)
            return {}

    def stop_inst(self, name: str, force: bool = False) -> bool:
        # 仓位保护：停止前检查持仓
        if not force:
            positions = self._check_instance_positions(name)
            if positions:
                pos_info = ", ".join(f"{c}({d['side']}{abs(d['szi']):.4f})" for c, d in positions.items())
                log.warning("[ORCH] 实例 %s 仍有活跃持仓: %s，拒绝停止（使用force=True强制停止）", name, pos_info)
                return False
        if name not in self.procs:
            return False
        ok = self.procs[name].stop(force)
        if ok:
            del self.procs[name]
        return ok

    def restart_inst(self, name: str) -> bool:
        # 重新加载配置，确保修改 config_v4.yaml 后 restart-instance 立即生效
        try:
            self.cfg = Cfg()
            self.gen = Gen(self.cfg)
            log.info("[ORCH] config reloaded for restart-instance")
        except Exception as e:
            log.warning("[ORCH] reload config failed, using cached: %s", e)
        if name in self.procs:
            self.stop_inst(name)
        ic = self.cfg.instances.get(name)
        if not ic:
            return False
        cp = self.gen.one(name, ic)
        p = Proc(name, cp, self)
        if p.start():
            self.procs[name] = p
            return True
        return False

    # ------------------------------------------------------------------
    # Emergency stop
    # C1: ES file path auto-computed
    # ------------------------------------------------------------------
    def es_all(self):
        log.critical("[ES] ===== GLOBAL EMERGENCY STOP =====")
        for n, ic in self.cfg.enabled().items():
            self._write_es(n, ic)
        gf = self.cfg.orch.get("emergency_stop_all_file",
                                str(BASE_DIR / "EMERGENCY_STOP_ALL"))
        Path(gf).write_text("GLOBAL_ES " + datetime.now().isoformat() + "\n")

    def es_inst(self, name: str):
        ic = self.cfg.instances.get(name)
        if not ic:
            log.error("[ES] %s not found", name)
            return
        log.critical("[ES] ===== instance ES: %s =====", name)
        self._write_es(name, ic)

    @staticmethod
    def _write_es(name: str, ic: dict):
        # C1: ES file in instance directory, not from config
        es_path = ORCH_DIR / name / "EMERGENCY_STOP"
        es_path.write_text("ES_" + name.upper() + " " + datetime.now().isoformat() + "\n")
        os.chmod(str(es_path), 0o600)
        log.critical("[ES] %s -> %s", name, es_path)
        marker = ORCH_DIR / name / "STOPPED"
        marker.write_text("ES " + datetime.now().isoformat() + "\n")
        os.chmod(str(marker), 0o600)
        log.critical("[ES] %s STOPPED marker written", name)

    @staticmethod
    def _is_stopped(name: str) -> bool:
        return (ORCH_DIR / name / "STOPPED").exists()

    @staticmethod
    def _clear_stopped(name: str):
        marker = ORCH_DIR / name / "STOPPED"
        if marker.exists():
            marker.unlink()
            log.info("[ORCH] %s STOPPED marker cleared", name)

    # ------------------------------------------------------------------
    # Command-file IPC
    # ------------------------------------------------------------------
    def _process_commands(self):
        if not CMD_DIR.exists():
            return
        for cmd_file in sorted(CMD_DIR.iterdir()):
            if not cmd_file.is_file():
                continue
            fname = cmd_file.name
            try:
                if fname.startswith("restart_"):
                    name = fname[len("restart_"):]
                    log.info("[CMD] restart %s", name)
                    self._clear_stopped(name)
                    self.restart_inst(name)
                elif fname.startswith("stop_"):
                    name = fname[len("stop_"):]
                    log.info("[CMD] stop %s", name)
                    self.stop_inst(name)
                else:
                    log.warning("[CMD] unknown command: %s", fname)
            except Exception as e:
                log.error("[CMD] failed to process %s: %s", fname, e)
            finally:
                try:
                    cmd_file.unlink()
                except Exception:
                    pass

    # ------------------------------------------------------------------
    # Monitor loop
    # ------------------------------------------------------------------
    def _monitor(self):
        iv = self.cfg.orch.get("health_check_interval", 10)
        mx = self.cfg.orch.get("max_restarts", 100)
        dl = self.cfg.orch.get("restart_delay", 30)
        ar = self.cfg.orch.get("auto_restart", True)

        while self._run:
            self._process_commands()

            for n in list(self.procs):
                p = self.procs[n]
                ec = p.poll()
                if ec is not None:
                    if p._stop:
                        log.info("[MON] %s stopped by orchestrator (code=%d)", n, ec)
                        continue

                    if self._is_stopped(n):
                        log.info("[MON] %s stopped via ES (code=%d), not restarting", n, ec)
                        continue

                    log.error("[MON] %s crashed code=%d restarts=%d/%d",
                              n, ec, p.restarts, mx)
                    if ar and p.restarts < mx:
                        p.restarts += 1
                        time.sleep(dl)
                        ic = self.cfg.instances.get(n, {})
                        cp = self.gen.one(n, ic)
                        p.cfg_path = cp
                        p.start()
                    else:
                        log.critical("[MON] %s max restarts reached", n)

            time.sleep(iv)

    # ------------------------------------------------------------------
    # Health HTTP (bind 127.0.0.1)
    # ------------------------------------------------------------------
    def _start_http(self):
        port = self.cfg.orch.get("health_port", 9000)
        ref = self

        class H(BaseHTTPRequestHandler):
            def do_GET(self):
                if self.path in ("/health", "/"):
                    d = ref._health_all()
                elif self.path.startswith("/instance/"):
                    nm = self.path.split("/instance/")[1].rstrip("/")
                    d = ref._health_one(nm)
                    if d is None:
                        self.send_response(404)
                        self.end_headers()
                        return
                else:
                    self.send_response(404)
                    self.end_headers()
                    return
                self.send_response(200)
                self.send_header("Content-Type", "application/json")
                self.end_headers()
                self.wfile.write(json.dumps(d, indent=2, ensure_ascii=False).encode())

            def log_message(self, fmt, *a):
                log.debug("[HTTP] " + (fmt % a))

        try:
            self._http = HTTPServer(("127.0.0.1", port), H)
            threading.Thread(
                target=self._http.serve_forever, daemon=True, name="http"
            ).start()
            log.info("[HTTP] http://127.0.0.1:%d/health", port)
        except Exception as e:
            log.error("[HTTP] fail: %s", e)

    def _health_all(self) -> dict:
        inst = {}
        ok = True
        for n, p in self.procs.items():
            ic = self.cfg.instances.get(n, {})
            pf = ic.get("file_prefix", n)
            ih = {
                "running": p.alive,
                "pid": p.pid,
                "uptime": round(p.uptime, 1),
                "restarts": p.restarts,
                "stopped": self._is_stopped(n),
            }
            hf = ORCH_DIR / n / ("health_v3_" + pf + ".json")
            if hf.exists():
                try:
                    with open(hf) as f:
                        h = json.load(f)
                    ih["v3_status"] = h.get("status", "?")
                    ih["ws"] = h.get("ws_connected", False)
                    ih["last_poll"] = h.get("last_poll")
                except Exception:
                    ih["v3_status"] = "unreadable"
            if not p.alive:
                ok = False
            inst[n] = ih
        return {
            "status": "healthy" if ok else "degraded",
            "ts": datetime.now().isoformat(),
            "pid": os.getpid(),
            "instances": inst,
            "count": len(self.procs),
            "running": sum(1 for p in self.procs.values() if p.alive),
        }

    def _health_one(self, name: str) -> Optional[dict]:
        if name not in self.procs:
            return None
        p = self.procs[name]
        r = {
            "name": name,
            "running": p.alive,
            "pid": p.pid,
            "uptime": round(p.uptime, 1),
            "stopped": self._is_stopped(name),
        }
        ic = self.cfg.instances.get(name, {})
        pf = ic.get("file_prefix", name)
        hf = ORCH_DIR / name / ("health_v3_" + pf + ".json")
        if hf.exists():
            try:
                with open(hf) as f:
                    r["v3"] = json.load(f)
            except Exception:
                pass
        return r

    # ------------------------------------------------------------------
    # B2: Log cleanup
    # ------------------------------------------------------------------
    @staticmethod
    def _cleanup_legacy_logs():
        """One-time cleanup of legacy V2 and stale log files on startup."""
        legacy_files = [
            BASE_DIR / "hl_copytrade.log",       # V2 main log
            BASE_DIR / "events.log",              # V2 events log
            BASE_DIR / ".env",                    # Redundant root .env (now managed by orchestrator)
            BASE_DIR / ".env.highfreq",           # Redundant root .env.highfreq
        ]
        removed = 0
        for f in legacy_files:
            if f.exists():
                try:
                    f.unlink()
                    removed += 1
                    log.info("[LOG-CLEAN] removed legacy file: %s", f.name)
                except Exception as e:
                    log.warning("[LOG-CLEAN] failed to remove %s: %s", f.name, e)
        # Also remove old root-level health files (now in .orchestrator/{name}/)
        for f in BASE_DIR.glob("health_v3*.json"):
            try:
                f.unlink()
                removed += 1
            except Exception:
                pass
        if removed:
            log.info("[LOG-CLEAN] cleaned %d legacy file(s)", removed)

    def _start_log_cleaner(self):
        """Start periodic log cleanup thread."""
        def _loop():
            while self._run:
                time.sleep(86400)  # 24 hours
                try:
                    self._periodic_cleanup()
                except Exception as e:
                    log.warning("[LOG-CLEAN] periodic error: %s", e)
        threading.Thread(target=_loop, daemon=True, name="log-cleaner").start()

    @staticmethod
    def _periodic_cleanup():
        """Remove rotated backup files older than 7 days."""
        now = time.time()
        max_age = 7 * 86400
        removed = 0
        # Clean rotated backups: *.log.1, *.log.2, etc.
        for f in ORCH_DIR.rglob("*"):
            if f.is_file() and re.match(r".*\.log\.\d+$", f.name):
                try:
                    if now - f.stat().st_mtime > max_age:
                        f.unlink()
                        removed += 1
                except Exception:
                    pass
        # Clean external cron logs if they grow too large
        for name in ["vps_health_check.log", "git_sync.log"]:
            ext_log = BASE_DIR / name
            if ext_log.exists():
                try:
                    sz = ext_log.stat().st_size
                    if sz > 5_242_880:  # > 5MB, truncate
                        # Keep last 1MB
                        with open(ext_log, "r") as rf:
                            rf.seek(max(0, sz - 1_048_576))
                            tail = rf.read()
                        with open(ext_log, "w") as wf:
                            wf.write(tail)
                        removed += 1
                except Exception:
                    pass
        if removed:
            log.info("[LOG-CLEAN] periodic: removed/truncated %d file(s)", removed)

    # ------------------------------------------------------------------
    # A3: Status display — no sensitive info
    # ------------------------------------------------------------------
    def print_status(self):
        eq = "=" * 70
        da = "-" * 70
        print("\n" + eq)
        print("  Hyperliquid Orchestrator v4.2 Status")
        print(eq)

        if PID_FILE.exists():
            pid = PID_FILE.read_text().strip()
            try:
                os.kill(int(pid), 0)
                print("  Orchestrator: RUNNING PID=" + pid)
            except Exception:
                print("  Orchestrator: STOPPED (stale pid)")
        else:
            print("  Orchestrator: NOT RUNNING")

        print("\n  " + da)
        print("  Instances:")
        for n, ic in self.cfg.instances.items():
            on = "ON" if ic.get("enabled") else "OFF"
            ratio = ic.get("fund_ratio", "?")
            port = ic.get("health_port", "?")
            disp = ic.get("display_name", n)
            stopped = " [STOPPED]" if self._is_stopped(n) else ""
            # A3: no address displayed
            print("  [%s] %-12s ratio=%s port=%s %s%s"
                  % (on, n, ratio, port, disp, stopped))

            pf = ic.get("file_prefix", n)
            hf = ORCH_DIR / n / ("health_v3_" + pf + ".json")
            if hf.exists():
                try:
                    with open(hf) as f:
                        h = json.load(f)
                    ws = "Y" if h.get("ws_connected") else "N"
                    print("       V3=%s WS=%s polls=%s last=%s"
                          % (h.get("status", "?"), ws,
                             h.get("poll_count", 0), h.get("last_poll", "?")))
                except Exception:
                    pass

        print("  " + da)
        print("  V3 Processes:")
        import glob as g
        found = False
        for pd in g.glob("/proc/[0-9]*"):
            try:
                pid = int(pd.split("/")[-1])
                cl = Path(pd + "/cmdline").read_bytes().decode("utf-8", errors="replace")
                if "hl_copytrade_v3.py" in cl and "orchestrator" not in cl:
                    found = True
                    cfg_info = ""
                    if "--config" in cl:
                        parts = cl.split("--config")
                        if len(parts) > 1:
                            cfg_info = parts[1].strip().strip("\x00").split("\x00")[0].strip()
                    print("  PID=%d: %s" % (pid, cl[:80]))
                    if cfg_info:
                        print("    cfg: " + cfg_info)
            except Exception:
                pass
        if not found:
            print("  (none)")

        if CMD_DIR.exists():
            cmds = list(CMD_DIR.iterdir())
            if cmds:
                print("  " + da)
                print("  Pending commands:")
                for c in cmds:
                    print("    " + c.name)

        print(eq + "\n")

    # ------------------------------------------------------------------
    # PID management
    # ------------------------------------------------------------------
    def _write_pid(self):
        PID_FILE.parent.mkdir(parents=True, exist_ok=True)
        PID_FILE.write_text(str(os.getpid()))

    @staticmethod
    def _check_stale_pid() -> bool:
        if not PID_FILE.exists():
            return True
        pid_str = PID_FILE.read_text().strip()
        try:
            pid = int(pid_str)
            os.kill(pid, 0)
            log.error("[PID] Orchestrator already running PID=%d", pid)
            return False
        except (ValueError, ProcessLookupError, PermissionError):
            log.warning("[PID] Stale PID file (pid=%s), cleaning up", pid_str)
            PID_FILE.unlink(missing_ok=True)
            return True

    # ------------------------------------------------------------------
    # Main loop
    # ------------------------------------------------------------------
    def run_forever(self):
        if not self._check_stale_pid():
            print("ERROR: Orchestrator already running. Use 'stop' first.")
            sys.exit(1)

        self.start_all()

        def _h(sig, _):
            log.info("[ORCH] signal %d", sig)
            self.stop_all()
            sys.exit(0)

        signal.signal(signal.SIGTERM, _h)
        signal.signal(signal.SIGINT, _h)
        log.info("[ORCH] running, Ctrl+C to stop")

        try:
            while self._run:
                time.sleep(1)
        except KeyboardInterrupt:
            self.stop_all()


# ===========================================================================
# CLI entry point
# ===========================================================================
def main():
    import argparse

    ap = argparse.ArgumentParser(description="HL Orchestrator v4.2")
    ap.add_argument("cmd", choices=[
        "start", "stop", "status", "stop-instance",
        "restart-instance", "emergency-stop", "generate-config", "health",
    ])
    ap.add_argument("inst", nargs="?", default=None)
    ap.add_argument("--force", action="store_true")
    args = ap.parse_args()

    orch = Orchestrator()

    if args.cmd == "start":
        orch.run_forever()

    elif args.cmd == "stop":
        if PID_FILE.exists():
            pid = int(PID_FILE.read_text().strip())
            try:
                if args.force:
                    os.kill(pid, signal.SIGKILL)
                    print("SIGKILL -> " + str(pid))
                else:
                    os.kill(pid, signal.SIGTERM)
                    print("SIGTERM -> " + str(pid))
            except ProcessLookupError:
                print("PID " + str(pid) + " gone")
                PID_FILE.unlink(missing_ok=True)
        else:
            print("No PID file")

    elif args.cmd == "status":
        orch.print_status()

    elif args.cmd in ("stop-instance", "restart-instance"):
        if not args.inst:
            print("ERROR: need instance name")
            sys.exit(1)
        ic = orch.cfg.instances.get(args.inst)
        if not ic:
            print("ERROR: " + args.inst + " not found")
            sys.exit(1)
        if PID_FILE.exists():
            try:
                pid = int(PID_FILE.read_text().strip())
                os.kill(pid, 0)
            except (ValueError, ProcessLookupError, PermissionError):
                print("WARNING: Orchestrator not running (stale PID). Command may not take effect.")
        else:
            print("WARNING: Orchestrator not running. Command may not take effect.")
        CMD_DIR.mkdir(parents=True, exist_ok=True)
        prefix = "stop_" if args.cmd == "stop-instance" else "restart_"
        cmd_file = CMD_DIR / (prefix + args.inst)
        cmd_file.write_text(args.cmd + " " + datetime.now().isoformat() + "\n")
        print(args.cmd.capitalize() + " command queued for " + args.inst)

    elif args.cmd == "emergency-stop":
        if args.inst:
            ic = orch.cfg.instances.get(args.inst)
            if not ic:
                print("ERROR: " + args.inst + " not found")
                sys.exit(1)
            orch.es_inst(args.inst)
            print("Emergency stop -> " + args.inst)
        else:
            orch.es_all()
            print("Global emergency stop sent")

    elif args.cmd == "generate-config":
        print("Generating configs:")
        for n, p in orch.gen.all().items():
            print("  " + n + ": " + str(p))

    elif args.cmd == "health":
        print(json.dumps(orch._health_all(), indent=2, ensure_ascii=False))


if __name__ == "__main__":
    main()
