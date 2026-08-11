#!/usr/bin/env python3
"""
HL CopyTrade System - Centralized Path Configuration
Supports: HL_BASE_DIR env var > auto-detect from this file location
"""
import os
from pathlib import Path

BASE_DIR = Path(os.environ.get("HL_BASE_DIR", str(Path(__file__).parent.resolve())))
DATA_DIR = BASE_DIR / "data"
LOG_DIR = BASE_DIR / "logs"
ORCH_DIR = BASE_DIR / ".orchestrator"
CONFIG_FILE = str(BASE_DIR / "config_v4.yaml")
VENV_PYTHON = str(BASE_DIR / "venv" / "bin" / "python3")

DATA_DIR.mkdir(parents=True, exist_ok=True)
LOG_DIR.mkdir(parents=True, exist_ok=True)
