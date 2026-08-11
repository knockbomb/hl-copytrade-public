# HL CopyTrade

Hyperliquid Copy Trading System - Automatically follow Leader account for perpetual contract trading.

> Last sync: 2026-08-11

## Features

- 🔄 Auto copy-trade: Monitor Leader position changes and sync to Follower accounts
- 📊 Multi-instance: Orchestrator manages low-frequency + high-frequency instances
- 🔐 API wallet auto-renewal: Detect wallet expiry and auto-create new wallet
- 📈 Net value tracking: Record account value changes, generate trading reports
- ⚠️ Multi-dimensional alerts: Margin usage, fund changes, address changes
- 🛡️ Emergency stop: One-click stop all trading

## Requirements

- Python 3.8+
- Linux (Ubuntu 20.04+ recommended)
- Hyperliquid API wallet (with trading permission)

## Quick Start

```bash
git clone https://github.com/knockbomb/hl-copytrade-public.git
cd hl-copytrade-public
python3 -m venv venv && source venv/bin/activate
pip install -r requirements.txt
cp .env.example .env
vim .env   # Fill in your wallet address and API private key
python3 hl_copytrade_v3.py
```

## Directory Structure

```
├── hl_copytrade_v3.py      # Core copy-trade engine
├── hl_orchestrator.py      # Multi-instance process manager
├── hl_phase2_monitor.py    # Monitoring & alerts
├── paths.py                # Path configuration
├── config_v4.yaml          # Trade configuration
├── .env.example            # Environment variables template
├── hl_copytrade_v3/        # Sub-modules
├── wallet_manager/         # API wallet management
└── install.sh              # Installation script
```

## Configuration

| Variable | Description |
|---------|------|
| `HL_USER_MAIN_ADDR` | Your HL wallet address |
| `HL_LEADER_ADDR` | Leader address to follow |
| `HL_API_PRIVATE_KEY` | API wallet private key |
| `HL_FUND_RATIO` | Fund ratio (0-1) |
| `HL_FEISHU_WEBHOOK` | Feishu/Lark webhook URL |

## ⚠️ Risk Warning

- This program is for learning and research only, not investment advice
- Crypto trading carries extreme risk, you may lose all your capital
- Please fully understand the risks before using real funds
- Start with small amounts for testing

## License

MIT
