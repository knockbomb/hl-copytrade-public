#!/bin/bash
# HL CopyTrade - Installation Script
set -e

echo "=========================================="
echo "  HL CopyTrade Installer"
echo "=========================================="

# Check Python
PYTHON_CMD=""
for cmd in python3 python; do
    if command -v $cmd &>/dev/null; then
        ver=$($cmd -c "import sys; print(sys.version_info[:2])" 2>/dev/null)
        major=$(echo $ver | grep -oP "\d+" | head -1)
        minor=$(echo $ver | grep -oP "\d+" | tail -1)
        if [ "$major" -ge 3 ] && [ "$minor" -ge 8 ]; then
            PYTHON_CMD=$cmd
            break
        fi
    fi
done

if [ -z "$PYTHON_CMD" ]; then
    echo "❌ Requires Python 3.8+"
    exit 1
fi
echo "✅ Python: $($PYTHON_CMD --version)"

# Create venv
echo "Creating virtual environment..."
$PYTHON_CMD -m venv venv
source venv/bin/activate

# Install dependencies
echo "Installing dependencies..."
pip install --upgrade pip -q
pip install -r requirements.txt -q

# Setup .env
if [ ! -f .env ]; then
    cp .env.example .env
    echo ""
    echo "⚠️  Please edit .env with your wallet info:"
    echo "   vim .env"
fi

echo ""
echo "✅ Done!"
echo "  1. vim .env           # Configure wallet"
echo "  2. vim config_v4.yaml  # Configure trade params"
echo "  3. python3 hl_copytrade_v3.py  # Start"
