#!/usr/bin/env bash
set -euo pipefail

echo "Bootstrapping project..."

# 0. Ensure python3-venv is available
if ! python3 -m venv --help >/dev/null 2>&1; then
  echo "python3-venv not installed. Installing..."
  sudo apt-get update
  sudo apt-get install -y python3-venv
fi

# 1. Create virtual environment in project root
VENV_DIR="venv"
if [ -d "$VENV_DIR" ]; then
  echo "Virtual env already exists at $VENV_DIR"
else
  python3 -m venv "$VENV_DIR"
  echo "Created virtual environment at $VENV_DIR"
fi

# 2. Activate venv for this script
# shellcheck disable=SC1090
source "$VENV_DIR/bin/activate"

# 3. Install / upgrade pip
pip install --upgrade pip

# 4. Scaffold directories
mkdir -p ingestion common tests .github/workflows scripts

# 5. Create __init__.py files
touch ingestion/__init__.py
touch common/__init__.py

# 6. Stub modules in ingestion
cat > ingestion/fetcher.py << 'EOF'
"""
ingestion.fetcher

Module to fetch raw blockchain data: blocks, transactions, logs.
"""
def fetch_blocks(start_block: int, end_block: int):
    raise NotImplementedError("fetch_blocks not implemented")

def fetch_transactions(block_range):
    raise NotImplementedError("fetch_transactions not implemented")
EOF

cat > ingestion/parser.py << 'EOF'
"""
ingestion.parser

Module to parse raw blockchain data into normalized schema.
"""
def parse_blocks(raw_blocks):
    raise NotImplementedError("parse_blocks not implemented")

def parse_transactions(raw_txns):
    raise NotImplementedError("parse_transactions not implemented")

def parse_logs(raw_logs):
    raise NotImplementedError("parse_logs not implemented")
EOF

cat > ingestion/checkpoint.py << 'EOF'
"""
ingestion.checkpoint

Manage checkpoint (last ingested block) logic.
"""
def get_last_ingested() -> int:
    raise NotImplementedError("get_last_ingested not implemented")

def update_last_ingested(block_number: int):
    raise NotImplementedError("update_last_ingested not implemented")
EOF

# 7. Common module stubs
cat > common/config.py << 'EOF'
"""
common.config

Load configuration from YAML / JSON.
"""
import yaml
import os

def load_config(path="config.yaml"):
    with open(path, "r") as f:
        return yaml.safe_load(f)
EOF

cat > common/utils.py << 'EOF'
"""
common.utils

Utility helper functions.
"""
def chunked(start: int, end: int, size: int):
    """
    Yield (start, end) subranges of given size.
    """
    cur = start
    while cur <= end:
        sub_end = min(cur + size - 1, end)
        yield (cur, sub_end)
        cur = sub_end + 1
EOF

cat > common/logging_setup.py << 'EOF'
"""
common.logging_setup

Set up standard logging for the project.
"""
import logging

def setup_logging(level: int = logging.INFO):
    logging.basicConfig(
        level=level,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s"
    )
EOF

# 8. README, requirements, config file
cat > README.md << 'EOF'
# onchain-network-insights

## Description
On-chain network analytics & forensic exploration of token flows and wallet behavior.

## Structure
- `ingestion/`: data fetching & parsing
- `common/`: shared utilities, config, logging
- `tests/`: unit tests
- `.github/workflows/`: CI workflows
- `venv/`: local Python environment

## Getting Started
```bash
bash scripts/bootstrap.sh
source venv/bin/activate
pytest  # run test suite
EOF

cat > requirements.txt << 'EOF'
PyYAML
requests
pytest
EOF

cat > config.yaml << 'EOF'

Example configuration
data_source: bigquery
bigquery:
project_id: your-project-id
dataset: ethereum
api:
etherscan_key: YOUR_API_KEY
checkpoint:
file: checkpoint.json
EOF

### 9. GitHub Actions workflow (CI) file
cat > .github/workflows/ci.yaml << 'EOF'
name: CI / Python

on:
push:
branches: [ main ]
pull_request:
branches: [ main ]

jobs:
test:
runs-on: ubuntu-latest
strategy:
matrix:
python-version: [3.10, 3.11, 3.12]
steps:
- name: Checkout code
uses: actions/checkout@v5

yaml
Copy code
  - name: Set up Python
    uses: actions/setup-python@v5
    with:
      python-version: \${{ matrix.python-version }}

  - name: Install dependencies
    run: |
      python -m pip install --upgrade pip
      pip install -r requirements.txt
      pip install pytest pytest-cov

  - name: Run tests
    run: |
      pytest --cov=ingestion --cov-report=xml --cov-report=html

  - name: Upload coverage report
    uses: actions/upload-artifact@v3
    with:
      name: coverage-report
      path: htmlcov/
lint:
runs-on: ubuntu-latest
steps:
- uses: actions/checkout@v5
- name: Set up Python
uses: actions/setup-python@v5
with:
python-version: "3.12"
- name: Install flake8
run: pip install flake8
- name: Run flake8
run: flake8 .
EOF

### 10. Stub tests
cat > tests/test_fetcher.py << 'EOF'
import pytest
from ingestion.fetcher import fetch_blocks, fetch_transactions

def test_fetch_blocks_not_implemented():
with pytest.raises(NotImplementedError):
fetch_blocks(0, 0)

def test_fetch_transactions_not_implemented():
with pytest.raises(NotImplementedError):
fetch_transactions((0, 0))
EOF

cat > tests/test_parser.py << 'EOF'
import pytest
from ingestion.parser import parse_blocks, parse_transactions, parse_logs

def test_parse_blocks_not_implemented():
with pytest.raises(NotImplementedError):
parse_blocks([])

def test_parse_transactions_not_implemented():
with pytest.raises(NotImplementedError):
parse_transactions([])

def test_parse_logs_not_implemented():
with pytest.raises(NotImplementedError):
parse_logs([])
EOF

### 11. Permissions
chmod +x scripts/bootstrap.sh

echo "Bootstrap finished!"
echo "Activate the virtual environment with:"
echo " source venv/bin/activate"
echo "Then run: pytest"
