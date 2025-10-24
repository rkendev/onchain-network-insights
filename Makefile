# ---- Variables ----
IMAGE ?= onchain-network-insights:latest
COV_MIN ?= 80

# Paths under test (adjust if you add/remove packages)
COV_TARGETS = ingestion etl storage dashboard

# ---- Docker helpers ----
.PHONY: build up logs down rebuild
build:
	docker compose build

up:
	docker compose up -d

logs:
	docker compose logs -f

down:
	docker compose down

rebuild:
	docker compose build --no-cache && docker compose up -d

# ---- Local dev / CI helpers ----
.PHONY: install test test-cov cov-html cov-badge check-cov

install:
	python -m pip install --upgrade pip
	pip install -r requirements.txt
	pip install pytest pytest-cov coverage-badge

test:
	pytest -q

test-cov:
	pytest -q --cov=$(COV_TARGETS) --cov-report=term --cov-report=xml

cov-html:
	coverage html -d htmlcov

cov-badge:
	coverage-badge -o coverage.svg -f

check-cov:
	@python - <<'PY'
import os, sys, xml.etree.ElementTree as ET
cov_min = float(os.environ.get("COV_MIN","80"))
rate = float(ET.parse("coverage.xml").getroot().attrib["line-rate"]) * 100
print(f"Coverage: {rate:.2f}% (min {cov_min:.2f}%)")
sys.exit(1 if rate + 1e-9 < cov_min else 0)
PY

# one-shot local run: coverage + html + badge + gate
.PHONY: coverage
coverage: test-cov cov-html cov-badge check-cov
	@echo "Coverage OK"
