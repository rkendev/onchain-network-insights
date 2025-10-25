# ---- Variables ----
PYTEST := pytest
PYTEST_Q := -q
COV_PKGS := --cov=ingestion --cov=etl --cov=storage --cov=dashboard
COV_REPORT := --cov-report=term-missing:skip-covered --cov-report=xml
TEST_PATH := tests

# ---- Default ----
.PHONY: all
all: test

# ---- Testing ----
.PHONY: test
test:
	$(PYTEST) $(PYTEST_Q) $(TEST_PATH)

.PHONY: coverage
coverage:
	$(PYTEST) $(PYTEST_Q) $(TEST_PATH) $(COV_PKGS) $(COV_REPORT)

# ---- Lint / Format (optional; keep if you already had them) ----
.PHONY: lint
lint:
	ruff check .

.PHONY: format
format:
	ruff format .

# ---- Docker helpers (optional; keep if you already had them) ----
.PHONY: build
build:
	docker build -t onchain-network-insights:latest .

.PHONY: up
up:
	docker compose up -d

.PHONY: down
down:
	docker compose down
