# Common tasks. Python tools come from requirements-dev.txt; Node from examples/node.
# Use a virtual environment, or override: make test PYTHON=.venv/bin/python

PYTHON ?= python3
NODE_DIR := examples/node

.PHONY: install test test-python test-node test-live lint format check sync-skill mock

install:
	$(PYTHON) -m pip install -r requirements-dev.txt
	cd $(NODE_DIR) && npm ci

test: test-python test-node

test-python:
	$(PYTHON) -m pytest

test-node:
	cd $(NODE_DIR) && npm test

# Connects to the real feed. Needs MM_DATA_TOKEN in the environment.
test-live:
	$(PYTHON) -m pytest -m live

lint:
	$(PYTHON) -m ruff check .
	$(PYTHON) -m ruff format --check .

format:
	$(PYTHON) -m ruff check --fix .
	$(PYTHON) -m ruff format .

# Serves the sample session on ws://127.0.0.1:8765/v1/stream. Needs only websockets.
mock:
	$(PYTHON) scripts/mock_feed.py

sync-skill:
	$(PYTHON) scripts/sync_skill.py

check: lint test
	$(PYTHON) scripts/sync_skill.py --check
	$(PYTHON) scripts/check_copy.py
	$(PYTHON) scripts/check_links.py
