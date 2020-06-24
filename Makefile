.PHONY: package help post publish serve test

PYTHON_BINARY          ?= "$(which python3.7)"
PYTHON_VIRTUALENV      ?= "venv"

.DEFAULT: help

help:
	@echo "Make Help"
	@echo ""
	@echo "make prepare-dev   - create virtual environment and install executable."

prepare-dev:
	python3 -m venv $(PYTHON_VIRTUALENV)
	pip install -U pip
	cd source; pip install -e .