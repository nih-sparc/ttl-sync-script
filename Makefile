.PHONY: install help

PYTHON_BINARY          ?= "$(which python3.7)"
PYTHON_VIRTUALENV      ?= venv

.DEFAULT: help

help:
	@echo "Make Help"
	@echo ""
	@echo "make install   - create virtual environment and install executable."
	@echo ""
	@echo "To run scripts from command line after install:"
	@echo "   1) activate the virtualenv"
	@echo "   2) run 'ttl_update --help"
	@echo ""

install:
	python3 -m venv $(PYTHON_VIRTUALENV);\
	source $(PYTHON_VIRTUALENV)/bin/activate;\
	cd source;\
	pip install -U pip;\
	pip install -e .
