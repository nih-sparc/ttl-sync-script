.PHONY: package help post publish serve test

PYTHON_BINARY          ?= "$(which python3.7)"
PYTHON_VIRTUALENV      ?= "venv"
SHELL = /bin/bash

WORKING_DIR          ?= "$(shell pwd)"
SERVICE_NAME         ?= "sparc-tools"
FUNCTION_NAME        ?= "$(shell echo ${SERVICE_NAME} | sed -e 's/-/_/g')"
PACKAGE_NAME         ?= "${SERVICE_NAME}-${VERSION_NUMBER}.zip"
LOCAL_IMAGE_TAG      ?= "${SERVICE_NAME}:${VERSION_NUMBER}"
LOCAL_CONTAINER_NAME ?= "${SERVICE_NAME}-${VERSION_NUMBER}"
LAMBDA_BUCKET        ?= "aws-sparc-backups"


.DEFAULT: help


help:
	@echo "Make Help"
	@echo ""
	@echo "make venv    - create virtual environment and install packages"

venv:
	@echo ""
	@echo ""
	@echo "Building virtualenv..."
	@mkdir -p $(PYTHON_VIRTUALENV)
	@virtualenv --no-site-packages --python=$(PYTHON_BINARY) $(PYTHON_VIRTUALENV) && \
        	source $(PYTHON_VIRTUALENV)/bin/activate && \
        	pip3 install -r requirements.txt && \
		pip3 install -r requirements_test.txt

