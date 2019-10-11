.PHONY: package help post publish serve test

PYTHON_BINARY          ?= "$(which python3.7)"
PYTHON_VIRTUALENV      ?= "venv"

ENVIRONMENT_NAME                  ?= prod
DYNAMODB_ENDPOINT                 ?= https://dynamodb.us-east-1.amazonaws.com/
SPARC_METADATA_DYNAMODB_TABLE_ID  ?= prod-sparc-metadata-table-use1

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

run:
	source $(PYTHON_VIRTUALENV)/bin/activate && \
        ENVIRONMENT_NAME=${ENVIRONMENT_NAME} \
        DYNAMODB_ENDPOINT=${DYNAMODB_ENDPOINT} \
        DRY_RUN=false \
	python3 sparc_tools/main.py

dryrun:
	source $(PYTHON_VIRTUALENV)/bin/activate && \
        ENVIRONMENT_NAME=${ENVIRONMENT_NAME} \
        DYNAMODB_ENDPOINT=${DYNAMODB_ENDPOINT} \
        DRY_RUN=true \
	python3 sparc_tools/main.py
