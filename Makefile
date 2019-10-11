.PHONY: package help post publish serve test

PYTHON_BINARY          ?= "$(which python3.7)"
PYTHON_VIRTUALENV      ?= "venv"

ENVIRONMENT_NAME                  ?=
DYNAMODB_ENDPOINT                 ?=
SPARC_METADATA_DYNAMODB_TABLE_ARN ?=
SPARC_METADATA_DYNAMODB_TABLE_ID  ?=
DRY_RUN                           ?=

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
        SPARC_METADATA_DYNAMODB_TABLE_ARN=${SPARC_METADATA_DYNAMODB_TABLE_ARN} \
        SPARC_METADATA_DYNAMODB_TABLE_ID=${SPARC_METADATA_DYNAMODB_TABLE_ID} \
        DRY_RUN=false \
	python3 sparc_tools/main.py

dryrun:
	source $(PYTHON_VIRTUALENV)/bin/activate && \
        ENVIRONMENT_NAME=${ENVIRONMENT_NAME} \
        DYNAMODB_ENDPOINT=${DYNAMODB_ENDPOINT} \
        SPARC_METADATA_DYNAMODB_TABLE_ARN=${SPARC_METADATA_DYNAMODB_TABLE_ARN} \
        SPARC_METADATA_DYNAMODB_TABLE_ID=${SPARC_METADATA_DYNAMODB_TABLE_ID} \
        DRY_RUN=true \
	python3 sparc_tools/main.py
