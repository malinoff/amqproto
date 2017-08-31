export VIRTUAL_ENV?=./venv

PROJ_NAME=amqproto

BIN=$(VIRTUAL_ENV)/bin

PIP=$(BIN)/pip
ISORT=$(BIN)/isort
PYFORMAT=$(BIN)/pyformat
FLAKE8=$(BIN)/flake8
PYLINT=$(BIN)/pylint
PYTEST=$(BIN)/pytest


codestyle-check: codestyle-autoformat
	$(FLAKE8) --count $(PROJ_NAME)
	$(PYLINT) $(PROJ_NAME)
	git diff --exit-code $(PROJ_NAME)
	echo "Your code is perfectly styled, congratz! :)"

codestyle-autoformat: deps
	$(ISORT) -p $(PROJ_NAME) -ls -sl -rc $(PROJ_NAME)
	$(PYFORMAT) -r -i $(PROJ_NAME)

functionaltests: deps
	which docker-compose >/dev/null 2>&1 && docker-compose up -d
	sleep 2
	$(PYTEST) -l --cov=$(PROJ_NAME) --cov-report=term-missing --cov-config=tests/functional/coveragerc tests/functional
	which docker-compose >/dev/null 2>&1 && docker-compose down

unittests: deps
	$(PYTEST) -l --cov=$(PROJ_NAME) --cov-report=term-missing:skip-covered --cov-config=tests/unit/coveragerc tests/unit

deps:
	$(PIP) install -e .[all]
